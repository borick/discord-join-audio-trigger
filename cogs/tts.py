# -*- coding: utf-8 -*-
import discord
from discord.ext import commands
import logging
import io
import math
import asyncio # Import asyncio
from typing import Optional, List, Dict, Any

import config
import data_manager
from utils import text_helpers # For normalize_for_tts
from core.playback_manager import PlaybackManager # Can import this for type hinting if desired

# Check TTS dependency
try:
    import edge_tts
    from pydub import AudioSegment # Need pydub here too for processing TTS output
    from pydub.exceptions import CouldntDecodeError
    TTS_READY = config.EDGE_TTS_AVAILABLE and config.PYDUB_AVAILABLE
except ImportError:
    TTS_READY = False

log = logging.getLogger('SoundBot.Cog.TTS')

# --- Autocomplete ---
async def tts_voice_autocomplete(ctx: discord.AutocompleteContext) -> List[discord.OptionChoice]:
    """Autocomplete for Edge-TTS voices using the FULL pre-generated list from config."""
    try:
        current_value = ctx.value.lower() if ctx.value else ""
        # Filter the full list based on user input (checking both display name and value)
        suggestions = [
            choice for choice in config.FULL_EDGE_TTS_VOICE_CHOICES
            if current_value in choice.name.lower() or current_value in choice.value.lower()
        ]
        # Limit to Discord's max suggestions
        return suggestions[:25]
    except Exception as e:
        log.error(f"Error during TTS voice autocomplete for user {ctx.interaction.user.id}: {e}", exc_info=True)
        return []


class TTSCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # Ensure playback_manager is correctly accessed (it's attached to bot)
        self.playback_manager: PlaybackManager = bot.playback_manager

    @commands.slash_command(name="setttsdefaults", description="Set your preferred default Edge-TTS voice.")
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def setttsdefaults(
        self,
        ctx: discord.ApplicationContext,
        voice: discord.Option(
            str,
            description="Your preferred default voice (start typing for list).",
            required=True,
            autocomplete=tts_voice_autocomplete,
            # Provide curated choices for easier selection initially
            choices=config.CURATED_EDGE_TTS_VOICE_CHOICES
            )
    ):
        """Sets the user's default TTS voice preference."""
        await ctx.defer(ephemeral=True)
        author = ctx.author
        user_id_str = str(author.id)
        log.info(f"COMMAND: /setttsdefaults by {author.name} ({user_id_str}), chosen voice: {voice}")

        # --- Validate Voice Selection ---
        valid_choice = False
        voice_display_name = voice # Default to the input value
        for choice in config.FULL_EDGE_TTS_VOICE_CHOICES: # Check against the FULL list
            if choice.value == voice:
                valid_choice = True
                voice_display_name = choice.name # Get the pretty display name
                break

        if not valid_choice:
            await ctx.followup.send(f"❌ Invalid voice ID provided: `{voice}`. Please choose from the list or use autocomplete.", ephemeral=True)
            return

        # --- Update Config ---
        # Get user config, creating entry if it doesn't exist
        user_config = self.bot.user_sound_config.setdefault(user_id_str, {})
        # Ensure 'tts_defaults' dictionary exists
        tts_defaults = user_config.setdefault('tts_defaults', {})
        # Set the voice
        tts_defaults['voice'] = voice

        data_manager.save_config(self.bot.user_sound_config) # Save the changes

        await ctx.followup.send(
            f"✅ TTS default voice updated!\n"
            f"• Voice: **{voice_display_name}** (`{voice}`)\n\n"
            f"This voice will be used for `/tts` when you don't specify one, and for your join message if you haven't set a custom sound.",
            ephemeral=True
        )


    @commands.slash_command(name="removettsdefaults", description="Remove your custom TTS voice default.")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def removettsdefaults(self, ctx: discord.ApplicationContext):
        """Removes the user's custom TTS voice preference."""
        await ctx.defer(ephemeral=True)
        author = ctx.author
        user_id_str = str(author.id)
        log.info(f"COMMAND: /removettsdefaults by {author.name} ({user_id_str})")

        user_config = self.bot.user_sound_config.get(user_id_str)

        # Check if user has config and if 'tts_defaults' exists within it
        if user_config and 'tts_defaults' in user_config:
            del user_config['tts_defaults'] # Remove the defaults dictionary
            log.info(f"Removed TTS defaults for {author.name}")

            # If user config is now empty, remove the user entry entirely
            if not user_config:
                if user_id_str in self.bot.user_sound_config:
                    del self.bot.user_sound_config[user_id_str]
                    log.info(f"Removed empty user config entry for {author.name} after TTS default removal.")

            data_manager.save_config(self.bot.user_sound_config) # Save changes

            # Get display name for the bot's default voice
            default_voice_display = config.DEFAULT_TTS_VOICE
            for choice in config.FULL_EDGE_TTS_VOICE_CHOICES:
                 if choice.value == config.DEFAULT_TTS_VOICE:
                     default_voice_display = choice.name
                     break

            await ctx.followup.send(
                f"🗑️ Custom TTS default voice removed.\n"
                f"The bot's default voice (**{default_voice_display}** / `{config.DEFAULT_TTS_VOICE}`) will now be used.",
                ephemeral=True
            )
        else:
            await ctx.followup.send("🤷 You don't have any custom TTS defaults configured.", ephemeral=True)


    @commands.slash_command(name="tts", description="Make the bot say something using Edge Text-to-Speech.")
    @commands.cooldown(1, 6, commands.BucketType.user) # Cooldown to prevent spam
    async def tts(
        self,
        ctx: discord.ApplicationContext,
        message: discord.Option(str, description=f"Text to speak (max {config.MAX_TTS_LENGTH} chars).", required=True),
        voice: discord.Option(
            str,
            description="Override TTS voice (start typing to search). Uses your default if omitted.",
            required=False,
            autocomplete=tts_voice_autocomplete,
            choices=config.CURATED_EDGE_TTS_VOICE_CHOICES # Show curated list first
            ),
        spell_out: discord.Option(bool, description="Read out each character individually?", default=False)
    ):
        """Generates and plays TTS audio in the user's voice channel."""
        await ctx.defer(ephemeral=True) # Defer privately initially

        if not TTS_READY:
            log.warning(f"TTS command invoked by {ctx.author.name} but TTS/Pydub is not available.")
            await ctx.followup.send("❌ TTS functionality is currently unavailable on the bot.", ephemeral=True)
            return

        user = ctx.author
        guild = ctx.guild

        # --- Pre-checks ---
        if not guild or not isinstance(user, discord.Member) or not user.voice or not user.voice.channel:
            await ctx.followup.send("You must be in a voice channel in this server to use TTS.", ephemeral=True)
            return
        if len(message) > config.MAX_TTS_LENGTH:
            await ctx.followup.send(f"❌ Message too long! Max length is {config.MAX_TTS_LENGTH} characters.", ephemeral=True)
            return
        if not message.strip():
            await ctx.followup.send("❌ Please provide some text for the bot to say.", ephemeral=True)
            return

        user_id_str = str(user.id)
        guild_id = guild.id
        log.info(f"COMMAND: /tts by {user.name} ({user_id_str}), Guild: {guild_id}, Voice: {voice}, Spell: {spell_out}, Msg: '{message[:50]}...'")

        # --- Determine Voice ---
        user_config = self.bot.user_sound_config.get(user_id_str, {})
        saved_defaults = user_config.get("tts_defaults", {})
        # Use provided voice > user default > bot default
        final_voice = voice if voice is not None else saved_defaults.get('voice', config.DEFAULT_TTS_VOICE)
        voice_source = "explicit" if voice is not None else ("saved default" if 'voice' in saved_defaults else "bot default")

        # Validate the final voice choice
        is_valid_voice = any(choice.value == final_voice for choice in config.FULL_EDGE_TTS_VOICE_CHOICES)
        if not is_valid_voice:
            log.warning(f"TTS: Invalid final voice '{final_voice}' ({voice_source}) selected for {user.name}. Falling back to default.")
            await ctx.followup.send(f"❌ Invalid voice ID (`{final_voice}`). Falling back to bot default.", ephemeral=True)
            # Revert to the guaranteed valid bot default
            final_voice = config.DEFAULT_TTS_VOICE
            voice_source = "bot default (fallback)"

        log.info(f"TTS Final Voice Selection: {final_voice} (Source: {voice_source}) for {user.name}")

        # --- Prepare Text ---
        audio_source: Optional[discord.PCMAudio] = None
        pcm_fp: Optional[io.BytesIO] = None
        try:
            original_message = message
            normalized_message = text_helpers.normalize_for_tts(original_message)

            if spell_out:
                # Insert spaces between characters for spelling effect
                text_to_speak = " ".join(filter(None, list(normalized_message))) # filter(None) removes empty strings if any
                log_msg_type = "Spelled"
            else:
                text_to_speak = normalized_message
                log_msg_type = "Normalized" if original_message != normalized_message else "Original"

            log_text_preview = text_to_speak[:150] # Log more for spelled out text
            log.info(f"TTS Command {log_msg_type} Input: '{original_message[:50]}...' -> '{log_text_preview}...'")

            if not text_to_speak.strip():
                await ctx.followup.send("❌ Message became empty after removing unsupported characters.", ephemeral=True)
                return

            # --- Generate TTS Audio (In Memory) ---
            log.info(f"TTS: Generating audio with Edge-TTS for '{user.name}' (voice={final_voice})...")
            mp3_bytes_list = []
            communicate = edge_tts.Communicate(text_to_speak, final_voice)
            async for chunk in communicate.stream():
                if chunk["type"] == "audio":
                    mp3_bytes_list.append(chunk["data"])

            if not mp3_bytes_list:
                raise ValueError("Edge-TTS generation yielded no audio data chunks.")

            mp3_data = b"".join(mp3_bytes_list)
            if len(mp3_data) == 0:
                raise ValueError("Edge-TTS generation resulted in empty audio data.")

            # --- Process TTS Audio with Pydub (Normalization, Format Conversion) ---
            log.debug("TTS: Processing generated MP3 data with Pydub...")
            with io.BytesIO(mp3_data) as mp3_fp:
                seg = AudioSegment.from_file(mp3_fp, format="mp3")
                log.debug(f"TTS: Loaded MP3 into Pydub (duration: {len(seg)}ms)")

                # Trim if exceeds max duration
                if len(seg) > config.MAX_PLAYBACK_DURATION_MS:
                    log.info(f"TTS: Trimming audio from {len(seg)}ms to {config.MAX_PLAYBACK_DURATION_MS}ms.")
                    seg = seg[:config.MAX_PLAYBACK_DURATION_MS]

                # Normalize loudness
                peak_dbfs = seg.max_dBFS
                if not math.isinf(peak_dbfs) and peak_dbfs > -90.0:
                    target_dbfs = config.TARGET_LOUDNESS_DBFS
                    change_in_dbfs = target_dbfs - peak_dbfs
                    gain_limit = 6.0 # Max +6dB gain
                    apply_gain = min(change_in_dbfs, gain_limit) if change_in_dbfs > 0 else change_in_dbfs
                    log.info(f"TTS AUDIO: Normalizing. Peak:{peak_dbfs:.2f} Target:{target_dbfs:.2f} ApplyGain:{apply_gain:.2f} dB.")
                    if apply_gain != change_in_dbfs:
                         log.info(f"TTS AUDIO: Gain limited to +{gain_limit}dB.")
                    seg = seg.apply_gain(apply_gain)
                elif math.isinf(peak_dbfs): log.warning("TTS AUDIO: Cannot normalize silent TTS audio.")
                else: log.warning(f"TTS AUDIO: Skipping normalization for very quiet TTS audio (Peak: {peak_dbfs:.2f})")

                # Convert to PCM S16 LE for Discord
                seg = seg.set_frame_rate(48000).set_channels(2)
                pcm_fp = io.BytesIO() # Keep pcm_fp in scope
                seg.export(pcm_fp, format="s16le")
                pcm_fp.seek(0)

                if pcm_fp.getbuffer().nbytes == 0:
                    raise ValueError("Pydub export resulted in empty PCM data.")

                log.debug(f"TTS: PCM processed in memory ({pcm_fp.getbuffer().nbytes} bytes)")
                audio_source = discord.PCMAudio(pcm_fp) # pcm_fp needs to be kept open until playback finishes!

            log.info(f"TTS: PCMAudio source created successfully for {user.name}.")

        except Exception as e:
            err_type = type(e).__name__
            msg = f"❌ Error generating/processing TTS ({err_type})."
            # Provide more specific error messages based on exception type
            if isinstance(e, (ValueError, RuntimeError)) and "TTS" in str(e): msg = f"❌ Error generating TTS: {e}"
            elif isinstance(e, CouldntDecodeError): msg = f"❌ Error processing TTS audio (Pydub): {e}"
            elif "trustchain" in str(e).lower() or "ssl" in str(e).lower(): msg = "❌ TTS Error: Secure connection issue. Try again later?"
            elif "voice not found" in str(e).lower(): msg = f"❌ Error: TTS service reported voice '{final_voice}' not found."

            await ctx.followup.send(msg, ephemeral=True)
            log.error(f"TTS: Failed generation/processing for {user.name} (Voice: {final_voice}): {e}", exc_info=True)
            # Ensure buffer is closed on error
            if pcm_fp and not pcm_fp.closed:
                 try: pcm_fp.close()
                 except Exception: pass
            return # Stop execution

        # --- Playback ---
        if not audio_source or not pcm_fp: # Should be caught by the except block, but safety first
            await ctx.followup.send("❌ Failed to prepare TTS audio source for playback.", ephemeral=True)
            log.error("TTS: Audio source or PCM buffer was None before playback attempt.")
            if pcm_fp and not pcm_fp.closed:
                 try: pcm_fp.close()
                 except Exception: pass
            return

        # Use playback manager to handle VC connection and playing
        # Pass the pre-generated source and the buffer that needs closing
        target_channel = user.voice.channel # Re-affirm target channel
        voice_display_name = final_voice # Get display name for message
        for choice in config.FULL_EDGE_TTS_VOICE_CHOICES:
            if choice.value == final_voice: voice_display_name = choice.name; break

        # Create a display message for the user (truncated)
        display_msg_truncated = original_message[:150] + ('...' if len(original_message) > 150 else '')
        spell_note = " (spelled out)" if spell_out else ""
        playback_display_name = f"TTS{spell_note} w/ {voice_display_name}: \"{display_msg_truncated}\""

        # === THIS IS THE CORRECTED LINE ===
        await self.playback_manager.play_single_sound(
            interaction=ctx.interaction,
            audio_source=audio_source,
            audio_buffer_to_close=pcm_fp, # Pass the buffer here!
            display_name=playback_display_name # Pass the text for user feedback
        )
        # ==================================

        # The play_single_sound method will handle sending the "Playing..." message
        # and the after_play_cleanup in playback_manager will close the pcm_fp buffer.


def setup(bot: commands.Bot):
    if not TTS_READY:
         log.warning("TTS Cog not loading: Missing edge-tts or pydub library.")
         return
    bot.add_cog(TTSCog(bot))
    log.info("TTS Cog loaded.")