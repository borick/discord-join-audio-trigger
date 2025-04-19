# cogs/events.py

import discord
from discord.ext import commands
import logging
import os
import asyncio
from typing import Optional, Any # Added Any for QueueItemType consistency if needed

# Local application imports
import config
import data_manager
from utils import file_helpers, text_helpers, voice_helpers
# Import the specific playback manager being used
from core.playback_manager import PlaybackManager, PlaybackMode # Import Enum too

# Check TTS availability
try:
    import edge_tts
    # Check pydub too, as it's needed for processing TTS output if normalization/trimming is desired later
    # Note: Current TTS generation saves directly, processing happens in playback_manager.play_next
    TTS_READY = config.EDGE_TTS_AVAILABLE
except ImportError:
    TTS_READY = False

log = logging.getLogger('SoundBot.Cog.Events')

class EventsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # Ensure playback_manager is accessed correctly from bot instance
        if not hasattr(bot, 'playback_manager') or not isinstance(bot.playback_manager, PlaybackManager):
             log.critical("EventsCog FATAL: bot.playback_manager not found or is not the correct PlaybackManager instance!")
             raise RuntimeError("PlaybackManager not initialized on Bot before loading EventsCog")
        self.playback_manager: PlaybackManager = bot.playback_manager
        # Ensure user config is loaded onto the bot instance
        if not hasattr(bot, 'user_sound_config'):
            log.critical("EventsCog FATAL: bot.user_sound_config not found!")
            raise RuntimeError("user_sound_config not initialized on Bot before loading EventsCog")
        if not hasattr(bot, 'guild_settings'):
             log.critical("EventsCog FATAL: bot.guild_settings not found!")
             raise RuntimeError("guild_settings not initialized on Bot before loading EventsCog")


    @commands.Cog.listener()
    async def on_ready(self):
        """Called once the bot is ready and operational."""
        log.info(f'Logged in as {self.bot.user.name} ({self.bot.user.id})')
        log.info(f"Using discord.py version {discord.__version__}")
        # Access config through bot object if attached, otherwise directly
        bot_config = getattr(self.bot, 'config', config)
        log.info(f"Max Playback Duration: {bot_config.MAX_PLAYBACK_DURATION_MS / 1000}s")
        log.info(f"Normalization Target: {bot_config.TARGET_LOUDNESS_DBFS} dBFS")
        log.info(f"Leave Timeout: {bot_config.AUTO_LEAVE_TIMEOUT_SECONDS}s")
        log.info(f"TTS Engine Available: {TTS_READY}") # Use local check result
        log.info(f"Pydub Available: {config.PYDUB_AVAILABLE}") # Check config status
        log.info(f"PyNaCl Available: {config.NACL_AVAILABLE}") # Check config status

        # Access user/guild data through bot object
        user_config = getattr(self.bot, 'user_sound_config', {})
        guild_settings = getattr(self.bot, 'guild_settings', {})
        log.info(f"Loaded {len(user_config)} user configs.")
        log.info(f"Loaded {len(guild_settings)} guild settings.")
        log.info(f"Sound Bot is operational. Monitoring {len(self.bot.guilds)} guilds.")

    @commands.Cog.listener()
    async def on_voice_state_update(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
        """Handles users joining/leaving VCs and the bot's own state changes."""
        guild = member.guild
        if not guild: return # Ignore DM voice states

        guild_id = guild.id
        user_id_str = str(member.id)

        # --- User Joins/Moves into a Channel ---
        if not member.bot and after.channel and before.channel != after.channel:
            channel_to_join = after.channel
            user_display_name = member.display_name
            log.info(f"EVENT: User {user_display_name} ({user_id_str}) entered {channel_to_join.name} in {guild.name}")

            vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)

            # If bot is already in the channel the user joined, cancel leave timer
            if vc and vc.is_connected() and vc.channel == channel_to_join:
                log.debug(f"User {user_display_name} joined bot's current channel ({vc.channel.name}). Cancelling any active leave timer.")
                voice_helpers.cancel_leave_timer(self.bot, guild_id, reason=f"user {user_display_name} joined")

            # --- Determine Join Sound/TTS ---
            sound_path: Optional[str] = None
            is_temp_sound = False # Track if the sound file is temporary (TTS)
            use_tts_join = False # Flag to indicate if TTS fallback is needed

            # Safely access user configurations from the bot instance
            user_config_all = getattr(self.bot, 'user_sound_config', {})
            user_config = user_config_all.get(user_id_str) # Get specific user's config dict
            join_sound_filename = user_config.get('join_sound') if user_config else None

            # 1. Check configured custom join sound
            if join_sound_filename:
                 log.debug(f"User {user_display_name} has configured join sound: {join_sound_filename}")
                 # Extract base name for searching (file_helpers.find_user_sound_path expects base name)
                 base_name_to_search = os.path.splitext(join_sound_filename)[0]
                 potential_path = file_helpers.find_user_sound_path(member.id, base_name_to_search)

                 if potential_path and os.path.exists(potential_path):
                     sound_path = potential_path
                     log.info(f"SOUND: Using configured join sound: '{os.path.basename(sound_path)}' for {user_display_name}")
                 else:
                     log.warning(f"SOUND: Configured join sound file '{join_sound_filename}' (expected base: '{base_name_to_search}', found path: {potential_path}) not found or inaccessible for {user_display_name}. Removing broken entry.")
                     # Remove the broken entry directly from user_config if it exists
                     if user_config and 'join_sound' in user_config:
                         del user_config['join_sound']
                         # Consider removing the user entry if it's now completely empty
                         # if not user_config:
                         #     user_config_all.pop(user_id_str, None)
                         data_manager.save_config(user_config_all) # Save changes
                     use_tts_join = True # Fallback to TTS
            else:
                 # No custom sound configured in the first place
                 use_tts_join = True
                 log.info(f"SOUND: No custom join sound configured for {user_display_name}. Using TTS join.")

            # 2. Generate TTS if needed (fallback or default)
            if use_tts_join:
                 if TTS_READY:
                      # Ensure sounds dir exists (safer check)
                      file_helpers.ensure_dir(config.SOUNDS_DIR)
                      # Generate a unique temp filename
                      tts_filename = f"tts_join_{member.id}_{os.urandom(4).hex()}.mp3"
                      tts_path = os.path.join(config.SOUNDS_DIR, tts_filename)
                      log.info(f"TTS JOIN: Generating for {user_display_name} ('{tts_filename}')...")

                      try:
                           # Get TTS defaults safely
                           tts_defaults = user_config.get("tts_defaults", {}) if user_config else {}
                           tts_voice = tts_defaults.get("voice", config.DEFAULT_TTS_VOICE)

                           # Validate voice
                           if not any(v.value == tts_voice for v in config.FULL_EDGE_TTS_VOICE_CHOICES):
                                log.warning(f"TTS JOIN: Invalid voice '{tts_voice}' configured for user {user_id_str}. Falling back to bot default '{config.DEFAULT_TTS_VOICE}'.")
                                tts_voice = config.DEFAULT_TTS_VOICE

                           log.debug(f"TTS JOIN: Using voice: {tts_voice}")

                           original_name = user_display_name
                           normalized_name = text_helpers.normalize_for_tts(original_name)
                           # Ensure text_to_speak is not empty after normalization
                           text_to_speak = f"{normalized_name} joined" if normalized_name.strip() else "Someone joined"

                           if original_name != normalized_name:
                                log.info(f"TTS JOIN: Normalized Name: '{original_name}' -> '{normalized_name}'")
                           log.info(f"TTS JOIN: Final Text to Speak: '{text_to_speak}'")

                           # Generate TTS audio
                           communicate = edge_tts.Communicate(text_to_speak, tts_voice)
                           await communicate.save(tts_path)

                           # Verify file creation and size
                           if not os.path.exists(tts_path) or os.path.getsize(tts_path) == 0:
                                raise RuntimeError(f"Edge-TTS failed to create a non-empty file: {tts_path}")

                           log.info(f"TTS JOIN: Successfully saved TTS file '{tts_filename}'")
                           sound_path = tts_path
                           is_temp_sound = True

                      except Exception as e:
                           log.error(f"TTS JOIN: Failed generation for {user_display_name} (voice={tts_voice}): {e}", exc_info=True)
                           sound_path = None # Ensure sound_path is None on failure
                           # Cleanup failed temp file if it exists
                           if os.path.exists(tts_path):
                                try: os.remove(tts_path); log.debug(f"Cleaned up failed temporary TTS file: {tts_path}")
                                except OSError as del_err: log.warning(f"TTS JOIN: Could not clean up failed temporary file '{tts_path}': {del_err}")
                 else:
                      log.error(f"TTS JOIN: Cannot generate for {user_display_name}, TTS prerequisites (edge-tts) not available.")
                      sound_path = None

            # --- Queue the sound if found/generated ---
            if sound_path:
                # Check bot permissions in the target channel BEFORE queueing
                bot_perms = channel_to_join.permissions_for(guild.me)
                if not bot_perms.connect or not bot_perms.speak:
                    log.warning(f"Missing Connect/Speak permission in '{channel_to_join.name}'. Cannot queue or play sound for {user_display_name}.")
                    # Cleanup temp TTS file if permissions are missing
                    if is_temp_sound and os.path.exists(sound_path):
                        try: os.remove(sound_path); log.debug(f"Cleaned up temporary TTS file due to missing permissions: {sound_path}")
                        except OSError: pass
                    return # Don't queue if we can't join/speak

                # --- Use PlaybackManager's queue ---
                # Tuple format: (member, sound_path, is_temp_tts)
                join_queue_item = (member, sound_path, is_temp_sound)

                # Add to the playback manager's queue
                queue_pos = await self.playback_manager.add_to_queue(guild_id, join_queue_item)
                log.info(f"Queued join sound for {user_display_name} (Position: {queue_pos}, Temp: {is_temp_sound})")

                # Ensure the bot is in the correct channel (or connects)
                # Use the playback manager's helper for this. Pass None for interaction.
                vc_ready = await self.playback_manager.ensure_voice_client(None, channel_to_join, action_type="JOIN SOUND")

                # If VC is ready, ensure playback starts if idle
                # add_to_queue and ensure_voice_client might already trigger this,
                # but calling start_playback_if_idle is safe as it has internal checks.
                if vc_ready:
                     log.debug(f"VC ready for {guild.name}, ensuring playback check occurs.")
                     await self.playback_manager.start_playback_if_idle(guild_id)
                else:
                     log.error(f"Failed to ensure voice client for join sound in {channel_to_join.name}. Sound remains queued for {user_display_name}.")
                     # Note: Sound is queued. It might play later if bot connects successfully.

            else:
                 log.info(f"SOUND/TTS JOIN: Could not find or generate a sound for {user_display_name}. Skipping playback.")

        # --- User Leaves/Moves Out ---
        elif not member.bot and before.channel and before.channel != after.channel:
            vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
            # If the user left the bot's current channel
            if vc and vc.is_connected() and vc.channel == before.channel:
                log.info(f"EVENT: User {member.display_name} left bot's channel ({before.channel.name}). Checking if bot should leave.")

                # Start leave timer check after a short delay
                async def delayed_leave_check():
                     await asyncio.sleep(1.5) # Slightly longer delay?
                     # Re-fetch VC state inside the delayed task
                     current_vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
                     # Check if bot is still in the *same* channel user left from
                     if current_vc and current_vc.is_connected() and current_vc.channel == before.channel:
                          log.debug(f"Running delayed leave check for {before.channel.name}")
                          await voice_helpers.start_leave_timer(self.bot, current_vc)
                     else:
                          log.debug(f"Skipping delayed leave check; bot no longer in {before.channel.name} or disconnected.")

                asyncio.create_task(delayed_leave_check(), name=f"DelayedLeaveCheck_{guild_id}")

        # --- Bot's Own Voice State Changes ---
        elif member.id == self.bot.user.id:
            # Bot Disconnected
            if before.channel and not after.channel:
                log.info(f"EVENT: Bot disconnected from {before.channel.name} in {guild.name}. Cleaning up resources.")
                # Cancel the main leave timer
                voice_helpers.cancel_leave_timer(self.bot, guild_id, reason="bot disconnected event")
                # Let PlaybackManager handle its internal state cleanup if it has a method,
                # otherwise perform manual cleanup. Use safe_disconnect might be better here.
                # Assuming safe_disconnect clears necessary state:
                # If the disconnect wasn't triggered by safe_disconnect itself, call it.
                # This check is tricky, maybe playback_manager needs a dedicated handler.
                # For now, basic manual cleanup:
                log.warning("Performing manual playback state cleanup on bot disconnect event.")
                self.playback_manager.currently_playing.pop(guild_id, None)
                self.playback_manager.guild_queues.pop(guild_id, None)
                if hasattr(self.playback_manager, 'playback_mode'):
                     self.playback_manager.playback_mode[guild_id] = PlaybackMode.IDLE
                if hasattr(self.playback_manager, 'active_single_buffers'):
                     buffer = self.playback_manager.active_single_buffers.pop(guild_id, None)
                     if buffer and not buffer.closed:
                         try: buffer.close()
                         except Exception: pass

            # Bot Moved Channels
            elif before.channel and after.channel and before.channel != after.channel:
                log.info(f"EVENT: Bot moved from {before.channel.name} to {after.channel.name} in {guild.name}.")
                # Cancel timer associated with the *old* channel state
                voice_helpers.cancel_leave_timer(self.bot, guild_id, reason="bot moved channels")
                # Check if should start timer in *new* channel if idle
                vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
                if vc and vc.is_connected() and not self.playback_manager.is_playing(guild_id):
                    log.debug("Bot moved and is idle, starting leave timer check for new channel.")
                    await voice_helpers.start_leave_timer(self.bot, vc)

            # Bot Connected (Initially)
            elif not before.channel and after.channel:
                log.info(f"EVENT: Bot connected to {after.channel.name} in {guild.name}.")
                # Start timer check if connected idle (less common, usually connects to play)
                vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
                if vc and vc.is_connected() and not self.playback_manager.is_playing(guild_id):
                    # Only start timer if mode is IDLE, otherwise playback is expected
                    if self.playback_manager.playback_mode.get(guild_id, PlaybackMode.IDLE) == PlaybackMode.IDLE:
                         log.debug("Bot connected idle, starting leave timer check.")
                         await voice_helpers.start_leave_timer(self.bot, vc)
                    else:
                         log.debug(f"Bot connected, but playback mode is {self.playback_manager.playback_mode.get(guild_id)}. Not starting idle timer.")


    @commands.Cog.listener()
    async def on_application_command_error(self, ctx: discord.ApplicationContext, error: discord.DiscordException):
        """Global handler for slash command errors originating from cogs."""
        # Check if the error originated from a cog command by checking ctx.cog
        if ctx.command and ctx.cog:
            command_name = ctx.command.qualified_name
            user_name = f"{ctx.author.name}({ctx.author.id})" if ctx.author else "Unknown User"
            guild_name = f"{ctx.guild.name}({ctx.guild.id})" if ctx.guild else "DM Context"
            log_prefix = f"CMD ERROR (/{command_name}, Cog: {ctx.cog.qualified_name}, User: {user_name}, Guild: {guild_name}):"

            # Helper function to send error messages
            async def send_error_response(message: str, log_level=logging.WARNING, delete_after=None, ephemeral=True):
                log_level_actual = logging.DEBUG if isinstance(error, commands.CommandNotFound) else log_level
                log.log(log_level_actual, f"{log_prefix} {message} (Error Type: {type(error).__name__}, Details: {error})")
                try:
                    # Use the playback manager's helper which handles is_done() checks
                    await self.playback_manager._try_respond(ctx.interaction, message, ephemeral=ephemeral, delete_after=delete_after)
                except discord.NotFound: # Catch just in case _try_respond fails internally
                     log.warning(f"{log_prefix} Interaction not found while attempting to send error response.")
                except Exception as e_resp:
                     log.error(f"{log_prefix} Unexpected error sending error response via _try_respond: {e_resp}", exc_info=e_resp)


            # --- Specific Error Handling ---
            if isinstance(error, commands.CommandOnCooldown):
                 await send_error_response(f"⏳ Command on cooldown. Please wait {error.retry_after:.1f} seconds.", delete_after=10)
            elif isinstance(error, commands.MissingPermissions):
                 perms = ', '.join(f"`{p}`" for p in error.missing_permissions)
                 await send_error_response(f"🚫 You lack the required permissions: {perms}", log_level=logging.WARNING)
            elif isinstance(error, commands.BotMissingPermissions):
                 perms = ', '.join(f"`{p}`" for p in error.missing_permissions)
                 await send_error_response(f"🚫 I lack the required permissions: {perms}. Please check my role settings.", log_level=logging.ERROR)
            elif isinstance(error, commands.CheckFailure):
                 # Provide a more specific message if possible based on the check, otherwise generic
                 log.warning(f"{log_prefix} CheckFailure encountered: {error}")
                 await send_error_response("🚫 You do not meet the requirements to use this command.")
            elif isinstance(error, commands.CommandInvokeError):
                 original = error.original
                 log.error(f"{log_prefix} An error occurred within the command code.", exc_info=original)
                 # Default user message
                 user_msg = "❌ An internal error occurred while executing the command. Please report this if it persists."
                 # More specific user messages based on the original error
                 if isinstance(original, FileNotFoundError) and ('ffmpeg' in str(original).lower() or 'ffprobe' in str(original).lower()):
                      user_msg = "❌ Internal Error: FFmpeg/FFprobe (needed for audio) not found or not accessible by the bot. Please contact the administrator."
                 elif config.PYDUB_AVAILABLE and isinstance(original, config.pydub_exceptions.CouldntDecodeError):
                      user_msg = "❌ Internal Error: Failed to decode an audio file. It might be corrupted or require FFmpeg to be installed and accessible."
                 elif isinstance(original, discord.errors.Forbidden):
                      user_msg = f"❌ Discord Permissions Error: I lack permissions needed for this action: {original.text}. Please check my roles/permissions."
                 elif "edge_tts" in str(type(original).__module__):
                      user_msg = f"❌ TTS Generation Error: ({type(original).__name__}). Check the input text, selected voice, or try again later."
                 # Add checks for other common errors (e.g., TimeoutError, specific API errors)
                 await send_error_response(user_msg, log_level=logging.ERROR)
            elif isinstance(error, discord.errors.NotFound):
                 # Typically means the interaction or a component message expired or was deleted
                 log.warning(f"{log_prefix} Interaction or related component not found (possibly timed out or deleted?). Error: {error}")
                 # Can't respond if the interaction is gone.
            elif isinstance(error, commands.CommandNotFound):
                 log.debug(f"{log_prefix} Unknown command invoked (unlikely for slash commands).")
                 # Don't usually need to respond to this for slash commands
            # Add more specific discord.py or custom errors here
            else:
                 # Log the unexpected error
                 log.error(f"{log_prefix} An unexpected Discord API or command system error occurred: {error}", exc_info=error)
                 # Send a generic error message
                 await send_error_response(f"❌ An unexpected error occurred ({type(error).__name__}).", log_level=logging.ERROR)
        else:
             # If the error didn't come from a cog command (e.g., bot-level check failure?)
             log_prefix = f"APP CMD ERROR (Non-Cog? Command: {ctx.command.name if ctx.command else 'N/A'}, User: {ctx.author.name if ctx.author else 'N/A'}):"
             log.error(f"{log_prefix} Unhandled application command error: {error}", exc_info=error)
             # Optionally try to send a very generic error message if possible
             try:
                 await self.playback_manager._try_respond(ctx.interaction, "An application command error occurred.", ephemeral=True)
             except Exception:
                 pass # Ignore if we can't respond


def setup(bot: commands.Bot):
    # Perform checks before adding cog
    if not hasattr(bot, 'playback_manager'):
         log.critical("Cannot load EventsCog: bot.playback_manager is not set.")
         return
    if not hasattr(bot, 'user_sound_config'):
        log.critical("Cannot load EventsCog: bot.user_sound_config is not set.")
        return
    if not hasattr(bot, 'guild_settings'):
        log.critical("Cannot load EventsCog: bot.guild_settings is not set.")
        return

    bot.add_cog(EventsCog(bot))
    log.info("Events Cog loaded.")