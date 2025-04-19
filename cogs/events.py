# -*- coding: utf-8 -*-
import discord
from discord.ext import commands
import logging
import os
import asyncio # Make sure asyncio is imported

import config
import data_manager
from utils import file_helpers, text_helpers
# PlaybackManager is accessed via self.bot.playback_manager

# Check for dependencies needed by this cog specifically (TTS)
try:
    import edge_tts
    EDGE_TTS_AVAILABLE = True
except ImportError:
    EDGE_TTS_AVAILABLE = False

log = logging.getLogger('SoundBot.Cog.Events')

class EventsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.playback_manager = bot.playback_manager # Shortcut

    @commands.Cog.listener()
    async def on_ready(self):
        """Called once the bot is ready and operational."""
        log.info(f'Logged in as {self.bot.user.name} ({self.bot.user.id})')
        log.info(f"Using discord.py version {discord.__version__}")
        log.info(f"Max Playback Duration: {self.bot.config.MAX_PLAYBACK_DURATION_MS / 1000}s")
        log.info(f"Normalization Target: {self.bot.config.TARGET_LOUDNESS_DBFS} dBFS")
        log.info(f"Leave Timeout: {self.bot.config.AUTO_LEAVE_TIMEOUT_SECONDS}s")
        log.info(f"TTS Engine Available: {config.EDGE_TTS_AVAILABLE}")
        log.info(f"Pydub Available: {config.PYDUB_AVAILABLE}")
        log.info(f"PyNaCl Available: {config.NACL_AVAILABLE}")
        log.info(f"Loaded {len(self.bot.user_sound_config)} user configs.")
        log.info(f"Loaded {len(self.bot.guild_settings)} guild settings.")
        log.info(f"Sound Bot is operational. Monitoring {len(self.bot.guilds)} guilds.")
        # You could add activity setting here:
        # await self.bot.change_presence(activity=discord.Game(name="/help | Playing sounds"))

    @commands.Cog.listener()
    async def on_voice_state_update(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
        """Handles users joining/leaving VCs and the bot's own state changes."""
        guild = member.guild
        if not guild: return # Ignore DMs or unexpected states
        guild_id = guild.id

        # --- User Join/Move Event ---
        # Triggered when a user connects to a VC (before.channel is None) or moves between VCs (before.channel != after.channel)
        if not member.bot and after.channel and before.channel != after.channel:
            channel_to_join = after.channel
            user_display_name = member.display_name # Use display name (nickname if set)
            user_id_str = str(member.id)
            log.info(f"EVENT: User {user_display_name} ({user_id_str}) entered {channel_to_join.name} in {guild.name}")

            # If bot is already in the channel user joined, cancel leave timer
            vc = discord.utils.get(self.bot.voice_clients, guild=guild)
            if vc and vc.is_connected() and vc.channel == channel_to_join:
                log.debug(f"User {user_display_name} joined bot's current channel ({vc.channel.name}). Cancelling timer.")
                self.playback_manager.cancel_leave_timer(guild_id, reason=f"user {user_display_name} joined")

            # --- Determine Sound to Play ---
            sound_path: Optional[str] = None
            use_tts_join = False
            is_temp_sound = False # Flag to indicate if the sound file should be deleted after playing

            user_config = self.bot.user_sound_config.get(user_id_str)

            # 1. Check for Custom Join Sound
            if user_config and "join_sound" in user_config:
                filename = user_config["join_sound"]
                potential_path = os.path.join(config.SOUNDS_DIR, filename) # Join sounds are stored in SOUNDS_DIR
                if os.path.exists(potential_path):
                    sound_path = potential_path
                    log.info(f"SOUND: Using configured join sound: '{filename}' for {user_display_name}")
                else:
                    log.warning(f"SOUND: Configured join sound '{filename}' not found for {user_display_name}. Removing broken entry, falling back to TTS.")
                    # Clean up broken config entry
                    del user_config["join_sound"]
                    if not user_config: # Remove user entry if it's now empty
                        if user_id_str in self.bot.user_sound_config: del self.bot.user_sound_config[user_id_str]
                    data_manager.save_config(self.bot.user_sound_config) # Save the change
                    use_tts_join = True
            else:
                # No custom join sound configured
                use_tts_join = True
                log.info(f"SOUND: No custom join sound for {user_display_name}. Using TTS join.")

            # 2. Generate TTS Join Sound if Needed
            if use_tts_join:
                if config.EDGE_TTS_AVAILABLE and EDGE_TTS_AVAILABLE: # Check both config const and runtime import
                    # Generate a unique temp filename for the TTS output
                    tts_filename = f"tts_join_{member.id}_{os.urandom(4).hex()}.mp3"
                    tts_path = os.path.join(config.SOUNDS_DIR, tts_filename) # Store temp TTS in SOUNDS_DIR
                    log.info(f"TTS JOIN: Generating for {user_display_name} ('{tts_filename}')...")

                    try:
                        tts_defaults = user_config.get("tts_defaults", {}) if user_config else {}
                        tts_voice = tts_defaults.get("voice", config.DEFAULT_TTS_VOICE)

                        # Validate voice choice (important!)
                        if not any(v.value == tts_voice for v in config.FULL_EDGE_TTS_VOICE_CHOICES):
                            log.warning(f"TTS JOIN: Invalid voice '{tts_voice}' configured for user {user_id_str}. Falling back to bot default '{config.DEFAULT_TTS_VOICE}'.")
                            tts_voice = config.DEFAULT_TTS_VOICE

                        log.debug(f"TTS JOIN: Using voice: {tts_voice}")

                        # Normalize the name for TTS
                        original_name = user_display_name
                        normalized_name = text_helpers.normalize_for_tts(original_name)
                        text_to_speak = f"{normalized_name} joined" if normalized_name.strip() else "Someone joined"

                        if original_name != normalized_name:
                            log.info(f"TTS JOIN: Normalized Name: '{original_name}' -> '{normalized_name}'")
                        log.info(f"TTS JOIN: Final Text to Speak: '{text_to_speak}'")

                        # Generate TTS using edge_tts
                        communicate = edge_tts.Communicate(text_to_speak, tts_voice)
                        await communicate.save(tts_path)

                        # Verify file creation and size
                        if not os.path.exists(tts_path) or os.path.getsize(tts_path) == 0:
                            raise RuntimeError(f"Edge-TTS failed to create a non-empty file: {tts_path}")

                        log.info(f"TTS JOIN: Successfully saved TTS file '{tts_filename}'")
                        sound_path = tts_path
                        is_temp_sound = True # Mark for deletion after playback

                    except Exception as e:
                        log.error(f"TTS JOIN: Failed generation for {user_display_name} (voice={tts_voice}): {e}", exc_info=True)
                        sound_path = None # Ensure sound_path is None on failure
                        # Clean up potentially empty/failed file
                        if os.path.exists(tts_path):
                            try:
                                os.remove(tts_path)
                                log.warning(f"TTS JOIN: Cleaned up failed temporary file: {tts_path}")
                            except OSError as del_err:
                                log.warning(f"TTS JOIN: Could not clean up failed temporary file '{tts_path}': {del_err}")
                else:
                     log.error(f"TTS JOIN: Cannot generate for {user_display_name}, edge-tts library not available.")
                     sound_path = None

            # --- Enqueue and Ensure Playback ---
            if sound_path:
                 # Check bot permissions in the target channel *before* connecting/queueing
                 bot_perms = channel_to_join.permissions_for(guild.me)
                 if not bot_perms.connect or not bot_perms.speak:
                     log.warning(f"Missing Connect/Speak permission in '{channel_to_join.name}'. Cannot play sound for {user_display_name}.")
                     # Clean up temp TTS file if created
                     if is_temp_sound and os.path.exists(sound_path):
                        try: os.remove(sound_path)
                        except OSError: pass
                     return # Don't proceed if permissions are missing

                 # Add to the playback manager's queue
                 await self.playback_manager.add_to_queue(guild_id, member, sound_path)

                 # Ensure the bot is connected and the playback task is running
                 try:
                     # --- Logic to connect/move/start playback for the event ---
                     vc_event = discord.utils.get(self.bot.voice_clients, guild=guild)
                     should_start_play_task = False
                     target_channel = channel_to_join
                     current_vc = vc_event # Start with the current VC state

                     if current_vc and current_vc.is_connected():
                         if current_vc.channel != target_channel:
                             log.info(f"VOICE (Event): Moving from '{current_vc.channel.name}' to '{target_channel.name}' for join sound.")
                             self.playback_manager.cancel_leave_timer(guild_id, reason="moving for join sound")
                             await current_vc.move_to(target_channel)
                             log.info(f"VOICE (Event): Moved successfully.")
                             # VC reference remains the same after move
                             should_start_play_task = True # Need to start playback after move
                         elif not current_vc.is_playing():
                              log.debug(f"VOICE (Event): Bot already in '{target_channel.name}' and idle.")
                              should_start_play_task = True # Start playback if idle
                         else:
                              log.info(f"VOICE (Event): Bot already playing in {guild.name}. Join sound queued.")
                              should_start_play_task = False # Already playing, queue will handle it
                     else:
                          log.info(f"VOICE (Event): Connecting to '{target_channel.name}' for join sound.")
                          self.playback_manager.cancel_leave_timer(guild_id, reason="connecting for join sound")
                          current_vc = await target_channel.connect(timeout=30.0, reconnect=True) # Assign the new VC
                          log.info(f"VOICE (Event): Connected successfully.")
                          should_start_play_task = True # Need to start playback after connect

                     # --- Start playback task if needed ---
                     if should_start_play_task and current_vc and current_vc.is_connected():
                         log.debug(f"Event: Triggering playback check for guild {guild_id}.")
                         # Check if a task is already running/scheduled for this guild
                         if guild_id not in self.playback_manager.guild_play_tasks or self.playback_manager.guild_play_tasks[guild_id].done():
                             task_name = f"QueueStart_Event_{guild_id}"
                             if self.playback_manager.guild_sound_queues.get(guild_id): # Check queue has items
                                 log.info(f"Event: Starting new play task '{task_name}'.")
                                 self.playback_manager.guild_play_tasks[guild_id] = asyncio.create_task(
                                     self.playback_manager.play_next_in_queue(guild),
                                     name=task_name
                                 )
                             else:
                                 log.debug(f"Event: Queue emptied before task '{task_name}' could start.")
                                 # If queue is empty now, check for idle timer start
                                 if not current_vc.is_playing():
                                     await self.playback_manager.start_leave_timer(current_vc)
                         else:
                             log.debug(f"Event: Play task already running/scheduled for guild {guild_id}.")

                     # ---- REMOVED INCORRECT LINE: self.playback_manager.ensure_playback_task(guild) ----

                     elif not current_vc or not current_vc.is_connected():
                          log.warning(f"Event: Could not connect/move, cannot start playback for {user_display_name}.")

                 except asyncio.TimeoutError:
                      log.error(f"VOICE (Event): Timeout connecting/moving to '{channel_to_join.name}'.")
                      if guild_id in self.playback_manager.guild_sound_queues: self.playback_manager.guild_sound_queues[guild_id].clear()
                 except discord.errors.ClientException as e:
                      log.warning(f"VOICE (Event): ClientException during connect/move: {e}")
                 except Exception as e:
                      log.error(f"VOICE (Event): Unexpected error during connect/move: {e}", exc_info=True)
                      if guild_id in self.playback_manager.guild_sound_queues: self.playback_manager.guild_sound_queues[guild_id].clear()

            else:
                 # sound_path is None (means custom sound missing and TTS failed/disabled)
                 log.error(f"SOUND/TTS JOIN: Could not find or generate a sound for {user_display_name}. Skipping playback.")


        # --- User Leave Event ---
        # Triggered when a user leaves a VC (after.channel is None) or moves (before.channel != after.channel)
        # We only care if they left the bot's channel
        elif not member.bot and before.channel and before.channel != after.channel:
            vc = discord.utils.get(self.bot.voice_clients, guild=guild)
            # Check if the user left the channel the bot is currently in
            if vc and vc.is_connected() and vc.channel == before.channel:
                log.info(f"EVENT: User {member.display_name} left bot's channel ({before.channel.name}). Checking if bot should leave.")
                # Use call_later to allow Discord state to fully update before checking if alone
                asyncio.get_event_loop().call_later(1.0, lambda: asyncio.create_task(self.playback_manager.start_leave_timer(vc)))


        # --- Bot's Own State Change ---
        elif member.id == self.bot.user.id:
            # Case 1: Bot Disconnected (manually or kicked)
            if before.channel and not after.channel:
                log.info(f"EVENT: Bot disconnected from {before.channel.name} in {guild.name}. Cleaning up resources.")
                # Playback manager's safe_disconnect should handle this, but we ensure cleanup here too.
                self.playback_manager.cancel_leave_timer(guild_id, reason="bot disconnected event")
                if guild_id in self.playback_manager.guild_play_tasks:
                    play_task = self.playback_manager.guild_play_tasks.pop(guild_id, None)
                    if play_task and not play_task.done():
                        play_task.cancel()
                        log.debug(f"Cleaned up play task for disconnected guild {guild_id} via event.")
                if guild_id in self.playback_manager.guild_sound_queues:
                    self.playback_manager.guild_sound_queues[guild_id].clear()
                    log.debug(f"Cleared sound queue for disconnected guild {guild_id} via event.")

            # Case 2: Bot Moved
            elif before.channel and after.channel and before.channel != after.channel:
                 log.info(f"EVENT: Bot moved from {before.channel.name} to {after.channel.name} in {guild.name}.")
                 self.playback_manager.cancel_leave_timer(guild_id, reason="bot moved channels")
                 # Check if idle in new channel
                 vc = discord.utils.get(self.bot.voice_clients, guild=guild)
                 if vc and vc.is_connected() and not vc.is_playing():
                      log.debug("Bot moved and is idle, starting leave timer check for new channel.")
                      await self.playback_manager.start_leave_timer(vc) # Use await here

            # Case 3: Bot Connected (initially)
            elif not before.channel and after.channel:
                 log.info(f"EVENT: Bot connected to {after.channel.name} in {guild.name}.")
                 # Bot just connected, likely due to a user join or command.
                 # Timer logic should be handled by the action that caused the connect.
                 # If it connected idle (unlikely but possible), check timer.
                 vc = discord.utils.get(self.bot.voice_clients, guild=guild)
                 if vc and vc.is_connected() and not vc.is_playing():
                     log.debug("Bot connected idle, starting leave timer check.")
                     await self.playback_manager.start_leave_timer(vc) # Use await here


    @commands.Cog.listener()
    async def on_application_command_error(self, ctx: discord.ApplicationContext, error: discord.DiscordException):
        """Global handler for slash command errors within cogs."""
        command_name = ctx.command.qualified_name if ctx.command else "Unknown Command"
        user_name = f"{ctx.author.name}({ctx.author.id})" if ctx.author else "Unknown User"
        guild_name = f"{ctx.guild.name}({ctx.guild.id})" if ctx.guild else "DM Context"
        log_prefix = f"CMD ERROR (/{command_name}, user: {user_name}, guild: {guild_name}):"

        async def send_error_response(message: str, log_level=logging.WARNING):
            """Helper to send response and log."""
            # Avoid logging CommandNotFound unless debugging
            log_level_actual = logging.DEBUG if isinstance(error, commands.CommandNotFound) else log_level
            log.log(log_level_actual, f"{log_prefix} {message} (Error Type: {type(error).__name__}, Details: {error})")
            try:
                # Use followup if interaction already deferred/responded
                responder = ctx.followup.send if ctx.interaction.response.is_done() else ctx.respond
                await responder(message, ephemeral=True)
            except discord.NotFound:
                log.warning(f"{log_prefix} Interaction not found while sending error response.")
            except discord.errors.InteractionResponded:
                 # If response failed because it was already done, try followup
                 try:
                     if ctx.interaction.response.is_done(): # Check again before followup
                        await ctx.followup.send(message, ephemeral=True)
                     else: # Should not happen if InteractionResponded was raised, but safety
                        await ctx.respond(message, ephemeral=True)
                 except Exception:
                      log.warning(f"{log_prefix} Interaction already responded to, followup failed too.")
            except discord.Forbidden:
                 log.error(f"{log_prefix} Missing permissions to send error response in channel {ctx.channel_id}.")
            except Exception as e_resp:
                 log.error(f"{log_prefix} Unexpected error sending error response: {e_resp}", exc_info=e_resp)

        # --- Specific Error Handling ---
        if isinstance(error, commands.CommandOnCooldown):
            await send_error_response(f"⏳ Command on cooldown. Please wait {error.retry_after:.1f} seconds.")
        elif isinstance(error, commands.MissingPermissions):
            perms = ', '.join(f"`{p}`" for p in error.missing_permissions)
            await send_error_response(f"🚫 You lack the required permissions: {perms}", log_level=logging.WARNING)
        elif isinstance(error, commands.BotMissingPermissions):
            perms = ', '.join(f"`{p}`" for p in error.missing_permissions)
            await send_error_response(f"🚫 I lack the required permissions: {perms}. Please check my role settings.", log_level=logging.ERROR)
        elif isinstance(error, commands.CheckFailure): # General permission/check failure
            await send_error_response("🚫 You do not have permission to use this command or perform this action.")
        elif isinstance(error, commands.CommandInvokeError):
            # Error originated from within the command's code
            original = error.original
            log.error(f"{log_prefix} An error occurred within the command code.", exc_info=original)
            user_msg = "❌ An internal error occurred while executing the command. Please report this if it persists."
            # Check for specific underlying errors for better user feedback
            if isinstance(original, FileNotFoundError) and ('ffmpeg' in str(original).lower() or 'ffprobe' in str(original).lower()):
                 user_msg = "❌ Internal Error: FFmpeg/FFprobe (needed for audio) not found by the bot. Please contact the administrator."
            elif isinstance(original, config.pydub_exceptions.CouldntDecodeError):
                 user_msg = "❌ Internal Error: Failed to decode an audio file. It might be corrupted or require FFmpeg."
            elif isinstance(original, discord.errors.Forbidden):
                  user_msg = f"❌ Internal Error: I encountered a permission issue while executing: {original.text}. Please check my permissions."
            elif "edge_tts" in str(type(original).__module__): # Check if it's an edge-tts specific error
                 user_msg = f"❌ Internal TTS Error: ({type(original).__name__}). There might be an issue with the TTS service, the input text, or the selected voice."

            await send_error_response(user_msg, log_level=logging.ERROR)
        elif isinstance(error, discord.errors.InteractionResponded):
             log.warning(f"{log_prefix} Interaction already responded to. Error: {error}")
             # Usually means code tried to respond twice, often recoverable. Don't message user unless necessary.
        elif isinstance(error, discord.errors.NotFound):
             log.warning(f"{log_prefix} Interaction or component not found (possibly timed out or deleted?). Error: {error}")
             # Don't always message user, could be normal (e.g., clicking old button)
        elif isinstance(error, commands.CommandNotFound):
             # This shouldn't happen with slash commands unless something is very wrong
             log.debug(f"{log_prefix} Unknown command invoked (this is unusual for slash commands).")
        # Add more specific discord.py error handlers if needed
        # (e.g., discord.InvalidArgument, discord.HTTPException for API errors)
        else:
             # Catch-all for other Discord/command errors
             log.error(f"{log_prefix} An unexpected Discord API or command system error occurred: {error}", exc_info=error)
             await send_error_response(f"❌ An unexpected error occurred ({type(error).__name__}).", log_level=logging.ERROR)


def setup(bot: commands.Bot):
    # Check dependencies before adding cog
    # if not EDGE_TTS_AVAILABLE:
    #     log.critical("Cannot load Events Cog: edge-tts library not found.")
    #     return # Prevent loading if critical dependency missing

    bot.add_cog(EventsCog(bot))
    log.info("Events Cog loaded.")