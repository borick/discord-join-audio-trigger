# -*- coding: utf-8 -*-
import discord
from discord.ext import commands
import asyncio
import os
import io
import math
import logging
from collections import deque
from typing import Dict, Any, Optional, Tuple, List

# Check required libraries (can rely on config checks, but local check is fine too)
try:
    from pydub import AudioSegment
    from pydub.exceptions import CouldntDecodeError
    PYDUB_AVAILABLE = True
except ImportError:
    PYDUB_AVAILABLE = False

import config # Bot config, paths, constants
import data_manager # Functions to load/save data (used indirectly via bot state)
# from utils import file_helpers # utils not directly needed here anymore

log = logging.getLogger('SoundBot.PlaybackManager')

class PlaybackManager:
    """Handles audio queues, playback, VC connection/state management."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config = bot.config # Direct access to config attached to bot
        # Guild-specific states managed here
        self.guild_sound_queues: Dict[int, deque[Tuple[discord.Member, str]]] = {}
        self.guild_play_tasks: Dict[int, asyncio.Task[Any]] = {}
        self.guild_leave_timers: Dict[int, asyncio.Task[Any]] = {}

    # --- Audio Processing ---
    def process_audio(self, sound_path: str, member_display_name: str = "User") -> Tuple[Optional[discord.PCMAudio], Optional[io.BytesIO]]:
        """
        Loads, TRIMS, normalizes, and prepares audio for Discord playback.
        Returns a tuple: (PCMAudio source or None, BytesIO buffer or None).
        The BytesIO buffer MUST be closed by the caller after playback is finished or fails.
        """
        if not PYDUB_AVAILABLE:
            log.error("AUDIO: Pydub library is not available. Cannot process audio.")
            return None, None
        if not os.path.exists(sound_path):
            log.error(f"AUDIO: File not found for processing: '{sound_path}'")
            return None, None

        audio_source: Optional[discord.PCMAudio] = None
        pcm_data_io: Optional[io.BytesIO] = None
        basename = os.path.basename(sound_path)

        try:
            log.debug(f"AUDIO: Loading '{basename}'...")
            # Determine format hint from extension
            ext = os.path.splitext(sound_path)[1].lower().strip('.')
            if not ext:
                log.warning(f"AUDIO: File '{basename}' has no extension. Assuming 'mp3' for loading.")
                ext = 'mp3' # Default guess

            # Load with Pydub
            try:
                # Attempt loading with format hint first
                audio_segment = AudioSegment.from_file(sound_path, format=ext)
            except CouldntDecodeError:
                # If decode fails, try without format hint (let Pydub/FFmpeg guess)
                log.warning(f"AUDIO: Pydub decode failed with format '{ext}' for '{basename}'. Retrying auto-detection.")
                audio_segment = AudioSegment.from_file(sound_path)
            except Exception as load_e:
                 # Handle other potential loading errors (file access, etc.)
                 log.error(f"AUDIO: Failed to load '{basename}' with Pydub: {load_e}", exc_info=True)
                 raise load_e # Re-raise to be caught by the outer try-except

            log.debug(f"AUDIO: Loaded '{basename}' successfully (Duration: {len(audio_segment)}ms)")

            # --- Trimming ---
            if len(audio_segment) > self.config.MAX_PLAYBACK_DURATION_MS:
                log.info(f"AUDIO: Trimming '{basename}' from {len(audio_segment)}ms to first {self.config.MAX_PLAYBACK_DURATION_MS}ms.")
                audio_segment = audio_segment[:self.config.MAX_PLAYBACK_DURATION_MS]
            else:
                log.debug(f"AUDIO: '{basename}' duration ({len(audio_segment)}ms) is within limit ({self.config.MAX_PLAYBACK_DURATION_MS}ms). No trimming needed.")

            # --- Normalization ---
            peak_dbfs = audio_segment.max_dBFS
            if not math.isinf(peak_dbfs) and peak_dbfs > -90.0: # Avoid normalizing silence or extremely quiet audio
                target_dbfs = self.config.TARGET_LOUDNESS_DBFS
                change_in_dbfs = target_dbfs - peak_dbfs
                log.info(f"AUDIO: Normalizing '{basename}'. Peak:{peak_dbfs:.2f} Target:{target_dbfs:.2f} Gain:{change_in_dbfs:.2f} dB.")

                # Apply gain limiting (e.g., max +6dB to prevent excessive amplification/clipping)
                gain_limit = 6.0
                apply_gain = min(change_in_dbfs, gain_limit) if change_in_dbfs > 0 else change_in_dbfs
                if apply_gain != change_in_dbfs:
                     log.info(f"AUDIO: Limiting gain to +{gain_limit}dB for '{basename}' (calculated: {change_in_dbfs:.2f}dB).")

                audio_segment = audio_segment.apply_gain(apply_gain)
            elif math.isinf(peak_dbfs):
                 log.warning(f"AUDIO: Cannot normalize silent audio '{basename}'. Peak is -inf.")
            else: # peak_dbfs <= -90.0
                 log.warning(f"AUDIO: Skipping normalization for very quiet audio '{basename}'. Peak: {peak_dbfs:.2f}")

            # --- Format Conversion for Discord (PCM) ---
            # Ensure stereo, 48kHz sample rate for compatibility
            audio_segment = audio_segment.set_frame_rate(48000).set_channels(2)

            # Export to PCM S16 LE in memory
            pcm_data_io = io.BytesIO()
            audio_segment.export(pcm_data_io, format="s16le") # Signed 16-bit little-endian PCM
            pcm_data_io.seek(0) # Reset buffer position to the beginning

            # Create Discord audio source if data exists
            if pcm_data_io.getbuffer().nbytes > 0:
                audio_source = discord.PCMAudio(pcm_data_io)
                log.debug(f"AUDIO: Successfully processed '{basename}' into PCMAudio.")
                return audio_source, pcm_data_io # Return source and the buffer to be closed later
            else:
                log.error(f"AUDIO: Exported raw audio for '{basename}' is empty!")
                if pcm_data_io: pcm_data_io.close() # Close empty buffer immediately
                return None, None

        except CouldntDecodeError as decode_err:
            # Specific error for FFmpeg issues or corrupted files
            log.error(f"AUDIO: Pydub CouldntDecodeError for '{basename}'. Is FFmpeg installed and in PATH? Is the file corrupt? Error: {decode_err}", exc_info=True)
            if pcm_data_io: pcm_data_io.close()
            return None, None
        except FileNotFoundError:
            # Should be caught earlier, but good to have
            log.error(f"AUDIO: File not found during processing: '{sound_path}'")
            if pcm_data_io: pcm_data_io.close()
            return None, None
        except Exception as e:
            # Catch-all for other unexpected errors during processing
            log.error(f"AUDIO: Unexpected error processing '{basename}': {e}", exc_info=True)
            if pcm_data_io and not pcm_data_io.closed:
                try: pcm_data_io.close()
                except Exception: pass
            return None, None


    # --- Queue and Playback Logic ---

    async def add_to_queue(self, guild_id: int, member: discord.Member, sound_path: str):
        """Adds a sound to the guild's playback queue."""
        if guild_id not in self.guild_sound_queues:
            self.guild_sound_queues[guild_id] = deque()
        self.guild_sound_queues[guild_id].append((member, sound_path))
        log.info(f"QUEUE: Added sound '{os.path.basename(sound_path)}' for {member.display_name} in guild {guild_id}. Queue size: {len(self.guild_sound_queues[guild_id])}")

    async def play_next_in_queue(self, guild: discord.Guild):
        """Plays the next sound in the guild's join queue if conditions are met."""
        guild_id = guild.id
        task_id_obj = asyncio.current_task()
        task_id = task_id_obj.get_name() if task_id_obj else f'Task_GID_{guild_id}'
        log_prefix = f"QUEUE PLAY [{task_id} GID:{guild_id}]"

        if task_id_obj and task_id_obj.cancelled():
            log.debug(f"{log_prefix}: Task cancelled externally, removing tracker.")
            if guild_id in self.guild_play_tasks and self.guild_play_tasks.get(guild_id) is task_id_obj:
                del self.guild_play_tasks[guild_id]
            return

        # Check if queue exists and has items
        if guild_id not in self.guild_sound_queues or not self.guild_sound_queues[guild_id]:
            log.debug(f"{log_prefix}: Queue empty/non-existent. Playback task ending.")
            if guild_id in self.guild_play_tasks and self.guild_play_tasks.get(guild_id) is task_id_obj:
                del self.guild_play_tasks[guild_id]
            # If the bot is connected and idle, start the leave timer check
            vc_check = discord.utils.get(self.bot.voice_clients, guild=guild)
            if vc_check and vc_check.is_connected() and not vc_check.is_playing():
                log.debug(f"{log_prefix}: Queue empty, triggering idle leave timer check.")
                # *** FIX: Use bot.loop.create_task here ***
                self.bot.loop.create_task(self.start_leave_timer(vc_check))
            return

        # Check voice client status
        vc = discord.utils.get(self.bot.voice_clients, guild=guild)
        if not vc or not vc.is_connected():
            log.warning(f"{log_prefix}: Task running, but bot not connected. Clearing queue and task.")
            if guild_id in self.guild_sound_queues: self.guild_sound_queues[guild_id].clear()
            if guild_id in self.guild_play_tasks and self.guild_play_tasks.get(guild_id) is task_id_obj: del self.guild_play_tasks[guild_id]
            self.cancel_leave_timer(guild_id, reason="bot not connected during queue check")
            return

        # Check if already playing
        if vc.is_playing():
            log.debug(f"{log_prefix}: Bot already playing, yielding.")
            # Don't remove the task tracker here, let the current playback finish and trigger the next check.
            return

        # --- Dequeue and Process ---
        try:
            member, sound_path = self.guild_sound_queues[guild_id].popleft()
            sound_basename = os.path.basename(sound_path)
            log.info(f"{log_prefix}: Processing '{sound_basename}' for {member.display_name}. Queue Left: {len(self.guild_sound_queues[guild_id])}")
        except IndexError:
            # Should be caught by the initial check, but handle race condition
            log.debug(f"{log_prefix}: Queue became empty unexpectedly during pop. Ending task.")
            if guild_id in self.guild_play_tasks and self.guild_play_tasks.get(guild_id) is task_id_obj: del self.guild_play_tasks[guild_id]
            if vc.is_connected() and not vc.is_playing():
                 # *** FIX: Use bot.loop.create_task here ***
                 self.bot.loop.create_task(self.start_leave_timer(vc))
            return

        # Check if it's a temporary TTS file needing deletion later
        is_temp_tts = sound_basename.startswith("tts_join_") and sound_basename.endswith(".mp3")

        # Process the audio file
        audio_source, audio_buffer_to_close = self.process_audio(sound_path, member.display_name)

        if audio_source:
            try:
                self.cancel_leave_timer(guild_id, reason="starting playback from queue")
                log.info(f"{log_prefix}: Playing '{sound_basename}' for {member.display_name}...")

                # Define the 'after' callback using a lambda to capture necessary context
                # Use bot.loop.call_soon_threadsafe to schedule the actual cleanup logic
                # This ensures the cleanup runs on the main event loop
                after_callback = lambda e: self.bot.loop.call_soon_threadsafe(
                    self.after_play_cleanup_threadsafe, # New wrapper function
                    e,
                    vc.guild.id, # Pass guild ID instead of full VC object
                    task_id_obj.get_name() if task_id_obj else None, # Pass task name/id
                    sound_path if is_temp_tts else None,
                    audio_buffer_to_close
                )

                vc.play(audio_source, after=after_callback)
                log.debug(f"{log_prefix}: vc.play() called for '{sound_basename}'.")
                # Keep the task tracker, it will be removed in after_play_cleanup or if queue empties

            except (discord.errors.ClientException, Exception) as e:
                log.error(f"{log_prefix}: Error calling vc.play() for '{sound_basename}': {type(e).__name__}: {e}", exc_info=True)
                # Manually trigger cleanup if play fails immediately
                # No need for threadsafe here as we are likely still in async context
                self.after_play_cleanup(
                    error=e,
                    voice_client=vc,
                    task_ref=task_id_obj,
                    path_to_delete=sound_path if is_temp_tts else None,
                    audio_buffer=audio_buffer_to_close
                )
        else:
            # Processing failed, audio_source is None
            log.warning(f"{log_prefix}: No valid audio source for '{sound_basename}'. Skipping playback.")
            # Clean up buffer if it exists
            if audio_buffer_to_close and not audio_buffer_to_close.closed:
                try:
                    audio_buffer_to_close.close()
                    log.debug(f"{log_prefix}: Closed buffer after failed processing for '{sound_basename}'.")
                except Exception: pass
            # Clean up temp file if it was a failed TTS attempt
            if is_temp_tts and os.path.exists(sound_path):
                try:
                    os.remove(sound_path)
                    log.info(f"{log_prefix}: Deleted FAILED temporary TTS file: {sound_path}")
                except OSError as e_del:
                    log.warning(f"{log_prefix}: Failed to delete failed TTS file '{sound_path}': {e_del}")

            # Immediately try to play the next item if processing failed
            log.debug(f"{log_prefix}: Triggering next queue check immediately after failed processing.")
            # *** FIX: Use bot.loop.create_task here ***
            self.bot.loop.create_task(self.play_next_in_queue(guild), name=f"QueueSkip_{guild_id}")

    # --- AFTER PLAY CLEANUP (Threadsafe Wrapper) ---
    def after_play_cleanup_threadsafe(self, error: Optional[Exception], guild_id: int, task_name: Optional[str], path_to_delete: Optional[str], audio_buffer: Optional[io.BytesIO]):
        """
        This function is called via loop.call_soon_threadsafe from the 'after' callback.
        It runs on the main event loop and can safely interact with bot state and schedule tasks.
        """
        log_prefix = f"AFTER_PLAY_TS (Guild {guild_id}): Task: {task_name or 'UnknownTask'}"
        log.debug(f"{log_prefix} Threadsafe cleanup initiated.")

        # --- Buffer & Temp File Cleanup (can happen safely here) ---
        if audio_buffer:
            try:
                if not audio_buffer.closed: audio_buffer.close()
                log.debug(f"{log_prefix} Closed audio buffer for '{os.path.basename(path_to_delete or 'sound')}'")
            except Exception as buf_e: log.warning(f"{log_prefix} Error closing audio buffer: {buf_e}")

        if path_to_delete:
            log.debug(f"{log_prefix} Attempting cleanup for temp file: {path_to_delete}")
            if os.path.exists(path_to_delete):
                try:
                    os.remove(path_to_delete)
                    log.info(f"{log_prefix} Deleted temporary file: {path_to_delete}")
                except OSError as e_del: log.warning(f"{log_prefix} Failed to delete temp file '{path_to_delete}': {e_del}")
            else: log.debug(f"{log_prefix} Temp file '{path_to_delete}' not found (already cleaned?).")

        # --- Check Voice Client and Trigger Next Action ---
        current_vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
        current_task = self.guild_play_tasks.get(guild_id) # Get current task tracked for the guild

        if not current_vc or not current_vc.is_connected():
            log.warning(f"{log_prefix} VC disconnected before threadsafe cleanup. Removing task tracker if it matches.")
            if current_task and current_task.get_name() == task_name:
                if not current_task.done():
                    try: current_task.cancel()
                    except Exception: pass
                self.guild_play_tasks.pop(guild_id, None)
            self.cancel_leave_timer(guild_id, reason="after_play_ts on disconnected VC")
            return

        if error:
            log.error(f'{log_prefix} Playback finished with error: {error}', exc_info=error)

        # --- Queue Handling ---
        is_queue_empty = guild_id not in self.guild_sound_queues or not self.guild_sound_queues[guild_id]

        if not is_queue_empty:
            log.debug(f"{log_prefix} Queue is NOT empty. Ensuring playback task continues.")
            # Only start a new task if the current one is done or missing
            if not current_task or current_task.done():
                log.debug(f"{log_prefix} Current play task done/missing. Creating new task.")
                next_task_name = f"QueueCheckAfterPlay_TS_{guild_id}"
                if self.guild_sound_queues.get(guild_id): # Check again for race condition
                    self.guild_play_tasks[guild_id] = self.bot.loop.create_task(
                        self.play_next_in_queue(current_vc.guild), name=next_task_name
                    )
                else:
                    log.debug(f"{log_prefix} Queue emptied concurrently. Not starting new task.")
                    # Since queue is now empty, proceed to idle check
                    self.bot.loop.create_task(self.start_leave_timer(current_vc))
            else:
                log.debug(f"{log_prefix} Existing play task found and not done. Letting it continue.")
                # No need to explicitly restart it here, its loop should call play_next_in_queue again.

        else:
            # Queue is now empty
            log.debug(f"{log_prefix} Queue is empty. Bot is now idle.")
            # Clean up the tracker for the task if it matches the one that just finished
            if current_task and current_task.get_name() == task_name:
                log.debug(f"{log_prefix} Removing completed play task tracker.")
                self.guild_play_tasks.pop(guild_id, None)
            elif current_task:
                 log.warning(f"{log_prefix} Task tracker existed but didn't match ({current_task.get_name()}) the finished task ref ({task_name}).")

            # Trigger the idle leave timer check
            log.debug(f"{log_prefix} Triggering idle leave timer check.")
            # *** FIX: Use bot.loop.create_task here ***
            self.bot.loop.create_task(self.start_leave_timer(current_vc))

    # --- DEPRECATED cleanup function - Keep for reference if needed, but use threadsafe one ---
    # def after_play_cleanup(self, error: Optional[Exception], voice_client: discord.VoiceClient, task_ref: Optional[asyncio.Task], path_to_delete: Optional[str] = None, audio_buffer: Optional[io.BytesIO] = None):
    #     """ [DEPRECATED] Use after_play_cleanup_threadsafe instead """
    #     # ... original logic ...
    #     pass

    # --- Voice Client Connection and State Management ---

    async def ensure_voice_client(self, interaction: discord.Interaction, target_channel: discord.VoiceChannel, action_type: str = "Playback") -> Optional[discord.VoiceClient]:
        """
        Connects/moves VC, checks permissions, and checks busy status before playback.
        Returns the VoiceClient if ready, otherwise None. Sends feedback via interaction.
        """
        responder = interaction.followup if interaction.response.is_done() else interaction.edit_original_response
        guild = interaction.guild
        user = interaction.user

        if not guild:
            await self._try_respond(interaction, "This command must be used in a server.", ephemeral=True)
            return None
        if not isinstance(user, discord.Member): # Should not happen in guild context, but check
             await self._try_respond(interaction, "Could not identify you as a server member.", ephemeral=True)
             return None

        guild_id = guild.id
        log_prefix = f"{action_type.upper()} GID:{guild_id}:"

        # 1. Check Permissions
        bot_perms = target_channel.permissions_for(guild.me)
        if not bot_perms.connect or not bot_perms.speak:
            msg = f"❌ I don't have permission to **Connect** or **Speak** in {target_channel.mention}."
            await self._try_respond(interaction, msg, ephemeral=True)
            log.warning(f"{log_prefix} Missing Connect/Speak perms in {target_channel.name}.")
            return None

        vc = discord.utils.get(self.bot.voice_clients, guild=guild)

        try:
            # 2. Handle Existing Connection
            if vc and vc.is_connected():
                # 2a. Check if Busy (already playing something)
                if vc.is_playing():
                     # Check if it's just the join sound queue running
                     join_queue_active = guild_id in self.guild_sound_queues and self.guild_sound_queues[guild_id]
                     msg = "⏳ Bot is currently playing join sounds. Your action is queued/wait." if join_queue_active else "⏳ Bot is currently playing another sound/TTS. Please wait."
                     log_msg = f"{log_prefix} Bot busy ({'join queue' if join_queue_active else 'other playback'}) in {guild.name}, user {user.name}'s request ignored/deferred."
                     await self._try_respond(interaction, msg, ephemeral=True)
                     log.info(log_msg)
                     return None # Indicate busy status

                # 2b. Check if in the Correct Channel
                elif vc.channel != target_channel:
                     # Determine if we should move
                     should_move = (user.voice and user.voice.channel == target_channel) or not self.should_bot_stay(guild_id)

                     if should_move:
                         log.info(f"{log_prefix} Moving from '{vc.channel.name}' to '{target_channel.name}' for {user.name}.")
                         self.cancel_leave_timer(guild_id, reason=f"moving for {action_type}")
                         await vc.move_to(target_channel)
                         log.info(f"{log_prefix} Moved successfully.")
                     else:
                         log.debug(f"{log_prefix} Not moving from '{vc.channel.name}' to '{target_channel.name}' (stay enabled, user not there).")
                         msg = f"ℹ️ I'm currently set to stay in {vc.channel.mention}. Please join that channel or disable the stay setting (admin: `/togglestay`)."
                         await self._try_respond(interaction, msg, ephemeral=True)
                         return None # Indicate bot should not move
                # else: Bot is connected to the right channel and idle - proceed

            # 3. Handle No Connection
            else:
                log.info(f"{log_prefix} Connecting to '{target_channel.name}' for {user.name}.")
                self.cancel_leave_timer(guild_id, reason=f"connecting for {action_type}")
                vc = await target_channel.connect(timeout=30.0, reconnect=True)
                log.info(f"{log_prefix} Connected successfully.")

            # 4. Final Check and Return
            if not vc or not vc.is_connected():
                 log.error(f"{log_prefix} Failed to establish voice client for {target_channel.name} after attempt.")
                 await self._try_respond(interaction, "❌ Failed to connect or move to the voice channel.", ephemeral=True)
                 return None

            self.cancel_leave_timer(guild_id, reason=f"VC ready for {action_type}")
            return vc

        except asyncio.TimeoutError:
            await self._try_respond(interaction, "❌ Connection to the voice channel timed out.", ephemeral=True)
            log.error(f"{log_prefix} Connection/Move Timeout to {target_channel.name}")
            return None
        except discord.errors.ClientException as e:
            msg = "⏳ Bot is busy with connection changes. Please wait a moment." if "already connect" in str(e).lower() else f"❌ Error connecting/moving: {e}. Check permissions or try again."
            await self._try_respond(interaction, msg, ephemeral=True)
            log.warning(f"{log_prefix} Connection/Move ClientException: {e}")
            return None
        except Exception as e:
            await self._try_respond(interaction, "❌ An unexpected error occurred while joining the voice channel.", ephemeral=True)
            log.error(f"{log_prefix} Connection/Move unexpected error: {e}", exc_info=True)
            return None

    # --- CORRECTED _try_respond ---
    async def _try_respond(self, interaction: discord.Interaction, message: str, **kwargs):
        """Helper to safely send interaction responses, handling followup webhooks."""
        try:
            if interaction.response.is_done():
                 # If already responded/deferred, use followup webhook
                 await interaction.followup.send(content=message, **kwargs)
            else:
                 # If first response, use respond
                 await interaction.response.send_message(content=message, **kwargs)
        except discord.NotFound:
             log.warning(f"Interaction not found while trying to respond: {message[:50]}...")
        except discord.errors.InteractionResponded:
             # This might happen in race conditions, try followup as fallback
             log.warning(f"Interaction already responded to, trying followup for: {message[:50]}...")
             try:
                 await interaction.followup.send(content=message, **kwargs)
             except Exception as followup_e:
                 log.error(f"Followup failed after InteractionResponded error: {followup_e}", exc_info=True)
        except discord.Forbidden as e:
             log.error(f"Missing permissions to send interaction response: {e}")
        except Exception as e:
             log.error(f"Unexpected error sending interaction response: {e}", exc_info=True)


    # --- Single Sound Playback (For Commands like /playsound, /playpublic, /tts) ---

    async def play_single_sound(self, interaction: discord.Interaction, sound_path: Optional[str] = None, audio_source: Optional[discord.PCMAudio] = None, audio_buffer_to_close: Optional[io.BytesIO] = None, display_name: Optional[str] = None):
        """
        Connects (if needed), plays a single sound (either from path or pre-processed source),
        and handles cleanup. Edits the original interaction response.
        """
        # Use the internal helper to respond - it handles followup logic
        # responder = interaction.followup if interaction.response.is_done() else interaction.edit_original_response

        user = interaction.user
        guild = interaction.guild

        if not guild or not isinstance(user, discord.Member) or not user.voice or not user.voice.channel:
            await self._try_respond(interaction, "You need to be in a voice channel in this server to use this.", ephemeral=True)
            if audio_buffer_to_close and not audio_buffer_to_close.closed: audio_buffer_to_close.close()
            return

        target_channel = user.voice.channel
        guild_id = guild.id
        action_type = "TTS PLAY" if (audio_source and not sound_path) else "SINGLE PLAY (File)"
        log_prefix = f"{action_type} GID:{guild_id}:"

        # Validate inputs
        if sound_path and audio_source:
             log.error(f"{log_prefix} Cannot call play_single_sound with both sound_path and audio_source.")
             if audio_buffer_to_close and not audio_buffer_to_close.closed: audio_buffer_to_close.close()
             await self._try_respond(interaction, "❌ Internal bot error: Invalid playback request.", ephemeral=True)
             return
        if not sound_path and not audio_source:
             log.error(f"{log_prefix} Cannot call play_single_sound without sound_path or audio_source.")
             await self._try_respond(interaction, "❌ Internal bot error: No sound provided for playback.", ephemeral=True)
             return
        if audio_source and not audio_buffer_to_close:
             log.error(f"{log_prefix} audio_source provided without audio_buffer_to_close.")
             await self._try_respond(interaction, "❌ Internal bot error: Audio buffer missing.", ephemeral=True)
             return

        # --- Get Ready ---
        voice_client = await self.ensure_voice_client(interaction, target_channel, action_type=action_type)
        if not voice_client:
             if audio_buffer_to_close and not audio_buffer_to_close.closed: audio_buffer_to_close.close()
             return

        # --- Process File if Path Provided ---
        final_audio_source = audio_source
        final_buffer_to_close = audio_buffer_to_close
        sound_display_name = display_name or "Sound" # Default display name

        if sound_path:
            if not os.path.exists(sound_path):
                await self._try_respond(interaction, f"❌ Error: Sound file not found: `{os.path.basename(sound_path)}`", ephemeral=True)
                log.error(f"{log_prefix} File not found: {sound_path}")
                return
            sound_display_name = os.path.splitext(os.path.basename(sound_path))[0]
            log.info(f"{log_prefix} Processing '{sound_display_name}' for {user.name}...")
            processed_source, processed_buffer = self.process_audio(sound_path, user.display_name)
            if not processed_source:
                await self._try_respond(interaction, f"❌ Error: Could not process audio file `{sound_display_name}`. It might be corrupted or unsupported.", ephemeral=True)
                log.error(f"{log_prefix} Failed to process file '{sound_path}'.")
                if processed_buffer and not processed_buffer.closed: processed_buffer.close()
                if voice_client.is_connected() and not voice_client.is_playing():
                    # *** FIX: Use bot.loop.create_task here ***
                    self.bot.loop.create_task(self.start_leave_timer(voice_client))
                return
            final_audio_source = processed_source
            final_buffer_to_close = processed_buffer

        # --- Play ---
        if not final_audio_source:
             await self._try_respond(interaction, "❌ Error preparing audio source.", ephemeral=True)
             log.error(f"{log_prefix} final_audio_source is None before playback.")
             if final_buffer_to_close and not final_buffer_to_close.closed: final_buffer_to_close.close()
             if voice_client.is_connected() and not voice_client.is_playing():
                  # *** FIX: Use bot.loop.create_task here ***
                  self.bot.loop.create_task(self.start_leave_timer(voice_client))
             return

        if voice_client.is_playing():
            log.warning(f"{log_prefix} VC became busy between check and play for '{sound_display_name}'. Aborting.")
            await self._try_respond(interaction, "⏳ Bot became busy just now. Please try again.", ephemeral=True)
            if final_buffer_to_close and not final_buffer_to_close.closed: final_buffer_to_close.close()
            return

        try:
            self.cancel_leave_timer(guild_id, reason=f"starting {action_type}")
            log.info(f"{log_prefix} Playing '{sound_display_name}' requested by {user.display_name}...")

            # Use lambda for 'after' with threadsafe wrapper
            after_callback = lambda e: self.bot.loop.call_soon_threadsafe(
                self.after_play_cleanup_threadsafe, # Use threadsafe wrapper
                e,
                voice_client.guild.id,
                None, # No specific task name for single plays
                None, # Single plays are not temporary files handled here
                final_buffer_to_close # Pass the buffer for cleanup
            )

            voice_client.play(final_audio_source, after=after_callback)

            # Send confirmation message using the helper
            duration_sec = self.config.MAX_PLAYBACK_DURATION_MS / 1000
            play_msg = f"▶️ Playing `{sound_display_name}` (max {duration_sec}s)..."
            if action_type == "TTS PLAY":
                 # Use the more descriptive display_name passed in for TTS
                 play_msg = f"🗣️ Playing {display_name} (max {duration_sec}s)..."
            # Send response (will be followup if deferred, initial if not)
            await self._try_respond(interaction, play_msg, ephemeral=False) # Make playback message public? Or keep ephemeral? Let's try public.

        except discord.errors.ClientException as e:
            await self._try_respond(interaction, "❌ Error: Bot is already playing or encountered a client issue.", ephemeral=True)
            log.error(f"{log_prefix} ClientException during play call: {e}", exc_info=True)
            # Manually trigger cleanup (no need for threadsafe wrapper here)
            self.after_play_cleanup(e, voice_client, None, None, final_buffer_to_close)
        except Exception as e:
            await self._try_respond(interaction, "❌ An unexpected error occurred during playback.", ephemeral=True)
            log.error(f"{log_prefix} Unexpected error during play call: {e}", exc_info=True)
            # Manually trigger cleanup
            self.after_play_cleanup(e, voice_client, None, None, final_buffer_to_close)


    # --- Idle/Leave Timer Logic ---

    def should_bot_stay(self, guild_id: int) -> bool:
        """Checks the guild setting for whether the bot should stay in channel when idle."""
        settings = self.bot.guild_settings.get(str(guild_id), {})
        stay = settings.get("stay_in_channel", False) # Default to False (don't stay)
        log.debug(f"Checked stay setting for guild {guild_id}: {stay}")
        return stay is True

    def is_bot_alone(self, vc: Optional[discord.VoiceClient]) -> bool:
        """Checks if the bot is the only member (human or bot) in its voice channel."""
        if not vc or not vc.channel:
            return False # Cannot determine if not in a channel
        member_count = len(vc.channel.members)
        is_alone = member_count <= 1
        member_names = [m.name for m in vc.channel.members]
        log.debug(f"ALONE CHECK (Guild: {vc.guild.id}, Chan: {vc.channel.name}): {member_count} total members ({member_names}). Alone: {is_alone}")
        return is_alone

    def cancel_leave_timer(self, guild_id: int, reason: str = "unknown"):
        """Cancels the automatic leave timer for a guild if it exists."""
        if guild_id in self.guild_leave_timers:
            timer_task = self.guild_leave_timers.pop(guild_id, None)
            if timer_task and not timer_task.done():
                try:
                    timer_task.cancel()
                    log.info(f"LEAVE TIMER: Cancelled for Guild {guild_id}. Reason: {reason}")
                except Exception as e:
                    log.warning(f"LEAVE TIMER: Error cancelling timer for Guild {guild_id}: {e}")
            elif timer_task:
                 log.debug(f"LEAVE TIMER: Attempted to cancel completed timer for Guild {guild_id}.")


    async def start_leave_timer(self, vc: discord.VoiceClient):
        """Starts the automatic leave timer if conditions are met (bot alone, stay disabled, idle)."""
        # Ensure this is called from async context (which it should be now)
        if not vc or not vc.is_connected() or not vc.guild:
            if vc: log.warning(f"start_leave_timer called with invalid VC state for guild {vc.guild.id if vc.guild else 'Unknown'}")
            return

        guild_id = vc.guild.id
        log_prefix = f"LEAVE TIMER (Guild {guild_id}):"

        # Always cancel existing timer before starting checks for a new one
        self.cancel_leave_timer(guild_id, reason="starting new timer check")

        # --- Check Conditions ---
        if self.should_bot_stay(guild_id):
            log.debug(f"{log_prefix} Not starting timer - 'stay' setting is enabled.")
            return
        if not self.is_bot_alone(vc):
             log.debug(f"{log_prefix} Not starting timer - bot is not alone.")
             return
        if vc.is_playing():
            log.debug(f"{log_prefix} Not starting timer - bot is currently playing.")
            return

        # --- Start Timer ---
        timeout = self.config.AUTO_LEAVE_TIMEOUT_SECONDS
        log.info(f"{log_prefix} Conditions met (alone, stay disabled, idle). Starting {timeout}s timer.")

        # Create the timer task using bot's loop
        timer_task = self.bot.loop.create_task(
            self._leave_after_delay(vc.guild.id, vc.channel.id, timeout), # Pass IDs instead of refs
            name=f"AutoLeave_{guild_id}"
        )
        self.guild_leave_timers[guild_id] = timer_task

    async def _leave_after_delay(self, g_id: int, initial_channel_id: int, delay: float):
        """Coroutine that waits and then checks conditions again before leaving."""
        log_prefix = f"LEAVE TIMER (Guild {g_id}):"

        try:
            await asyncio.sleep(delay)

            # --- Re-check conditions after delay ---
            log.debug(f"{log_prefix} Timer expired. Re-checking conditions...")
            current_vc = discord.utils.get(self.bot.voice_clients, guild__id=g_id)
            guild = self.bot.get_guild(g_id) # Get guild object
            if not guild:
                log.warning(f"{log_prefix} Guild {g_id} not found after delay. Aborting leave.")
                return

            original_channel = guild.get_channel(initial_channel_id) # Get channel object

            # Check if bot disconnected or moved during the wait
            if not current_vc or not current_vc.is_connected() or current_vc.channel.id != initial_channel_id:
                log.info(f"{log_prefix} Bot disconnected/moved from '{original_channel.name if original_channel else initial_channel_id}' during wait. Aborting leave.")
                return # Timer resolves, no action needed

            # Check conditions again
            if self.should_bot_stay(g_id):
                log.info(f"{log_prefix} 'Stay' enabled during wait. Aborting leave.")
                return
            if not self.is_bot_alone(current_vc):
                log.info(f"{log_prefix} Bot no longer alone in {current_vc.channel.name}. Aborting leave.")
                return
            if current_vc.is_playing():
                 log.info(f"{log_prefix} Bot started playing again during wait. Aborting leave.")
                 return

            # --- Conditions still met: Disconnect ---
            log.info(f"{log_prefix} Conditions still met in {current_vc.channel.name}. Triggering automatic disconnect.")
            await self.safe_disconnect(current_vc, manual_leave=False, reason="timer expired")

        except asyncio.CancelledError:
            log.info(f"{log_prefix} Timer explicitly cancelled.")
        except Exception as e:
            log.error(f"{log_prefix} Error during leave timer delay/check: {e}", exc_info=True)
        finally:
            # Clean up the timer task reference from the dictionary regardless of how it ended
            # Check by name or reference if possible, be careful with task objects
            if g_id in self.guild_leave_timers and self.guild_leave_timers[g_id] is asyncio.current_task():
                 del self.guild_leave_timers[g_id]
                 log.debug(f"{log_prefix} Cleaned up timer task reference from manager.")


    async def safe_disconnect(self, vc: Optional[discord.VoiceClient], *, manual_leave: bool = False, reason: str = "disconnect"):
        """Handles disconnecting the bot, considering stay settings and cleaning up tasks/timers."""
        if not vc or not vc.is_connected() or not vc.guild:
            log.debug(f"safe_disconnect called but VC is already disconnected or invalid (Guild: {vc.guild.id if vc else 'N/A'}).")
            return

        guild = vc.guild
        guild_id = guild.id
        log_prefix = f"DISCONNECT GID:{guild_id}:"

        # Always cancel any pending leave timer when disconnect is initiated
        self.cancel_leave_timer(guild_id, reason=f"safe_disconnect triggered ({reason})")

        # Check if we should actually disconnect
        if not manual_leave and self.should_bot_stay(guild_id):
            log.debug(f"{log_prefix} Disconnect skipped: 'Stay in channel' is enabled and leave is not manual.")
            # Even if staying, cleanup potentially completed/stuck play tasks if idle
            is_playing_check = vc.is_playing()
            is_queue_empty_check = guild_id not in self.guild_sound_queues or not self.guild_sound_queues[guild_id]
            if not is_playing_check and is_queue_empty_check:
                 if guild_id in self.guild_play_tasks:
                     play_task = self.guild_play_tasks.pop(guild_id, None)
                     if play_task:
                         if not play_task.done():
                             try: play_task.cancel()
                             except Exception: pass
                             log.debug(f"{log_prefix} STAY MODE: Cancelled lingering play task for idle bot.")
                         else:
                              log.debug(f"{log_prefix} STAY MODE: Cleaned up completed play task tracker for idle bot.")
            return # Do not disconnect

        # --- Proceed with Disconnect ---
        disconnect_reason_log = "Manual /leave" if manual_leave else f"Auto ({reason})"
        log.info(f"{log_prefix} Disconnecting from {guild.name} (Reason: {disconnect_reason_log}).")

        try:
            # Stop playback before disconnecting if necessary
            if vc.is_playing():
                log_level = logging.WARNING if not manual_leave else logging.DEBUG
                log.log(log_level, f"{log_prefix} Calling vc.stop() before disconnecting (Manual: {manual_leave}).")
                vc.stop() # Stop current playback

            # Disconnect from the voice channel
            await vc.disconnect(force=False) # Use force=False for graceful disconnect
            log.info(f"{log_prefix} Bot disconnected from '{guild.name}'.")

            # --- Post-Disconnect Cleanup (already handled by on_voice_state_update event generally) ---
            # Redundant cleanup here just in case event is missed/delayed
            if guild_id in self.guild_play_tasks:
                play_task = self.guild_play_tasks.pop(guild_id, None)
                if play_task and not play_task.done():
                    try: play_task.cancel()
                    except Exception: pass
                    log.debug(f"{log_prefix} Cancelled active play task after disconnect (redundant check).")
            if guild_id in self.guild_sound_queues:
                self.guild_sound_queues[guild_id].clear()
                log.debug(f"{log_prefix} Cleared sound queue after disconnect (redundant check).")

        except Exception as e:
            log.error(f"{log_prefix} Error during disconnect from {guild.name}: {e}", exc_info=True)
            # Attempt to clean up anyway?
            if guild_id in self.guild_play_tasks: self.guild_play_tasks.pop(guild_id, None)
            if guild_id in self.guild_sound_queues: self.guild_sound_queues.pop(guild_id, None)