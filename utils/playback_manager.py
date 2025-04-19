# -*- coding: utf-8 -*-
import discord
import asyncio
import logging
import io
from collections import deque
from typing import Dict, Any, Optional, Tuple

from utils import audio_processor, voice_helpers # Import helpers
import config # For paths etc.

log = logging.getLogger('SoundBot.PlaybackManager')

class PlaybackManager:
    def __init__(self, bot: discord.Bot):
        self.bot = bot
        # State managed by this class
        self.guild_sound_queues: Dict[int, deque[Tuple[discord.Member, str, bool]]] = {} # (member, sound_path, is_temp_tts)
        self.guild_play_tasks: Dict[int, asyncio.Task[Any]] = {}
        self.active_audio_buffers: Dict[int, io.BytesIO] = {} # Store buffer being played per guild

    def is_queue_empty(self, guild_id: int) -> bool:
        """Checks if the sound queue for a guild is empty."""
        return guild_id not in self.guild_sound_queues or not self.guild_sound_queues[guild_id]

    def queue_sound(self, member: discord.Member, sound_path: str, is_temp_tts: bool = False):
        """Adds a sound (file path or temp TTS path) to the guild's queue."""
        guild_id = member.guild.id
        if guild_id not in self.guild_sound_queues:
            self.guild_sound_queues[guild_id] = deque()

        self.guild_sound_queues[guild_id].append((member, sound_path, is_temp_tts))
        log.info(f"QUEUE: Added sound for {member.display_name} (TempTTS: {is_temp_tts}). Guild {guild_id} Queue size: {len(self.guild_sound_queues[guild_id])}")

    def ensure_playback_task(self, guild: discord.Guild):
        """Ensures a playback task is running for the guild if the queue is not empty and bot is idle."""
        guild_id = guild.id
        vc = discord.utils.get(self.bot.voice_clients, guild=guild)

        if not vc or not vc.is_connected():
            log.warning(f"EnsurePlaybackTask: Bot not connected in {guild.name}, cannot start task.")
            self.cleanup_guild_state(guild_id, "bot not connected")
            return

        if not self.is_queue_empty(guild_id) and not vc.is_playing():
            if guild_id not in self.guild_play_tasks or self.guild_play_tasks[guild_id].done():
                task_name = f"QueueStart_{guild_id}"
                log.info(f"PLAYBACK: Starting task '{task_name}' for guild {guild_id}.")
                voice_helpers.cancel_leave_timer(self.bot, guild_id, reason="starting playback task")
                self.guild_play_tasks[guild_id] = self.bot.loop.create_task(self._play_next_in_queue(guild), name=task_name)
            else:
                log.debug(f"PLAYBACK: Task for {guild_id} already running/scheduled.")
        elif self.is_queue_empty(guild_id) and not vc.is_playing():
             log.debug(f"PLAYBACK: Queue empty and bot idle for {guild_id}. Triggering leave timer check.")
             self.bot.loop.create_task(voice_helpers.start_leave_timer(self.bot, vc))


    async def _play_next_in_queue(self, guild: discord.Guild):
        """Internal method to process and play the next sound from the queue."""
        guild_id = guild.id
        task_id_obj = asyncio.current_task()
        task_id = task_id_obj.get_name() if task_id_obj else 'Unknown'
        log.debug(f"QUEUE CHECK [{task_id}]: Guild {guild_id}")

        if task_id_obj and task_id_obj.cancelled():
            log.debug(f"QUEUE CHECK [{task_id}]: Task cancelled externally for guild {guild_id}.")
            self.cleanup_play_task(guild_id, reason="task cancelled externally", task_ref=task_id_obj)
            return

        if self.is_queue_empty(guild_id):
            log.debug(f"QUEUE [{task_id}]: Empty for {guild_id}. Playback task ending.")
            self.cleanup_play_task(guild_id, reason="queue empty", task_ref=task_id_obj)
            vc_check = discord.utils.get(self.bot.voice_clients, guild=guild)
            if vc_check and vc_check.is_connected() and not vc_check.is_playing():
                 self.bot.loop.create_task(voice_helpers.start_leave_timer(self.bot, vc_check))
            return

        vc = discord.utils.get(self.bot.voice_clients, guild=guild)
        if not vc or not vc.is_connected():
            log.warning(f"QUEUE [{task_id}]: Task running for {guild_id}, but bot not connected. Cleaning up.")
            self.cleanup_guild_state(guild_id, "bot not connected during queue check")
            return

        if vc.is_playing():
            log.debug(f"QUEUE [{task_id}]: Bot already playing in {guild_id}, yielding.")
            # Ensure task persists if playing something else
            if guild_id not in self.guild_play_tasks or self.guild_play_tasks[guild_id].done():
                 log.warning(f"QUEUE [{task_id}]: Bot playing but no active task tracker for {guild_id}. Re-assigning.")
                 self.guild_play_tasks[guild_id] = task_id_obj # Re-assign current task
            return

        # Get next item
        try:
            member, sound_path, is_temp_tts = self.guild_sound_queues[guild_id].popleft()
            log.info(f"QUEUE [{task_id}]: Processing {member.display_name} in {guild.name}. Path: {os.path.basename(sound_path)}. TempTTS: {is_temp_tts}. Queue Left: {len(self.guild_sound_queues[guild_id])}")
        except IndexError:
            log.debug(f"QUEUE [{task_id}]: Became empty unexpectedly for {guild_id} during pop. Ending task.")
            self.cleanup_play_task(guild_id, reason="queue empty on pop", task_ref=task_id_obj)
            if vc and vc.is_connected() and not vc.is_playing(): self.bot.loop.create_task(voice_helpers.start_leave_timer(self.bot, vc))
            return

        # Process audio
        audio_source, audio_buffer = audio_processor.process_audio(sound_path, member.display_name)

        if audio_source and audio_buffer:
            try:
                # Store buffer reference BEFORE playing
                self.active_audio_buffers[guild_id] = audio_buffer

                voice_helpers.cancel_leave_timer(self.bot, guild_id, reason="starting playback")
                log.info(f"QUEUE PLAYBACK [{task_id}]: Playing for {member.display_name}...")

                vc.play(
                    audio_source,
                    after=lambda e: self.bot.loop.create_task(
                        self._after_play_handler(e, guild_id, sound_path, is_temp_tts)
                    )
                )
                log.debug(f"QUEUE PLAYBACK [{task_id}]: vc.play() called for {member.display_name}.")
            except (discord.errors.ClientException, Exception) as e:
                log.error(f"QUEUE PLAYBACK ERROR [{task_id}] while calling vc.play(): {type(e).__name__}: {e}", exc_info=True)
                # Manually trigger cleanup if play fails immediately
                await self._after_play_handler(e, guild_id, sound_path, is_temp_tts)
        else:
            # Audio processing failed
            log.warning(f"QUEUE PLAYBACK [{task_id}]: No valid audio source for {member.display_name} ({os.path.basename(sound_path)}). Skipping.")
            # Clean up buffer if it exists even if source is None
            if audio_buffer and not audio_buffer.closed:
                 try: audio_buffer.close(); log.debug("Cleaned up buffer after failed processing.")
                 except Exception: pass
            # Manually attempt file cleanup for FAILED temporary TTS
            if is_temp_tts and os.path.exists(sound_path):
                try:
                    os.remove(sound_path)
                    log.info(f"CLEANUP: Deleted FAILED temporary TTS file: {sound_path}")
                except OSError as e_del:
                    log.warning(f"CLEANUP: Failed to delete failed TTS file '{sound_path}': {e_del}")
            # Schedule the next check immediately
            self.ensure_playback_task(guild)


    async def _after_play_handler(self, error: Optional[Exception], guild_id: int, played_path: Optional[str], was_temp_tts: bool):
        """Async handler called after playback finishes or errors."""
        log_prefix_cleanup = f"AFTER_PLAY_HANDLER (Guild {guild_id}):"
        log.debug(f"{log_prefix_cleanup} Initiated. Error: {error}")

        # --- Close Audio Buffer ---
        buffer_to_close = self.active_audio_buffers.pop(guild_id, None)
        if buffer_to_close:
            try:
                if not buffer_to_close.closed:
                    buffer_to_close.close()
                    log.debug(f"{log_prefix_cleanup} Closed audio buffer for '{os.path.basename(played_path or 'sound')}'.")
            except Exception as buf_e:
                log.warning(f"{log_prefix_cleanup} Error closing audio buffer: {buf_e}")
        else:
             log.debug(f"{log_prefix_cleanup} No active audio buffer found to close for this guild.")


        # --- Delete Temporary TTS File ---
        if was_temp_tts and played_path:
            log.debug(f"{log_prefix_cleanup} Attempting cleanup for temp TTS file: {played_path}")
            if os.path.exists(played_path):
                try:
                    os.remove(played_path)
                    log.info(f"{log_prefix_cleanup} Deleted temporary TTS file: {played_path}")
                except OSError as e_del:
                    log.warning(f"{log_prefix_cleanup} Failed to delete temporary TTS file '{played_path}': {e_del}")
            else:
                log.debug(f"{log_prefix_cleanup} Temp TTS file '{played_path}' not found for deletion.")

        # --- Log Error ---
        if error:
            log.error(f'{log_prefix_cleanup} Playback Error: {error}', exc_info=error)

        # --- Check Queue and Trigger Next Action ---
        vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
        if not vc or not vc.is_connected():
            log.warning(f"{log_prefix_cleanup} VC disconnected. Cleaning up state.")
            self.cleanup_guild_state(guild_id, "VC disconnected in after_play")
            return

        # Ensure the next playback task runs if queue is not empty
        self.ensure_playback_task(vc.guild)


    def cleanup_play_task(self, guild_id: int, reason: str = "unknown", task_ref: Optional[asyncio.Task] = None):
        """Safely removes the play task tracker for a guild."""
        current_task = self.guild_play_tasks.pop(guild_id, None)
        if current_task:
            # If a specific task reference was passed (e.g., from the task itself),
            # only log cleanup if the popped task matches the reference.
            # This prevents double logging if cleanup happens concurrently.
            if task_ref is None or current_task is task_ref:
                 log.debug(f"Cleaned up play task tracker for guild {guild_id}. Reason: {reason}")
            # Optionally cancel if the task is still running (shouldn't usually happen here)
            # if not current_task.done():
            #    try: current_task.cancel()
            #    except Exception: pass


    def cleanup_guild_state(self, guild_id: int, reason: str = "unknown"):
        """Cleans up all playback-related state for a guild."""
        log.debug(f"Cleaning up all playback state for guild {guild_id}. Reason: {reason}")
        # Cancel and remove play task
        play_task = self.guild_play_tasks.pop(guild_id, None)
        if play_task and not play_task.done():
            try: play_task.cancel()
            except Exception: pass
            log.debug(f"Cancelled play task during full cleanup for guild {guild_id}.")

        # Clear queue
        if guild_id in self.guild_sound_queues:
            self.guild_sound_queues[guild_id].clear()
            log.debug(f"Cleared sound queue for guild {guild_id}.")

        # Close and remove active buffer
        buffer = self.active_audio_buffers.pop(guild_id, None)
        if buffer and not buffer.closed:
            try: buffer.close()
            except Exception: pass
            log.debug(f"Closed active audio buffer during full cleanup for guild {guild_id}.")

        # Cancel leave timer (handled by voice_helpers, but call here for safety)
        voice_helpers.cancel_leave_timer(self.bot, guild_id, reason=f"full cleanup ({reason})")


    async def play_sound_now(self, interaction: discord.Interaction, sound_path: str, is_temp_tts: bool = False):
        """
        Connects (if needed), processes, plays a single sound immediately,
        bypassing the join queue. Manages its own buffer and cleanup.
        Sends feedback via the interaction.
        """
        responder = interaction.followup if interaction.response.is_done() else interaction.edit_original_response
        user = interaction.user
        guild = interaction.guild

        if not guild or not isinstance(user, discord.Member) or not user.voice or not user.voice.channel:
            try: await responder(content="You need to be in a voice channel to play sounds.")
            except discord.NotFound: pass
            except Exception as e: log.warning(f"Error responding in play_sound_now (no VC): {e}")
            return

        target_channel = user.voice.channel
        guild_id = guild.id

        if not os.path.exists(sound_path):
            try: await responder(content="❌ Error: The sound file seems to be missing.")
            except discord.NotFound: pass
            except Exception as e: log.warning(f"Error responding in play_sound_now (file missing): {e}")
            log.error(f"PLAY_NOW: File not found: {sound_path}")
            return

        # Ensure bot is ready (connects/moves, checks perms, checks busy)
        vc = await voice_helpers.ensure_voice_client_ready(interaction, target_channel, action_type="PLAY_NOW")
        if not vc:
            # ensure_voice_client_ready already sent feedback
            return

        # --- Process Audio ---
        sound_basename = os.path.basename(sound_path)
        log.info(f"PLAY_NOW: Processing '{sound_basename}' for {user.name}...")
        audio_source, audio_buffer = audio_processor.process_audio(sound_path, user.display_name)

        if not audio_source or not audio_buffer:
            try: await responder(content="❌ Error: Could not process the audio file.")
            except discord.NotFound: pass
            except Exception as e: log.warning(f"Error responding in play_sound_now (processing failed): {e}")
            log.error(f"PLAY_NOW: Failed to get audio source for '{sound_path}' requested by {user.name}")
            if audio_buffer and not audio_buffer.closed:
                 try: audio_buffer.close()
                 except Exception: pass
            # Start leave timer check if bot is now idle
            if vc.is_connected() and not vc.is_playing():
                self.bot.loop.create_task(voice_helpers.start_leave_timer(self.bot, vc))
            return

        # --- Play Audio ---
        try:
            # Store buffer reference BEFORE playing
            # Note: This might conflict if a queue task runs concurrently.
            # Consider a separate dict for immediate plays or locking. For now, overwrite.
            if guild_id in self.active_audio_buffers:
                 old_buffer = self.active_audio_buffers.pop(guild_id, None)
                 if old_buffer and not old_buffer.closed:
                     log.warning(f"PLAY_NOW: Overwriting existing active buffer for guild {guild_id}.")
                     try: old_buffer.close()
                     except Exception: pass
            self.active_audio_buffers[guild_id] = audio_buffer

            voice_helpers.cancel_leave_timer(self.bot, guild_id, reason="starting immediate sound")
            sound_display_name = os.path.splitext(sound_basename)[0]
            log.info(f"PLAY_NOW: Playing '{sound_display_name}' requested by {user.display_name}...")

            # Use the standard after_play_handler, passing necessary info
            vc.play(
                audio_source,
                after=lambda e: self.bot.loop.create_task(
                    self._after_play_handler(e, guild_id, sound_path, is_temp_tts)
                )
            )

            # Edit the original deferred response
            play_msg = f"▶️ Playing `{sound_display_name}`"
            if is_temp_tts:
                 # Extract original text if possible (e.g., from interaction options if TTS command)
                 # This is tricky here, maybe just a generic TTS message
                 play_msg = f"🗣️ Playing TTS..." # Generic for TTS via play_sound_now
            play_msg += f" (max {config.MAX_PLAYBACK_DURATION_MS / 1000}s)..."

            try: await responder(content=play_msg)
            except discord.NotFound: pass
            except Exception as e: log.warning(f"Error responding in play_sound_now (playing msg): {e}")

        except discord.errors.ClientException as e:
            try: await responder(content="❌ Error: Bot is already playing or encountered a client issue.")
            except discord.NotFound: pass
            except Exception as resp_e: log.warning(f"Error responding in play_sound_now (client exc): {resp_e}")
            log.error(f"PLAY_NOW ERROR (ClientException): {e}", exc_info=True)
            # Manually trigger cleanup if play fails
            await self._after_play_handler(e, guild_id, sound_path, is_temp_tts)
        except Exception as e:
            try: await responder(content="❌ An unexpected error occurred during playback.")
            except discord.NotFound: pass
            except Exception as resp_e: log.warning(f"Error responding in play_sound_now (unexpected exc): {resp_e}")
            log.error(f"PLAY_NOW ERROR (Unexpected): {e}", exc_info=True)
            # Manually trigger cleanup
            await self._after_play_handler(e, guild_id, sound_path, is_temp_tts)

