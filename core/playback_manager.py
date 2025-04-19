# core/playback_manager.py
# -*- coding: utf-8 -*-
import discord
from discord.ext import commands
import asyncio
import os
import io
import math
import logging
from collections import deque
from typing import Dict, Any, Optional, Tuple, List, Union
import datetime
import shutil

# Check required libraries
try:
    from pydub import AudioSegment
    from pydub.exceptions import CouldntDecodeError
    PYDUB_AVAILABLE = True
except ImportError:
    PYDUB_AVAILABLE = False

import config

log = logging.getLogger('SoundBot.PlaybackManager')

# --- Type Hints ---
MusicQueueItem = Dict[str, Any]
JoinQueueItem = Dict[str, Any]
TTSQueueItem = Dict[str, Any]
QueueItem = Union[MusicQueueItem, JoinQueueItem, TTSQueueItem]


class PlaybackManager:
    """Handles unified audio queue, playback, VC connection/state management."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config = bot.config
        self.guild_queues: Dict[int, deque[QueueItem]] = {}
        self.guild_play_tasks: Dict[int, asyncio.Task[Any]] = {}
        self.guild_leave_timers: Dict[int, asyncio.Task[Any]] = {}
        self.currently_playing: Dict[int, Optional[QueueItem]] = {}

    def process_audio(self, sound_path: str, display_name: str = "Audio", *, apply_duration_limit: bool = True) -> Tuple[Optional[discord.PCMAudio], Optional[io.BytesIO]]:
        """ Loads, TRIMS, normalizes, and prepares audio for Discord playback. """
        if not PYDUB_AVAILABLE: log.error("AUDIO: Pydub unavailable."); return None, None
        if not os.path.exists(sound_path): log.error(f"AUDIO: File not found: {sound_path}"); return None, None

        audio_source: Optional[discord.PCMAudio] = None
        pcm_data_io: Optional[io.BytesIO] = None
        basename = os.path.basename(sound_path)
        try:
            log.debug(f"AUDIO: Loading '{basename}' (Display Hint: {display_name})...")
            ext = os.path.splitext(sound_path)[1].lower().strip('.')
            format_hint = ext if ext else 'mp3'
            try: audio_segment = AudioSegment.from_file(sound_path, format=format_hint)
            except CouldntDecodeError: log.warning(f"AUDIO: Decode failed with format '{format_hint}', retrying auto."); audio_segment = AudioSegment.from_file(sound_path)
            except Exception as load_e: log.error(f"AUDIO: Load failed: {load_e}", exc_info=True); raise load_e
            log.debug(f"AUDIO: Loaded '{basename}' ({len(audio_segment)}ms)")
            # Trimming
            if apply_duration_limit: # Check the new parameter
                if len(audio_segment) > self.config.MAX_PLAYBACK_DURATION_MS:
                    log.info(f"AUDIO (Limit Enabled): Trimming '{basename}' to {self.config.MAX_PLAYBACK_DURATION_MS}ms.")
                    audio_segment = audio_segment[:self.config.MAX_PLAYBACK_DURATION_MS]
                else:
                    log.debug(f"AUDIO (Limit Enabled): Duration within limit for '{basename}'.")
            else:
                log.info(f"AUDIO (Limit Disabled): Skipping duration trim for '{basename}' ({len(audio_segment)}ms).")
            # Normalization
            apply_normalization = True
            if apply_normalization:
                peak_dbfs = audio_segment.max_dBFS
                if not math.isinf(peak_dbfs) and peak_dbfs > -90.0:
                    target_dbfs = self.config.TARGET_LOUDNESS_DBFS; change_in_dbfs = target_dbfs - peak_dbfs
                    gain_limit = 6.0; apply_gain = min(change_in_dbfs, gain_limit) if change_in_dbfs > 0 else change_in_dbfs
                    if apply_gain != change_in_dbfs: log.info(f"AUDIO: Gain limited to +{gain_limit}dB.")
                    log.info(f"AUDIO: Normalizing '{basename}'. Peak:{peak_dbfs:.2f} Target:{target_dbfs:.2f} ApplyGain:{apply_gain:.2f} dB.")
                    audio_segment = audio_segment.apply_gain(apply_gain)
                elif math.isinf(peak_dbfs): log.warning(f"AUDIO: Cannot normalize silent audio '{basename}'.")
                else: log.warning(f"AUDIO: Skipping normalization for quiet audio '{basename}'. Peak: {peak_dbfs:.2f}")
            else: log.debug(f"AUDIO: Normalization skipped.")
            # Format Conversion
            log.debug(f"AUDIO: Converting '{basename}' to PCM S16LE (48kHz, Stereo)...")
            audio_segment = audio_segment.set_frame_rate(48000).set_channels(2)
            pcm_data_io = io.BytesIO(); audio_segment.export(pcm_data_io, format="s16le"); pcm_data_io.seek(0)
            if pcm_data_io.getbuffer().nbytes > 0: audio_source = discord.PCMAudio(pcm_data_io); log.debug(f"AUDIO: Processed '{basename}' ({pcm_data_io.getbuffer().nbytes} bytes)."); return audio_source, pcm_data_io
            else: log.error(f"AUDIO: Exported PCM empty!"); pcm_data_io.close(); return None, None
        except CouldntDecodeError as decode_err: log.error(f"AUDIO: Pydub Decode Error: {decode_err}", exc_info=True)
        except FileNotFoundError: log.error(f"AUDIO: File not found processing: '{sound_path}'")
        except Exception as e: log.error(f"AUDIO: Unexpected processing error: {e}", exc_info=True)
        finally: # Ensure buffer closed if processing failed
            if audio_source is None and pcm_data_io and not pcm_data_io.closed:
                try: pcm_data_io.close()
                except Exception: pass
        return None, None


    # --- Unified Queue Logic ---
    async def add_to_queue(self, guild_id: int, item: QueueItem):
        """Adds an item to the guild's queue and ensures playback starts if idle."""
        if guild_id not in self.guild_queues: self.guild_queues[guild_id] = deque()
        self.guild_queues[guild_id].append(item)
        item_type = item.get('type', 'unknown'); item_name = item.get('title', os.path.basename(item.get('path', 'N/A')))
        log.info(f"QUEUE GID:{guild_id}: Added '{item_type}' ('{item_name}'). Queue size: {len(self.guild_queues[guild_id])}")
        vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
        if vc and vc.is_connected() and not vc.is_playing(): await self.start_playback_if_idle(vc.guild)

    async def start_playback_if_idle(self, guild: discord.Guild):
        """Starts the play_next_item task if the bot is idle and queue has items."""
        guild_id = guild.id
        vc = discord.utils.get(self.bot.voice_clients, guild=guild)
        if vc and vc.is_connected() and not vc.is_playing():
            queue_has_items = guild_id in self.guild_queues and self.guild_queues[guild_id]
            task_is_running = guild_id in self.guild_play_tasks and not self.guild_play_tasks[guild_id].done()
            if queue_has_items and not task_is_running:
                log.info(f"PLAYBACK GID:{guild_id}: Bot idle, queue has items, no active task. Starting task.")
                task_name = f"PlayTask_{guild_id}"; new_task = self.bot.loop.create_task(self.play_next_item(guild), name=task_name); self.guild_play_tasks[guild_id] = new_task
            elif not queue_has_items: log.debug(f"PLAYBACK GID:{guild_id}: Idle, queue empty. Checking leave timer."); self.bot.loop.create_task(self.start_leave_timer(vc))
            else: log.debug(f"PLAYBACK GID:{guild_id}: Idle, queue has items, but task already exists/running.")
        elif vc and vc.is_playing(): log.debug(f"PLAYBACK GID:{guild_id}: Start check called, bot playing.")

    async def play_next_item(self, guild: discord.Guild):
        """Plays the next available item from the unified queue."""
        guild_id = guild.id; task_id_obj = asyncio.current_task(); task_id = task_id_obj.get_name() if task_id_obj else f'Task_GID_{guild_id}'; log_prefix = f"PLAY NEXT [{task_id} GID:{guild_id}]"

        # --- Pre-checks ---
        if task_id_obj and task_id_obj.cancelled():
            log.debug(f"{log_prefix}: Task cancelled.")
            if guild_id in self.guild_play_tasks and self.guild_play_tasks.get(guild_id) is task_id_obj: del self.guild_play_tasks[guild_id]
            self.currently_playing.pop(guild_id, None); return
        vc = discord.utils.get(self.bot.voice_clients, guild=guild)
        if not vc or not vc.is_connected():
            log.warning(f"{log_prefix}: Not connected.");
            if guild_id in self.guild_queues: self.guild_queues[guild_id].clear()
            if guild_id in self.guild_play_tasks and self.guild_play_tasks.get(guild_id) is task_id_obj: del self.guild_play_tasks[guild_id]
            self.currently_playing.pop(guild_id, None); self.cancel_leave_timer(guild_id, reason="bot disconnected"); return
        if vc.is_playing(): log.debug(f"{log_prefix}: Already playing."); return
        if guild_id not in self.guild_queues or not self.guild_queues[guild_id]:
            log.debug(f"{log_prefix}: Queue empty.");
            if guild_id in self.guild_play_tasks and self.guild_play_tasks.get(guild_id) is task_id_obj: del self.guild_play_tasks[guild_id]
            self.currently_playing.pop(guild_id, None); self.bot.loop.create_task(self.start_leave_timer(vc)); return

        # --- Dequeue Item ---
        item: Optional[QueueItem] = None
        try:
            item = self.guild_queues[guild_id].popleft(); self.currently_playing[guild_id] = item
            item_type = item.get('type', 'unknown'); sound_path = item.get('path'); item_name = item.get('title', os.path.basename(sound_path or 'N/A'))
            log.info(f"{log_prefix}: Dequeued '{item_type}' ('{item_name}'). Queue Left: {len(self.guild_queues[guild_id])}")
        # *** FIX: Corrected except IndexError block ***
        except IndexError:
            log.debug(f"{log_prefix}: Queue empty on pop.")
            # Clean up task tracker if this task is the one responsible
            if guild_id in self.guild_play_tasks and self.guild_play_tasks.get(guild_id) is task_id_obj:
                del self.guild_play_tasks[guild_id]
            self.currently_playing.pop(guild_id, None)
            # Check if we should leave
            self.bot.loop.create_task(self.start_leave_timer(vc))
            return # Stop this task instance
        # *** END FIX ***
        except Exception as e_dequeue:
            log.error(f"{log_prefix}: Dequeue error: {e_dequeue}", exc_info=True)
            self.currently_playing.pop(guild_id, None)
            if guild_id in self.guild_play_tasks and self.guild_play_tasks.get(guild_id) is task_id_obj: del self.guild_play_tasks[guild_id]
            self.bot.loop.create_task(self.start_leave_timer(vc)); return

        # Validate Item Path
        sound_path = item.get('path') if item else None
        if not sound_path or not os.path.exists(sound_path): log.error(f"{log_prefix}: Invalid/missing path: {sound_path}"); self._cleanup_played_item_file(item); self.currently_playing.pop(guild_id, None); self.bot.loop.create_task(self.play_next_item(guild)); return

        # Process Audio
        display_name = "Audio"; item_type = item.get('type');
        if item_type == 'join': display_name = f"Join sound for {item.get('member').display_name}"
        elif item_type == 'music': display_name = item.get('title', 'Music Track')
        elif item_type == 'tts': display_name = item.get('text_preview', 'TTS Message')
        should_limit = (item_type != 'music') # Limit unless it's music
        log.debug(f"{log_prefix}: Processing audio for '{item_name}'. Apply duration limit: {should_limit}")
        audio_source, audio_buffer_to_close = self.process_audio(
            sound_path,
            display_name,
            apply_duration_limit=should_limit # Pass the flag here
        )

        # Play Processed Audio
        if audio_source:
            try:
                self.cancel_leave_timer(guild_id, reason=f"starting playback ({item_type})"); log.info(f"{log_prefix}: Playing '{display_name}'...")
                after_callback = lambda e: self.bot.loop.call_soon_threadsafe(self.after_play_cleanup_threadsafe, e, guild_id, task_id_obj.get_name() if task_id_obj else None, item, audio_buffer_to_close)
                vc.play(audio_source, after=after_callback); log.debug(f"{log_prefix}: vc.play() called.")
            except (discord.errors.ClientException, Exception) as e: log.error(f"{log_prefix}: vc.play() error: {e}", exc_info=True); self.after_play_cleanup(e, guild_id, task_id_obj.get_name() if task_id_obj else None, item, audio_buffer_to_close) # Direct cleanup on immediate error
        else: # Processing failed
            log.warning(f"{log_prefix}: No audio source for '{os.path.basename(sound_path)}'. Skipping.");
            if audio_buffer_to_close and not audio_buffer_to_close.closed:
                try:
                    audio_buffer_to_close.close()
                except Exception:
                    pass
            self._cleanup_played_item_file(item); self.currently_playing.pop(guild_id, None); self.bot.loop.create_task(self.play_next_item(guild))

    # --- Cleanup Logic ---
    def _cleanup_played_item_file(self, item: Optional[QueueItem]):
        """Deletes the file associated with a played item if necessary."""
        if not item: log.debug("FILE CLEANUP: None item."); return
        item_type = item.get('type'); path_to_delete = item.get('path'); should_delete = False; log_prefix = "FILE CLEANUP:"
        if item_type == 'join' and item.get('is_temp_tts', False): should_delete = True; log_prefix += f" (Temp TTS GID:{item.get('guild_id', 'N/A')}):"
        elif item_type == 'music': should_delete = True; log_prefix += f" (Music Cache GID:{item.get('guild_id', 'N/A')}):"
        # Add other types here if needed
        if should_delete and path_to_delete:
            log.debug(f"{log_prefix} Deleting: {path_to_delete}")
            if os.path.exists(path_to_delete):
                try: os.remove(path_to_delete); log.info(f"{log_prefix} Deleted: {path_to_delete}")
                except OSError as e_del: log.warning(f"{log_prefix} Failed delete OSError '{path_to_delete}': {e_del}")
                except Exception as e_del_other: log.error(f"{log_prefix} Failed delete Exception '{path_to_delete}': {e_del_other}", exc_info=True)
            else: log.debug(f"{log_prefix} File not found '{path_to_delete}'.")
        elif item_type and path_to_delete: log.debug(f"FILE CLEANUP: No deletion needed for type '{item_type}', path '{path_to_delete}'.")

   # core/playback_manager.py

    def after_play_cleanup_threadsafe(self, error: Optional[Exception], guild_id: int, task_name: Optional[str], item: Optional[QueueItem], audio_buffer: Optional[io.BytesIO]):
        """ Threadsafe cleanup called from the 'after' callback via bot loop. """
        # Determine a more descriptive log name if task_name is None (direct play)
        log_task_hint = task_name if task_name else f"DirectPlay_GID_{guild_id}"
        log_prefix = f"AFTER_PLAY_TS ({log_task_hint}):"

        item_type_log = item.get('type', 'unknown') if item else 'N/A'
        item_path_log = os.path.basename(item.get('path', 'N/A')) if item else 'N/A'
        # Log more initial info including the item details
        log.info(f"{log_prefix} Cleanup initiated. Item Type: '{item_type_log}', Path: '{item_path_log}'. Error: {error}")

        if audio_buffer:
            try:
                if not audio_buffer.closed:
                    audio_buffer.close()
                    log.debug(f"{log_prefix} Closed audio buffer for '{item_path_log}'.")
            except Exception as buf_e:
                log.warning(f"{log_prefix} Error closing buffer: {buf_e}")

        # Handle potential temporary file deletion (like TTS join sounds)
        self._cleanup_played_item_file(item)

        # --- Explicitly log state clearing ---
        current_playing_item_before_pop = self.currently_playing.get(guild_id)
        popped_item_details = "N/A"
        if current_playing_item_before_pop:
            popped_item_details = f"Type='{current_playing_item_before_pop.get('type', '?')}', Path='{os.path.basename(current_playing_item_before_pop.get('path', '?'))}'"

        log.debug(f"{log_prefix} State before pop: currently_playing[{guild_id}] = {popped_item_details}")
        log.debug(f"{log_prefix} Item passed to cleanup: Type='{item_type_log}', Path='{item_path_log}'")

        popped_item = None
        if item and current_playing_item_before_pop is item:
            # The finished item matches the one we stored - normal case
            popped_item = self.currently_playing.pop(guild_id, None)
            log.info(f"{log_prefix} Cleared 'currently_playing' state (Exact item matched). Pop successful: {popped_item is not None}")
        elif item and current_playing_item_before_pop:
            # Mismatch - maybe state got overwritten or callback is delayed? Log and clear anyway.
            log.warning(f"{log_prefix} Finished item MISMATCH! State='{popped_item_details}', Finished='{item_type_log}/{item_path_log}'. Clearing state anyway.")
            popped_item = self.currently_playing.pop(guild_id, None)
        elif item:
            # Item finished, but nothing was in our state tracker
            log.warning(f"{log_prefix} Item finished ('{item_path_log}'), but no 'currently_playing' state was recorded for guild {guild_id}.")
        else:
            log.debug(f"{log_prefix} Cleanup called with no specific item provided (error={error}). Checking/clearing state if present.")
            # If cleanup was called due to an error without an item, clear any potentially stuck state
            if self.currently_playing.get(guild_id):
                 log.warning(f"{log_prefix} Clearing potentially stale 'currently_playing' state due to error/no item.")
                 self.currently_playing.pop(guild_id, None)
        # --- End explicit logging ---

        current_vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
        # Task check needs to happen *after* potential state clearing
        current_task = self.guild_play_tasks.get(guild_id)

        if not current_vc or not current_vc.is_connected():
            log.warning(f"{log_prefix} VC disconnected during cleanup for '{item_path_log}'. Cleaning up task tracker.")
            # Check if the current task matches the one that finished (if it was a task)
            if current_task and (task_name is None or current_task.get_name() == task_name):
                if not current_task.done():
                    try: current_task.cancel()
                    except Exception: pass
                self.guild_play_tasks.pop(guild_id, None)
                log.debug(f"{log_prefix} Removed task tracker '{task_name or 'direct'}' due to VC disconnect.")
            self.cancel_leave_timer(guild_id, reason="after_play_ts on disconnected VC")
            return # Exit early if no VC

        # --- Check queue status AFTER state potentially cleared ---
        queue_is_empty = guild_id not in self.guild_queues or not self.guild_queues[guild_id]

        if not queue_is_empty:
            # Queue has items, ensure playback continues
            queue_len = len(self.guild_queues[guild_id])
            log.info(f"{log_prefix} Queue has items ({queue_len} left). Ensuring playback continues for next item.")
            # Ensure task restarts if needed (e.g., if the original task ended prematurely)
            if not current_task or current_task.done():
                log.info(f"{log_prefix} Playback task '{task_name or 'direct'}' was done or missing. Starting new task for queue.")
                next_task_name = f"PlayTask_Restart_{guild_id}"
                # Ensure the new task is stored correctly
                self.guild_play_tasks[guild_id] = self.bot.loop.create_task(self.play_next_item(current_vc.guild), name=next_task_name)
            else:
                # Task still running, it should loop and pick up the next item
                log.debug(f"{log_prefix} Task '{current_task.get_name()}' still exists and not done. Letting it loop for next item.")
        else:
            # Queue is empty OR this was a direct play. Bot should be idle.
            log.info(f"{log_prefix} Queue empty or direct play '{item_path_log}' finished. Bot should be idle now.")

            # Remove the task tracker if it corresponds to the completed task/direct play
            if current_task and (task_name is None or current_task.get_name() == task_name):
                 log.debug(f"{log_prefix} Removing completed task tracker: {current_task.get_name()}.")
                 self.guild_play_tasks.pop(guild_id, None)
            elif current_task:
                 # This might happen if a new task started between play ending and cleanup running
                 log.warning(f"{log_prefix} Task tracker mismatch on cleanup. Current Task: {current_task.get_name()}, Finished Task Hint: {task_name}.")
                 # Do not remove the wrong task tracker

            # Trigger leave timer check *only after* confirming idle state and cleaning up task
            log.info(f"{log_prefix} Triggering leave timer check as bot is now idle.")
            self.bot.loop.create_task(self.start_leave_timer(current_vc))

    def after_play_cleanup(self, error: Optional[Exception], guild_id: int, task_name: Optional[str], item: Optional[QueueItem], audio_buffer: Optional[io.BytesIO]):
        """ [DEPRECATED] Handles cleanup ONLY if play fails immediately (within async context). """
        log_prefix = f"AFTER_PLAY_DIRECT (Guild {guild_id}): Task: {task_name or 'NoTask'}"
        log.warning(f"{log_prefix} Direct cleanup called (likely immediate vc.play error).")
        if error: log.error(f'{log_prefix} Playback failed immediately: {error}', exc_info=error)
        if audio_buffer and not audio_buffer.closed:
            try:
                audio_buffer.close(); log.debug(f"{log_prefix} Closed buffer.")
            except Exception as buf_e:
                log.warning(f"{log_prefix} Error closing buffer: {buf_e}")
            self._cleanup_played_item_file(item);
            self.currently_playing.pop(guild_id, None)
            guild = self.bot.get_guild(guild_id)
            if guild:
                self.bot.loop.create_task(self.play_next_item(guild)) # Try next

    # --- Voice Client Connection ---
    async def ensure_voice_client(self, interaction: discord.Interaction, target_channel: discord.VoiceChannel, action_type: str = "Playback") -> Optional[discord.VoiceClient]:
        """ Ensures bot is connected to the target VC and idle, ready for playback. """
        user = interaction.user; guild = interaction.guild
        if not guild: await self._try_respond(interaction, "Use in server.", ephemeral=True); return None
        if not isinstance(user, discord.Member): await self._try_respond(interaction, "Not member.", ephemeral=True); return None
        guild_id = guild.id; log_prefix = f"{action_type.upper()} GID:{guild_id}:"
        bot_perms = target_channel.permissions_for(guild.me)
        if not bot_perms.connect or not bot_perms.speak: await self._try_respond(interaction, f"❌ Need Connect/Speak in {target_channel.mention}.", ephemeral=True); log.warning(f"{log_prefix} Missing perms."); return None
        vc = discord.utils.get(self.bot.voice_clients, guild=guild)
        try:
            if vc and vc.is_connected():
                if vc.is_playing(): await self._try_respond(interaction, "⏳ Bot busy playing.", ephemeral=True); log.info(f"{log_prefix} Bot busy playing."); return None
                elif vc.channel != target_channel:
                     should_move = (user.voice and user.voice.channel == target_channel) or not self.should_bot_stay(guild_id)
                     if should_move: log.info(f"{log_prefix} Moving to '{target_channel.name}'."); self.cancel_leave_timer(guild_id, reason=f"moving for {action_type}"); await vc.move_to(target_channel); log.info(f"{log_prefix} Moved.")
                     else: await self._try_respond(interaction, f"ℹ️ Staying in {vc.channel.mention}.", ephemeral=True); return None
            else: log.info(f"{log_prefix} Connecting to '{target_channel.name}'."); self.cancel_leave_timer(guild_id, reason=f"connecting for {action_type}"); vc = await target_channel.connect(timeout=30.0, reconnect=True); log.info(f"{log_prefix} Connected.")
            if not vc or not vc.is_connected(): log.error(f"{log_prefix} Failed VC establish."); await self._try_respond(interaction, "❌ Failed connect/move.", ephemeral=True); return None
            self.cancel_leave_timer(guild_id, reason=f"VC ready for {action_type}"); return vc
        except asyncio.TimeoutError: await self._try_respond(interaction, "❌ Timeout.", ephemeral=True); log.error(f"{log_prefix} Timeout."); return None
        except discord.errors.ClientException as e: await self._try_respond(interaction, f"❌ Client Error: {e}", ephemeral=True); log.warning(f"{log_prefix} ClientException: {e}"); return None
        except Exception as e: await self._try_respond(interaction, "❌ Unexpected VC error.", ephemeral=True); log.error(f"{log_prefix} Unexpected VC error: {e}", exc_info=True); return None


    # --- _try_respond (Corrected Helper) ---
    async def _try_respond(self, interaction: discord.Interaction, message: Optional[str] = None, **kwargs):
        """Helper to safely send/edit interaction responses, handling followup."""
        try:
            if interaction.response.is_done():
                log.debug("Interaction response is done, using followup.send.")
                if message or kwargs: await interaction.followup.send(content=message, **kwargs)
                else: log.warning("Attempted followup.send without content or kwargs.")
            else:
                if interaction.response._response_type is not None:
                     log.debug("Interaction not done but responded/deferred, using edit_original_response.")
                     kwarg_copy = kwargs.copy(); kwarg_copy.pop('ephemeral', None)
                     if message or kwarg_copy.get('embed') or kwarg_copy.get('view') or kwarg_copy.get('file') or kwarg_copy.get('files'): await interaction.edit_original_response(content=message, **kwarg_copy)
                     else: log.warning("Attempted edit_original_response without content or relevant kwargs.")
                else:
                     log.debug("Interaction not done and not responded, using response.send_message.")
                     await interaction.response.send_message(content=message, **kwargs)
        except discord.errors.InteractionResponded:
            log.warning(f"Interaction already responded (race condition?), trying followup for: {message[:50] if message else 'No Message'}...")
            try:
                if message or kwargs: await interaction.followup.send(content=message, **kwargs)
            except discord.NotFound: log.warning(f"Followup attempt failed: Interaction not found.")
            except Exception as followup_e: log.error(f"Followup failed after InteractionResponded error: {followup_e}", exc_info=True)
        except discord.NotFound: log.warning(f"Interaction not found during response attempt: {message[:50] if message else 'No Message'}...")
        except discord.Forbidden as e: log.error(f"Response forbidden: {e}")
        except Exception as e: log.error(f"Unexpected response error: {e}", exc_info=True)


    # core/playback_manager.py

    async def play_single_sound(self, interaction: discord.Interaction, sound_path: Optional[str] = None, audio_source: Optional[discord.PCMAudio] = None, audio_buffer_to_close: Optional[io.BytesIO] = None, display_name: Optional[str] = None):
        """ Plays a single sound/TTS, handling VC and cleanup. """
        user = interaction.user; guild = interaction.guild
        if not guild or not isinstance(user, discord.Member) or not user.voice or not user.voice.channel:
            await self._try_respond(interaction, "Need VC.", ephemeral=True)
            # Ensure buffer is closed if we exit early
            if audio_buffer_to_close and not audio_buffer_to_close.closed:
                try: audio_buffer_to_close.close()
                except Exception: pass
            return

        target_channel = user.voice.channel; guild_id = guild.id
        action_type = "TTS PLAY" if (audio_source and not sound_path) else "SINGLE PLAY (File)"; log_prefix = f"{action_type} GID:{guild_id}:"

        # Argument validation
        if (sound_path and audio_source) or (not sound_path and not audio_source):
            log.error(f"{log_prefix} Invalid input combo (needs either sound_path OR audio_source/buffer).")
            await self._try_respond(interaction, "❌ Bot error (Invalid arguments).", ephemeral=True)
            # Ensure buffer is closed on error
            if audio_buffer_to_close and not audio_buffer_to_close.closed:
                try: audio_buffer_to_close.close()
                except Exception: pass
            return
        # Ensure buffer exists if source exists (for TTS case)
        if audio_source and not audio_buffer_to_close:
             log.error(f"{log_prefix} Invalid input combo (audio_source provided without audio_buffer_to_close).")
             await self._try_respond(interaction, "❌ Bot error (Missing audio buffer).", ephemeral=True)
             # No buffer to close here as it wasn't provided
             return

        voice_client = await self.ensure_voice_client(interaction, target_channel, action_type=action_type)
        if not voice_client:
            # ensure_voice_client sends feedback, just need to clean up buffer
            if audio_buffer_to_close and not audio_buffer_to_close.closed:
                try: audio_buffer_to_close.close()
                except Exception: pass
            return

        final_audio_source = audio_source
        final_buffer_to_close = audio_buffer_to_close
        sound_display_name = display_name or "Sound"

        # --- Create a minimal item for state tracking and cleanup ---
        item_for_state_and_cleanup = {
            'type': 'direct_play',
            'path': sound_path or 'tts_direct', # Store path if available, otherwise indicate TTS
            'guild_id': guild_id # Useful for logging in cleanup
        }
        # --- End Item Creation ---

        if sound_path: # Process file only if sound_path was given
            if not os.path.exists(sound_path):
                await self._try_respond(interaction, f"❌ File not found: `{os.path.basename(sound_path)}`", ephemeral=True)
                return # No buffer created yet
            sound_display_name = os.path.splitext(os.path.basename(sound_path))[0]
            log.info(f"{log_prefix} Processing '{sound_display_name}'...")
            processed_source, processed_buffer = self.process_audio(sound_path, user.display_name)
            if not processed_source:
                await self._try_respond(interaction, f"❌ Failed process `{sound_display_name}`.", ephemeral=True)
                if processed_buffer and not processed_buffer.closed:
                     try: processed_buffer.close()
                     except Exception: pass
                # No playback happening, check if timer needed
                self.bot.loop.create_task(self.start_leave_timer(voice_client))
                return
            final_audio_source = processed_source
            final_buffer_to_close = processed_buffer # This is the buffer we need to close later
            item_for_state_and_cleanup['path'] = sound_path # Update path in item

        # --- Final check before playing ---
        if not final_audio_source:
            await self._try_respond(interaction, "❌ Error preparing audio source.", ephemeral=True)
            if final_buffer_to_close and not final_buffer_to_close.closed: # Clean up if buffer exists
                try: final_buffer_to_close.close()
                except Exception: pass
            return

        await asyncio.sleep(0.1)

        # --- Check if already playing (using internal state now too) ---
        if voice_client.is_playing() or self.currently_playing.get(guild_id):
            log.warning(f"{log_prefix} Attempted play while busy (vc.is_playing={voice_client.is_playing()}, self.currently_playing={self.currently_playing.get(guild_id) is not None}). Request by {user.name}.")
            await self._try_respond(interaction, "⏳ Busy playing another sound. Please wait.", ephemeral=True)
            if final_buffer_to_close and not final_buffer_to_close.closed: # Clean up the buffer for the *new* sound
                try: final_buffer_to_close.close()
                except Exception: pass
            return
        # --- End Busy Check ---

        try:
            # --- Set currently playing *before* calling play ---
            self.currently_playing[guild_id] = item_for_state_and_cleanup
            # --- End Set State ---

            self.cancel_leave_timer(guild_id, reason=f"starting {action_type}")
            log.info(f"{log_prefix} Playing '{sound_display_name}'...") # This log should now appear

            # Pass the state item to the callback
            after_callback = lambda e: self.bot.loop.call_soon_threadsafe(
                self.after_play_cleanup_threadsafe,
                e,
                voice_client.guild.id,
                None, # No specific task name for direct play
                item_for_state_and_cleanup, # Pass the item
                final_buffer_to_close # Pass the buffer
            )
            voice_client.play(final_audio_source, after=after_callback)

            # Send confirmation message *after* successfully calling play
            duration_ms = self.config.MAX_PLAYBACK_DURATION_MS
            duration_sec_str = f"{duration_ms / 1000:.1f}".rstrip('0').rstrip('.') # Format seconds nicely
            play_msg = f"▶️ Playing `{sound_display_name}` (max {duration_sec_str}s)..."
            if action_type == "TTS PLAY":
                 play_msg = f"🗣️ Playing TTS: \"{display_name}\"..." if display_name else "🗣️ Playing TTS..."
            await self._try_respond(interaction, play_msg, ephemeral=False) # Use ephemeral=False for play confirmation

        except discord.errors.ClientException as e:
            await self._try_respond(interaction, "❌ Client error during playback.", ephemeral=True)
            log.error(f"{log_prefix} ClientException on play: {e}", exc_info=True)
            # --- Cleanup state and buffer on immediate error ---
            self.currently_playing.pop(guild_id, None)
            if final_buffer_to_close and not final_buffer_to_close.closed:
                try: final_buffer_to_close.close()
                except Exception: pass
            # Trigger leave timer check as playback failed
            self.bot.loop.create_task(self.start_leave_timer(voice_client))
            # --- End Cleanup ---
        except Exception as e:
            await self._try_respond(interaction, "❌ Unexpected playback error.", ephemeral=True)
            log.error(f"{log_prefix} Unexpected error on play: {e}", exc_info=True)
             # --- Cleanup state and buffer on immediate error ---
            self.currently_playing.pop(guild_id, None)
            if final_buffer_to_close and not final_buffer_to_close.closed:
                try: final_buffer_to_close.close()
                except Exception: pass
            # Trigger leave timer check as playback failed
            self.bot.loop.create_task(self.start_leave_timer(voice_client))
            # --- End Cleanup ---


    # --- Idle/Leave Timer Logic ---
    def should_bot_stay(self, guild_id: int) -> bool:
        settings = self.bot.guild_settings.get(str(guild_id), {}); stay = settings.get("stay_in_channel", False); log.debug(f"Stay check GID:{guild_id}: {stay}"); return stay is True
    def is_bot_alone(self, vc: Optional[discord.VoiceClient]) -> bool:
        if not vc or not vc.channel: return False; member_count = len(vc.channel.members); is_alone = member_count <= 1; log.debug(f"ALONE CHECK GID:{vc.guild.id}: {member_count} members. Alone: {is_alone}"); return is_alone
    # core/playback_manager.py

# core/playback_manager.py

    def cancel_leave_timer(self, guild_id: int, reason: str = "unknown"):
        if guild_id in self.guild_leave_timers:
            # --- Moved this line inside the 'if' block ---
            timer_task = self.guild_leave_timers.pop(guild_id, None) # Pop returns the task or None

            # --- Now this check is safe ---
            if timer_task and not timer_task.done():
                try:
                    timer_task.cancel()
                    log.info(f"LEAVE TIMER GID:{guild_id}: Cancelled. Reason: {reason}")
                except Exception as e:
                    log.warning(f"LEAVE TIMER GID:{guild_id}: Error cancelling: {e}")
            elif timer_task: # It existed but was already done
                log.debug(f"LEAVE TIMER GID:{guild_id}: Attempted cancel on completed timer.")
        # If guild_id wasn't in the dict, nothing happens, which is correct.
    async def start_leave_timer(self, vc: discord.VoiceClient):
        if not vc or not vc.is_connected() or not vc.guild: return
        guild_id = vc.guild.id; log_prefix = f"LEAVE TIMER (Guild {guild_id}):"; self.cancel_leave_timer(guild_id, reason="starting new check")
        if self.should_bot_stay(guild_id): log.debug(f"{log_prefix} Stay enabled."); return
        if not self.is_bot_alone(vc): log.debug(f"{log_prefix} Not alone."); return
        if vc.is_playing(): log.debug(f"{log_prefix} Playing."); return
        timeout = self.config.AUTO_LEAVE_TIMEOUT_SECONDS; log.info(f"{log_prefix} Starting {timeout}s timer.")
        timer_task = self.bot.loop.create_task(self._leave_after_delay(vc.guild.id, vc.channel.id, timeout), name=f"AutoLeave_{guild_id}"); self.guild_leave_timers[guild_id] = timer_task
    async def _leave_after_delay(self, g_id: int, initial_channel_id: int, delay: float):
        log_prefix = f"LEAVE TIMER (Guild {g_id}):"; task_ref = asyncio.current_task()
        try:
             await asyncio.sleep(delay); log.debug(f"{log_prefix} Timer expired."); current_vc = discord.utils.get(self.bot.voice_clients, guild__id=g_id)
             guild = self.bot.get_guild(g_id); original_channel = guild.get_channel(initial_channel_id) if guild else None
             if not current_vc or not current_vc.is_connected() or current_vc.channel.id != initial_channel_id: log.info(f"{log_prefix} Bot moved/disconnected."); return
             if self.should_bot_stay(g_id): log.info(f"{log_prefix} Stay enabled during wait."); return
             if not self.is_bot_alone(current_vc): log.info(f"{log_prefix} Not alone anymore."); return
             if current_vc.is_playing(): log.info(f"{log_prefix} Started playing again."); return
             log.info(f"{log_prefix} Conditions met. Disconnecting."); await self.safe_disconnect(current_vc, manual_leave=False, reason="timer expired")
        except asyncio.CancelledError: log.info(f"{log_prefix} Timer cancelled.")
        except Exception as e: log.error(f"{log_prefix} Error during timer: {e}", exc_info=True)
        finally: # Cleanup timer reference
             if g_id in self.guild_leave_timers and self.guild_leave_timers.get(g_id) is task_ref: del self.guild_leave_timers[g_id]; log.debug(f"{log_prefix} Cleaned up timer task ref.")


    # --- Safe Disconnect ---
    async def safe_disconnect(self, vc: Optional[discord.VoiceClient], *, manual_leave: bool = False, reason: str = "disconnect"):
        if not vc or not vc.is_connected() or not vc.guild: return
        guild = vc.guild; guild_id = guild.id; log_prefix = f"DISCONNECT GID:{guild_id}:"; self.cancel_leave_timer(guild_id, reason=f"safe_disconnect ({reason})")
        if not manual_leave and self.should_bot_stay(guild_id): log.debug(f"{log_prefix} Stay enabled, skip disconnect."); return
        disconnect_reason_log = "Manual" if manual_leave else f"Auto ({reason})"; log.info(f"{log_prefix} Disconnecting from {guild.name} (Reason: {disconnect_reason_log}).")
        try:
            if vc.is_playing(): log.info(f"{log_prefix} Stopping playback."); vc.stop()
            await vc.disconnect(force=False); log.info(f"{log_prefix} Bot disconnected.")
            # Redundant cleanup
            if guild_id in self.guild_play_tasks: task = self.guild_play_tasks.pop(guild_id, None); # cancel if needed
            if guild_id in self.guild_queues: self.guild_queues[guild_id].clear()
            self.currently_playing.pop(guild_id, None)
        except Exception as e: log.error(f"{log_prefix} Disconnect error: {e}", exc_info=True)