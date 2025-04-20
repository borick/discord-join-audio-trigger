import discord
import asyncio
import logging
import io
import os
from collections import defaultdict
from typing import Dict, List, Optional, Union, Any
import time
import functools
from discord.ext import commands
from enum import Enum, auto

# Local application imports
import config
from utils import audio_processor

# Define Enum for playback status (ensure this is defined)
class PlaybackMode(Enum):
    IDLE = auto()
    QUEUE = auto()
    SINGLE_SOUND = auto()

from core.music_types import MusicQueueItem, DownloadStatus

log = logging.getLogger('SoundBot.PlaybackManager')

# Define QueueItemType using Any for flexibility
QueueItemType = Any

# Configuration for idle timeout
IDLE_TIMEOUT_SECONDS = getattr(config, 'AUTO_LEAVE_TIMEOUT_SECONDS', 14400) # Default 4 hours

class PlaybackManager:
    """Manages voice connections, queues, and playback state for guilds."""
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.guild_queues: Dict[int, List[QueueItemType]] = defaultdict(list)
        self.currently_playing: Dict[int, Optional[QueueItemType]] = defaultdict(lambda: None)
        self.guild_locks: Dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.idle_timers: Dict[int, asyncio.Task] = {}
        self.playback_mode: Dict[int, PlaybackMode] = defaultdict(lambda: PlaybackMode.IDLE)
        self.active_single_buffers: Dict[int, io.BytesIO] = {}

    # core/playback_manager.py

    async def ensure_voice_client(
        self,
        interaction: Optional[discord.Interaction], # Interaction is optional
        target_channel: discord.VoiceChannel,
        action_type: str = "ACTION"
    ) -> Optional[discord.VoiceClient]:
        """
        Ensures the bot is connected to the target voice channel.
        Connects if not connected, moves if necessary and possible.
        Returns the VoiceClient on success, None on failure.
        Sends feedback via interaction ONLY if interaction is provided.
        """
        guild = target_channel.guild
        if not guild: # Should not happen with VoiceChannel but safe check
            log.error("ensure_voice_client called with invalid target_channel (no guild)")
            return None
        guild_id = guild.id
        current_vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)

        # --- No user variable needed for core logic ---
        # user = interaction.user if interaction else None # Not needed here anymore

        if current_vc:
            if current_vc.channel == target_channel:
                log.debug(f"Ensure VC: Already connected to {target_channel.name} in GID:{guild_id}")
                return current_vc
            else:
                # Check permissions in target channel
                my_perms = target_channel.permissions_for(guild.me)
                if not my_perms.connect or not my_perms.speak:
                    log.warning(f"Ensure VC: Missing permissions to move/speak in {target_channel.name} (GID:{guild_id})")
                    # Only try to respond if interaction exists
                    if interaction:
                        await self._try_respond(interaction, f"❌ I don't have permissions to join or speak in {target_channel.mention}.", ephemeral=True)
                    return None
                # Try to move
                try:
                    log.info(f"Ensure VC: Moving from {current_vc.channel.name} to {target_channel.name} (GID:{guild_id}) for {action_type}")
                    await current_vc.move_to(target_channel)
                    log.debug(f"Ensure VC: Move successful to {target_channel.name}.")
                    return current_vc
                except asyncio.TimeoutError:
                    log.error(f"Ensure VC: Timeout moving to {target_channel.name} (GID:{guild_id})")
                    if interaction: await self._try_respond(interaction, "❌ Timed out trying to move voice channels.", ephemeral=True)
                    return None
                except Exception as e:
                    log.error(f"Ensure VC: Error moving to {target_channel.name} (GID:{guild_id}): {e}", exc_info=True)
                    if interaction: await self._try_respond(interaction, "❌ An error occurred while moving voice channels.", ephemeral=True)
                    return None
        else:
            # Not connected, try connecting
            my_perms = target_channel.permissions_for(guild.me)
            if not my_perms.connect or not my_perms.speak:
                log.warning(f"Ensure VC: Missing permissions to connect/speak in {target_channel.name} (GID:{guild_id})")
                if interaction: await self._try_respond(interaction, f"❌ I don't have permissions to join or speak in {target_channel.mention}.", ephemeral=True)
                return None
            try:
                log.info(f"Ensure VC: Connecting to {target_channel.name} (GID:{guild_id}) for {action_type}")
                vc = await target_channel.connect(timeout=30.0, reconnect=True)
                log.debug(f"Ensure VC: Connect successful to {target_channel.name}.")
                return vc
            except asyncio.TimeoutError:
                log.error(f"Ensure VC: Timeout connecting to {target_channel.name} (GID:{guild_id})")
                if interaction: await self._try_respond(interaction, "❌ Timed out trying to connect to the voice channel.", ephemeral=True)
                return None
            except discord.ClientException as e:
                log.error(f"Ensure VC: Discord ClientException connecting to {target_channel.name} (GID:{guild_id}): {e}")
                msg = "⏳ Already connecting/connected. Please wait." if "already connect" in str(e).lower() else f"❌ Error connecting: {e}"
                if interaction: await self._try_respond(interaction, msg, ephemeral=True)
                return None
            except Exception as e:
                log.error(f"Ensure VC: Unexpected error connecting to {target_channel.name} (GID:{guild_id}): {e}", exc_info=True)
                if interaction: await self._try_respond(interaction, "❌ An unexpected error occurred while connecting.", ephemeral=True)
                return None
    async def safe_disconnect(self, vc: discord.VoiceClient, manual_leave: bool = False, reason: str = "Unknown"):
        """Stops playback, clears state, cancels timers and disconnects."""
        if not vc or not vc.guild:
            log.warning("safe_disconnect called with invalid VC")
            return
        guild_id = vc.guild.id
        log.info(f"Initiating safe disconnect for GID:{guild_id}. Reason: {reason}")
        from utils import voice_helpers
        voice_helpers.cancel_leave_timer(self.bot, guild_id, reason=f"safe_disconnect ({reason})")
        async with self.guild_locks[guild_id]:
            if vc.is_playing():
                log.debug(f"Stopping active player for GID:{guild_id} during disconnect.")
                vc.stop()
            self.currently_playing.pop(guild_id, None)
            self.guild_queues.pop(guild_id, None)
            self.playback_mode[guild_id] = PlaybackMode.IDLE
            self._cancel_idle_timer(guild_id)
            buffer = self.active_single_buffers.pop(guild_id, None)
            if buffer and not buffer.closed:
                # --- CORRECTED SYNTAX ---
                try:
                    buffer.close()
                except Exception:
                    pass # Ignore close errors here
                # ------------------------
                log.debug(f"Closed lingering single-play buffer during disconnect for GID:{guild_id}")
            log.debug(f"Cleared playback state for GID:{guild_id}")
            try:
                current_vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
                if current_vc and current_vc.is_connected():
                    await current_vc.disconnect(force=False)
                    log.info(f"Successfully disconnected from voice in GID:{guild_id}.")
                else:
                    log.info(f"Already disconnected before final disconnect call in GID:{guild_id}")
            except Exception as e:
                log.error(f"Error during voice client disconnect for GID:{guild_id}: {e}", exc_info=True)

    def get_queue(self, guild_id: int) -> List[QueueItemType]:
        return self.guild_queues.get(guild_id, [])

    def get_current_item(self, guild_id: int) -> Optional[QueueItemType]:
        return self.currently_playing.get(guild_id)

    async def add_to_queue(self, guild_id: int, item: QueueItemType) -> int:
        """Adds an item to the end of the guild's queue. Returns new queue position."""
        item_title_safe = getattr(item, 'title', str(item))[:50] # Moved up
        log.debug(f"ADD_TO_QUEUE (Core - GID:{guild_id}): Received item '{item_title_safe}'. Type: {type(item).__name__}") # ADD THIS
        async with self.guild_locks[guild_id]:
            log.debug(f"ADD_TO_QUEUE (Core - GID:{guild_id}): Lock acquired.") # ADD THIS
            # Ensure the queue list exists using setdefault just in case
            queue = self.guild_queues.setdefault(guild_id, [])
            log.debug(f"ADD_TO_QUEUE (Core - GID:{guild_id}): Queue length BEFORE append: {len(queue)}") # ADD THIS

            queue.append(item)
            position = len(queue)

            # Confirm the item is physically in the list right after append
            log.debug(f"ADD_TO_QUEUE (Core - GID:{guild_id}): Queue length AFTER append: {position}. Last item type: {type(queue[-1]).__name__ if queue else 'N/A'}") # ADD THIS
            if not queue or queue[-1] != item:
                log.error(f"ADD_TO_QUEUE (Core - GID:{guild_id}): CRITICAL! Item appended but queue[-1] doesn't match!")


            log.info(f"ADD_TO_QUEUE: GID {guild_id} - Appended item '{item_title_safe}'. New Length: {position}. Type: {type(item).__name__}") # Existing log

            vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
            trigger_playback_check = False # Flag
            if vc and vc.is_connected() and not self.is_playing(guild_id) and position == 1:
                 current_mode = self.playback_mode.get(guild_id, PlaybackMode.IDLE)
                 if current_mode in [PlaybackMode.IDLE, PlaybackMode.QUEUE]:
                    log.info(f"ADD_TO_QUEUE: GID {guild_id} - Conditions met to trigger playback check (Idle/Queue, VC Connected, Not Playing, Pos 1). Mode: {current_mode}")
                    self.playback_mode[guild_id] = PlaybackMode.QUEUE # Set mode
                    trigger_playback_check = True
                 else:
                    log.debug(f"ADD_TO_QUEUE: GID {guild_id} - Item added at Pos 1, but Mode is {current_mode}, not triggering playback.")
            elif not vc or not vc.is_connected():
                 log.debug(f"ADD_TO_QUEUE: GID {guild_id} - VC not connected, not triggering playback check.")
            elif self.is_playing(guild_id):
                 log.debug(f"ADD_TO_QUEUE: GID {guild_id} - Bot already playing, not triggering playback check.")
            elif position != 1:
                 log.debug(f"ADD_TO_QUEUE: GID {guild_id} - Item added at Pos {position} (not 1), not triggering playback check.")

        # --- Trigger outside lock ---
        if trigger_playback_check and vc: # Ensure vc is still valid
            log.debug(f"ADD_TO_QUEUE: GID {guild_id} - Calling start_playback_if_idle task.")
            # Using create_task is safer here than awaiting directly within add_to_queue
            self.bot.loop.create_task(self.start_playback_if_idle(guild_id), name=f"PlayCheck_Add_{guild_id}")
        
        log.debug(f"ADD_TO_QUEUE (Core - GID:{guild_id}): Releasing lock.")
        return position # Return position regardless of whether playback was triggered now

    async def insert_into_queue(self, guild_id: int, index: int, item: QueueItemType):
        async with self.guild_locks[guild_id]:
            queue = self.guild_queues[guild_id]
            index = max(0, min(index, len(queue)))
            queue.insert(index, item)
            log.debug(f"Inserted item at index {index} for GID {guild_id}. New length: {len(queue)}")
            vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
            if index == 0 and vc and vc.is_connected() and not self.is_playing(guild_id):
                if self.playback_mode.get(guild_id, PlaybackMode.IDLE) in [PlaybackMode.IDLE, PlaybackMode.QUEUE]:
                    log.info(f"Item inserted at front of idle queue. Triggering playback check for GID {guild_id}.")
                    self.playback_mode[guild_id] = PlaybackMode.QUEUE
                    self.bot.loop.create_task(self._play_next(guild_id, vc), name=f"PlayNextTask_Ins_{guild_id}")
                else:
                     log.debug(f"Item inserted at front for GID {guild_id}, but mode is {self.playback_mode.get(guild_id)}, not starting playback automatically.")

    async def remove_from_queue(self, guild_id: int, index: int) -> Optional[QueueItemType]:
        async with self.guild_locks[guild_id]:
            queue = self.guild_queues.get(guild_id)
            if queue and 0 <= index < len(queue):
                removed_item = queue.pop(index)
                log.debug(f"Removed item at index {index} for GID {guild_id}.")
                return removed_item
            else:
                log.warning(f"Attempted to remove item at invalid index {index} for GID {guild_id}. Queue length: {len(queue) if queue else 0}")
                return None

    async def clear_queue(self, guild_id: int):
        async with self.guild_locks[guild_id]:
            if guild_id in self.guild_queues:
                count = len(self.guild_queues[guild_id])
                self.guild_queues.pop(guild_id, None)
                log.info(f"Cleared queue ({count} items) for GID {guild_id}")
            else:
                log.debug(f"Queue already empty or non-existent for GID {guild_id}, clear request ignored.")

    def is_playing(self, guild_id: int) -> bool:
        vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
        return bool(vc and vc.is_playing() and self.currently_playing.get(guild_id) is not None)

    async def start_playback_if_idle(self, guild_id: int):
        vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
        async with self.guild_locks[guild_id]:
            if vc and vc.is_connected() and not self.is_playing(guild_id) and self.guild_queues.get(guild_id):
                if self.playback_mode.get(guild_id, PlaybackMode.IDLE) in [PlaybackMode.IDLE, PlaybackMode.QUEUE]:
                    log.info(f"Playback idle for GID {guild_id}, queue not empty. Starting playback loop.")
                    self.playback_mode[guild_id] = PlaybackMode.QUEUE
                    self.bot.loop.create_task(self._play_next(guild_id, vc), name=f"PlayNextTask_StartIdle_{guild_id}")
                else:
                    log.debug(f"Start playback check for GID {guild_id}: Mode is {self.playback_mode.get(guild_id)}, not starting.")
            elif vc and vc.is_connected() and not self.is_playing(guild_id) and not self.guild_queues.get(guild_id):
                log.debug(f"Start playback check for GID {guild_id}: Queue is empty, ensuring idle timer starts.")
                self._start_idle_timer(guild_id, vc)
            elif not vc:
                 log.debug(f"Start playback check for GID {guild_id}: Bot not in voice channel.")

    async def skip_track(self, guild_id: int) -> bool:
        vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
        async with self.guild_locks[guild_id]:
            if self.is_playing(guild_id) and vc:
                log.info(f"Skipping track for GID {guild_id}")
                vc.stop()
                return True
            else:
                log.warning(f"Skip requested for GID {guild_id}, but nothing is playing.")
                return False

    async def stop_playback(self, guild_id: int, clear_queue: bool = True, leave_channel: bool = True):
        log.info(f"Received stop command for GID {guild_id}. Clear: {clear_queue}, Leave: {leave_channel}")
        vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
        async with self.guild_locks[guild_id]:
            self.playback_mode[guild_id] = PlaybackMode.IDLE
            self._cancel_idle_timer(guild_id)
            if vc and vc.is_playing():
                log.debug(f"Stopping player for GID {guild_id} due to stop command.")
                vc.stop()
            self.currently_playing.pop(guild_id, None)
            if clear_queue:
                if guild_id in self.guild_queues:
                    count = len(self.guild_queues[guild_id])
                    self.guild_queues.pop(guild_id, None)
                    log.info(f"Cleared queue ({count} items) for GID {guild_id} due to stop command.")
            buffer = self.active_single_buffers.pop(guild_id, None)
            if buffer and not buffer.closed:
                # --- CORRECTED SYNTAX ---
                try:
                    buffer.close()
                except Exception:
                    pass
                # ------------------------
                log.debug(f"Closed lingering single-play buffer during stop command for GID:{guild_id}")
            if leave_channel and vc and vc.is_connected():
                 self.bot.loop.create_task(self.safe_disconnect(vc, manual_leave=True, reason="stop_playback command"))
            elif vc and vc.is_connected():
                if not self.guild_queues.get(guild_id) and not vc.is_playing():
                    self._start_idle_timer(guild_id, vc)

    async def _play_next(self, guild_id: int, vc: discord.VoiceClient):
        """The core loop that plays the next available song in the queue."""
        log.debug(f"Entered _play_next for GID {guild_id}")
        lock_acquired = False
        try:
            await asyncio.wait_for(self.guild_locks[guild_id].acquire(), timeout=5.0)
            lock_acquired = True
            log.debug(f"Acquired lock for GID {guild_id} in _play_next")

            current_mode = self.playback_mode.get(guild_id, PlaybackMode.IDLE)
            if current_mode != PlaybackMode.QUEUE:
                 log.info(f"Playback mode is {current_mode}, not QUEUE. Aborting _play_next for GID {guild_id}.")
                 if current_mode == PlaybackMode.IDLE and vc.is_connected() and not vc.is_playing():
                      self._start_idle_timer(guild_id, vc)
                 return

            last_item = self.currently_playing.pop(guild_id, None)
            if last_item:
                log.debug(f"Cleaned up previously playing item tracker for GID: {guild_id}. Type: {type(last_item).__name__}")
                if isinstance(last_item, MusicQueueItem) and hasattr(last_item, 'last_played_at'):
                    last_item.last_played_at = time.time()

            if not vc or not vc.is_connected():
                log.warning(f"VC disconnected before _play_next could run for GID {guild_id}. Aborting playback.")
                self.guild_queues.pop(guild_id, None)
                self.currently_playing.pop(guild_id, None)
                self.playback_mode[guild_id] = PlaybackMode.IDLE
                self._cancel_idle_timer(guild_id)
                return

            queue = self.guild_queues.get(guild_id)
            if not queue:
                log.info(f"Queue empty for GID {guild_id}. Playback finished.")
                self.currently_playing.pop(guild_id, None)
                self.playback_mode[guild_id] = PlaybackMode.IDLE
                self._start_idle_timer(guild_id, vc)
                return

            next_item_played = False
            while queue:
                item_to_try = queue[0]
                item_type = type(item_to_try).__name__
                log.debug(f"_play_next: GID {guild_id} - Examining queue item. Type: {item_type}")

                if isinstance(item_to_try, MusicQueueItem):
                    status = item_to_try.download_status
                    title = getattr(item_to_try, 'title', 'Unknown Title')
                    log.debug(f"Music Item: '{title[:50]}' Status Enum: {status}")

                    if status == DownloadStatus.READY:
                        audio_source = item_to_try.get_playback_source()
                        if audio_source:
                            dequeued_item = queue.pop(0)
                            self.currently_playing[guild_id] = dequeued_item
                            self._cancel_idle_timer(guild_id)
                            self.playback_mode[guild_id] = PlaybackMode.QUEUE
                            after_callback = functools.partial(self._playback_finished_callback, guild_id, vc)
                            log.info(f"Playing '{title}' in GID {guild_id}")
                            vc.play(audio_source, after=after_callback)
                            next_item_played = True
                            break
                        else:
                            log.error(f"Music Item '{title}' status READY but get_playback_source failed. Skipping. GID: {guild_id}")
                            item_to_try.download_status = DownloadStatus.FAILED
                            queue.pop(0)
                            continue
                    elif status == DownloadStatus.FAILED:
                        log.warning(f"Skipping failed Music Item: '{title}'. GID: {guild_id}")
                        queue.pop(0)
                        continue
                    elif status == DownloadStatus.PENDING or status == DownloadStatus.DOWNLOADING:
                        log.info(f"Music Item '{title}' not ready ({status}). Waiting for downloader. GID {guild_id}")
                        break
                    else:
                        log.error(f"Unexpected Music Item status '{status}' for item '{title}'. Treating as Failed. GID: {guild_id}")
                        item_to_try.download_status = DownloadStatus.FAILED
                        queue.pop(0)
                        continue

                elif isinstance(item_to_try, tuple) and len(item_to_try) == 3 and isinstance(item_to_try[1], str):
                    member, sound_path, is_temp_tts = item_to_try
                    sound_basename = os.path.basename(sound_path)
                    log.info(f"_play_next: GID {guild_id} - Attempting to process join sound tuple: '{sound_basename}' for {member.display_name}")

                    log.debug(f"_play_next: GID {guild_id} - Calling audio_processor for join sound: {sound_path}")
                    try:
                        audio_source, audio_buffer = audio_processor.process_audio(sound_path)
                        log.debug(f"_play_next: GID {guild_id} - Audio processor result for '{sound_basename}': Source valid={audio_source is not None}, Buffer valid={audio_buffer is not None}")
                    except Exception as proc_err:
                        log.error(f"_play_next: GID {guild_id} - Exception during audio_processor.process_audio for '{sound_path}': {proc_err}", exc_info=True)
                        audio_source, audio_buffer = None, None

                    if audio_source and audio_buffer:
                        log.debug(f"_play_next: GID {guild_id} - Join sound audio processed successfully. Buffer size: {audio_buffer.getbuffer().nbytes} bytes.")
                        dequeued_item_tuple = queue.pop(0)
                        self.currently_playing[guild_id] = dequeued_item_tuple
                        self._cancel_idle_timer(guild_id)
                        self.playback_mode[guild_id] = PlaybackMode.QUEUE

                        def after_join_sound(error: Optional[Exception]):
                            gid_cb = guild_id
                            path_cb = sound_path
                            temp_cb = is_temp_tts
                            buffer_cb = audio_buffer
                            member_name_cb = member.display_name
                            log.debug(f"after_join_sound: GID {gid_cb} - Callback triggered for '{os.path.basename(path_cb)}' (User: {member_name_cb}). Error: {error}")
                            if buffer_cb:
                                if not buffer_cb.closed:
                                    # --- CORRECTED SYNTAX ---
                                    try:
                                        buffer_cb.close()
                                    except Exception as buf_e:
                                        log.warning(f"after_join_sound: GID {gid_cb} - Error closing buffer: {buf_e}")
                                    # ------------------------
                                log.debug(f"after_join_sound: GID {gid_cb} - Closed audio buffer.")
                            else: log.warning(f"after_join_sound: GID {gid_cb} - Buffer was None in callback.")
                            if temp_cb and path_cb and os.path.exists(path_cb):
                                try:
                                    os.remove(path_cb)
                                    log.info(f"after_join_sound: GID {gid_cb} - Deleted temporary TTS file: {path_cb}")
                                except Exception as e:
                                    log.warning(f"after_join_sound: GID {gid_cb} - Failed to delete temp join sound {path_cb}: {e}")
                            log.debug(f"after_join_sound: Scheduling _playback_finished_task for GID {gid_cb}")
                            self.bot.loop.call_soon_threadsafe(
                                lambda: self.bot.loop.create_task(self._playback_finished_task(gid_cb, vc, error))
                            )
                            log.debug(f"after_join_sound: Finished scheduling for GID {gid_cb}")
                        # --- End of after_join_sound definition ---

                        try:
                            log.info(f"Playing join sound '{sound_basename}' for {member.display_name} in GID {guild_id}")
                            vc.play(audio_source, after=after_join_sound)
                            log.debug(f"vc.play() called for join sound '{sound_basename}'")
                            next_item_played = True
                            break
                        except discord.ClientException as play_exc:
                             log.error(f"_play_next: GID {guild_id} - ClientException during vc.play() for join sound '{sound_basename}': {play_exc}", exc_info=True)
                             if audio_buffer and not audio_buffer.closed:
                                 # --- CORRECTED SYNTAX ---
                                 try: audio_buffer.close()
                                 except Exception: pass
                                 # ------------------------
                             log.error(f"_play_next: GID {guild_id} - Failed to start playback for join sound '{sound_basename}'. Skipping.")
                             if queue and queue[0] == item_to_try: queue.pop(0)
                             continue
                        except Exception as play_exc_other:
                             log.error(f"_play_next: GID {guild_id} - Unexpected Exception during vc.play() for join sound '{sound_basename}': {play_exc_other}", exc_info=True)
                             if audio_buffer and not audio_buffer.closed:
                                 # --- CORRECTED SYNTAX ---
                                 try: audio_buffer.close()
                                 except Exception: pass
                                 # ------------------------
                             log.error(f"_play_next: GID {guild_id} - Failed to start playback for join sound '{sound_basename}'. Skipping.")
                             if queue and queue[0] == item_to_try: queue.pop(0)
                             continue
                    else:
                        log.error(f"_play_next: GID {guild_id} - Failed to process join sound '{sound_basename}' for {member.display_name} (audio_processor returned None). Skipping.")
                        queue.pop(0)
                        if is_temp_tts and os.path.exists(sound_path):
                            try: os.remove(sound_path)
                            except Exception as e: log.warning(f"Failed to delete failed temp join sound {sound_path}: {e}")
                        continue
                else:
                    log.error(f"Unknown item type in queue for GID {guild_id}: {item_to_try}. Skipping.")
                    queue.pop(0)
                    continue
            # --- End of while queue loop ---

            if not next_item_played:
                if not queue:
                    log.info(f"Processed queue for GID {guild_id}, no playable items found, queue now empty.")
                    self.currently_playing.pop(guild_id, None)
                    self.playback_mode[guild_id] = PlaybackMode.IDLE
                    self._start_idle_timer(guild_id, vc)
                else:
                    log.debug(f"Stopped processing queue for GID {guild_id}, likely waiting for download.")

        except asyncio.TimeoutError:
            log.error(f"Timeout acquiring lock for GID {guild_id} in _play_next. Playback may be stalled.")
            if lock_acquired and self.guild_locks[guild_id].locked():
                self.guild_locks[guild_id].release()
        except Exception as e:
            log.error(f"Unexpected error in _play_next for GID {guild_id}: {e}", exc_info=True)
            self.currently_playing.pop(guild_id, None)
            self.playback_mode[guild_id] = PlaybackMode.IDLE
            if vc and vc.is_connected():
                 self._start_idle_timer(guild_id, vc)
        finally:
            if lock_acquired and self.guild_locks[guild_id].locked():
                log.debug(f"Releasing lock for GID {guild_id} in _play_next")
                self.guild_locks[guild_id].release()

    def _playback_finished_callback(self, guild_id: int, vc: discord.VoiceClient, error: Optional[Exception]):
        """Generic callback executed by discord.py after vc.play() finishes (used by music items)."""
        log.debug(f"_playback_finished_callback: GID {guild_id}. Error: {error}")
        if error: log.error(f"Playback error reported in generic callback for GID {guild_id}: {error}", exc_info=error)
        self.bot.loop.call_soon_threadsafe(
            lambda: self.bot.loop.create_task(self._playback_finished_task(guild_id, vc, error))
        )

    async def _playback_finished_task(self, guild_id: int, vc: discord.VoiceClient, error: Optional[Exception]):
        """Async task handler for playback completion."""
        log.debug(f"Async finish handler task started for GID {guild_id}. Error: {error}")
        schedule_next_play = False

        current_vc_check = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
        if not current_vc_check or not current_vc_check.is_connected():
            log.warning(f"Finish handler: VC disconnected for GID {guild_id} before task execution. Cleaning up state.")
            self.currently_playing.pop(guild_id, None)
            self.guild_queues.pop(guild_id, None)
            self.playback_mode[guild_id] = PlaybackMode.IDLE
            self._cancel_idle_timer(guild_id)
            return

        vc = current_vc_check

        async with self.guild_locks[guild_id]:
            log.debug(f"Finish handler acquired lock for GID {guild_id}.")
            current_mode = self.playback_mode.get(guild_id, PlaybackMode.IDLE)
            log.debug(f"Finish handler check: GID {guild_id}, Mode: {current_mode}")

            if current_mode == PlaybackMode.QUEUE:
                 log.debug(f"Finish handler: Mode is QUEUE, setting flag to schedule _play_next for GID {guild_id}.")
                 schedule_next_play = True
            elif current_mode == PlaybackMode.SINGLE_SOUND:
                 log.warning(f"Finish handler: Mode was still SINGLE_SOUND for GID {guild_id}. Reverting to IDLE.")
                 self.playback_mode[guild_id] = PlaybackMode.IDLE
                 if not vc.is_playing(): self._start_idle_timer(guild_id, vc)
            else: # IDLE
                 log.debug(f"Finish handler: Mode is {current_mode}. No queue playback scheduled. Checking idle timer.")
                 if not self.is_playing(guild_id) and not self.guild_queues.get(guild_id):
                     self._start_idle_timer(guild_id, vc)

            log.debug(f"Finish handler releasing lock for GID {guild_id}.")
        # --- Lock released ---

        if schedule_next_play:
            log.debug(f"Finish handler: Scheduling separate _play_next task for GID {guild_id}.")
            self.bot.loop.create_task(self._play_next(guild_id, vc), name=f"PlayNextTask_Finish_{guild_id}")
        else:
             log.debug(f"Finish handler: No need to schedule _play_next for GID {guild_id}.")

    def _start_idle_timer(self, guild_id: int, vc: discord.VoiceClient):
        """Starts or resets the idle disconnect timer."""
        if IDLE_TIMEOUT_SECONDS <= 0: return
        self._cancel_idle_timer(guild_id)
        log.debug(f"Starting idle timer ({IDLE_TIMEOUT_SECONDS}s) for GID {guild_id}")
        self.idle_timers[guild_id] = self.bot.loop.create_task(
            self._idle_task(guild_id, vc), name=f"IdleTask_{guild_id}"
        )

    def _cancel_idle_timer(self, guild_id: int):
        """Cancels the idle timer if it exists."""
        timer_task = self.idle_timers.pop(guild_id, None)
        if timer_task and not timer_task.done():
            log.debug(f"Cancelling idle timer for GID {guild_id}")
            timer_task.cancel()

    async def _idle_task(self, guild_id: int, vc: discord.VoiceClient):
        """The task that waits and then checks for idle disconnect."""
        try:
             await asyncio.sleep(IDLE_TIMEOUT_SECONDS)
             log.info(f"Idle timer expired for GID {guild_id}. Checking state...")
             async with self.guild_locks[guild_id]:
                 current_vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
                 if (current_vc and vc.channel and current_vc.channel == vc.channel and
                     not self.is_playing(guild_id) and
                     not self.guild_queues.get(guild_id) and
                     self.playback_mode.get(guild_id, PlaybackMode.IDLE) == PlaybackMode.IDLE):
                     log.info(f"Bot is idle in GID {guild_id}. Disconnecting.")
                     self.bot.loop.create_task(self.safe_disconnect(vc, manual_leave=False, reason="Idle timeout"))
                 else:
                     log.debug(f"Idle timer expired for GID {guild_id}, but conditions changed. No action needed.")
        except asyncio.CancelledError:
             log.debug(f"Idle timer task cancelled for GID {guild_id}.")
        except Exception as e:
             log.error(f"Error in idle timer task for GID {guild_id}: {e}", exc_info=True)
        finally:
             self.idle_timers.pop(guild_id, None)

    async def play_single_sound(
        self, interaction: discord.Interaction, sound_path: str, display_name: Optional[str] = None
    ) -> bool:
        """Plays a single audio file (from path) immediately, interrupting the queue."""
        if not interaction or not interaction.guild or not isinstance(interaction.user, discord.Member) or not interaction.user.voice:
             log.warning("play_single_sound called with invalid interaction state.")
             if interaction: await self._try_respond(interaction, "❌ Cannot play sound: Invalid user or voice state.", ephemeral=True)
             return False
        guild, user, target_channel = interaction.guild, interaction.user, user.voice.channel
        guild_id = guild.id
        sound_basename = os.path.basename(sound_path)
        log_display_name = display_name or sound_basename
        log.info(f"Request to play single sound file '{log_display_name}' in GID {guild_id}")
        if not os.path.exists(sound_path):
            log.error(f"Single sound file not found: {sound_path}")
            await self._try_respond(interaction, "❌ Internal error: Could not find the audio file to play.", ephemeral=True)
            return False

        lock_acquired, audio_buffer = False, None
        try:
            await asyncio.wait_for(self.guild_locks[guild_id].acquire(), timeout=10.0)
            lock_acquired = True
            log.debug(f"Acquired lock for GID {guild_id} in play_single_sound")
            vc = await self.ensure_voice_client(interaction, target_channel, "SINGLE SOUND")
            if not vc: return False

            log.debug(f"Processing single sound file '{sound_basename}' using audio_processor...")
            try:
                audio_source, audio_buffer = audio_processor.process_audio(sound_path)
                log.debug(f"Audio processor result for '{sound_basename}': Source valid={audio_source is not None}, Buffer valid={audio_buffer is not None}")
            except Exception as proc_err:
                 log.error(f"Exception during audio_processor.process_audio for '{sound_path}' in play_single_sound: {proc_err}", exc_info=True)
                 audio_source, audio_buffer = None, None

            if not audio_source or not audio_buffer:
                 log.error(f"Failed to process single sound file '{sound_path}' for GID {guild_id}")
                 await self._try_respond(interaction, "❌ Error processing the audio file.", ephemeral=True)
                 if audio_buffer and not audio_buffer.closed:
                     # --- CORRECTED SYNTAX ---
                     try: audio_buffer.close()
                     except Exception: pass
                     # ------------------------
                 return False
            log.debug(f"Audio processed successfully for '{sound_basename}'.")

            original_mode = self.playback_mode.get(guild_id, PlaybackMode.IDLE)
            self.playback_mode[guild_id] = PlaybackMode.SINGLE_SOUND
            log.debug(f"Set playback mode to SINGLE_SOUND for GID {guild_id}")
            if vc.is_playing(): vc.stop()
            self._cancel_idle_timer(guild_id)

            def single_sound_finished(error: Optional[Exception]):
                async def async_cleanup():
                    gid_cb, original_mode_cb, path_cb, buffer_cb = guild_id, original_mode, sound_path, audio_buffer
                    log.debug(f"single_sound_finished callback triggered for GID {gid_cb}. Sound: {os.path.basename(path_cb)}. Error: {error}")
                    if buffer_cb:
                         if not buffer_cb.closed:
                            # --- CORRECTED SYNTAX ---
                            try: buffer_cb.close()
                            except Exception as e: log.warning(f"Error closing single sound (file) buffer: {e}")
                            # ------------------------
                         log.debug(f"Closed buffer for single sound file '{os.path.basename(path_cb)}' in GID {gid_cb}")
                    else: log.warning(f"Single sound (file) finished callback missing buffer for GID {gid_cb}")
                    stored_buffer = self.active_single_buffers.pop(gid_cb, None)
                    if stored_buffer != buffer_cb and stored_buffer is not None:
                         log.warning(f"Mismatched buffer found/removed in active_single_buffers for GID {gid_cb}")
                         if not stored_buffer.closed:
                            # --- CORRECTED SYNTAX ---
                            try: stored_buffer.close()
                            except Exception: pass
                            # ------------------------
                    async with self.guild_locks[gid_cb]:
                        if self.playback_mode.get(gid_cb) == PlaybackMode.SINGLE_SOUND:
                             self.playback_mode[gid_cb] = original_mode_cb
                             log.info(f"Reverted playback mode to {original_mode_cb} for GID {gid_cb} after single sound file.")
                             current_vc_cb = discord.utils.get(self.bot.voice_clients, guild__id=gid_cb)
                             if current_vc_cb and current_vc_cb.is_connected():
                                 if original_mode_cb == PlaybackMode.QUEUE and self.guild_queues.get(gid_cb):
                                     log.info(f"Attempting to resume queue playback for GID {gid_cb}.")
                                     self.bot.loop.create_task(self._play_next(gid_cb, current_vc_cb), name=f"PlayNextTask_SingleFinish_{gid_cb}")
                                 elif not self.is_playing(gid_cb):
                                     log.info(f"Single sound file finished, no queue/originally idle for GID {gid_cb}. Starting idle timer.")
                                     self._start_idle_timer(gid_cb, current_vc_cb)
                             else: log.warning(f"Cannot resume queue/start timer for GID {gid_cb} after single sound, VC disconnected.")
                        else: log.warning(f"Single sound (file) finished for GID {gid_cb}, but mode was already {self.playback_mode.get(gid_cb)}. Not reverting/resuming.")
                    if error: log.error(f"Error during single sound file playback for GID {gid_cb}: {error}", exc_info=error)
                asyncio.run_coroutine_threadsafe(async_cleanup(), self.bot.loop)
            # --- End callback ---

            old_buffer = self.active_single_buffers.pop(guild_id, None)
            if old_buffer and not old_buffer.closed:
                # --- CORRECTED SYNTAX ---
                try: old_buffer.close()
                except Exception: pass
                # ------------------------
            self.active_single_buffers[guild_id] = audio_buffer

            vc.play(audio_source, after=single_sound_finished)
            log.info(f"Started playing single sound file '{log_display_name}' in GID {guild_id}")
            await self._try_respond(interaction, f"▶️ Playing `{log_display_name}`...", ephemeral=False)
            return True
        except asyncio.TimeoutError:
            log.error(f"Timeout acquiring lock for GID {guild_id} in play_single_sound.")
            await self._try_respond(interaction, "❌ Could not acquire playback lock.", ephemeral=True)
            if audio_buffer and not audio_buffer.closed:
                # --- CORRECTED SYNTAX ---
                try: audio_buffer.close()
                except Exception: pass
                # ------------------------
            return False
        except discord.ClientException as e:
             log.error(f"ClientException during single sound file playback: {e}", exc_info=True)
             await self._try_respond(interaction, f"❌ Playback error: {e}", ephemeral=True)
             self.playback_mode[guild_id] = PlaybackMode.IDLE
             if audio_buffer and not audio_buffer.closed:
                 # --- CORRECTED SYNTAX ---
                 try: audio_buffer.close()
                 except Exception: pass
                 # ------------------------
             self.active_single_buffers.pop(guild_id, None)
             return False
        except Exception as e:
            log.error(f"Unexpected error in play_single_sound: {e}", exc_info=True)
            self.playback_mode[guild_id] = PlaybackMode.IDLE
            if audio_buffer and not audio_buffer.closed:
                # --- CORRECTED SYNTAX ---
                try: audio_buffer.close()
                except Exception: pass
                # ------------------------
            self.active_single_buffers.pop(guild_id, None)
            await self._try_respond(interaction, "❌ Unexpected error playing sound.", ephemeral=True)
            return False
        finally:
            if lock_acquired and self.guild_locks[guild_id].locked():
                log.debug(f"Releasing lock for GID {guild_id} in play_single_sound")
                self.guild_locks[guild_id].release()

    async def play_audio_source_now(
        self, interaction: discord.Interaction, audio_source: discord.PCMAudio, audio_buffer_to_close: io.BytesIO, display_name: Optional[str] = None
    ) -> bool:
        """Plays a prepared audio source (e.g., from TTS) immediately."""
        if not interaction or not interaction.guild or not isinstance(interaction.user, discord.Member) or not interaction.user.voice:
             log.warning("play_audio_source_now called with invalid interaction state.")
             if interaction: await self._try_respond(interaction, "❌ Invalid user/voice state.", ephemeral=True)
             if audio_buffer_to_close and not audio_buffer_to_close.closed:
                 # --- CORRECTED SYNTAX ---
                 try: audio_buffer_to_close.close()
                 except Exception: pass
                 # ------------------------
             return False
        guild, user, target_channel = interaction.guild, interaction.user, user.voice.channel
        guild_id = guild.id
        log_display_name = display_name or "Audio Source"
        log.info(f"Request to play single audio source '{log_display_name}' in GID {guild_id}")
        if not audio_source or not audio_buffer_to_close:
            log.error(f"play_audio_source_now called with missing audio_source or buffer for GID {guild_id}")
            await self._try_respond(interaction, "❌ Internal error: Missing audio data.", ephemeral=True)
            if audio_buffer_to_close and not audio_buffer_to_close.closed:
                # --- CORRECTED SYNTAX ---
                try: audio_buffer_to_close.close()
                except Exception: pass
                # ------------------------
            return False

        lock_acquired = False
        try:
            await asyncio.wait_for(self.guild_locks[guild_id].acquire(), timeout=10.0)
            lock_acquired = True
            log.debug(f"Acquired lock for GID {guild_id} in play_audio_source_now")
            vc = await self.ensure_voice_client(interaction, target_channel, "DIRECT AUDIO PLAY")
            if not vc:
                if audio_buffer_to_close and not audio_buffer_to_close.closed:
                    # --- CORRECTED SYNTAX ---
                    try: audio_buffer_to_close.close()
                    except Exception: pass
                    # ------------------------
                return False

            original_mode = self.playback_mode.get(guild_id, PlaybackMode.IDLE)
            self.playback_mode[guild_id] = PlaybackMode.SINGLE_SOUND
            log.debug(f"Set playback mode to SINGLE_SOUND for GID {guild_id} (direct source)")
            if vc.is_playing(): vc.stop()
            self._cancel_idle_timer(guild_id)

            def direct_source_finished(error: Optional[Exception]):
                async def async_cleanup():
                    gid_cb, original_mode_cb, buffer_cb = guild_id, original_mode, audio_buffer_to_close
                    log.debug(f"direct_source_finished callback triggered for GID {gid_cb}. Source: {log_display_name}. Error: {error}")
                    if buffer_cb:
                         if not buffer_cb.closed:
                            # --- CORRECTED SYNTAX ---
                            try: buffer_cb.close()
                            except Exception as e: log.warning(f"Error closing direct source buffer: {e}")
                            # ------------------------
                         log.debug(f"Closed buffer for direct source play in GID {gid_cb}")
                    else: log.warning(f"Direct source finished callback missing buffer for GID {gid_cb}")
                    async with self.guild_locks[gid_cb]:
                        if self.playback_mode.get(gid_cb) == PlaybackMode.SINGLE_SOUND:
                             self.playback_mode[gid_cb] = original_mode_cb
                             log.info(f"Reverted playback mode to {original_mode_cb} for GID {gid_cb} after direct source.")
                             current_vc_cb = discord.utils.get(self.bot.voice_clients, guild__id=gid_cb)
                             if current_vc_cb and current_vc_cb.is_connected():
                                 if original_mode_cb == PlaybackMode.QUEUE and self.guild_queues.get(gid_cb):
                                     log.info(f"Attempting to resume queue playback for GID {gid_cb}.")
                                     self.bot.loop.create_task(self._play_next(gid_cb, current_vc_cb), name=f"PlayNextTask_DirectFinish_{gid_cb}")
                                 elif not self.is_playing(gid_cb):
                                     log.info(f"Direct source finished, no queue/originally idle for GID {gid_cb}. Starting idle timer.")
                                     self._start_idle_timer(gid_cb, current_vc_cb)
                             else: log.warning(f"Cannot resume queue/start timer for GID {gid_cb} after direct source, VC disconnected.")
                        else: log.warning(f"Direct source finished for GID {gid_cb}, but mode was already {self.playback_mode.get(gid_cb)}. Not reverting/resuming.")
                    if error: log.error(f"Error during direct source playback for GID {gid_cb}: {error}", exc_info=error)
                asyncio.run_coroutine_threadsafe(async_cleanup(), self.bot.loop)
            # --- End callback ---

            vc.play(audio_source, after=direct_source_finished)
            log.info(f"Started playing direct audio source '{log_display_name}' in GID {guild_id}")
            await self._try_respond(interaction, f"🗣️ Playing `{log_display_name}`...", ephemeral=False)
            return True
        except asyncio.TimeoutError:
            log.error(f"Timeout acquiring lock for GID {guild_id} in play_audio_source_now.")
            await self._try_respond(interaction, "❌ Could not acquire playback lock.", ephemeral=True)
            if audio_buffer_to_close and not audio_buffer_to_close.closed:
                # --- CORRECTED SYNTAX ---
                try: audio_buffer_to_close.close()
                except Exception: pass
                # ------------------------
            return False
        except discord.ClientException as e:
             log.error(f"ClientException during direct source playback: {e}", exc_info=True)
             await self._try_respond(interaction, f"❌ Playback error: {e}", ephemeral=True)
             self.playback_mode[guild_id] = PlaybackMode.IDLE
             if audio_buffer_to_close and not audio_buffer_to_close.closed:
                 # --- CORRECTED SYNTAX ---
                 try: audio_buffer_to_close.close()
                 except Exception: pass
                 # ------------------------
             return False
        except Exception as e:
            log.error(f"Unexpected error in play_audio_source_now: {e}", exc_info=True)
            self.playback_mode[guild_id] = PlaybackMode.IDLE
            if audio_buffer_to_close and not audio_buffer_to_close.closed:
                # --- CORRECTED SYNTAX ---
                try: audio_buffer_to_close.close()
                except Exception: pass
                # ------------------------
            await self._try_respond(interaction, "❌ Unexpected error playing sound.", ephemeral=True)
            return False
        finally:
            if lock_acquired and self.guild_locks[guild_id].locked():
                log.debug(f"Releasing lock for GID {guild_id} in play_audio_source_now")
                self.guild_locks[guild_id].release()

    async def _try_respond(self, interaction: discord.Interaction, message: Optional[str] = None, **kwargs):
        """Helper to respond to an interaction, catching errors if it already expired/responded."""
        if not interaction: return
        content = kwargs.pop('content', message)
        is_ephemeral = kwargs.pop('ephemeral', False)
        delete_after = kwargs.pop('delete_after', None)
        try:
            if interaction.response.is_done():
                await interaction.edit_original_response(content=content, **kwargs) # Cannot set ephemeral/delete_after here
            else:
                await interaction.response.send_message(content=content, ephemeral=is_ephemeral, delete_after=delete_after, **kwargs)
        except discord.NotFound:
            log.warning(f"Interaction response/edit failed (NotFound): {interaction.id}")
        except discord.HTTPException as e:
            if e.code == 40060: # InteractionAlreadyResponded
                 log.warning(f"Interaction response failed (Already Responded): {interaction.id}. Trying followup...")
                 try: await interaction.followup.send(content=content, ephemeral=is_ephemeral, delete_after=delete_after, **kwargs)
                 except discord.NotFound: log.warning(f"Followup failed (NotFound): {interaction.id}")
                 except Exception as followup_e: log.error(f"Error sending followup: {followup_e}", exc_info=True)
            else: log.warning(f"Interaction response/edit failed (HTTPException {e.status} / {e.code}): {interaction.id}")
        except Exception as e:
            log.error(f"Unexpected error responding/editing interaction {interaction.id}: {e}", exc_info=True)

# --- End of PlaybackManager class ---