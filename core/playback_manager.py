# core/playback_manager.py

import discord
import asyncio
import logging
import io
import os
from collections import defaultdict
from typing import Dict, List, Optional, Union, Any # <--- Make sure 'Any' is imported
import time
import functools
from discord.ext import commands
from enum import Enum, auto

# Local application imports
import config
from utils import audio_processor

# --- Define QueueItemType ---
QueueItemType = Any # Define the type alias here
# -----------------------------

log = logging.getLogger('SoundBot.PlaybackManager')

# --- Added PlaybackMode Enum ---
class PlaybackMode(Enum):
    IDLE = auto()
    QUEUE = auto()
    SINGLE_SOUND = auto()
# -----------------------------

IDLE_TIMEOUT_SECONDS = getattr(config, 'MUSIC_IDLE_TIMEOUT', 300)

class PlaybackManager:
    """Manages voice connections, queues, and playback state for guilds."""
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.guild_queues: Dict[int, List[QueueItemType]] = defaultdict(list)
        self.currently_playing: Dict[int, Optional[QueueItemType]] = defaultdict(lambda: None)
        self.guild_locks: Dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.idle_timers: Dict[int, asyncio.Task] = {}
        self.playback_mode: Dict[int, PlaybackMode] = defaultdict(lambda: PlaybackMode.IDLE)
        # Buffer storage specifically for sounds played via play_single_sound
        self.active_single_buffers: Dict[int, io.BytesIO] = {}

    async def ensure_voice_client(
        self,
        interaction: Optional[discord.Interaction],
        target_channel: discord.VoiceChannel,
        action_type: str = "ACTION"
    ) -> Optional[discord.VoiceClient]:
        """
        Ensures the bot is connected to the target voice channel.
        Connects if not connected, moves if necessary and possible.
        Returns the VoiceClient on success, None on failure.
        Sends feedback via interaction if provided.
        """
        guild = target_channel.guild
        current_vc = discord.utils.get(self.bot.voice_clients, guild=guild)
        guild_id = guild.id # Added for logging consistency

        if current_vc:
            if current_vc.channel == target_channel:
                log.debug(f"Already connected to {target_channel.name} in GID:{guild_id}")
                return current_vc
            else:
                my_perms = target_channel.permissions_for(guild.me)
                if not my_perms.connect or not my_perms.speak:
                    log.warning(f"Missing permissions to move/speak in {target_channel.name} (GID:{guild_id})")
                    if interaction: await self._try_respond(interaction, f"❌ I don't have permissions to join or speak in {target_channel.mention}.", ephemeral=True)
                    return None
                try:
                    log.info(f"Moving from {current_vc.channel.name} to {target_channel.name} (GID:{guild_id}) for {action_type}")
                    await current_vc.move_to(target_channel)
                    return current_vc
                except asyncio.TimeoutError:
                    log.error(f"Timeout moving to {target_channel.name} (GID:{guild_id})")
                    if interaction: await self._try_respond(interaction, "❌ Timed out trying to move voice channels.", ephemeral=True)
                    return None
                except Exception as e:
                    log.error(f"Error moving to {target_channel.name} (GID:{guild_id}): {e}", exc_info=True)
                    if interaction: await self._try_respond(interaction, "❌ An error occurred while moving voice channels.", ephemeral=True)
                    return None
        else:
            my_perms = target_channel.permissions_for(guild.me)
            if not my_perms.connect or not my_perms.speak:
                log.warning(f"Missing permissions to connect/speak in {target_channel.name} (GID:{guild_id})")
                if interaction: await self._try_respond(interaction, f"❌ I don't have permissions to join or speak in {target_channel.mention}.", ephemeral=True)
                return None
            try:
                log.info(f"Connecting to {target_channel.name} (GID:{guild_id}) for {action_type}")
                vc = await target_channel.connect()
                return vc
            except asyncio.TimeoutError:
                log.error(f"Timeout connecting to {target_channel.name} (GID:{guild_id})")
                if interaction: await self._try_respond(interaction, "❌ Timed out trying to connect to the voice channel.", ephemeral=True)
                return None
            except discord.ClientException as e:
                # Handle cases where the bot might already be connecting
                log.error(f"Discord ClientException connecting to {target_channel.name} (GID:{guild_id}): {e}")
                if "already connected" in str(e).lower():
                     if interaction: await self._try_respond(interaction, "⏳ Already connecting/connected to a voice channel. Please wait.", ephemeral=True)
                elif "already connecting" in str(e).lower(): # Discord.py can sometimes use this phrase too
                     if interaction: await self._try_respond(interaction, "⏳ Already connecting to the voice channel. Please wait.", ephemeral=True)
                else:
                     if interaction: await self._try_respond(interaction, f"❌ Error connecting: {e}", ephemeral=True)
                return None
            except Exception as e:
                log.error(f"Unexpected error connecting to {target_channel.name} (GID:{guild_id}): {e}", exc_info=True)
                if interaction: await self._try_respond(interaction, "❌ An unexpected error occurred while connecting.", ephemeral=True)
                return None

    async def safe_disconnect(self, vc: discord.VoiceClient, manual_leave: bool = False, reason: str = "Unknown"):
        """Stops playback, clears state, cancels timers and disconnects."""
        if not vc or not vc.guild:
            log.warning("safe_disconnect called with invalid VC")
            return

        guild_id = vc.guild.id
        log.info(f"Initiating safe disconnect for GID:{guild_id}. Reason: {reason}")

        # Cancel the main bot leave timer (handled by voice_helpers)
        # Import voice_helpers if needed at the top
        # from utils import voice_helpers
        # voice_helpers.cancel_leave_timer(self.bot, guild_id, reason=f"safe_disconnect ({reason})")

        async with self.guild_locks[guild_id]:
            if vc.is_playing():
                log.debug(f"Stopping active player for GID:{guild_id} during disconnect.")
                vc.stop()

            # Clear specific playback manager state
            self.currently_playing.pop(guild_id, None)
            self.guild_queues.pop(guild_id, None)
            self.playback_mode[guild_id] = PlaybackMode.IDLE
            self._cancel_idle_timer(guild_id) # Cancel music-specific idle timer

            # Clean up any lingering single-play buffers
            buffer = self.active_single_buffers.pop(guild_id, None)
            if buffer and not buffer.closed:
                try: buffer.close()
                except Exception: pass
                log.debug(f"Closed lingering single-play buffer during disconnect for GID:{guild_id}")


            log.debug(f"Cleared playback state for GID:{guild_id}")

            try:
                # Check connection again *inside* lock before disconnect
                current_vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
                if current_vc and current_vc.is_connected():
                    await current_vc.disconnect(force=False)
                    log.info(f"Successfully disconnected from voice in GID:{guild_id}.")
                else:
                    log.info(f"Already disconnected before final disconnect call in GID:{guild_id}")

            except Exception as e:
                log.error(f"Error during voice client disconnect for GID:{guild_id}: {e}", exc_info=True)


    def get_queue(self, guild_id: int) -> List[QueueItemType]:
        """Returns the queue for a given guild."""
        return self.guild_queues.get(guild_id, [])

    def get_current_item(self, guild_id: int) -> Optional[QueueItemType]:
        """Returns the currently playing item for a given guild."""
        return self.currently_playing.get(guild_id)

    async def add_to_queue(self, guild_id: int, item: QueueItemType) -> int:
        """Adds an item to the end of the guild's queue. Returns new queue position."""
        async with self.guild_locks[guild_id]:
            queue = self.guild_queues[guild_id]
            queue.append(item)
            position = len(queue)
            log.debug(f"Added item to queue for GID {guild_id}. New length: {position}. Item Type: {type(item).__name__}")

            # Automatically trigger playback check if conditions are met
            vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
            if vc and vc.is_connected() and not self.is_playing(guild_id) and position == 1:
                 # Check if the current mode allows queue playback
                 if self.playback_mode[guild_id] in [PlaybackMode.IDLE, PlaybackMode.QUEUE]:
                    log.info(f"Queue was empty/idle, new item added. Triggering playback check for GID {guild_id}.")
                    # Ensure the mode is set correctly before starting
                    self.playback_mode[guild_id] = PlaybackMode.QUEUE
                    self.bot.loop.create_task(self._play_next(guild_id, vc))
                 else:
                    log.debug(f"Item added to queue for GID {guild_id}, but mode is {self.playback_mode[guild_id]}, not starting playback automatically.")

            return position


    async def insert_into_queue(self, guild_id: int, index: int, item: QueueItemType):
        """Inserts an item at a specific index in the guild's queue."""
        async with self.guild_locks[guild_id]:
            queue = self.guild_queues[guild_id]
            # Clamp index to valid range
            index = max(0, min(index, len(queue)))
            queue.insert(index, item)
            log.debug(f"Inserted item at index {index} for GID {guild_id}. New length: {len(queue)}")

            # Trigger playback check if inserted at front and idle
            vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
            if index == 0 and vc and vc.is_connected() and not self.is_playing(guild_id):
                if self.playback_mode[guild_id] in [PlaybackMode.IDLE, PlaybackMode.QUEUE]:
                    log.info(f"Item inserted at front of idle queue. Triggering playback check for GID {guild_id}.")
                    self.playback_mode[guild_id] = PlaybackMode.QUEUE
                    self.bot.loop.create_task(self._play_next(guild_id, vc))
                else:
                     log.debug(f"Item inserted at front for GID {guild_id}, but mode is {self.playback_mode[guild_id]}, not starting playback automatically.")


    async def remove_from_queue(self, guild_id: int, index: int) -> Optional[QueueItemType]:
        """Removes an item from the queue by index. Returns the removed item or None."""
        async with self.guild_locks[guild_id]:
            queue = self.guild_queues.get(guild_id)
            if queue and 0 <= index < len(queue):
                removed_item = queue.pop(index)
                log.debug(f"Removed item at index {index} for GID {guild_id}.")
                # Add cleanup logic here if removed item needs it (e.g., delete temp file)
                # Consider adding type checks if queue can hold different types
                return removed_item
            else:
                log.warning(f"Attempted to remove item at invalid index {index} for GID {guild_id}. Queue length: {len(queue) if queue else 0}")
                return None

    async def clear_queue(self, guild_id: int):
        """Clears the queue for a specific guild."""
        async with self.guild_locks[guild_id]:
            if guild_id in self.guild_queues:
                count = len(self.guild_queues[guild_id])
                # Add cleanup for each item if needed before clearing
                # for item in self.guild_queues[guild_id]: ...
                self.guild_queues.pop(guild_id, None)
                log.info(f"Cleared queue ({count} items) for GID {guild_id}")
            else:
                log.debug(f"Queue already empty or non-existent for GID {guild_id}, clear request ignored.")

    def is_playing(self, guild_id: int) -> bool:
        """Checks if the bot is currently actively playing audio in a guild."""
        vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
        # Check VC playing status AND if we have a currently_playing item tracked
        return bool(vc and vc.is_playing() and guild_id in self.currently_playing and self.currently_playing[guild_id] is not None)

    async def start_playback_if_idle(self, guild_id: int):
        """Checks if playback should start (connected, not playing, queue has items) and initiates it."""
        vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
        # Acquire lock to prevent race conditions with adding/removing items
        async with self.guild_locks[guild_id]:
            # Check conditions *inside* the lock
            if vc and vc.is_connected() and not self.is_playing(guild_id) and self.guild_queues.get(guild_id):
                # Ensure playback mode allows starting queue playback
                if self.playback_mode[guild_id] in [PlaybackMode.IDLE, PlaybackMode.QUEUE]:
                    log.info(f"Playback idle for GID {guild_id}, queue not empty. Starting playback loop.")
                    self.playback_mode[guild_id] = PlaybackMode.QUEUE # Set mode explicitly
                    self.bot.loop.create_task(self._play_next(guild_id, vc))
                else:
                    log.debug(f"Start playback check for GID {guild_id}: Mode is {self.playback_mode[guild_id]}, not starting.")
            elif vc and vc.is_connected() and not self.is_playing(guild_id) and not self.guild_queues.get(guild_id):
                log.debug(f"Start playback check for GID {guild_id}: Queue is empty, ensuring idle timer starts.")
                self._start_idle_timer(guild_id, vc)

    async def skip_track(self, guild_id: int) -> bool:
        """Stops the current track, letting the 'after' callback handle the next."""
        vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
        # Use lock to ensure state consistency during skip
        async with self.guild_locks[guild_id]:
            if self.is_playing(guild_id) and vc:
                log.info(f"Skipping track for GID {guild_id}")
                # vc.stop() triggers the 'after' callback, which will call _play_next
                vc.stop()
                # Note: currently_playing is cleared within _play_next or the callback chain
                return True
            else:
                log.warning(f"Skip requested for GID {guild_id}, but nothing is playing.")
                return False

    async def stop_playback(self, guild_id: int, clear_queue: bool = True, leave_channel: bool = True):
        """Stops playback completely, optionally clears queue and leaves channel."""
        log.info(f"Received stop command for GID {guild_id}. Clear: {clear_queue}, Leave: {leave_channel}")
        vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)

        async with self.guild_locks[guild_id]:
            # Set mode to IDLE immediately to prevent further automatic playback starts
            self.playback_mode[guild_id] = PlaybackMode.IDLE
            self._cancel_idle_timer(guild_id) # Cancel music idle timer

            if vc and vc.is_playing():
                log.debug(f"Stopping player for GID {guild_id} due to stop command.")
                vc.stop() # This will trigger the 'after' callback, but the IDLE mode should prevent restart

            # Clear playback state immediately
            self.currently_playing.pop(guild_id, None)

            if clear_queue:
                if guild_id in self.guild_queues:
                    count = len(self.guild_queues[guild_id])
                    # TODO: Add cleanup for items being cleared if necessary
                    self.guild_queues.pop(guild_id, None)
                    log.info(f"Cleared queue ({count} items) for GID {guild_id} due to stop command.")

            # Clean up any single-play buffers that might be lingering (less likely here, but safe)
            buffer = self.active_single_buffers.pop(guild_id, None)
            if buffer and not buffer.closed:
                try: buffer.close()
                except Exception: pass
                log.debug(f"Closed lingering single-play buffer during stop command for GID:{guild_id}")

            if leave_channel and vc and vc.is_connected():
                 # Call safe_disconnect for proper handling, including general leave timer cancellation
                 # Use create_task to avoid blocking the lock if disconnect is slow
                 self.bot.loop.create_task(self.safe_disconnect(vc, manual_leave=True, reason="stop_playback command"))
            elif vc and vc.is_connected():
                # If not leaving, but queue is clear and not playing, start the idle timer
                if not self.guild_queues.get(guild_id) and not vc.is_playing():
                    self._start_idle_timer(guild_id, vc)


    async def _play_next(self, guild_id: int, vc: discord.VoiceClient):
        """The core loop that plays the next available song in the queue."""
        log.debug(f"Entered _play_next for GID {guild_id}")
        lock_acquired = False
        try:
            # Use a shorter timeout for the lock acquisition in the play loop
            await asyncio.wait_for(self.guild_locks[guild_id].acquire(), timeout=5.0)
            lock_acquired = True
            log.debug(f"Acquired lock for GID {guild_id} in _play_next")

            # --- Critical Check: Ensure correct PlaybackMode ---
            # If mode changed while waiting for lock, abort queue playback
            if self.playback_mode[guild_id] != PlaybackMode.QUEUE:
                 log.info(f"Playback mode changed to {self.playback_mode[guild_id]} during _play_next for GID {guild_id}. Aborting queue playback.")
                 # If now idle, start idle timer
                 if self.playback_mode[guild_id] == PlaybackMode.IDLE and vc.is_connected() and not vc.is_playing():
                      self._start_idle_timer(guild_id, vc)
                 return # Exit the function, do not proceed with queue
            # -----------------------------------------------------


            # Clean up the item that just finished (if any)
            last_item = self.currently_playing.pop(guild_id, None)
            if last_item:
                log.debug(f"Cleaned up previously playing item tracker for GID: {guild_id}. Type: {type(last_item).__name__}")
                # Update last played timestamp for music items if applicable
                if hasattr(last_item, 'last_played_at'):
                    last_item.last_played_at = time.time()
                # Add specific cleanup for other item types if needed

            # Check Voice Client status
            if not vc or not vc.is_connected():
                log.warning(f"VC disconnected before _play_next could run for GID {guild_id}. Aborting playback.")
                # Clear state related to this guild
                self.guild_queues.pop(guild_id, None)
                self.currently_playing.pop(guild_id, None)
                self.playback_mode[guild_id] = PlaybackMode.IDLE
                self._cancel_idle_timer(guild_id)
                return

            queue = self.guild_queues.get(guild_id)
            if not queue:
                log.info(f"Queue empty for GID {guild_id}. Playback finished.")
                self.currently_playing.pop(guild_id, None) # Ensure clear if somehow missed
                self.playback_mode[guild_id] = PlaybackMode.IDLE
                self._start_idle_timer(guild_id, vc)
                return

            # --- Loop to find the next playable item ---
            next_item_played = False
            while queue:
                item_to_try = queue[0]
                item_type = type(item_to_try).__name__
                log.debug(f"Checking queue item for GID {guild_id}: Type: {item_type}")

                # --- Handle MusicQueueItem (adjust if other types exist) ---
                if hasattr(item_to_try, 'download_status') and hasattr(item_to_try, 'get_playback_source'):
                    status = item_to_try.download_status
                    title = getattr(item_to_try, 'title', 'Unknown Title')
                    log.debug(f"Music Item: '{title[:50]}' Status: {status}")

                    if status == "ready":
                        audio_source = item_to_try.get_playback_source()
                        if audio_source:
                            dequeued_item = queue.pop(0)
                            self.currently_playing[guild_id] = dequeued_item
                            self._cancel_idle_timer(guild_id) # Cancel timer while playing music
                            # Ensure mode is QUEUE before playing
                            self.playback_mode[guild_id] = PlaybackMode.QUEUE
                            after_callback = functools.partial(self._playback_finished_callback, guild_id, vc)
                            log.info(f"Playing '{title}' in GID {guild_id}")
                            vc.play(audio_source, after=after_callback)
                            next_item_played = True
                            break # Exit the while loop, playback started
                        else:
                            log.error(f"Item '{title}' status is 'ready' but failed to get audio source (file missing/corrupt?). Skipping. GID: {guild_id}")
                            item_to_try.download_status = "failed" # Mark as failed
                            queue.pop(0) # Remove from queue
                            continue # Try next item
                    elif status == "failed":
                        log.warning(f"Skipping previously failed item in queue: '{title}'. GID: {guild_id}")
                        queue.pop(0) # Remove from queue
                        continue # Try next item
                    elif status in ["pending", "downloading"]:
                        log.info(f"Next item '{title}' not ready (Status: {status}). Waiting for downloader. GID {guild_id}")
                        break # Exit while loop, wait for downloader task to make it ready
                    else:
                        log.error(f"Unknown download status '{status}' for item '{title}'. Skipping. GID: {guild_id}")
                        item_to_try.download_status = "failed" # Mark as failed
                        queue.pop(0) # Remove from queue
                        continue # Try next item

                # --- Handle simple path items (e.g., from join events) ---
                elif isinstance(item_to_try, tuple) and len(item_to_try) == 3 and isinstance(item_to_try[1], str):
                    member, sound_path, is_temp_tts = item_to_try
                    sound_basename = os.path.basename(sound_path)
                    log.debug(f"Join Sound Item: '{sound_basename}' for {member.display_name}")

                    # Process the audio using the processor
                    audio_source, audio_buffer = audio_processor.process_audio(sound_path)

                    if audio_source and audio_buffer:
                         dequeued_item_tuple = queue.pop(0)
                         # Store the buffer for cleanup in the callback
                         # Use a dedicated storage or handle differently if conflicts arise
                         self.active_single_buffers[guild_id] = audio_buffer
                         # Track what's playing (can store the tuple or a simplified object)
                         self.currently_playing[guild_id] = dequeued_item_tuple # Store the tuple itself
                         self._cancel_idle_timer(guild_id) # Cancel timer while playing join sound
                         self.playback_mode[guild_id] = PlaybackMode.QUEUE # Still part of queue processing

                         # Create callback that also handles buffer cleanup
                         def after_join_sound(error):
                             gid_cb = guild_id
                             path_cb = sound_path
                             temp_cb = is_temp_tts
                             buffer_cb = audio_buffer # Capture buffer in closure

                             # Clean up buffer first
                             if buffer_cb and not buffer_cb.closed:
                                 try: buffer_cb.close()
                                 except Exception: pass
                                 log.debug(f"Closed buffer for join sound {os.path.basename(path_cb)} in GID {gid_cb}")

                             # Clean up temp file if needed
                             if temp_cb and path_cb and os.path.exists(path_cb):
                                 try: os.remove(path_cb)
                                 except Exception as e: log.warning(f"Failed to delete temp join sound {path_cb}: {e}")

                             # Trigger the main finish handler
                             self.bot.loop.create_task(self._playback_finished_task(gid_cb, vc, error))


                         log.info(f"Playing join sound '{sound_basename}' for {member.display_name} in GID {guild_id}")
                         vc.play(audio_source, after=after_join_sound)
                         next_item_played = True
                         break # Exit while loop

                    else:
                         log.error(f"Failed to process join sound '{sound_basename}' for {member.display_name}. Skipping. GID: {guild_id}")
                         queue.pop(0) # Remove broken item
                         # Cleanup temp file immediately if it failed processing
                         if is_temp_tts and os.path.exists(sound_path):
                             try: os.remove(sound_path)
                             except Exception as e: log.warning(f"Failed to delete failed temp join sound {sound_path}: {e}")
                         continue # Try next item

                else:
                    log.error(f"Unknown item type in queue for GID {guild_id}: {item_to_try}. Skipping.")
                    queue.pop(0) # Remove unknown item
                    continue # Try next item
            # --- End of while loop ---

            # --- Post-loop checks ---
            if not next_item_played:
                if not queue:
                    # Queue became empty without playing anything (e.g., all items failed/skipped)
                    log.info(f"Processed queue for GID {guild_id}, no playable items found, queue now empty.")
                    self.currently_playing.pop(guild_id, None)
                    self.playback_mode[guild_id] = PlaybackMode.IDLE
                    self._start_idle_timer(guild_id, vc)
                else:
                    # Loop ended, but queue still has items (likely waiting for download)
                    log.debug(f"Stopped processing queue for GID {guild_id}, likely waiting for download.")
                    # Do NOT start idle timer here, downloader task should trigger playback later
            # -----------------------

        except asyncio.TimeoutError:
            log.error(f"Timeout acquiring lock for GID {guild_id} in _play_next. Playback may be stalled.")
            # Attempt to release lock if we somehow acquired it partially? Unlikely but safe.
            if lock_acquired and self.guild_locks[guild_id].locked():
                self.guild_locks[guild_id].release()
        except Exception as e:
            log.error(f"Unexpected error in _play_next for GID {guild_id}: {e}", exc_info=True)
            self.currently_playing.pop(guild_id, None) # Clear playing state on error
            self.playback_mode[guild_id] = PlaybackMode.IDLE # Revert to idle on error
            if vc and vc.is_connected(): # Start idle timer if connected after error
                 self._start_idle_timer(guild_id, vc)
        finally:
            # Ensure lock is always released
            if lock_acquired and self.guild_locks[guild_id].locked():
                log.debug(f"Releasing lock for GID {guild_id} in _play_next")
                self.guild_locks[guild_id].release()


    def _playback_finished_callback(self, guild_id: int, vc: discord.VoiceClient, error: Optional[Exception]):
        """
        Callback executed by discord.py after vc.play() finishes.
        Schedules the async task handler.
        NOTE: This runs in a separate thread context provided by discord.py.
              Do not perform complex async operations directly here.
        """
        if error:
            log.error(f"Playback error reported in callback for GID {guild_id}: {error}") # Log immediately
        else:
            log.debug(f"Playback finished naturally callback for GID {guild_id}.")

        # Schedule the async part of the handler to run in the bot's event loop
        self.bot.loop.call_soon_threadsafe(
            lambda: self.bot.loop.create_task(self._playback_finished_task(guild_id, vc, error))
        )

    async def _playback_finished_task(self, guild_id: int, vc: discord.VoiceClient, error: Optional[Exception]):
        """
        Async task handler for playback completion. Runs in the bot's event loop.
        Acquires lock and decides whether to play next or go idle.
        """
        log.debug(f"Async finish handler started for GID {guild_id}. Error: {error}")
        async with self.guild_locks[guild_id]:
            current_mode = self.playback_mode[guild_id]
            log.debug(f"Finish handler check: GID {guild_id}, Mode: {current_mode}")

            # Only proceed to next if we are in QUEUE mode.
            # If SINGLE_SOUND finished, its specific callback handles reverting mode.
            # If IDLE (e.g., due to /stop), do nothing.
            if current_mode == PlaybackMode.QUEUE:
                 log.debug(f"Scheduling _play_next via finish handler for GID {guild_id}")
                 # Call _play_next directly as we are already in the loop and hold the lock
                 # Using create_task here could lead to multiple _play_next running if callback is rapid
                 await self._play_next(guild_id, vc) # Await it directly
            else:
                 log.debug(f"Not scheduling _play_next via finish handler for GID {guild_id} as mode is {current_mode}")
                 # If now idle (e.g., single sound finished and queue is empty), start timer
                 if current_mode == PlaybackMode.IDLE and vc.is_connected() and not vc.is_playing() and not self.guild_queues.get(guild_id):
                     self._start_idle_timer(guild_id, vc)


    def _start_idle_timer(self, guild_id: int, vc: discord.VoiceClient):
        """Starts or resets the music idle disconnect timer."""
        if IDLE_TIMEOUT_SECONDS <= 0: return # Allow disabling with 0 or negative
        self._cancel_idle_timer(guild_id) # Cancel any existing timer first
        log.debug(f"Starting MUSIC idle timer ({IDLE_TIMEOUT_SECONDS}s) for GID {guild_id}")
        self.idle_timers[guild_id] = self.bot.loop.create_task(
            self._idle_task(guild_id, vc)
        )

    def _cancel_idle_timer(self, guild_id: int):
        """Cancels the music idle timer if it exists."""
        if guild_id in self.idle_timers:
            log.debug(f"Cancelling MUSIC idle timer for GID {guild_id}")
            self.idle_timers[guild_id].cancel()
            self.idle_timers.pop(guild_id, None) # Remove reference

    async def _idle_task(self, guild_id: int, vc: discord.VoiceClient):
        """The task that waits and then checks for music idle disconnect."""
        try:
             await asyncio.sleep(IDLE_TIMEOUT_SECONDS)
             log.info(f"Music Idle timer expired for GID {guild_id}. Checking state...")

             # Check conditions *inside* lock
             async with self.guild_locks[guild_id]:
                 # Check if still connected, not playing, queue empty, and mode is idle
                 current_vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
                 if (current_vc == vc and vc.is_connected() and
                     not self.is_playing(guild_id) and
                     not self.guild_queues.get(guild_id) and
                     self.playback_mode[guild_id] == PlaybackMode.IDLE):

                     log.info(f"Bot is idle from music in GID {guild_id}. Disconnecting.")

                     # Send departure message (optional)
                     if vc.channel:
                         try:
                             # Try system channel first, then any channel bot can send to
                             target_channel = vc.guild.system_channel or next((c for c in vc.guild.text_channels if c.permissions_for(vc.guild.me).send_messages), None)
                             if target_channel:
                                 await target_channel.send(f"👋 Leaving {vc.channel.mention} due to music inactivity.")
                         except Exception as send_e:
                             log.warning(f"Could not send music idle departure message for GID {guild_id}: {send_e}")

                     # Disconnect using safe_disconnect
                     # Use create_task to avoid blocking if disconnect is slow
                     self.bot.loop.create_task(self.safe_disconnect(vc, manual_leave=False, reason="Music Idle timeout"))

                 else:
                     log.debug(f"Music Idle timer expired for GID {guild_id}, but conditions changed (playing/queued/disconnected/mode!=idle). No action needed.")

        except asyncio.CancelledError:
             log.debug(f"Music idle timer task cancelled for GID {guild_id}.")
        except Exception as e:
             log.error(f"Error in music idle timer task for GID {guild_id}: {e}", exc_info=True)
        finally:
             # Ensure timer reference is removed after task finishes/cancels/errors
             self.idle_timers.pop(guild_id, None)

    async def play_single_sound(
            self,
            interaction: discord.Interaction,
            sound_path: str,
            display_name: Optional[str] = None
        ) -> bool:
        """
        Plays a single audio file immediately, interrupting the queue.
        Uses audio_processor for trimming/normalization.
        Returns True if playback started, False otherwise.
        Sends feedback via the interaction.
        """
        # Ensure required arguments are present
        if not interaction or not interaction.guild or not isinstance(interaction.user, discord.Member) or not interaction.user.voice:
             log.warning("play_single_sound called with invalid interaction state.")
             # Try to send feedback if possible
             if interaction: await self._try_respond(interaction, "❌ Cannot play sound: Invalid user or voice state.", ephemeral=True)
             return False

        guild = interaction.guild
        user = interaction.user
        target_channel = user.voice.channel
        guild_id = guild.id
        sound_basename = os.path.basename(sound_path)
        log_display_name = display_name or sound_basename

        log.info(f"Request to play single sound '{log_display_name}' in GID {guild_id}")

        if not os.path.exists(sound_path):
            log.error(f"Single sound file not found: {sound_path}")
            await self._try_respond(interaction, "❌ Internal error: Could not find the audio file to play.", ephemeral=True)
            return False

        lock_acquired = False
        audio_buffer = None # Define buffer variable here
        try:
            await asyncio.wait_for(self.guild_locks[guild_id].acquire(), timeout=10.0)
            lock_acquired = True
            log.debug(f"Acquired lock for GID {guild_id} in play_single_sound")

            # Ensure VC is ready
            vc = await self.ensure_voice_client(interaction, target_channel, "SINGLE SOUND")
            if not vc:
                log.warning(f"Failed to get VC for single sound in GID {guild_id}")
                # ensure_voice_client sends feedback, so just return
                return False

            # --- Process Audio ---
            log.debug(f"Processing single sound '{sound_basename}' using audio_processor...")
            audio_source, audio_buffer = audio_processor.process_audio(sound_path)

            if not audio_source or not audio_buffer:
                 log.error(f"Failed to process single sound '{sound_path}' for GID {guild_id}")
                 await self._try_respond(interaction, "❌ Error processing the audio file.", ephemeral=True)
                 # Clean up buffer if it exists but source failed
                 if audio_buffer and not audio_buffer.closed:
                     try: audio_buffer.close()
                     except Exception: pass
                 return False
            log.debug(f"Audio processed successfully for '{sound_basename}'.")
            # ---------------------


            original_mode = self.playback_mode[guild_id]
            self.playback_mode[guild_id] = PlaybackMode.SINGLE_SOUND
            log.debug(f"Set playback mode to SINGLE_SOUND for GID {guild_id}")

            if vc.is_playing():
                log.info(f"Stopping current playback in GID {guild_id} to play single sound.")
                vc.stop() # Stop current playback; its 'after' should respect the new mode

            self._cancel_idle_timer(guild_id) # Cancel music timer

            # --- Define the 'after' callback ---
            # This now needs to handle buffer cleanup and mode reversion
            def single_sound_finished(error: Optional[Exception]):
                # --- Use run_coroutine_threadsafe for async operations ---
                async def async_cleanup():
                    gid_cb = guild_id
                    original_mode_cb = original_mode
                    path_cb = sound_path # Capture path for logging
                    log.debug(f"Single sound finished callback triggered for GID {gid_cb}. Error: {error}")

                    # Pop and close the buffer associated with this guild's single play
                    buffer_to_close = self.active_single_buffers.pop(gid_cb, None)
                    if buffer_to_close:
                        if not buffer_to_close.closed:
                            try: buffer_to_close.close()
                            except Exception as buf_e: log.warning(f"Error closing single sound buffer for GID {gid_cb}: {buf_e}")
                        log.debug(f"Closed buffer for single sound '{os.path.basename(path_cb)}' in GID {gid_cb}")
                    else:
                        log.warning(f"No active single sound buffer found to close for GID {gid_cb}")

                    # Acquire lock before changing mode or starting queue
                    async with self.guild_locks[gid_cb]:
                        # Only revert mode if it's still SINGLE_SOUND
                        if self.playback_mode[gid_cb] == PlaybackMode.SINGLE_SOUND:
                             self.playback_mode[gid_cb] = original_mode_cb
                             log.info(f"Reverted playback mode to {original_mode_cb} for GID {gid_cb} after single sound.")

                             # If original mode was QUEUE and queue is not empty, try resuming
                             if original_mode_cb == PlaybackMode.QUEUE and self.guild_queues.get(gid_cb):
                                 log.info(f"Attempting to resume queue playback for GID {gid_cb}.")
                                 # Check VC again before starting
                                 current_vc_cb = discord.utils.get(self.bot.voice_clients, guild__id=gid_cb)
                                 if current_vc_cb and current_vc_cb.is_connected():
                                     await self._play_next(gid_cb, current_vc_cb) # Call directly with lock held
                                 else:
                                     log.warning(f"Cannot resume queue for GID {gid_cb}, VC disconnected.")
                             # If original mode was IDLE or QUEUE (but queue now empty), start idle timer
                             elif not self.is_playing(gid_cb):
                                 log.info(f"Single sound finished, no queue to resume or originally idle for GID {gid_cb}. Starting idle timer.")
                                 current_vc_cb = discord.utils.get(self.bot.voice_clients, guild__id=gid_cb)
                                 if current_vc_cb and current_vc_cb.is_connected():
                                     self._start_idle_timer(gid_cb, current_vc_cb)
                        else:
                             log.warning(f"Single sound finished for GID {gid_cb}, but mode was already {self.playback_mode[gid_cb]}. Not reverting or resuming queue.")

                    if error:
                         log.error(f"Error during single sound playback for GID {gid_cb}: {error}", exc_info=error)

                # --- Schedule the async cleanup ---
                asyncio.run_coroutine_threadsafe(async_cleanup(), self.bot.loop)
            # --- End of callback definition ---


            # --- Store buffer and start playback ---
            # Clean up any previous buffer first (should be rare)
            old_buffer = self.active_single_buffers.pop(guild_id, None)
            if old_buffer and not old_buffer.closed:
                 log.warning(f"Overwriting existing single sound buffer for GID {guild_id}")
                 try: old_buffer.close()
                 except Exception: pass
            self.active_single_buffers[guild_id] = audio_buffer # Store the buffer

            vc.play(audio_source, after=single_sound_finished)
            log.info(f"Started playing single sound '{log_display_name}' in GID {guild_id}")

            # Send confirmation feedback
            await self._try_respond(interaction, f"▶️ Playing `{log_display_name}`...", ephemeral=False) # Send non-ephemeral confirmation

            return True # Playback started

        except asyncio.TimeoutError:
            log.error(f"Timeout acquiring lock for GID {guild_id} in play_single_sound.")
            await self._try_respond(interaction, "❌ Could not acquire playback lock, please try again shortly.", ephemeral=True)
            if audio_buffer and not audio_buffer.closed: # Cleanup buffer on timeout
                try: audio_buffer.close()
                except Exception: pass
            return False
        except discord.ClientException as e:
             log.error(f"ClientException during single sound playback for GID {guild_id}: {e}", exc_info=True)
             await self._try_respond(interaction, f"❌ Playback error: {e}", ephemeral=True)
             self.playback_mode[guild_id] = PlaybackMode.IDLE # Revert mode on error
             if audio_buffer and not audio_buffer.closed: # Cleanup buffer
                 try: audio_buffer.close()
                 except Exception: pass
             return False
        except Exception as e:
            log.error(f"Unexpected error in play_single_sound for GID {guild_id}: {e}", exc_info=True)
            self.playback_mode[guild_id] = PlaybackMode.IDLE # Revert mode on unexpected error
            if audio_buffer and not audio_buffer.closed: # Cleanup buffer
                try: audio_buffer.close()
                except Exception: pass
            await self._try_respond(interaction, "❌ An unexpected error occurred while trying to play the sound.", ephemeral=True)
            return False
        finally:
            if lock_acquired and self.guild_locks[guild_id].locked():
                log.debug(f"Releasing lock for GID {guild_id} in play_single_sound")
                self.guild_locks[guild_id].release()

    # --- End of play_single_sound ---

    async def _try_respond(self, interaction: discord.Interaction, message: Optional[str] = None, **kwargs):
        """Helper to respond to an interaction, catching errors if it already expired/responded."""
        if not interaction: return # Guard against None interaction

        content = kwargs.pop('content', message) # Allow overriding content via kwargs

        is_ephemeral = kwargs.pop('ephemeral', False)

        try:
            if interaction.response.is_done():
                # Use edit_original if already responded (followup is for new messages)
                await interaction.edit_original_response(content=content, ephemeral=is_ephemeral, **kwargs)
            else:
                # Use send_message for the initial response
                await interaction.response.send_message(content=content, **kwargs)
        except discord.NotFound:
            log.warning(f"Interaction response failed (NotFound - likely expired): {interaction.id}")
        except discord.HTTPException as e:
            # InteractionAlreadyResponded is an HTTPException
            if e.code == 40060: # Interaction has already been responded to
                 log.warning(f"Interaction response failed (Already Responded): {interaction.id}. Trying followup...")
                 # If initial response failed because it was *already* responded to (e.g. defer), try followup
                 try:
                     await interaction.followup.send(content=content, ephemeral=is_ephemeral, **kwargs)
                 except discord.NotFound:
                      log.warning(f"Followup failed (NotFound) for interaction: {interaction.id}")
                 except Exception as followup_e:
                      log.error(f"Error sending followup for interaction {interaction.id}: {followup_e}", exc_info=True)
            else:
                 log.warning(f"Interaction response failed (HTTPException {e.status} / {e.code}): {interaction.id}")
        except Exception as e:
            log.error(f"Unexpected error responding to interaction {interaction.id}: {e}", exc_info=True)


    async def play_audio_source_now(
            self,
            interaction: discord.Interaction,
            audio_source: discord.PCMAudio, # Takes the source directly
            audio_buffer_to_close: io.BytesIO, # Takes the buffer to close
            display_name: Optional[str] = None # For logging/feedback
        ) -> bool:
            """
            Plays a prepared audio source immediately, interrupting the queue.
            Manages the provided buffer for cleanup.
            Used for sources like TTS that are generated in memory.
            """
            if not interaction or not interaction.guild or not isinstance(interaction.user, discord.Member) or not interaction.user.voice:
                log.warning("play_audio_source_now called with invalid interaction state.")
                if interaction: await self._try_respond(interaction, "❌ Cannot play sound: Invalid user or voice state.", ephemeral=True)
                return False

            guild = interaction.guild
            user = interaction.user
            target_channel = user.voice.channel
            guild_id = guild.id
            log_display_name = display_name or "Audio Source"

            log.info(f"Request to play single audio source '{log_display_name}' in GID {guild_id}")

            if not audio_source or not audio_buffer_to_close:
                log.error(f"play_audio_source_now called with missing audio_source or buffer for GID {guild_id}")
                await self._try_respond(interaction, "❌ Internal error: Missing audio data to play.", ephemeral=True)
                return False

            lock_acquired = False
            try:
                await asyncio.wait_for(self.guild_locks[guild_id].acquire(), timeout=10.0)
                lock_acquired = True
                log.debug(f"Acquired lock for GID {guild_id} in play_audio_source_now")

                vc = await self.ensure_voice_client(interaction, target_channel, "DIRECT AUDIO PLAY")
                if not vc:
                    log.warning(f"Failed to get VC for direct audio source in GID {guild_id}")
                    # ensure_voice_client sends feedback
                    return False

                original_mode = self.playback_mode[guild_id]
                self.playback_mode[guild_id] = PlaybackMode.SINGLE_SOUND # Treat like single sound mode
                log.debug(f"Set playback mode to SINGLE_SOUND for GID {guild_id} (direct source)")

                if vc.is_playing():
                    log.info(f"Stopping current playback in GID {guild_id} to play direct audio source.")
                    vc.stop()

                self._cancel_idle_timer(guild_id)

                # --- Define the 'after' callback specifically for this method ---
                def direct_source_finished(error: Optional[Exception]):


                    async def async_cleanup():
                        gid_cb = guild_id
                        original_mode_cb = original_mode
                        buffer_cb = audio_buffer_to_close # Capture the specific buffer
                        log.debug(f"Direct source finished callback triggered for GID {gid_cb}. Error: {error}")

                        # Close the provided buffer
                        if buffer_cb:
                            if not buffer_cb.closed:
                                try: buffer_cb.close()
                                except Exception as buf_e: log.warning(f"Error closing direct source buffer for GID {gid_cb}: {buf_e}")
                            log.debug(f"Closed buffer for direct source play in GID {gid_cb}")
                        else:
                            # This shouldn't happen based on the check above, but log if it does
                            log.warning(f"Direct source finished callback missing buffer for GID {gid_cb}")

                        if error:
                            log.error(f"Error during direct source playback for GID {gid_cb}: {error}", exc_info=error)

                    asyncio.run_coroutine_threadsafe(async_cleanup(), self.bot.loop)
                # --- End of callback definition ---

                # --- Start playback ---
                # Note: We don't store this buffer in self.active_single_buffers,
                # the callback closes the one passed in directly.
                vc.play(audio_source, after=direct_source_finished)
                log.info(f"Started playing direct audio source '{log_display_name}' in GID {guild_id}")

                await self._try_respond(interaction, f"🗣️ Playing `{log_display_name}`...", ephemeral=False) # TTS confirmation

                return True # Playback started

            except asyncio.TimeoutError:
                log.error(f"Timeout acquiring lock for GID {guild_id} in play_audio_source_now.")
                await self._try_respond(interaction, "❌ Could not acquire playback lock, please try again shortly.", ephemeral=True)
                # Cleanup buffer if lock fails
                if audio_buffer_to_close and not audio_buffer_to_close.closed:
                    try: audio_buffer_to_close.close()
                    except Exception: pass
                return False
            except discord.ClientException as e:
                log.error(f"ClientException during direct source playback for GID {guild_id}: {e}", exc_info=True)
                await self._try_respond(interaction, f"❌ Playback error: {e}", ephemeral=True)
                self.playback_mode[guild_id] = PlaybackMode.IDLE # Revert mode on error
                if audio_buffer_to_close and not audio_buffer_to_close.closed: # Cleanup buffer
                    try: audio_buffer_to_close.close()
                    except Exception: pass
                return False
            except Exception as e:
                log.error(f"Unexpected error in play_audio_source_now for GID {guild_id}: {e}", exc_info=True)
                self.playback_mode[guild_id] = PlaybackMode.IDLE # Revert mode
                if audio_buffer_to_close and not audio_buffer_to_close.closed: # Cleanup buffer
                    try: audio_buffer_to_close.close()
                    except Exception: pass
                await self._try_respond(interaction, "❌ An unexpected error occurred while trying to play the sound.", ephemeral=True)
                return False
            finally:
                if lock_acquired and self.guild_locks[guild_id].locked():
                    log.debug(f"Releasing lock for GID {guild_id} in play_audio_source_now")
                    self.guild_locks[guild_id].release()
                    
    # --- End of PlaybackManager class ---