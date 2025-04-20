# -*- coding: utf-8 -*-
import discord
import shutil
from discord.ext import commands, tasks
import logging
import asyncio
import os
import yt_dlp
from functools import partial
from typing import Optional, List, Dict, Any, Union
import datetime
import time
from dataclasses import dataclass, field
from enum import Enum

# Ensure these are imported from the core location
from core.music_types import MusicQueueItem, DownloadStatus

import config # Import your config module
from core.playback_manager import PlaybackManager
from utils import file_helpers

log = logging.getLogger('SoundBot.Cog.Music')

# --- Configuration (ensure these match your config.py or adjust as needed) ---
CACHE_DIR = getattr(config, 'MUSIC_CACHE_DIR', 'music_cache')
CACHE_TTL_SECONDS = getattr(config, 'MUSIC_CACHE_TTL_DAYS', 30) * 86400 # Default 30 days
DOWNLOAD_AHEAD_COUNT = getattr(config, 'MUSIC_DOWNLOAD_AHEAD', 2) # How many songs ahead to download
DOWNLOAD_CHECK_INTERVAL_SECONDS = getattr(config, 'MUSIC_DOWNLOAD_INTERVAL', 5) # Check queue every 5s
CLEANUP_CHECK_INTERVAL_SECONDS = getattr(config, 'MUSIC_CLEANUP_INTERVAL', 3600) # Check cache every hour
YTDL_MAX_DURATION = getattr(config, 'YTDL_MAX_DURATION', 600) # Max duration in seconds (default 10 mins)
YTDL_MAX_FILESIZE = getattr(config, 'YTDL_MAX_FILESIZE_MB', 50) * 1024 * 1024 # Max filesize in MB

file_helpers.ensure_dir(CACHE_DIR)

YTDL_OUT_TEMPLATE = os.path.join(CACHE_DIR, '%(extractor)s-%(id)s-%(title).50s.%(ext)s')

YTDL_OPTS = {
    'format': 'bestaudio/best',
    'outtmpl': YTDL_OUT_TEMPLATE,
    'restrictfilenames': True,
    'noplaylist': True,
    'nocheckcertificate': True,
    'ignoreerrors': False,
    'logtostderr': False,
    'quiet': True,
    'no_warnings': True,
    'default_search': 'ytsearch1', # Search YouTube and return 1 result
    'source_address': '0.0.0.0', # Bind to all interfaces to avoid connection issues
    'postprocessors': [{
        'key': 'FFmpegExtractAudio',
        'preferredcodec': 'opus', # Opus is generally good for Discord
        'preferredquality': '192', # 192 kbps quality
    }],
    'max_filesize': YTDL_MAX_FILESIZE,
    # Use match_filter_func for cleaner duration filtering
    'match_filter': yt_dlp.utils.match_filter_func(f'duration < {YTDL_MAX_DURATION}') if YTDL_MAX_DURATION > 0 else None,
}

# ---------------------------------------------------------------------------
# Classes MusicQueueItem and DownloadStatus are defined in core/music_types.py
# Ensure they are NOT redefined here.
# ---------------------------------------------------------------------------

class MusicCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        if not hasattr(bot, 'playback_manager'):
            log.critical("PlaybackManager not found on bot. MusicCog requires it to be initialized first.")
            raise RuntimeError("PlaybackManager not found on bot.")
        self.playback_manager: PlaybackManager = bot.playback_manager
        self._downloader_task_instance = self.downloader_task.start()
        self._cleanup_task_instance = self.cache_cleanup_task.start()
        log.info(f"MusicCog initialized. Downloader interval: {DOWNLOAD_CHECK_INTERVAL_SECONDS}s, Cleanup interval: {CLEANUP_CHECK_INTERVAL_SECONDS}s, Cache TTL: {CACHE_TTL_SECONDS}s")

    def cog_unload(self):
        """Cog cleanup."""
        if self._downloader_task_instance:
            self._downloader_task_instance.cancel()
        if self._cleanup_task_instance:
            self._cleanup_task_instance.cancel()
        log.info("MusicCog background tasks cancelled.")

    async def _extract_info(self, query: str) -> Optional[Dict[str, Any]]:
        """Runs yt-dlp extract_info in executor."""
        log.debug(f"Running yt-dlp info extraction for: {query[:100]}")
        try:
            # Create a fresh YTDL instance each time to potentially avoid state issues
            ytdl_opts_copy = YTDL_OPTS.copy()
            # Ensure postprocessor uses a standard key name recognised by yt-dlp
            for pp in ytdl_opts_copy.get('postprocessors', []):
                 if 'key' not in pp and 'processor_name' in pp: # Handle older key name if needed
                     pp['key'] = pp.pop('processor_name')

            ydl_instance = yt_dlp.YoutubeDL(ytdl_opts_copy)
            partial_func = partial(ydl_instance.extract_info, query, download=False)
            loop = asyncio.get_running_loop()
            data = await loop.run_in_executor(None, partial_func)

            if not data:
                log.warning(f"yt-dlp extract_info returned no data for query: {query[:100]}")
                return None

            # Handle playlists and searches returning multiple entries
            if 'entries' in data:
                if not data['entries']:
                    log.warning(f"yt-dlp result has empty 'entries' for query: {query[:100]}")
                    return None
                # Use the first entry from search/playlist
                log.debug(f"Search yielded {len(data['entries'])} results for '{query[:100]}', using first.")
                video_info = data['entries'][0]
            else:
                # Direct URL or single video result
                log.debug(f"Direct URL/Single result processed for query: {query[:100]}")
                video_info = data

            # Double-check duration if filter is active
            if YTDL_MAX_DURATION > 0 and video_info.get('duration', 0) > YTDL_MAX_DURATION:
                 log.warning(f"Video '{video_info.get('title', 'N/A')}' duration ({video_info.get('duration')}) exceeds limit ({YTDL_MAX_DURATION}). Skipping.")
                 return None

            return video_info

        except yt_dlp.utils.DownloadError as e:
            # Log specific download errors (like unavailable videos, geo-restrictions)
            if 'video unavailable' in str(e).lower():
                 log.warning(f"yt-dlp: Video unavailable for '{query[:100]}'.")
            elif 'confirm your age' in str(e).lower():
                 log.warning(f"yt-dlp: Age restricted video found for '{query[:100]}'.")
            else:
                 log.warning(f"yt-dlp DownloadError during info extraction for '{query[:100]}': {e}")
            return None
        except Exception as e:
            log.error(f"Unexpected error running yt-dlp extract_info for '{query[:100]}': {e}", exc_info=True)
            return None

    async def _download_audio(self, video_info: Dict[str, Any]) -> Optional[str]:
        """Downloads audio using yt-dlp info in executor. Returns file path or None."""
        url = video_info.get('webpage_url') or video_info.get('original_url') or video_info.get('url')
        title = video_info.get('title', 'Unknown Title')
        if not url:
            log.error(f"No valid URL found in video info for download. Title: '{title}'")
            return None

        log.info(f"Attempting download for: '{title[:70]}' ({url})")
        try:
            # Use a separate function to run the blocking download
            def download_sync(url_to_download, opts):
                log.debug(f"Download sync starting for '{title[:70]}' in executor thread.")
                # Create a fresh instance for download too
                opts_copy = opts.copy()
                for pp in opts_copy.get('postprocessors', []):
                    if 'key' not in pp and 'processor_name' in pp:
                        pp['key'] = pp.pop('processor_name')

                with yt_dlp.YoutubeDL(opts_copy) as ydl:
                    # We pass download=True here
                    info = ydl.extract_info(url_to_download, download=True)
                    # prepare_filename needs the *info* dictionary returned by extract_info
                    downloaded_path = ydl.prepare_filename(info)

                    # Sometimes the actual path is nested if post-processing occurred
                    if 'requested_downloads' in info and info['requested_downloads']:
                        actual_filepath = info['requested_downloads'][0].get('filepath')
                        if actual_filepath and os.path.exists(actual_filepath):
                             downloaded_path = actual_filepath
                             log.debug(f"Using path from 'requested_downloads': {downloaded_path}")
                        else:
                             log.warning(f"Path in 'requested_downloads' invalid ({actual_filepath}), falling back to prepared: {downloaded_path}")
                    elif 'filepath' in info: # Fallback check
                         if os.path.exists(info['filepath']):
                              downloaded_path = info['filepath']
                              log.debug(f"Using path from 'filepath': {downloaded_path}")
                         else:
                              log.warning(f"Path in 'filepath' invalid ({info['filepath']}), falling back to prepared: {downloaded_path}")


                log.debug(f"Download sync finished for '{title[:70]}'. Determined path: {downloaded_path}")
                return downloaded_path

            partial_func = partial(download_sync, url, YTDL_OPTS)
            loop = asyncio.get_running_loop()
            final_path = await loop.run_in_executor(None, partial_func)

            if final_path and os.path.exists(final_path):
                log.info(f"Download successful: '{title[:70]}' -> '{os.path.basename(final_path)}'")
                # Update the modification time to keep it from being cleaned up immediately
                os.utime(final_path, None)
                return final_path
            else:
                log.error(f"Download finished for '{title[:70]}' but could not confirm final file path or file doesn't exist. Determined path: {final_path}")
                return None

        except yt_dlp.utils.DownloadError as e:
            log.error(f"yt-dlp DownloadError during download of '{title[:70]}': {e}")
            # Don't log full traceback for common download errors unless debugging heavily
            if 'HTTP Error 403' in str(e):
                 log.warning("Download failed with 403 Forbidden. Might be region-locked or require login.")
            return None
        except Exception as e:
            log.error(f"Unexpected error during yt-dlp download of '{title[:70]}': {e}", exc_info=True)
            return None

    @tasks.loop(seconds=DOWNLOAD_CHECK_INTERVAL_SECONDS)
    async def downloader_task(self):
        log.debug(f"[Downloader Task Loop] ===== TASK ENTRY POINT =====") # ADDED: Top level marker
        try:
            log.debug("[Downloader Task Loop] Accessing playback_manager queues...") # ADDED: Before accessing queues
            # Make sure we are accessing the correct dictionary
            guild_queues_dict = self.playback_manager.guild_queues
            active_guild_ids = list(guild_queues_dict.keys())
            log.debug(f"[Downloader Task Loop] Accessed queues. Active GIDs: {active_guild_ids}") # ADDED: After accessing queues

            if not active_guild_ids:
                log.debug("[Downloader Task Loop] No active guild queues found.")
                # log.debug(f"[Downloader Task Loop] ===== TASK EXIT POINT (No Guilds) =====") # Keep exit log below in finally
                return # Exit early if no guilds have queues

            log.debug(f"[Downloader Task Loop] Checking guilds: {active_guild_ids}")

            for guild_id in active_guild_ids:
                log.debug(f"[Downloader Task Loop] Processing GID: {guild_id}")
                # Get the specific queue for this guild
                queue = guild_queues_dict.get(guild_id) # Use .get() for safety

                if not queue: # Check if queue is None or empty
                    log.debug(f"[Downloader Task Loop] Queue empty or None for GID: {guild_id}, skipping.")
                    continue

                items_to_download: List[MusicQueueItem] = []
                currently_downloading = 0
                items_pending_in_scope = 0

                # Use slice to avoid modifying list while iterating if needed, though enumerate should be safe
                for i, item in enumerate(list(queue)): # Iterate over a copy? or just ensure no mid-loop removal? enumerate is usually fine.
                    log.debug(f"[Downloader Task Loop] GID {guild_id}: Examining item at index {i}. Type: {type(item).__name__}")
                    # --- Add explicit isinstance check log ---
                    is_music_item_instance = isinstance(item, MusicQueueItem)
                    log.debug(f"[Downloader Task Loop] GID {guild_id}: Is instance of MusicQueueItem? {is_music_item_instance}")
                    # --- End explicit check log ---

                    if is_music_item_instance: # Use the variable here
                         item_title_safe = getattr(item, 'title', 'Unknown Title')[:30]
                         item_status = getattr(item, 'download_status', 'STATUS_MISSING') # Use getattr for safety
                         log.debug(f"[Downloader Task Loop] GID {guild_id}: Item '{item_title_safe}...', Status: {item_status}")

                         if item_status == DownloadStatus.PENDING:
                            items_pending_in_scope += 1
                            # Only consider downloading items near the front of the queue
                            if i < DOWNLOAD_AHEAD_COUNT:
                                log.debug(f"[Downloader Task Loop] GID {guild_id}: Found PENDING item '{item_title_safe}' at index {i}. Adding to download list.")
                                items_to_download.append(item)
                            else:
                                log.debug(f"[Downloader Task Loop] GID {guild_id}: Found PENDING item '{item_title_safe}' at index {i}, but exceeds DOWNLOAD_AHEAD_COUNT ({DOWNLOAD_AHEAD_COUNT}).")
                         elif item_status == DownloadStatus.DOWNLOADING:
                            currently_downloading += 1
                            log.debug(f"[Downloader Task Loop] GID {guild_id}: Item '{item_title_safe}' is currently DOWNLOADING.")
                         # Implicitly skip READY and FAILED items in this section
                    else:
                        log.warning(f"[Downloader Task Loop] Found non-MusicQueueItem in queue for GID {guild_id} at index {i}. Type: {type(item).__name__}. Skipping.")
                        continue # Skip to the next item in the queue

                log.debug(f"[Downloader Task Loop] GID: {guild_id} - Found Pending (overall): {items_pending_in_scope}, To Download (in scope): {len(items_to_download)}, Currently Downloading: {currently_downloading}")

                available_slots = max(0, DOWNLOAD_AHEAD_COUNT - currently_downloading)
                if not items_to_download or available_slots <= 0:
                    log.debug(f"[Downloader Task Loop] GID: {guild_id}: No items to download now (need {len(items_to_download)}, avail slots {available_slots}).")
                    continue # Move to the next guild

                log.debug(f"[Downloader Task Loop] GID: {guild_id}: Attempting to download up to {available_slots} items.")
                for item_to_download in items_to_download[:available_slots]:
                    # Double-check status before starting download in case it changed
                    if item_to_download.download_status == DownloadStatus.PENDING:
                        item_title_safe = getattr(item_to_download, 'title', 'Unknown Title')[:50]
                        log.info(f"[Downloader] Guild {guild_id}: Identified pending item '{item_title_safe}...', starting download process.")
                        item_to_download.download_status = DownloadStatus.DOWNLOADING
                        try:
                            log.debug(f"[Downloader] Guild {guild_id}: Calling _download_audio for '{item_title_safe}'...")
                            download_path = await self._download_audio(item_to_download.video_info)
                            log.debug(f"[Downloader] Guild {guild_id}: _download_audio finished for '{item_title_safe}'. Path: {download_path}")

                            if download_path and os.path.exists(download_path):
                                item_to_download.download_path = download_path
                                item_to_download.download_status = DownloadStatus.READY
                                log.info(f"[Downloader] Guild {guild_id}: Item '{item_title_safe}...' ready. Path: {download_path}")

                                # Check if this newly ready item is now at the front and the bot is idle
                                current_queue_after_download = self.playback_manager.get_queue(guild_id)
                                if current_queue_after_download and current_queue_after_download[0] == item_to_download:
                                    if not self.playback_manager.is_playing(guild_id):
                                        log.info(f"[Downloader] Guild {guild_id}: First item '{item_title_safe}...' is ready and bot is idle, ensuring playback starts.")
                                        # Use create_task to avoid blocking the downloader loop
                                        self.bot.loop.create_task(self.playback_manager.start_playback_if_idle(guild_id))
                                    else:
                                        log.debug(f"[Downloader] Guild {guild_id}: First item '{item_title_safe}...' is ready, but bot is already playing.")
                                else:
                                     # Check if the first item is ready now, even if it wasn't the one just downloaded
                                     if current_queue_after_download and isinstance(current_queue_after_download[0], MusicQueueItem) and current_queue_after_download[0].download_status == DownloadStatus.READY:
                                          if not self.playback_manager.is_playing(guild_id):
                                               log.info(f"[Downloader] Guild {guild_id}: Item '{item_title_safe}...' ready (not first), but first item *is* ready and bot idle. Triggering playback check.")
                                               self.bot.loop.create_task(self.playback_manager.start_playback_if_idle(guild_id))

                                     log.debug(f"[Downloader] Guild {guild_id}: Item '{item_title_safe}...' ready, but it's not the first item in the queue (or queue changed/first item not ready).")


                            else:
                                item_to_download.download_status = DownloadStatus.FAILED
                                log.error(f"[Downloader] Guild {guild_id}: Failed to download item '{item_title_safe}...'. _download_audio returned invalid path or file missing: {download_path}")

                        except Exception as download_err:
                            log.error(f"[Downloader] Guild {guild_id}: Exception during download attempt for '{item_title_safe}': {download_err}", exc_info=True)
                            # Ensure status is marked FAILED even if exception occurred mid-process
                            if hasattr(item_to_download, 'download_status'):
                                item_to_download.download_status = DownloadStatus.FAILED
                    else:
                        log.warning(f"[Downloader Task Loop] GID: {guild_id}: Item '{getattr(item_to_download, 'title', 'Unknown')[:30]}' found in download list but status was not PENDING ({getattr(item_to_download, 'download_status', 'N/A')}). Skipping.")

            log.debug(f"[Downloader Task Loop] Finished processing guilds for this iteration.") # ADDED: Before the finally block

        except asyncio.CancelledError:
             log.info("[Downloader Task Loop] Task cancelled.")
             raise # Re-raise cancellation
        except Exception as e:
            # Make the error log more prominent
            log.error(f"[Downloader Task Loop] !!! EXCEPTION IN LOOP !!! Type: {type(e).__name__}, Error: {e}", exc_info=True) # MODIFIED: More visible error log
        finally:
            log.debug(f"[Downloader Task Loop] ===== TASK EXIT POINT (End of Iteration) =====") # ADDED: To confirm loop completion/exit

    @downloader_task.before_loop
    async def before_downloader_task(self):
        log.debug("before_downloader_task: Waiting for bot to be ready...")
        await self.bot.wait_until_ready()
        log.info("Downloader task starting...")

    @tasks.loop(seconds=CLEANUP_CHECK_INTERVAL_SECONDS)
    async def cache_cleanup_task(self):
        now = time.time()
        log.info(f"[Cache Cleanup] Running scan of '{CACHE_DIR}'...")
        removed_count = 0
        removed_size = 0
        active_paths = set()

        # Collect paths of items currently playing or in the queue and marked as READY/DOWNLOADING
        try:
             # Use .items() for direct access to keys and values
             for guild_id, queue in self.playback_manager.guild_queues.items():
                 current_item = self.playback_manager.get_current_item(guild_id)
                 if current_item and isinstance(current_item, MusicQueueItem) and current_item.download_path:
                     active_paths.add(os.path.abspath(current_item.download_path))

                 for item in queue:
                     if isinstance(item, MusicQueueItem) and item.download_path:
                         # Only protect files that are ready or currently being downloaded
                         if item.download_status in [DownloadStatus.READY, DownloadStatus.DOWNLOADING]:
                             active_paths.add(os.path.abspath(item.download_path))
        except Exception as e:
             log.error(f"[Cache Cleanup] Error collecting active paths: {e}", exc_info=True)


        log.debug(f"[Cache Cleanup] Found {len(active_paths)} potential active file paths.")

        try:
            if not os.path.isdir(CACHE_DIR):
                 log.warning(f"[Cache Cleanup] Cache directory '{CACHE_DIR}' does not exist. Skipping scan.")
                 return

            for filename in os.listdir(CACHE_DIR):
                file_path = os.path.join(CACHE_DIR, filename)
                abs_file_path = os.path.abspath(file_path)

                # Skip directories, only process files
                if not os.path.isfile(file_path):
                    continue

                # Skip files that are currently active
                if abs_file_path in active_paths:
                    log.debug(f"[Cache Cleanup] Skipping active file: {filename}")
                    continue

                try:
                    # Get the last modification time
                    last_modified = os.path.getmtime(file_path)
                    age = now - last_modified

                    # Check if the file is older than the TTL
                    if age > CACHE_TTL_SECONDS:
                        log.info(f"[Cache Cleanup] Removing stale file: {filename} (Age: {age:.0f}s > {CACHE_TTL_SECONDS}s)")
                        file_size = os.path.getsize(file_path)
                        os.remove(file_path)
                        removed_count += 1
                        removed_size += file_size
                    else:
                         log.debug(f"[Cache Cleanup] Keeping file: {filename} (Age: {age:.0f}s <= {CACHE_TTL_SECONDS}s)")

                except FileNotFoundError:
                    # File might have been deleted between listdir and stat/remove
                    log.warning(f"[Cache Cleanup] File not found during processing: {filename}")
                    continue
                except OSError as e:
                    log.error(f"[Cache Cleanup] Error accessing/removing file {filename}: {e}")

            if removed_count > 0:
                log.info(f"[Cache Cleanup] Finished. Removed {removed_count} files (Total size: {removed_size / (1024*1024):.2f} MB).")
            else:
                log.info("[Cache Cleanup] Finished. No stale files found to remove.")

        except Exception as e:
            log.error(f"[Cache Cleanup] Unexpected error during cache scan: {e}", exc_info=True)


    @cache_cleanup_task.before_loop
    async def before_cleanup_task(self):
        log.debug("before_cleanup_task: Waiting for bot to be ready...")
        await self.bot.wait_until_ready()
        log.info("Cache Cleanup task starting...")

    # --- Slash Commands ---

    @commands.slash_command(name="play", description="Adds a song to the queue from a URL or search.")
    @commands.cooldown(1, 3, commands.BucketType.user) # Cooldown per user
    async def play(
        self,
        ctx: discord.ApplicationContext,
        query: discord.Option(str, description="YouTube URL or search term(s)", required=True)
    ):
        """Adds song(s) to the queue."""
        await ctx.defer() # Defer response as extraction can take time
        user = ctx.author
        guild = ctx.guild

        if not guild or not isinstance(user, discord.Member) or not user.voice or not user.voice.channel:
            await self.playback_manager._try_respond(ctx.interaction, "You need to be in a voice channel to play music!", ephemeral=True)
            return

        target_channel = user.voice.channel

        # Ensure bot is connected to the correct channel
        vc = await self.playback_manager.ensure_voice_client(ctx.interaction, target_channel, action_type="MUSIC PLAY")
        if not vc:
            # ensure_voice_client sends feedback if interaction is provided
            return

        # Give feedback that searching has started
        await self.playback_manager._try_respond(ctx.interaction, f"🔎 Searching for `{query[:100]}...`", ephemeral=False)

        video_info = await self._extract_info(query)

        if not video_info:
             log.error(f"PLAY CMD (GID:{ctx.guild.id}): _extract_info returned None/empty for query '{query[:100]}'. Aborting add.")
             await self.playback_manager._try_respond(ctx.interaction, f"❌ Could not find results for `{query[:100]}`. Try being more specific or check the URL.", ephemeral=True, delete_after=20)
             return

        guild_id = guild.id # Define guild_id

        # Define queue_item FIRST
        queue_item = MusicQueueItem(
            requester_id=user.id,
            requester_name=user.display_name,
            guild_id=guild_id, # Use guild_id here
            voice_channel_id=target_channel.id,
            text_channel_id=ctx.channel_id, # Store text channel for potential future use
            query=query, # Store original query
            video_info=video_info, # Store extracted info
        )

        # NOW the log statement can access queue_item (Removed the problematic log as per previous step)
        # log.debug(f"PLAY CMD (GID:{guild_id}): Attempting to add item '{queue_item.title[:50]}' to queue...")

        try:
            queue_pos = await self.playback_manager.add_to_queue(guild_id, queue_item)
            log.debug(f"PLAY CMD (GID:{guild_id}): add_to_queue returned position {queue_pos}. Item Type: {type(queue_item).__name__}") # Keep this log
            # Log the state of the queues *immediately* after adding
            log.debug(f"PLAY CMD (GID:{guild_id}): Current queues dict keys: {list(self.playback_manager.guild_queues.keys())}")
            if guild_id in self.playback_manager.guild_queues:
                log.debug(f"PLAY CMD (GID:{guild_id}): Queue length now: {len(self.playback_manager.guild_queues[guild_id])}") # Keep this log
            else:
                log.debug(f"PLAY CMD (GID:{guild_id}): Guild ID *still* not in queues dict after add!") # Keep this log

        except Exception as add_err:
            log.error(f"PLAY CMD (GID:{guild_id}): !!! EXCEPTION during add_to_queue !!!: {add_err}", exc_info=True) # Keep this exception logging
            await self.playback_manager._try_respond(ctx.interaction, "❌ An internal error occurred trying to add the song to the queue.", ephemeral=True)
            return # Stop processing if add failed

        # Check if the item is actually retrievable right after adding
        retrieved_queue = self.playback_manager.get_queue(guild_id)
        if not retrieved_queue or retrieved_queue[-1] != queue_item:
             log.error(f"PLAY CMD (GID:{guild_id}): CRITICAL! Item added to queue but not found/mismatch immediately after! Queue length: {len(retrieved_queue)}")


        is_playing_now = self.playback_manager.is_playing(guild_id)
        log.debug(f"PLAY CMD (GID:{guild_id}): Is playing right after add? {is_playing_now}") # Keep this

        # Send confirmation embed
        embed = discord.Embed(
            # Use title property from the dataclass
            title=f"Queued: {queue_item.title}",
            url=queue_item.original_url, # Use property
            color=discord.Color.green()
        )
        embed.add_field(name="Channel", value=queue_item.uploader, inline=True) # Use property
        embed.add_field(name="Duration", value=queue_item.duration_str, inline=True) # Use property
        embed.set_footer(text=f"Requested by {user.display_name} | Position: {queue_pos}")
        if queue_item.thumbnail: # Use property
            embed.set_thumbnail(url=queue_item.thumbnail)

        # Edit the original "Searching..." message with the embed
        await self.playback_manager._try_respond(ctx.interaction, message="", embed=embed, ephemeral=False)


    @commands.slash_command(name="skip", description="Skips the currently playing song.")
    @commands.cooldown(1, 2, commands.BucketType.guild) # Cooldown per guild
    async def skip(self, ctx: discord.ApplicationContext):
        """Skips the current song."""
        await ctx.defer(ephemeral=False) # Respond publicly
        guild = ctx.guild
        user = ctx.author

        if not guild:
            await ctx.followup.send("This command must be used in a server.", ephemeral=True); return

        guild_id = guild.id
        vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)

        if not vc or not vc.is_connected():
            await ctx.followup.send("I'm not connected to a voice channel.", ephemeral=True)
            return

        if not self.playback_manager.is_playing(guild_id):
            await ctx.followup.send("I'm not playing anything right now.", ephemeral=True)
            return

        log.info(f"COMMAND /skip invoked by {user.name} in GID:{guild_id}")
        current_item = self.playback_manager.get_current_item(guild_id)
        title = "Current Track"
        if isinstance(current_item, MusicQueueItem):
            title = current_item.title

        skipped = await self.playback_manager.skip_track(guild_id) # skip_track now handles vc.stop()
        if skipped:
            await ctx.followup.send(f"⏭️ Skipped **{title}**.")
        else:
            # This case should be rare now if is_playing check passed
            await ctx.followup.send(f"Could not skip **{title}** (wasn't playing?).", ephemeral=True)


    @commands.slash_command(name="stop", description="Stops playback, clears the queue, and leaves the channel.")
    @commands.cooldown(1, 5, commands.BucketType.guild)
    async def stop(self, ctx: discord.ApplicationContext):
        """Stops music, clears queue, leaves VC."""
        await ctx.defer(ephemeral=False) # Respond publicly
        guild = ctx.guild
        user = ctx.author

        if not guild: await ctx.followup.send("This command must be used in a server.", ephemeral=True); return

        guild_id = guild.id
        vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)

        if not vc or not vc.is_connected():
            await ctx.followup.send("I'm not connected to a voice channel.", ephemeral=True); return

        log.info(f"COMMAND /stop invoked by {user.name} in GID:{guild_id}")
        # Tell PlaybackManager to handle stop, clear, and leave
        await self.playback_manager.stop_playback(guild_id, clear_queue=True, leave_channel=True)
        await ctx.followup.send("⏹️ Playback stopped, queue cleared, and I left the channel.")


    @commands.slash_command(name="queue", description="Shows the current music queue.")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def queue(self, ctx: discord.ApplicationContext):
        """Displays the song queue."""
        await ctx.defer(ephemeral=True) # Ephemeral response for queue display
        guild = ctx.guild

        if not guild: await ctx.followup.send("This command must be used in a server.", ephemeral=True); return

        guild_id = guild.id
        current_item = self.playback_manager.get_current_item(guild_id)
        queue_list = self.playback_manager.get_queue(guild_id) # Use consistent naming

        if not current_item and not queue_list:
            await ctx.followup.send("The queue is empty!", ephemeral=True)
            return

        embed = discord.Embed(title="Music Queue", color=discord.Color.blurple())
        desc = ""

        if current_item:
            if isinstance(current_item, MusicQueueItem):
                # Add status emoji for currently playing too? Maybe just play icon.
                status_emoji = "▶️" # Play icon for currently playing
                # Use properties from the dataclass
                desc += f"{status_emoji} **Now Playing:** [{current_item.title}]({current_item.original_url}) | `{current_item.duration_str}` | Req by: {current_item.requester_name}\n\n"
            else:
                # Handle non-music items if they somehow end up as current_item
                item_type_name = type(current_item).__name__
                desc += f"▶️ **Now Playing:** Unknown Item Type (`{item_type_name}`)\n\n"

        if queue_list:
            desc += "**Up Next:**\n"
            max_display = 10 # Limit embed description length
            for i, item in enumerate(queue_list[:max_display]):
                if isinstance(item, MusicQueueItem):
                    # Use status emoji based on download status
                    status_emoji = {
                        DownloadStatus.READY: "✅",
                        DownloadStatus.DOWNLOADING: "⏳",
                        DownloadStatus.FAILED: "❌",
                        DownloadStatus.PENDING: "⌚"
                    }.get(item.download_status, "❓") # Default to question mark
                    # Use properties from the dataclass
                    desc += f"`{i+1}.` {status_emoji} [{item.title}]({item.original_url}) | `{item.duration_str}` | Req by: {item.requester_name}\n"
                else:
                     # Handle non-music items in queue
                    item_type_name = type(item).__name__
                    desc += f"`{i+1}.` ❓ Unknown Item Type (`{item_type_name}`)\n"

            if len(queue_list) > max_display:
                desc += f"\n...and {len(queue_list) - max_display} more."

        elif not current_item: # Only say queue is empty if nothing is playing either
             desc += "Queue is empty."


        # Ensure description doesn't exceed Discord limits
        if len(desc) > 4000: # Max embed description length
             desc = desc[:4000] + "\n... (Queue too long to display fully)"

        embed.description = desc
        total_songs = len(queue_list) + (1 if current_item else 0)
        embed.set_footer(text=f"Total songs: {total_songs}")

        await ctx.followup.send(embed=embed, ephemeral=True)


    @commands.slash_command(name="remove", description="Removes a song from the queue by its position.")
    @commands.cooldown(1, 2, commands.BucketType.user)
    async def remove(
        self,
        ctx: discord.ApplicationContext,
        position: discord.Option(int, description="The queue position number to remove (from /queue)", required=True, min_value=1) # Add min_value
    ):
        """Removes a song from the queue."""
        await ctx.defer(ephemeral=False) # Public confirmation
        guild = ctx.guild

        if not guild: await ctx.followup.send("Use in server.", ephemeral=True); return

        guild_id = guild.id
        queue_list = self.playback_manager.get_queue(guild_id) # Use consistent name

        if not queue_list:
            await ctx.followup.send("The queue is already empty.", ephemeral=True)
            return

        # Check bounds (position is 1-based, index is 0-based)
        if not 1 <= position <= len(queue_list):
            await ctx.followup.send(f"Invalid position. Must be between 1 and {len(queue_list)}.", ephemeral=True)
            return

        index_to_remove = position - 1 # Convert 1-based position to 0-based index
        removed_item = await self.playback_manager.remove_from_queue(guild_id, index_to_remove)

        if removed_item:
            title = "Item"
            if isinstance(removed_item, MusicQueueItem):
                title = removed_item.title
            log.info(f"COMMAND /remove: User {ctx.author.name} removed item at index {index_to_remove} ('{title}') from GID:{guild_id}")
            await ctx.followup.send(f"🗑️ Removed **{title}** from position {position}.")
        else:
             # This might happen if queue changed between getting length and removing
            log.warning(f"COMMAND /remove: Failed to remove item at index {index_to_remove} for GID:{guild_id}. Queue might have changed.")
            await ctx.followup.send(f"Could not remove item at position {position}. Queue might have changed.", ephemeral=True)


    @commands.slash_command(name="insert", description="Inserts a song at a specific queue position.")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def insert(
        self,
        ctx: discord.ApplicationContext,
        position: discord.Option(int, description="Queue position to insert at (1 for next)", required=True, min_value=1), # Add min_value
        query: discord.Option(str, description="YouTube URL or search term(s)", required=True)
    ):
        """Inserts a song into the queue."""
        await ctx.defer() # Defer response
        user = ctx.author
        guild = ctx.guild

        if not guild or not isinstance(user, discord.Member) or not user.voice or not user.voice.channel:
            await self.playback_manager._try_respond(ctx.interaction, "You need to be in a voice channel first!", ephemeral=True)
            return

        target_channel = user.voice.channel
        guild_id = guild.id

        # Get current queue size *before* potentially adding
        # Use the manager method to access the queue safely
        current_queue_size = len(self.playback_manager.get_queue(guild_id))

        # Validate position (1 to size+1 allowed)
        if not 1 <= position <= current_queue_size + 1:
            await self.playback_manager._try_respond(ctx.interaction, f"Invalid position. Must be between 1 and {current_queue_size + 1}.", ephemeral=True)
            return

        vc = await self.playback_manager.ensure_voice_client(ctx.interaction, target_channel, action_type="MUSIC INSERT")
        if not vc: return # Feedback sent by ensure_voice_client

        await self.playback_manager._try_respond(ctx.interaction, f"🔎 Searching for `{query[:100]}`...", ephemeral=False)
        video_info = await self._extract_info(query)

        if not video_info:
            log.error(f"INSERT CMD (GID:{guild_id}): _extract_info returned None/empty for query '{query[:100]}'. Aborting insert.")
            await self.playback_manager._try_respond(ctx.interaction, f"❌ Could not find results for `{query[:100]}`.", ephemeral=True, delete_after=15)
            return

        # Create the queue item
        queue_item = MusicQueueItem(
            requester_id=user.id,
            requester_name=user.display_name,
            guild_id=guild.id,
            voice_channel_id=target_channel.id,
            text_channel_id=ctx.channel_id,
            query=query,
            video_info=video_info,
        )

        insert_index = position - 1 # Convert 1-based position to 0-based index
        await self.playback_manager.insert_into_queue(guild_id, insert_index, queue_item)

        log.info(f"COMMAND /insert: User {user.name} inserted '{queue_item.title}' at index {insert_index} in GID:{guild_id}")

        # Send confirmation embed
        embed = discord.Embed(
            title=f"Inserted: {queue_item.title}",
            url=queue_item.original_url,
            color=discord.Color.blue()
        )
        embed.add_field(name="Channel", value=queue_item.uploader, inline=True)
        embed.add_field(name="Duration", value=queue_item.duration_str, inline=True)
        embed.set_footer(text=f"Requested by {user.display_name} | Position: {position}") # Show 1-based position
        if queue_item.thumbnail:
            embed.set_thumbnail(url=queue_item.thumbnail)

        await self.playback_manager._try_respond(ctx.interaction, message="", embed=embed, ephemeral=False)


# --- Setup Function ---
def setup(bot: commands.Bot):
    """Loads the Music Cog."""
    log.info("Running setup for MusicCog...")
    # Check essential dependencies
    try:
        log.debug("Attempting to import yt_dlp...")
        import yt_dlp
        log.info("yt_dlp imported successfully.")
    except ImportError:
        log.critical("Music Cog requires 'yt-dlp'. Please install it (`pip install yt-dlp`). Cog not loaded.")
        return # Stop loading if yt-dlp is missing

    # Check for FFmpeg (optional but recommended)
    ffmpeg_path = shutil.which("ffmpeg")
    ffprobe_path = shutil.which("ffprobe")
    if not ffmpeg_path:
        log.warning("Music Cog: 'ffmpeg' executable not found in PATH. Playback might fail or be limited for some formats.")
    else:
        log.info(f"Music Cog: Found ffmpeg executable at: {ffmpeg_path}")
    if not ffprobe_path:
         log.warning("Music Cog: 'ffprobe' executable not found in PATH. Some metadata extraction might fail.")
    else:
         log.info(f"Music Cog: Found ffprobe executable at: {ffprobe_path}")


    # Ensure PlaybackManager is loaded first
    if not hasattr(bot, 'playback_manager') or not isinstance(bot.playback_manager, PlaybackManager):
        log.critical("PlaybackManager instance not found on the bot object ('bot.playback_manager').")
        log.critical("Initialize PlaybackManager *before* loading the MusicCog. Cog not loaded.")
        return
    else:
        log.info("PlaybackManager found on bot instance.")

    # Add the cog
    try:
        log.info("Attempting to instantiate and add MusicCog...")
        music_cog_instance = MusicCog(bot)
        bot.add_cog(music_cog_instance)
        log.info("Music Cog added successfully via bot.add_cog.")
    except Exception as e:
        log.error(f"Error during MusicCog instantiation or add_cog: {e}", exc_info=True)