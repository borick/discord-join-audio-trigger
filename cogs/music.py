# cogs/music.py
# -*- coding: utf-8 -*-
import discord
import shutil
from discord.ext import commands, tasks
import logging
import asyncio
import os
import yt_dlp # Needs pip install yt-dlp
from functools import partial
from typing import Optional, List, Dict, Any, Union
import datetime
import time # For cache cleanup
from dataclasses import dataclass, field # For structured QueueItem

# --- Project Imports ---
import config # For MUSIC_CACHE_DIR, MUSIC_CACHE_TTL_DAYS etc.
# Assuming PlaybackManager is in core
from core.playback_manager import PlaybackManager
from utils import file_helpers # For ensure_dir

log = logging.getLogger('SoundBot.Cog.Music')

# --- Configuration ---
CACHE_DIR = getattr(config, 'MUSIC_CACHE_DIR', 'music_cache')
CACHE_TTL_SECONDS = getattr(config, 'MUSIC_CACHE_TTL_DAYS', 7) * 86400 # Default 7 days
DOWNLOAD_AHEAD_COUNT = getattr(config, 'MUSIC_DOWNLOAD_AHEAD', 2) # How many songs to download ahead
DOWNLOAD_CHECK_INTERVAL_SECONDS = getattr(config, 'MUSIC_DOWNLOAD_INTERVAL', 15)
CLEANUP_CHECK_INTERVAL_SECONDS = getattr(config, 'MUSIC_CLEANUP_INTERVAL', 3600) # Once per hour

# Ensure cache directory exists
file_helpers.ensure_dir(CACHE_DIR)
YTDL_OUT_TEMPLATE = os.path.join(CACHE_DIR, '%(extractor)s-%(id)s-%(title)s.%(ext)s')

# --- yt-dlp Options ---
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
    'default_search': 'ytsearch1',
    'source_address': '0.0.0.0',
    #--- Postprocessing for specific format (optional) ---
    'postprocessors': [{
        'key': 'FFmpegExtractAudio',
        'preferredcodec': 'opus', # Opus is generally efficient
        'preferredquality': '192', # Audio quality
    }],
    # --- Filter examples ---
    'max_filesize': 50 * 1024 * 1024, # 50MB limit
    'match_filter': yt_dlp.utils.match_filter_func('duration < 600'), # Max 10 minutes
}

# --- Download Status Enum (Optional but good practice) ---
from enum import Enum
class DownloadStatus(Enum):
    PENDING = "pending"
    DOWNLOADING = "downloading"
    READY = "ready"
    FAILED = "failed"
# Status can also be represented by simple strings

# --- Queue Item Structure ---
@dataclass
class MusicQueueItem:
    requester_id: int
    requester_name: str
    guild_id: int
    voice_channel_id: int
    text_channel_id: int # Where the request was made, for feedback
    query: str # Original query for reference
    video_info: Dict[str, Any] # Result from ytdl extract_info(download=False)
    download_status: DownloadStatus.PENDING # pending, downloading, ready, failed
    added_at: float = field(default_factory=time.time)
    download_path: Optional[str] = None
    last_played_at: Optional[float] = None # Tracked by PlaybackManager on play
    
    type: str = "music"

    # --- Convenience Properties from video_info ---
    @property
    def title(self) -> str:
        return self.video_info.get('title', 'Unknown Title')

    @property
    def original_url(self) -> str:
        return self.video_info.get('webpage_url') or self.video_info.get('original_url', 'N/A')

    @property
    def uploader(self) -> str:
        return self.video_info.get('uploader', 'Unknown Uploader')

    @property
    def duration_sec(self) -> Optional[int]:
        return self.video_info.get('duration')

    @property
    def duration_str(self) -> str:
        sec = self.duration_sec
        return str(datetime.timedelta(seconds=sec)) if sec else "N/A"

    @property
    def thumbnail(self) -> Optional[str]:
        return self.video_info.get('thumbnail')

    def get_playback_source(self) -> Optional[discord.AudioSource]:
        """Returns a playable FFmpegPCMAudioSource if ready, None otherwise."""
        if self.download_status == DownloadStatus.READY and self.download_path and os.path.exists(self.download_path):
            # Recommended: Add FFmpeg options for reconnect/stream resumption if needed
            # ffmpeg_options = { 'before_options': '-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5', 'options': '-vn' }
            ffmpeg_options = {'options': '-vn'} # Basic: No video
            return discord.FFmpegPCMAudio(self.download_path, **ffmpeg_options)
        return None

# --- Music Cog ---
class MusicCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # Ensure PlaybackManager is initialized on the bot BEFORE this cog is loaded
        if not hasattr(bot, 'playback_manager'):
             log.error("PlaybackManager not found on bot. MusicCog requires it to be initialized first.")
             # Or raise an exception to prevent loading
             raise RuntimeError("PlaybackManager not found on bot.")
        self.playback_manager: PlaybackManager = bot.playback_manager

        self._downloader_task_instance = self.downloader_task.start()
        self._cleanup_task_instance = self.cache_cleanup_task.start()
        log.info(f"MusicCog initialized. Downloader interval: {DOWNLOAD_CHECK_INTERVAL_SECONDS}s, Cleanup interval: {CLEANUP_CHECK_INTERVAL_SECONDS}s, Cache TTL: {CACHE_TTL_SECONDS}s")

    def cog_unload(self):
        """Cog cleanup."""
        self._downloader_task_instance.cancel()
        self._cleanup_task_instance.cancel()
        log.info("MusicCog background tasks cancelled.")

    # --- Helper: Run yt-dlp (Info Extraction) ---
    async def _extract_info(self, query: str) -> Optional[Dict[str, Any]]:
        """Runs yt-dlp extract_info in executor."""
        log.debug(f"Running yt-dlp info extraction for: {query[:100]}")
        try:
            partial_func = partial(yt_dlp.YoutubeDL(YTDL_OPTS).extract_info, query, download=False)
            loop = asyncio.get_running_loop()
            data = await loop.run_in_executor(None, partial_func)

            if not data:
                log.warning("yt-dlp extract_info returned no data.")
                return None
            if 'entries' in data:
                if not data['entries']:
                     log.warning("yt-dlp search result has no 'entries'.")
                     return None
                log.debug(f"Search yielded {len(data['entries'])} results, using first.")
                return data['entries'][0]
            else:
                log.debug("Direct URL processed.")
                return data
        except yt_dlp.utils.DownloadError as e:
             log.warning(f"yt-dlp DownloadError during info extraction: {e}")
             return None
        except Exception as e:
             log.error(f"Unexpected error running yt-dlp extract_info: {e}", exc_info=True)
             return None

    # --- Helper: Run yt-dlp (Download) ---
    async def _download_audio(self, video_info: Dict[str, Any]) -> Optional[str]:
        """Downloads audio using yt-dlp info in executor. Returns file path or None."""
        url = video_info.get('webpage_url') or video_info.get('original_url') or video_info.get('url')
        title = video_info.get('title', 'Unknown Title')
        if not url:
            log.error("No URL found in video info for download.")
            return None

        log.info(f"Attempting download for: '{title}' ({url})")
        try:
            # Important: Create a new YDL instance within the executor function
            # to potentially apply item-specific options if needed later,
            # and ensure thread safety if YDL instances aren't fully thread-safe.
            def download_sync(url_to_download, opts):
                with yt_dlp.YoutubeDL(opts) as ydl:
                    return ydl.extract_info(url_to_download, download=True)

            partial_func = partial(download_sync, url, YTDL_OPTS)
            loop = asyncio.get_running_loop()
            download_data = await loop.run_in_executor(None, partial_func)

            if not download_data:
                 log.error(f"yt-dlp download returned no data for '{title}'.")
                 return None

            # Determine the downloaded path reliably
            downloaded_path = None
            if download_data.get('requested_downloads'):
                 downloaded_path = download_data['requested_downloads'][0].get('filepath')
            elif download_data.get('filepath'): # Fallback
                 downloaded_path = download_data['filepath']
            else:
                # Less reliable: construct from template if download successful but path missing
                 try:
                     with yt_dlp.YoutubeDL(YTDL_OPTS) as ydl_temp:
                         # Re-prepare filename using info *from the download result*
                         prepared_path = ydl_temp.prepare_filename(download_data)
                         if os.path.exists(prepared_path):
                             downloaded_path = prepared_path
                         else:
                             log.warning(f"Could not determine downloaded path via prepare_filename fallback for '{title}'.")
                 except Exception as prep_e:
                      log.warning(f"Error during prepare_filename fallback: {prep_e}")

            if downloaded_path and os.path.exists(downloaded_path):
                log.info(f"Download successful: '{title}' -> '{os.path.basename(downloaded_path)}'")
                # Update file's modification time to reflect download time (atime updates on access)
                os.utime(downloaded_path, None) # Set access/mod time to now
                return downloaded_path
            else:
                log.error(f"Download finished for '{title}' but could not find file path. Data keys: {download_data.keys()}")
                return None

        except yt_dlp.utils.DownloadError as e:
             log.error(f"yt-dlp DownloadError during download of '{title}': {e}")
             # Check for specific errors if needed (e.g., age restriction, private video)
             return None
        except Exception as e:
            log.error(f"Unexpected error during yt-dlp download of '{title}': {e}", exc_info=True)
            return None

    # --- Background Task: Downloader ---
    @tasks.loop(seconds=DOWNLOAD_CHECK_INTERVAL_SECONDS)
    async def downloader_task(self):
        # log.debug("Downloader task running...") # Can be noisy
        active_guild_ids = list(self.playback_manager.guild_queues.keys()) # Copy keys

        for guild_id in active_guild_ids:
            queue = self.playback_manager.get_queue(guild_id)
            if not queue: continue

            # Identify items needing download (pending, within the lookahead window)
            items_to_download: List[MusicQueueItem] = []
            currently_downloading = 0
            for i, item in enumerate(queue):
                 # Only consider music items that are pending
                if isinstance(item, MusicQueueItem) and item.download_status == DownloadStatus.PENDING:
                    if i < DOWNLOAD_AHEAD_COUNT: # Check if within the desired download range
                         items_to_download.append(item)
                    else:
                        break # Don't check further than needed for this guild
                elif isinstance(item, MusicQueueItem) and item.download_status == DownloadStatus.DOWNLOADING:
                     currently_downloading += 1

            # Limit concurrent downloads per guild (optional, simple approach here)
            available_slots = max(0, DOWNLOAD_AHEAD_COUNT - currently_downloading)

            for item in items_to_download[:available_slots]: # Process only available slots
                if item.download_status == DownloadStatus.PENDING: # Double check status
                    log.info(f"[Downloader] Guild {guild_id}: Found pending item '{item.title[:50]}...', starting download.")
                    item.download_status = DownloadStatus.DOWNLOADING
                    download_path = await self._download_audio(item.video_info)

                    if download_path:
                        item.download_path = download_path
                        item.download_status = DownloadStatus.READY
                        log.info(f"[Downloader] Guild {guild_id}: Item '{item.title[:50]}...' ready.")
                        # If this was the *first* item and nothing is playing, nudge the player
                        if queue.index(item) == 0 and not self.playback_manager.is_playing(guild_id):
                             log.info(f"[Downloader] Guild {guild_id}: First item ready, ensuring playback starts.")
                             # The playback manager should ideally handle this transition automatically
                             # in its _play_next or equivalent logic when an item becomes ready.
                             # If not, explicitly call a method here:
                             await self.playback_manager.start_playback_if_idle(guild_id)
                    else:
                        item.download_status = DownloadStatus.FAILED
                        log.error(f"[Downloader] Guild {guild_id}: Failed to download item '{item.title[:50]}...'.")
                        # Optional: Send feedback to the text channel where it was requested
                        # try:
                        #     channel = self.bot.get_channel(item.text_channel_id) or await self.bot.fetch_channel(item.text_channel_id)
                        #     await channel.send(f"❌ Failed to download '{item.title}'. It will be skipped.")
                        # except (discord.NotFound, discord.Forbidden, AttributeError):
                        #     log.warning(f"Could not send download failure message to channel {item.text_channel_id}")


    @downloader_task.before_loop
    async def before_downloader_task(self):
        await self.bot.wait_until_ready()
        log.info("Downloader task starting...")

    # --- Background Task: Cache Cleanup ---
    @tasks.loop(seconds=CLEANUP_CHECK_INTERVAL_SECONDS)
    async def cache_cleanup_task(self):
        now = time.time()
        log.info(f"[Cache Cleanup] Running scan of '{CACHE_DIR}'...")
        removed_count = 0
        removed_size = 0
        active_paths = set()

        # Gather all currently queued or potentially playing file paths
        for guild_id, queue in self.playback_manager.guild_queues.items():
            current_item = self.playback_manager.get_current_item(guild_id)
            if current_item and isinstance(current_item, MusicQueueItem) and current_item.download_path:
                active_paths.add(os.path.abspath(current_item.download_path))

            for item in queue:
                 if isinstance(item, MusicQueueItem) and item.download_path:
                    # Consider items 'active' if they are ready or downloading
                    if item.download_status in ["ready", "downloading"]:
                         active_paths.add(os.path.abspath(item.download_path))

        # Scan the cache directory
        try:
            for filename in os.listdir(CACHE_DIR):
                file_path = os.path.join(CACHE_DIR, filename)
                abs_file_path = os.path.abspath(file_path)

                if not os.path.isfile(file_path):
                    continue # Skip directories or other non-files

                # Check if the file is currently active
                if abs_file_path in active_paths:
                     # log.debug(f"[Cache Cleanup] Skipping active file: {filename}")
                    continue

                try:
                    # Use modification time as a proxy for last use/download time
                    # Access time (atime) can be unreliable depending on the OS/filesystem mount options
                    last_modified = os.path.getmtime(file_path)
                    age = now - last_modified

                    if age > CACHE_TTL_SECONDS:
                        log.info(f"[Cache Cleanup] Removing stale file: {filename} (Age: {age:.0f}s > {CACHE_TTL_SECONDS}s)")
                        file_size = os.path.getsize(file_path)
                        os.remove(file_path)
                        removed_count += 1
                        removed_size += file_size
                    # else:
                    #    log.debug(f"[Cache Cleanup] Keeping file: {filename} (Age: {age:.0f}s <= {CACHE_TTL_SECONDS}s)")

                except FileNotFoundError:
                    continue # File might have been removed by another process/cleanup run
                except OSError as e:
                    log.error(f"[Cache Cleanup] Error accessing/removing file {filename}: {e}")

            if removed_count > 0:
                log.info(f"[Cache Cleanup] Finished. Removed {removed_count} files (Total size: {removed_size / (1024*1024):.2f} MB).")
            else:
                 log.info("[Cache Cleanup] Finished. No stale files found.")

        except Exception as e:
            log.error(f"[Cache Cleanup] Unexpected error during scan: {e}", exc_info=True)


    @cache_cleanup_task.before_loop
    async def before_cleanup_task(self):
        await self.bot.wait_until_ready()
        log.info("Cache Cleanup task starting...")


    # --- Commands ---

    @commands.slash_command(name="play", description="Adds a song/playlist to the queue from a URL or search.")
    @commands.cooldown(1, 3, commands.BucketType.user)
    async def play(
        self,
        ctx: discord.ApplicationContext,
        query: discord.Option(str, description="YouTube URL or search term(s)", required=True)
    ):
        """Adds song(s) to the queue."""
        await ctx.defer() # Defer publicly while searching
        user = ctx.author
        guild = ctx.guild

        if not guild or not isinstance(user, discord.Member) or not user.voice or not user.voice.channel:
            await self.playback_manager._try_respond(ctx.interaction, "You need to be in a voice channel to play music!", ephemeral=True)
            return

        target_channel = user.voice.channel

        # 1. Ensure Bot can join/play (Connect if necessary)
        vc = await self.playback_manager.ensure_voice_client(ctx.interaction, target_channel, action_type="MUSIC QUEUE")
        if not vc:
            # ensure_voice_client likely sent feedback
            return

        # 2. Get Video Info
        await self.playback_manager._try_respond(ctx.interaction, f" searching for `{query[:100]}`...", ephemeral=False)
        video_info = await self._extract_info(query)

        if not video_info:
            await self.playback_manager._try_respond(ctx.interaction, f"❌ Could not find results for `{query[:100]}`.", ephemeral=True, delete_after=15)
            return

        # 3. Prepare Queue Item (Status: pending)
        queue_item = MusicQueueItem(
            requester_id=user.id,
            requester_name=user.display_name,
            guild_id=guild.id,
            voice_channel_id=target_channel.id,
            text_channel_id=ctx.channel_id,
            query=query,
            video_info=video_info,
            download_status="pending" # Download will happen in background task
        )

        # 4. Add to Queue
        guild_id = guild.id
        queue_pos = await self.playback_manager.add_to_queue(guild_id, queue_item)

        # 5. Send Feedback
        is_playing = self.playback_manager.is_playing(guild_id)
        embed = discord.Embed(
            title=f"Queued: {queue_item.title}",
            url=queue_item.original_url,
            color=discord.Color.green() if not is_playing and queue_pos == 1 else discord.Color.light_grey()
        )
        embed.add_field(name="Channel", value=queue_item.uploader, inline=True)
        embed.add_field(name="Duration", value=queue_item.duration_str, inline=True)
        embed.set_footer(text=f"Requested by {user.display_name} | Position: {queue_pos}")
        if queue_item.thumbnail:
            embed.set_thumbnail(url=queue_item.thumbnail)

        await self.playback_manager._try_respond(ctx.interaction, message="", embed=embed, ephemeral=False)

        # No need to explicitly start download here, the background task will pick it up.
        # No need to explicitly start playback here, add_to_queue/downloader should handle it.


    @commands.slash_command(name="skip", description="Skips the currently playing song.")
    @commands.cooldown(1, 2, commands.BucketType.guild)
    async def skip(self, ctx: discord.ApplicationContext):
        """Skips the current song."""
        await ctx.defer(ephemeral=True)
        guild = ctx.guild
        user = ctx.author

        if not guild or not self.playback_manager.is_playing(guild.id):
            await ctx.followup.send("I'm not playing anything right now.", ephemeral=True)
            return

        vc = discord.utils.get(self.bot.voice_clients, guild=guild)
        if not vc or not vc.is_connected():
             await ctx.followup.send("I'm not connected to a voice channel.", ephemeral=True)
             return # Should not happen if is_playing is true, but safety check

        log.info(f"COMMAND /skip invoked by {user.name} in GID:{guild.id}")

        current_item = self.playback_manager.get_current_item(guild.id)
        title = current_item.title if isinstance(current_item, MusicQueueItem) else "Current Track"

        # Stop the current track. The PlaybackManager's 'after' callback
        # (or equivalent logic) should handle playing the next one.
        vc.stop()

        await ctx.followup.send(f"⏭️ Skipped **{title}**.", ephemeral=False) # Announce skip publicly


    @commands.slash_command(name="stop", description="Stops playback, clears the queue, and leaves the channel.")
    @commands.cooldown(1, 5, commands.BucketType.guild)
    async def stop(self, ctx: discord.ApplicationContext):
        """Stops music, clears queue, leaves VC."""
        await ctx.defer(ephemeral=True)
        guild = ctx.guild
        user = ctx.author

        if not guild: await ctx.followup.send("Use in server.", ephemeral=True); return

        vc = discord.utils.get(self.bot.voice_clients, guild=guild)
        if not vc or not vc.is_connected():
            await ctx.followup.send("I'm not connected to a voice channel.", ephemeral=True); return

        log.info(f"COMMAND /stop invoked by {user.name} in GID:{guild.id}")

        # Use PlaybackManager's method to handle stopping and clearing
        await self.playback_manager.stop_playback(guild.id, clear_queue=True, leave_channel=True)

        await ctx.followup.send("⏹️ Playback stopped, queue cleared, and I left the channel.", ephemeral=False) # Announce stop


    @commands.slash_command(name="queue", description="Shows the current music queue.")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def queue(self, ctx: discord.ApplicationContext):
        """Displays the song queue."""
        await ctx.defer(ephemeral=True) # Usually better ephemeral unless queue is always shared
        guild = ctx.guild
        if not guild: await ctx.followup.send("Use in server.", ephemeral=True); return

        guild_id = guild.id
        current_item = self.playback_manager.get_current_item(guild_id)
        queue = self.playback_manager.get_queue(guild_id)

        if not current_item and not queue:
            await ctx.followup.send("The queue is empty!", ephemeral=True)
            return

        embed = discord.Embed(title="Music Queue", color=discord.Color.blurple())
        desc = ""

        # Display Currently Playing
        if current_item and isinstance(current_item, MusicQueueItem):
             status_emoji = {
                 "ready": "▶️",
                 "downloading": "⏳",
                 "failed": "❌",
                 "pending": "⌚" # Should ideally not be playing if pending
             }.get(current_item.download_status, "❓")
             desc += f"{status_emoji} **Now Playing:** [{current_item.title}]({current_item.original_url}) | `{current_item.duration_str}` | Requested by: {current_item.requester_name}\n\n"
        elif current_item: # Handle non-music items if your manager supports them
             desc += f"▶️ **Now Playing:** Unknown Item Type\n\n"


        # Display Upcoming Queue
        if queue:
            desc += "**Up Next:**\n"
            # Limit display length to avoid embed limits
            max_display = 10
            for i, item in enumerate(queue[:max_display]):
                 if isinstance(item, MusicQueueItem):
                      status_emoji = {
                          "ready": "✅",
                          "downloading": "⏳",
                          "failed": "❌",
                          "pending": "⌚"
                      }.get(item.download_status, "❓")
                      desc += f"`{i+1}.` {status_emoji} [{item.title}]({item.original_url}) | `{item.duration_str}` | Req by: {item.requester_name}\n"
                 else:
                     desc += f"`{i+1}.` ❓ Unknown Item Type\n"

            if len(queue) > max_display:
                 desc += f"\n...and {len(queue) - max_display} more."
        else:
             desc += "Queue is empty."

        # Check description length
        if len(desc) > 4000: # Embed description limit is 4096
             desc = desc[:4000] + "\n... (Queue too long to display fully)"

        embed.description = desc
        embed.set_footer(text=f"Total songs: {len(queue) + (1 if current_item else 0)}")

        await ctx.followup.send(embed=embed, ephemeral=True)


    @commands.slash_command(name="remove", description="Removes a song from the queue by its position.")
    @commands.cooldown(1, 2, commands.BucketType.user)
    async def remove(
        self,
        ctx: discord.ApplicationContext,
        position: discord.Option(int, description="The queue position number to remove (from /queue)", required=True)
    ):
        """Removes a song from the queue."""
        await ctx.defer(ephemeral=True)
        guild = ctx.guild
        if not guild: await ctx.followup.send("Use in server.", ephemeral=True); return

        guild_id = guild.id
        queue = self.playback_manager.get_queue(guild_id)

        if not queue:
            await ctx.followup.send("The queue is already empty.", ephemeral=True)
            return

        if not 1 <= position <= len(queue):
            await ctx.followup.send(f"Invalid position. Must be between 1 and {len(queue)}.", ephemeral=True)
            return

        # Adjust position to 0-based index
        index_to_remove = position - 1

        removed_item = await self.playback_manager.remove_from_queue(guild_id, index_to_remove)

        if removed_item and isinstance(removed_item, MusicQueueItem):
            log.info(f"COMMAND /remove: User {ctx.author.name} removed item at index {index_to_remove} ('{removed_item.title}') from GID:{guild_id}")
            # Note: File cleanup is handled by the background task, no need to delete file here.
            await ctx.followup.send(f"🗑️ Removed **{removed_item.title}** from position {position}.", ephemeral=False) # Announce removal
        elif removed_item:
             log.info(f"COMMAND /remove: User {ctx.author.name} removed non-music item at index {index_to_remove} from GID:{guild_id}")
             await ctx.followup.send(f"🗑️ Removed item from position {position}.", ephemeral=False)
        else:
             # This might happen if the queue changed between check and removal (less likely with defer)
             await ctx.followup.send(f"Could not remove item at position {position}. Queue might have changed.", ephemeral=True)


    @commands.slash_command(name="insert", description="Inserts a song at a specific queue position.")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def insert(
        self,
        ctx: discord.ApplicationContext,
        position: discord.Option(int, description="Queue position to insert at (1 for next)", required=True),
        query: discord.Option(str, description="YouTube URL or search term(s)", required=True)
    ):
        """Inserts a song into the queue."""
        await ctx.defer() # Defer publicly while searching
        user = ctx.author
        guild = ctx.guild

        if not guild or not isinstance(user, discord.Member) or not user.voice or not user.voice.channel:
            await self.playback_manager._try_respond(ctx.interaction, "You need to be in a voice channel first!", ephemeral=True)
            return

        target_channel = user.voice.channel
        guild_id = guild.id
        current_queue_size = len(self.playback_manager.get_queue(guild_id))

        # Validate position (allow inserting at the very end)
        if not 1 <= position <= current_queue_size + 1:
            await self.playback_manager._try_respond(ctx.interaction, f"Invalid position. Must be between 1 and {current_queue_size + 1}.", ephemeral=True)
            return

        # 1. Ensure Bot can join/play (Connect if necessary)
        vc = await self.playback_manager.ensure_voice_client(ctx.interaction, target_channel, action_type="MUSIC QUEUE")
        if not vc: return

        # 2. Get Video Info
        await self.playback_manager._try_respond(ctx.interaction, f" searching for `{query[:100]}`...", ephemeral=False)
        video_info = await self._extract_info(query)
        if not video_info:
            await self.playback_manager._try_respond(ctx.interaction, f"❌ Could not find results for `{query[:100]}`.", ephemeral=True, delete_after=15)
            return

        # 3. Prepare Queue Item
        queue_item = MusicQueueItem(
            requester_id=user.id,
            requester_name=user.display_name,
            guild_id=guild.id,
            voice_channel_id=target_channel.id,
            text_channel_id=ctx.channel_id,
            query=query,
            video_info=video_info,
            download_status="pending"
        )

        # 4. Insert into Queue (adjust position for 0-based index)
        insert_index = position - 1
        await self.playback_manager.insert_into_queue(guild_id, insert_index, queue_item)
        log.info(f"COMMAND /insert: User {user.name} inserted '{queue_item.title}' at index {insert_index} in GID:{guild_id}")


        # 5. Send Feedback
        is_playing = self.playback_manager.is_playing(guild_id)
        embed = discord.Embed(
            title=f"Inserted: {queue_item.title}",
            url=queue_item.original_url,
            color=discord.Color.blue()
        )
        embed.add_field(name="Channel", value=queue_item.uploader, inline=True)
        embed.add_field(name="Duration", value=queue_item.duration_str, inline=True)
        # Determine the final position after insertion
        final_position = insert_index + 1
        embed.set_footer(text=f"Requested by {user.display_name} | Position: {final_position}")
        if queue_item.thumbnail:
            embed.set_thumbnail(url=queue_item.thumbnail)

        await self.playback_manager._try_respond(ctx.interaction, message="", embed=embed, ephemeral=False)

        # Background task will handle download. If inserted at pos 1, downloader/player logic should pick it up.


# --- Setup Function ---
async def setup(bot: commands.Bot):
    # Dependency Checks
    try:
        import yt_dlp
    except ImportError:
        log.critical("Music Cog requires 'yt-dlp'. Please install it (`pip install yt-dlp`). Cog not loaded.")
        return # Prevent loading

    if not shutil.which("ffmpeg"):
         log.warning("Music Cog: 'ffmpeg' executable not found in PATH. Playback might fail or be limited.")

    # Ensure PlaybackManager is ready on the bot object before adding the cog
    if not hasattr(bot, 'playback_manager'):
         log.critical("PlaybackManager instance not found on the bot object ('bot.playback_manager').")
         log.critical("Initialize PlaybackManager *before* loading the MusicCog. Cog not loaded.")
         # Example: bot.playback_manager = PlaybackManager(bot) in your main bot file before load_extension
         return # Prevent loading

    await bot.add_cog(MusicCog(bot))
    log.info("Music Cog loaded successfully.")