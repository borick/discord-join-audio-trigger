# cogs/music.py
# -*- coding: utf-8 -*-
import discord
import shutil
from discord.ext import commands
import logging
import asyncio
import os
import yt_dlp # Needs pip install yt-dlp
from functools import partial
from typing import Optional, List, Dict, Any
import datetime
import config # For MUSIC_CACHE_DIR etc.
from core.playback_manager import PlaybackManager, QueueItem # Import manager and type hint
from utils import file_helpers # For ensure_dir

log = logging.getLogger('SoundBot.Cog.Music')

# --- yt-dlp Options ---
# Ensure cache directory exists
if hasattr(config, 'MUSIC_CACHE_DIR'):
    file_helpers.ensure_dir(config.MUSIC_CACHE_DIR)
    YTDL_OUT_TEMPLATE = os.path.join(config.MUSIC_CACHE_DIR, '%(extractor)s-%(id)s-%(title)s.%(ext)s')
else:
    # Fallback if MUSIC_CACHE_DIR is not in config (use a default subdir)
    file_helpers.ensure_dir("music_cache")
    YTDL_OUT_TEMPLATE = os.path.join("music_cache", '%(extractor)s-%(id)s-%(title)s.%(ext)s')

YTDL_OPTS = {
    'format': 'bestaudio/best', # Prioritize best audio, fallback to best overall
    'outtmpl': YTDL_OUT_TEMPLATE,
    'restrictfilenames': True,
    'noplaylist': True,
    'nocheckcertificate': True,
    'ignoreerrors': False, # Change to True to skip unavailable videos in searches?
    'logtostderr': False,
    'quiet': True,
    'no_warnings': True,
    'default_search': 'ytsearch1', # Auto search and pick first result if not URL
    'source_address': '0.0.0.0', # Bind to all IPs, might help in some environments
    # --- Postprocessor to embed thumbnail (optional, requires atomicparsley or ffmpeg) ---
    # 'postprocessors': [{
    #     'key': 'FFmpegExtractAudio',
    #     'preferredcodec': 'm4a', # or 'mp3', 'opus' etc.
    # }],
    # --- Optional: Limit download size/duration ---
    # 'max_filesize': 50 * 1024 * 1024, # e.g., 50MB limit
    # 'match_filter': yt_dlp.utils.match_filter_func('duration < 600'), # e.g., Max 10 minutes
}


class MusicCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.playback_manager: PlaybackManager = bot.playback_manager # Shortcut

    # --- Helper for yt-dlp ---
    async def run_ytdl(self, query: str) -> Optional[Dict[str, Any]]:
        """Runs yt-dlp in an executor to avoid blocking."""
        log.debug(f"Running yt-dlp for query: {query[:100]}")
        try:
            # Use functools.partial to pass args to the function running in the executor
            # Create a new YDL instance *inside* the executor function for thread safety
            partial_func = partial(yt_dlp.YoutubeDL(YTDL_OPTS).extract_info, query, download=False)
            loop = asyncio.get_running_loop()
            data = await loop.run_in_executor(None, partial_func) # None uses default ThreadPoolExecutor

            if not data:
                log.warning("yt-dlp extract_info returned no data.")
                return None

            # If 'entries' exists, it was a search/playlist, take the first result
            if 'entries' in data:
                if not data['entries']:
                     log.warning("yt-dlp search result has no 'entries'.")
                     return None
                log.debug(f"Search successful, using first result: {data['entries'][0].get('title', 'N/A')}")
                return data['entries'][0]
            else:
                # It was likely a direct URL
                log.debug(f"Direct URL processed: {data.get('title', 'N/A')}")
                return data

        except yt_dlp.utils.DownloadError as e:
             log.error(f"yt-dlp DownloadError: {e}") # Often network issues, unavailable video, etc.
             return None # Indicate error
        except Exception as e:
             log.error(f"Unexpected error running yt-dlp extract_info: {e}", exc_info=True)
             return None # Indicate error


    async def download_audio(self, video_info: Dict[str, Any]) -> Optional[str]:
        """Downloads audio using yt-dlp info in an executor. Returns file path or None."""
        url = video_info.get('webpage_url') or video_info.get('original_url') or video_info.get('url')
        title = video_info.get('title', 'Unknown Title')
        if not url:
            log.error("No URL found in video info for download.")
            return None

        log.info(f"Attempting download for: '{title}' ({url})")
        try:
            # Create YDL instance inside executor function
            partial_func = partial(yt_dlp.YoutubeDL(YTDL_OPTS).extract_info, url, download=True)
            loop = asyncio.get_running_loop()
            download_data = await loop.run_in_executor(None, partial_func)

            if not download_data:
                 log.error("yt-dlp download returned no data.")
                 return None

            # yt-dlp returns the *filename* it downloaded to when download=True
            # However, the exact key might vary, 'requested_downloads' is more reliable
            downloaded_path = None
            if download_data.get('requested_downloads'):
                 downloaded_path = download_data['requested_downloads'][0]['filepath']
            elif download_data.get('filepath'): # Fallback for older versions?
                 downloaded_path = download_data['filepath']
            elif YTDL_OPTS.get('outtmpl'):
                 # Try constructing the path based on template (less reliable)
                 try:
                     ydl_temp = yt_dlp.YoutubeDL(YTDL_OPTS)
                     downloaded_path = ydl_temp.prepare_filename(download_data)
                 except Exception as prep_e:
                      log.warning(f"Could not determine downloaded path via prepare_filename: {prep_e}")


            if downloaded_path and os.path.exists(downloaded_path):
                log.info(f"Download successful: '{title}' saved to '{downloaded_path}'")
                return downloaded_path
            else:
                log.error(f"Download finished but could not find file path. Data: {download_data.get('requested_downloads', 'N/A')}")
                return None

        except yt_dlp.utils.DownloadError as e:
             log.error(f"yt-dlp DownloadError during download: {e}")
             return None
        except Exception as e:
            log.error(f"Unexpected error during yt-dlp download: {e}", exc_info=True)
            return None

    # --- Commands ---
    @commands.slash_command(name="play", description="Plays audio from a YouTube URL or search query.")
    @commands.cooldown(1, 5, commands.BucketType.user) # Limit command usage slightly
    async def play(
        self,
        ctx: discord.ApplicationContext,
        query: discord.Option(str, description="YouTube URL or search term(s)", required=True)
    ):
        """Plays audio from YouTube."""
        await ctx.defer() # Defer publicly
        user = ctx.author
        guild = ctx.guild

        if not guild or not isinstance(user, discord.Member) or not user.voice or not user.voice.channel:
            await self.playback_manager._try_respond(ctx.interaction, "You need to be in a voice channel first!", ephemeral=True)
            return

        target_channel = user.voice.channel

        # 1. Ensure Bot can join/play
        vc = await self.playback_manager.ensure_voice_client(ctx.interaction, target_channel, action_type="MUSIC PLAY")
        if not vc:
            # ensure_voice_client already sent feedback
            return

        # 2. Get Video Info (Search or URL)
        await self.playback_manager._try_respond(ctx.interaction, f" searching for `{query[:100]}`...", ephemeral=False) # Edit initial defer
        video_info = await self.run_ytdl(query)

        if not video_info:
            await self.playback_manager._try_respond(ctx.interaction, f"❌ Could not find results for `{query[:100]}`.", ephemeral=True, delete_after=15)
            return

        title = video_info.get('title', 'Unknown Title')
        original_url = video_info.get('webpage_url') or video_info.get('original_url', 'N/A')
        uploader = video_info.get('uploader', 'Unknown Uploader')
        duration_sec = video_info.get('duration')
        duration_str = str(datetime.timedelta(seconds=duration_sec)) if duration_sec else "N/A"

        # 3. Download Audio
        await self.playback_manager._try_respond(ctx.interaction, f" Downloading `{title[:50]}...`", ephemeral=False)
        downloaded_path = await self.download_audio(video_info)

        if not downloaded_path:
             await self.playback_manager._try_respond(ctx.interaction, f"❌ Failed to download audio for `{title[:50]}...`.", ephemeral=True, delete_after=15)
             return

        # 4. Prepare Queue Item
        queue_item: MusicQueueItem = {
            'type': 'music',
            'requester_id': user.id,
            'requester_name': user.display_name,
            'path': downloaded_path,
            'title': title,
            'original_url': original_url,
            'uploader': uploader,
            'duration_str': duration_str,
            'voice_channel_id': target_channel.id,
        }

        # 5. Add to Queue and Maybe Start Playback
        guild_id = guild.id
        is_playing = vc.is_playing()
        queue_was_empty = not self.playback_manager.guild_queues.get(guild_id)

        await self.playback_manager.add_to_queue(guild_id, queue_item)

        # Send feedback
        queue_pos = len(self.playback_manager.guild_queues.get(guild_id, [])) # Position is current length
        embed = discord.Embed(
            title=f"Queued: {title}",
            url=original_url,
            color=discord.Color.green() if not is_playing and queue_was_empty else discord.Color.light_grey()
        )
        embed.add_field(name="Channel", value=uploader, inline=True)
        embed.add_field(name="Duration", value=duration_str, inline=True)
        embed.set_footer(text=f"Requested by {user.display_name} | Position: {queue_pos}")
        thumbnail = video_info.get('thumbnail')
        if thumbnail: embed.set_thumbnail(url=thumbnail)

        await self.playback_manager._try_respond(ctx.interaction, content="", embed=embed, ephemeral=False) # Send embed confirming queue add

        # Note: add_to_queue now calls start_playback_if_idle, so no need to explicitly start here


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

        # Clear queue
        if guild.id in self.playback_manager.guild_queues:
            self.playback_manager.guild_queues[guild.id].clear()
            log.debug(f"Cleared queue for GID:{guild.id}")

        # Stop playback task
        if guild.id in self.playback_manager.guild_play_tasks:
             task = self.playback_manager.guild_play_tasks.pop(guild.id, None)
             if task and not task.done(): task.cancel()
             log.debug(f"Cancelled play task for GID:{guild.id}")
        self.playback_manager.currently_playing.pop(guild.id, None) # Clear current item

        # Stop audio player
        if vc.is_playing(): vc.stop()

        # Disconnect (respecting stay?) - For /stop, probably should always leave.
        await self.playback_manager.safe_disconnect(vc, manual_leave=True, reason="/stop command")

        await ctx.followup.send("⏹️ Playback stopped, queue cleared, and I left the channel.", ephemeral=True)

    # TODO: Add /skip, /queue commands

def setup(bot: commands.Bot):
    # Check if yt-dlp is installed before loading
    try:
        import yt_dlp
        YTDLP_OK = True
    except ImportError:
        log.error("Music Cog not loading: yt-dlp library not found. Please install: pip install yt-dlp")
        YTDLP_OK = False

    if YTDLP_OK:
        # Check for FFmpeg (optional but highly recommended)
        if not shutil.which("ffmpeg"):
             log.warning("Music Cog: FFmpeg executable not found in PATH. Compatibility with some audio formats may be limited.")

        bot.add_cog(MusicCog(bot))
        log.info("Music Cog loaded.")
    else:
         log.error("Music Cog failed to load due to missing yt-dlp.")