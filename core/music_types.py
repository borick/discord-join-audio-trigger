# core/music_types.py

import asyncio
from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, Any, Optional
import discord # For discord.AudioSource type hint if needed later
import time
import datetime
import os # For os.path.exists
import logging

log = logging.getLogger('SoundBot.MusicTypes')

# --- Enums and Dataclasses ---
class DownloadStatus(Enum):
    PENDING = "pending"
    DOWNLOADING = "downloading"
    READY = "ready"
    FAILED = "failed"

@dataclass
class MusicQueueItem:
    requester_id: int
    requester_name: str
    guild_id: int
    voice_channel_id: int
    text_channel_id: int
    query: str
    video_info: Dict[str, Any] # Result from extract_info
    download_status: DownloadStatus = DownloadStatus.PENDING # Default status
    added_at: float = field(default_factory=time.time)
    download_path: Optional[str] = None
    last_played_at: Optional[float] = None
    type: str = "music" # To differentiate from other queue items

    # --- Properties ---
    @property
    def title(self) -> str:
        return self.video_info.get('title', 'Unknown Title')

    @property
    def original_url(self) -> str:
        return self.video_info.get('webpage_url') or self.video_info.get('original_url', self.video_info.get('url', 'N/A'))

    @property
    def uploader(self) -> str:
        return self.video_info.get('uploader', 'Unknown Uploader')

    @property
    def duration_sec(self) -> Optional[int]:
        return self.video_info.get('duration')

    @property
    def duration_str(self) -> str:
        sec = self.duration_sec
        return str(datetime.timedelta(seconds=sec)) if sec is not None else "N/A"

    @property
    def thumbnail(self) -> Optional[str]:
        thumbnails = self.video_info.get('thumbnails')
        if isinstance(thumbnails, list) and thumbnails:
            return thumbnails[-1].get('url')
        return self.video_info.get('thumbnail')

    async def get_playback_source(self) -> Optional[discord.AudioSource]:
        if self.download_status == DownloadStatus.READY and self.download_path and os.path.exists(self.download_path):
            try:
                ffmpeg_options = {'options': '-vn'}
                loop = asyncio.get_running_loop()

                # Define a synchronous function for the blocking part
                def _create_ffmpeg_source():
                    # This runs in the executor thread
                    return discord.FFmpegPCMAudio(self.download_path, **ffmpeg_options)

                # Run the blocking function in the executor
                audio_source = await loop.run_in_executor(None, _create_ffmpeg_source)
                # 'None' uses the default ThreadPoolExecutor

                return audio_source
            except Exception as e:
                log.error(f"[ERROR] Failed to create FFmpegPCMAudio source for {self.download_path}: {e}", exc_info=True)
                self.download_status = DownloadStatus.FAILED # Mark as failed if creation fails
                return None
        else:
            log.warning(f"get_playback_source called but not ready. Status: {self.download_status}, Path: {self.download_path}")
            if self.download_status != DownloadStatus.FAILED:
                 # If path exists but status isn't READY, maybe reset? Or handle upstream.
                 # For now, just ensure it fails if conditions aren't met.
                 pass # Or potentially set status to FAILED if path exists but status is wrong.
            return None