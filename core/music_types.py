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

                def _create_ffmpeg_source():
                    log.debug(f"Creating FFmpegPCMAudio source for: {self.download_path}")
                    return discord.FFmpegPCMAudio(self.download_path, **ffmpeg_options)

                log.debug(f"Running FFmpegPCMAudio creation in executor for: {self.download_path}")
                audio_source = await loop.run_in_executor(None, _create_ffmpeg_source)
                log.debug(f"Successfully created FFmpegPCMAudio source for: {self.download_path}")
                return audio_source
            except Exception as e:
                log.error(f"[ERROR] Failed to create FFmpegPCMAudio source for {self.download_path}: {e}", exc_info=True)
                self.download_status = DownloadStatus.FAILED
                return None
        else:
            # Log why it's failing if conditions aren't met
            log.warning(f"get_playback_source called but conditions not met. Status: {self.download_status}, Path: {self.download_path}, Exists: {os.path.exists(self.download_path) if self.download_path else 'N/A'}")
            return None