# -*- coding: utf-8 -*-
import os
import io
import math
import logging
from typing import Optional, Tuple

import discord

# Import pydub safely
try:
    from pydub import AudioSegment
    from pydub.exceptions import CouldntDecodeError
    PYDUB_AVAILABLE = True
except ImportError:
    logging.critical("CRITICAL: Pydub library not found. Please install it: pip install pydub ffmpeg")
    PYDUB_AVAILABLE = False

import config # Import config for constants

log = logging.getLogger('SoundBot.AudioProcessor')

def process_audio(sound_path: str, member_display_name: str = "User") -> Tuple[Optional[discord.PCMAudio], Optional[io.BytesIO]]:
    """
    Loads, TRIMS, normalizes, and prepares audio for Discord playback.
    Returns a tuple: (PCMAudio source or None, BytesIO buffer or None).
    The BytesIO buffer MUST be closed by the caller after playback is finished or fails.
    """
    if not PYDUB_AVAILABLE:
        log.error("AUDIO: Pydub library is not available. Cannot process audio.")
        return None, None
    if not os.path.exists(sound_path):
        log.error(f"AUDIO: File not found: '{sound_path}'")
        return None, None

    audio_source: Optional[discord.PCMAudio] = None
    pcm_data_io: Optional[io.BytesIO] = None # Initialize buffer variable
    basename = os.path.basename(sound_path)

    try:
        log.debug(f"AUDIO: Loading '{basename}'...")
        ext = os.path.splitext(sound_path)[1].lower().strip('. ') or 'mp3'
        if not ext:
             log.warning(f"AUDIO: File '{basename}' has no extension. Assuming mp3.")
             ext = 'mp3'

        # Load audio using Pydub
        try:
            audio_segment = AudioSegment.from_file(sound_path, format=ext)
        except CouldntDecodeError as decode_err:
            raise decode_err # Re-raise specifically for the outer handler
        except Exception as load_e:
            log.warning(f"AUDIO: Initial load failed for '{basename}', trying explicit format if possible. Error: {load_e}")
            if ext == 'm4a': audio_segment = AudioSegment.from_file(sound_path, format="m4a")
            elif ext == 'aac': audio_segment = AudioSegment.from_file(sound_path, format="aac")
            elif ext == 'ogg': audio_segment = AudioSegment.from_file(sound_path, format="ogg")
            else: raise load_e

        # Trim audio
        if len(audio_segment) > config.MAX_PLAYBACK_DURATION_MS:
            log.info(f"AUDIO: Trimming '{basename}' from {len(audio_segment)}ms to first {config.MAX_PLAYBACK_DURATION_MS}ms.")
            audio_segment = audio_segment[:config.MAX_PLAYBACK_DURATION_MS]
        else:
            log.debug(f"AUDIO: '{basename}' is {len(audio_segment)}ms (<= {config.MAX_PLAYBACK_DURATION_MS}ms), no trimming needed.")

        # Normalize loudness
        peak_dbfs = audio_segment.max_dBFS
        if not math.isinf(peak_dbfs) and peak_dbfs > -90.0:
            change_in_dbfs = config.TARGET_LOUDNESS_DBFS - peak_dbfs
            log.info(f"AUDIO: Normalizing '{basename}'. Peak:{peak_dbfs:.2f} Target:{config.TARGET_LOUDNESS_DBFS:.2f} Gain:{change_in_dbfs:.2f} dB.")
            gain_limit = 6.0 # Limit positive gain
            apply_gain = min(change_in_dbfs, gain_limit) if change_in_dbfs > 0 else change_in_dbfs
            if apply_gain != change_in_dbfs:
                log.info(f"AUDIO: Limiting gain to +{gain_limit}dB for '{basename}' (calculated: {change_in_dbfs:.2f}dB).")
            audio_segment = audio_segment.apply_gain(apply_gain)
        elif math.isinf(peak_dbfs):
            log.warning(f"AUDIO: Cannot normalize silent audio '{basename}'. Peak is -inf.")
        else:
             log.warning(f"AUDIO: Skipping normalization for very quiet audio '{basename}'. Peak: {peak_dbfs:.2f}")

        # Resample and set channels for Discord
        audio_segment = audio_segment.set_frame_rate(48000).set_channels(2)

        # Export to PCM S16LE in memory
        pcm_data_io = io.BytesIO()
        audio_segment.export(pcm_data_io, format="s16le")
        pcm_data_io.seek(0)

        if pcm_data_io.getbuffer().nbytes > 0:
            audio_source = discord.PCMAudio(pcm_data_io)
            log.debug(f"AUDIO: Successfully processed '{basename}'")
            return audio_source, pcm_data_io # Return source and buffer
        else:
            log.error(f"AUDIO: Exported raw audio for '{basename}' is empty!")
            if pcm_data_io: pcm_data_io.close()
            return None, None

    except CouldntDecodeError as decode_err:
        log.error(f"AUDIO: Pydub CouldntDecodeError for '{basename}'. Is FFmpeg installed and in PATH? Is the file corrupt? Error: {decode_err}", exc_info=True)
        if pcm_data_io: pcm_data_io.close()
        return None, None
    except FileNotFoundError:
         log.error(f"AUDIO: File not found during processing: '{sound_path}'")
         if pcm_data_io: pcm_data_io.close()
         return None, None
    except Exception as e:
        log.error(f"AUDIO: Unexpected error processing '{basename}': {e}", exc_info=True)
        if pcm_data_io and not pcm_data_io.closed:
            try: pcm_data_io.close()
            except Exception: pass
        return None, None
