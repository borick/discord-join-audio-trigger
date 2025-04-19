# -*- coding: utf-8 -*-
import discord
from discord.ext import commands
import os
import sys
import struct
import logging
import asyncio
import platform
from typing import Dict, Any

# --- Import Core Components ---
import config # Bot config, paths, constants
import data_manager # Functions to load/save data
from core.playback_manager import PlaybackManager # Handles audio queues and playback
from utils import file_helpers # For ensure_dir and initial checks

# --- Logging Setup ---
# Define log format
log_formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')
# Define handlers
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
# Configure root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO) # Set root level (e.g., INFO)
root_logger.addHandler(console_handler)
# Configure specific loggers
logging.getLogger('discord').setLevel(logging.WARNING) # Reduce discord lib noise
logging.getLogger('SoundBot').setLevel(logging.INFO) # Main bot logger
# Add more specific log levels if needed (e.g., logging.getLogger('SoundBot.PlaybackManager').setLevel(logging.DEBUG))
log = logging.getLogger('SoundBot.Main')

# --- Initial Dependency Checks ---
# Check for required libraries early
try: from pydub import AudioSegment; PYDUB_OK = True
except ImportError: log.critical("CRITICAL: Pydub library not found. Install: pip install pydub ffmpeg"); PYDUB_OK = False
try: import edge_tts; EDGE_TTS_OK = True
except ImportError: log.critical("CRITICAL: edge-tts library not found. Install: pip install edge-tts"); EDGE_TTS_OK = False
try: import nacl; NACL_OK = True
except ImportError: log.critical("CRITICAL: PyNaCl library not found. Voice WILL NOT WORK. Install: pip install PyNaCl"); NACL_OK = False

if not config.BOT_TOKEN or not PYDUB_OK or not EDGE_TTS_OK or not NACL_OK:
    log.critical("CRITICAL ERROR: Bot token missing or core libraries failed to import. Exiting.")
    exit(1)

# --- Opus Loading Check ---
opus_load_success = False
if discord.opus.is_loaded():
    log.info("Opus library already loaded.")
    opus_load_success = True
else:
    log.info("Opus library not initially loaded. Attempting default load...")
    try:
        # Try loading bundled libopus DLL first (Windows)
        if sys.platform == 'win32':
            basedir = os.path.dirname(os.path.abspath(discord.opus.__file__))
            _bitness = struct.calcsize('P') * 8
            _target = 'x64' if _bitness > 32 else 'x86'
            _filename = os.path.join(basedir, 'bin', f'libopus-0.{_target}.dll')
            if os.path.exists(_filename):
                discord.opus.load_opus(_filename)
                opus_load_success = discord.opus.is_loaded()
                if opus_load_success: log.info(f"Successfully loaded bundled Opus DLL: {_filename}")
                else: log.warning("Attempted bundled Opus DLL, but is_loaded() is still False.")
            else: log.warning(f"Bundled Opus DLL not found: {_filename}")

        # If not loaded yet, try find_library (Linux/macOS/other)
        if not opus_load_success:
            import ctypes.util # Import only if needed
            found_path = ctypes.util.find_library('opus')
            if found_path:
                discord.opus.load_opus(found_path)
                opus_load_success = discord.opus.is_loaded()
                if opus_load_success: log.info(f"Successfully loaded Opus via find_library: {found_path}")
                else: log.warning("Found Opus via find_library, but is_loaded() is still False.")
            else: log.warning("Could not find Opus library using ctypes.util.find_library('opus').")

        # Final fallback: try loading 'opus' directly
        if not opus_load_success:
            try:
                discord.opus.load_opus('opus')
                opus_load_success = discord.opus.is_loaded()
                if opus_load_success: log.info("Successfully loaded Opus using generic name 'opus'.")
            except OSError: pass # Ignore if generic load fails

    except Exception as e:
        log.error(f"Error occurred during Opus load attempt: {e}", exc_info=True)

# Report final Opus status
if opus_load_success:
    try:
        version = discord.opus._OpusStruct.get_opus_version()
        log.info(f"Opus library loading confirmed. Version: {version}")
    except Exception as e: log.warning(f"Opus loaded, but failed to get version string: {e}")
else:
    log.error("❌ FAILED to confirm Opus library loading. Voice stability issues possible.")
    # Consider adding instructions or links for troubleshooting Opus installation

# --- Ensure Directories Exist ---
file_helpers.ensure_dir(config.SOUNDS_DIR)
file_helpers.ensure_dir(config.USER_SOUNDS_DIR)
file_helpers.ensure_dir(config.PUBLIC_SOUNDS_DIR)

# --- Bot Intents ---
intents = discord.Intents.default()
intents.voice_states = True # Needed for join/leave events and VC state
intents.guilds = True       # Needed for guild information and commands
intents.message_content = False # Not needed for slash commands
intents.members = True      # NEEDED to accurately get display names and check channel members

# --- Bot Class Definition (Optional Subclass) ---
# You could subclass discord.Bot here if you want to add more custom attributes/methods directly
# class SoundEffectBot(discord.Bot):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         # Add custom attributes here
#         self.user_sound_config: Dict[str, Dict[str, Any]] = {}
#         self.guild_settings: Dict[str, Dict[str, Any]] = {}
#         self.guild_leave_timers: Dict[int, asyncio.Task[Any]] = {}
#         self.playback_manager: Optional[PlaybackManager] = None
#         self.config = config # Make config easily accessible

# --- Bot Instance Creation ---
bot = discord.Bot(intents=intents) # Or use SoundEffectBot(...) if subclassed

# --- Load Initial Data ---
log.info("Loading initial user and guild data...")
initial_user_config, initial_guild_settings = data_manager.load_all_data()

# --- Attach Data and Managers to Bot Instance ---
# This makes them accessible within Cogs via self.bot.*
bot.user_sound_config: Dict[str, Dict[str, Any]] = initial_user_config
bot.guild_settings: Dict[str, Dict[str, Any]] = initial_guild_settings
bot.guild_leave_timers: Dict[int, asyncio.Task[Any]] = {} # Initialize timer dict
bot.config = config # Attach config module
bot.playback_manager = PlaybackManager(bot) # Instantiate and attach PlaybackManager
log.info("PlaybackManager initialized.")

# --- Load Cogs ---
log.info("Loading Cogs...")
# Define the order if necessary, otherwise load alphabetically
cog_files = [
    'events', 'admin', 'join_sounds', 'user_sounds', 'public_sounds', 'tts'
]
# Construct cog paths relative to this file's directory
cogs_dir = os.path.join(os.path.dirname(__file__), 'cogs')

loaded_cogs = 0
for cog_name in cog_files:
    cog_path = f"cogs.{cog_name}"
    try:
        bot.load_extension(cog_path)
        log.info(f"Successfully loaded Cog: {cog_path}")
        loaded_cogs += 1
    except discord.errors.ExtensionNotFound:
        log.error(f"Cog not found: {cog_path}. Skipping.")
    except discord.errors.ExtensionAlreadyLoaded:
        log.warning(f"Cog already loaded: {cog_path}. Skipping.")
    except Exception as e:
        log.error(f"Failed to load Cog {cog_path}: {e}", exc_info=True)

log.info(f"Finished loading Cogs ({loaded_cogs}/{len(cog_files)} successful).")

# --- Run the Bot ---
if __name__ == "__main__":
    log.info(f"Starting Bot (Python {platform.python_version()}, discord.py {discord.__version__})")
    try:
        bot.run(config.BOT_TOKEN)
    except discord.errors.LoginFailure:
        log.critical("CRITICAL STARTUP ERROR: Login Failure - Invalid BOT_TOKEN.")
    except discord.errors.PrivilegedIntentsRequired as e:
        log.critical(f"CRITICAL STARTUP ERROR: Missing Privileged Intents: {e}. Enable in Dev Portal.")
    except Exception as e:
        log.critical(f"FATAL RUNTIME ERROR: {e}", exc_info=True)
    finally:
        log.info("Bot process has ended.")
