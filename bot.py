# bot.py

import discord
from discord.ext import commands
import os
import json
import asyncio
from gtts import gTTS, gTTSError # Import specific error
import logging
import io # Required for BytesIO
import math # For checking infinite values in dBFS
from collections import deque # Efficient queue structure
import re # For cleaning filenames
from typing import List, Optional, Tuple, Dict, Any # For type hinting
import shutil # For copying/moving files

# Load environment variables first
from dotenv import load_dotenv
load_dotenv()

# Import pydub safely
try:
    from pydub import AudioSegment
    from pydub.exceptions import CouldntDecodeError
    PYDUB_AVAILABLE = True
except ImportError:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')
    logging.critical("CRITICAL: Pydub library not found. Please install it: pip install pydub ffmpeg")
    PYDUB_AVAILABLE = False

# --- Configuration ---
BOT_TOKEN = os.getenv('BOT_TOKEN')
SOUNDS_DIR = "sounds" # For join sounds AND temporary TTS storage if needed
USER_SOUNDS_DIR = "usersounds"
PUBLIC_SOUNDS_DIR = "publicsounds"
CONFIG_FILE = "user_sounds.json"
GUILD_SETTINGS_FILE = "guild_settings.json" # <<<--- NEW: For guild-specific settings
TARGET_LOUDNESS_DBFS = -14.0
MAX_USER_SOUND_SIZE_MB = 5
MAX_USER_SOUNDS_PER_USER = 25
ALLOWED_EXTENSIONS = ['.mp3', '.wav', '.ogg', '.m4a', '.aac']
MAX_TTS_LENGTH = 250 # Max characters for TTS command
DEFAULT_TTS_LANGUAGE = 'en' # Bot's default if no user pref/override
DEFAULT_TTS_SLOW = False    # Bot's default if no user pref/override
MAX_PLAYBACK_DURATION_MS = 10 * 1000 # Max duration in milliseconds (10 seconds)

TTS_LANGUAGE_CHOICES = [ # Keep this explicit for clarity
    discord.OptionChoice(name="English (US - Default)", value="en"),
    discord.OptionChoice(name="English (UK)", value="en-uk"),
    discord.OptionChoice(name="English (Australia)", value="en-au"),
    discord.OptionChoice(name="English (India)", value="en-in"),
    discord.OptionChoice(name="Spanish (Spain)", value="es-es"),
    discord.OptionChoice(name="French (France)", value="fr-fr"),
    discord.OptionChoice(name="French (Canada)", value="fr-ca"),
    discord.OptionChoice(name="German", value="de"),
    discord.OptionChoice(name="Japanese", value="ja"),
    discord.OptionChoice(name="Korean", value="ko"),
]

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')
discord_logger = logging.getLogger('discord')
discord_logger.setLevel(logging.WARNING)
bot_logger = logging.getLogger('SoundBot')
bot_logger.setLevel(logging.INFO)

# --- Validate Critical Config ---
if not BOT_TOKEN or not PYDUB_AVAILABLE:
    bot_logger.critical("CRITICAL ERROR: Bot token missing or Pydub failed to import.")
    exit()

# --- Intents ---
intents = discord.Intents.default()
intents.voice_states = True
intents.guilds = True
intents.message_content = False

# --- Bot Definition ---
bot = discord.Bot(intents=intents)

# --- Data Storage & Helpers ---
# User config: { "user_id_str": { "join_sound": "filename.mp3", "tts_defaults": {"language": "fr", "slow": true} } }
user_sound_config: Dict[str, Dict[str, Any]] = {}
# Guild settings: { "guild_id_str": { "stay_in_channel": bool } }
guild_settings: Dict[str, Dict[str, Any]] = {} # <<<--- NEW
guild_sound_queues = {}
guild_play_tasks = {}

# --- Config/Dir Functions ---
def load_config():
    global user_sound_config
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f: user_sound_config = json.load(f)
            upgraded_count = 0
            for user_id, data in list(user_sound_config.items()):
                if isinstance(data, str): # Old format detected
                    user_sound_config[user_id] = {"join_sound": data}
                    upgraded_count += 1
            if upgraded_count > 0:
                bot_logger.info(f"Upgraded {upgraded_count} old user configs to new format.")
                save_config() # Save the upgraded format immediately
            bot_logger.info(f"Loaded {len(user_sound_config)} user configs from {CONFIG_FILE}")
        except (json.JSONDecodeError, Exception) as e:
             bot_logger.error(f"Error loading {CONFIG_FILE}: {e}", exc_info=True)
             user_sound_config = {}
    else:
        user_sound_config = {}
        bot_logger.info(f"{CONFIG_FILE} not found. Starting fresh.")

def save_config():
     try:
        with open(CONFIG_FILE, 'w') as f: json.dump(user_sound_config, f, indent=4)
        bot_logger.debug(f"Saved {len(user_sound_config)} user configs to {CONFIG_FILE}")
     except Exception as e:
         bot_logger.error(f"Error saving {CONFIG_FILE}: {e}", exc_info=True)

# --- NEW Guild Settings Functions ---
def load_guild_settings():
    """Loads guild-specific settings from GUILD_SETTINGS_FILE."""
    global guild_settings
    if os.path.exists(GUILD_SETTINGS_FILE):
        try:
            with open(GUILD_SETTINGS_FILE, 'r') as f:
                # Load and ensure keys are strings (JSON standard)
                loaded_data = json.load(f)
                # Ensure all keys are strings
                guild_settings = {str(k): v for k, v in loaded_data.items()}
            bot_logger.info(f"Loaded {len(guild_settings)} guild settings from {GUILD_SETTINGS_FILE}")
        except (json.JSONDecodeError, Exception) as e:
             bot_logger.error(f"Error loading {GUILD_SETTINGS_FILE}: {e}", exc_info=True)
             guild_settings = {}
    else:
        guild_settings = {}
        bot_logger.info(f"{GUILD_SETTINGS_FILE} not found. Starting with no persistent guild settings.")

def save_guild_settings():
    """Saves guild-specific settings to GUILD_SETTINGS_FILE."""
    try:
        with open(GUILD_SETTINGS_FILE, 'w') as f:
            json.dump(guild_settings, f, indent=4)
        bot_logger.debug(f"Saved {len(guild_settings)} guild settings to {GUILD_SETTINGS_FILE}")
    except Exception as e:
         bot_logger.error(f"Error saving {GUILD_SETTINGS_FILE}: {e}", exc_info=True)
# --- END NEW Guild Settings Functions ---

def ensure_dir(dir_path: str):
    if not os.path.exists(dir_path):
        try:
            os.makedirs(dir_path)
            bot_logger.info(f"Created directory: {dir_path}")
        except Exception as e:
            bot_logger.critical(f"CRITICAL: Could not create directory '{dir_path}': {e}", exc_info=True)
            if dir_path in [SOUNDS_DIR, USER_SOUNDS_DIR, PUBLIC_SOUNDS_DIR]:
                exit(f"Failed to create essential directory: {dir_path}")

ensure_dir(SOUNDS_DIR); ensure_dir(USER_SOUNDS_DIR); ensure_dir(PUBLIC_SOUNDS_DIR)

# --- Bot Events ---
@bot.event
async def on_ready():
    bot_logger.info(f'Logged in as {bot.user.name} ({bot.user.id})')
    load_config()
    load_guild_settings() # <<<--- NEW
    bot_logger.info(f"Py-cord: {discord.__version__}, Norm Target: {TARGET_LOUDNESS_DBFS}dBFS")
    bot_logger.info(f"Allowed: {', '.join(ALLOWED_EXTENSIONS)}, Max TTS: {MAX_TTS_LENGTH}")
    bot_logger.info(f"Playback limited to first {MAX_PLAYBACK_DURATION_MS / 1000} seconds.")
    bot_logger.info(f"Dirs: {os.path.abspath(SOUNDS_DIR)}, {os.path.abspath(USER_SOUNDS_DIR)}, {os.path.abspath(PUBLIC_SOUNDS_DIR)}")
    bot_logger.info("Sound Bot is operational.")

# --- Audio Processing Helper [UNCHANGED] ---
def process_audio(sound_path: str, member_display_name: str = "User") -> Optional[discord.PCMAudio]:
    """Loads, TRIMS, normalizes, and prepares audio returning a PCMAudio source or None."""
    if not PYDUB_AVAILABLE or not os.path.exists(sound_path):
        bot_logger.error(f"AUDIO: Pydub missing or File not found: '{sound_path}'")
        return None

    audio_source = None
    basename = os.path.basename(sound_path)
    try:
        bot_logger.debug(f"AUDIO: Loading '{basename}'...")
        ext = os.path.splitext(sound_path)[1].lower().strip('. ') or 'mp3' # Use lowercase extension or default to mp3
        audio_segment = AudioSegment.from_file(sound_path, format=ext)

        # --- Trim audio to MAX_PLAYBACK_DURATION_MS ---
        if len(audio_segment) > MAX_PLAYBACK_DURATION_MS:
            bot_logger.info(f"AUDIO: Trimming '{basename}' from {len(audio_segment)}ms to first {MAX_PLAYBACK_DURATION_MS}ms.")
            audio_segment = audio_segment[:MAX_PLAYBACK_DURATION_MS]
        else:
            bot_logger.debug(f"AUDIO: '{basename}' is {len(audio_segment)}ms (<= {MAX_PLAYBACK_DURATION_MS}ms), no trimming needed.")

        # --- Normalization (Moved after potential trimming) ---
        peak_dbfs = audio_segment.max_dBFS
        if not math.isinf(peak_dbfs) and peak_dbfs > -90.0: # Avoid normalizing silence
            change_in_dbfs = TARGET_LOUDNESS_DBFS - peak_dbfs
            bot_logger.info(f"AUDIO: Normalizing '{basename}'. Peak:{peak_dbfs:.2f} Target:{TARGET_LOUDNESS_DBFS:.2f} Gain:{change_in_dbfs:.2f} dB.")
            if change_in_dbfs < 0: # Only apply negative gain (attenuation)
                audio_segment = audio_segment.apply_gain(change_in_dbfs)
            else:
                bot_logger.info(f"AUDIO: Skipping positive gain for '{basename}'.")
        elif math.isinf(peak_dbfs):
            bot_logger.warning(f"AUDIO: Cannot normalize silent audio '{basename}'. Peak is -inf.")
        else: # peak_dbfs <= -90.0
             bot_logger.warning(f"AUDIO: Skipping normalization for very quiet audio '{basename}'. Peak: {peak_dbfs:.2f}")

        # Resampling and Channel Conversion
        audio_segment = audio_segment.set_frame_rate(48000).set_channels(2)

        # Export to PCM
        pcm_data_io = io.BytesIO()
        audio_segment.export(pcm_data_io, format="s16le") # Use little-endian signed 16-bit PCM
        pcm_data_io.seek(0)

        if pcm_data_io.getbuffer().nbytes > 0:
            audio_source = discord.PCMAudio(pcm_data_io)
            bot_logger.debug(f"AUDIO: Successfully processed '{basename}'")
        else:
            bot_logger.error(f"AUDIO: Exported raw audio for '{basename}' is empty!")

    except CouldntDecodeError:
        bot_logger.error(f"AUDIO: Pydub CouldntDecodeError for '{basename}'. Is FFmpeg installed and in PATH? Is the file corrupt?", exc_info=True)
    except FileNotFoundError:
         bot_logger.error(f"AUDIO: File not found during processing: '{sound_path}'")
    except Exception as e:
        bot_logger.error(f"AUDIO: Unexpected error processing '{basename}': {e}", exc_info=True)

    return audio_source

# --- Core Join Sound Queue Logic [UNCHANGED] ---
async def play_next_in_queue(guild: discord.Guild):
    guild_id = guild.id
    task_id = asyncio.current_task().get_name() if asyncio.current_task() else 'Unknown'
    bot_logger.debug(f"QUEUE CHECK [{task_id}]: Guild {guild_id}")

    # Ensure task cancellation removes it from tracker immediately
    if guild_id in guild_play_tasks and guild_play_tasks[guild_id] is asyncio.current_task() and asyncio.current_task().cancelled():
        bot_logger.debug(f"QUEUE CHECK [{task_id}]: Task cancelled externally for guild {guild_id}, removing tracker.")
        del guild_play_tasks[guild_id]
        return

    if guild_id not in guild_sound_queues or not guild_sound_queues[guild_id]:
        bot_logger.debug(f"QUEUE [{task_id}]: Empty/Non-existent for {guild_id}. Triggering disconnect check.")
        # Let after_play_handler logic (which calls safe_disconnect) handle this
        vc = discord.utils.get(bot.voice_clients, guild=guild)
        # Schedule the disconnect check instead of calling directly
        if vc and vc.is_connected():
             bot.loop.create_task(safe_disconnect(vc), name=f"SafeDisconnectQueueEmpty_{guild_id}")
        if guild_id in guild_play_tasks and guild_play_tasks[guild_id] is asyncio.current_task(): del guild_play_tasks[guild_id]
        return

    vc = discord.utils.get(bot.voice_clients, guild=guild)
    if not vc or not vc.is_connected():
        bot_logger.warning(f"QUEUE [{task_id}]: Task running for {guild_id}, but bot not connected. Clearing.")
        if guild_id in guild_sound_queues: guild_sound_queues[guild_id].clear()
        if guild_id in guild_play_tasks and guild_play_tasks[guild_id] is asyncio.current_task(): del guild_play_tasks[guild_id]
        return

    if vc.is_playing():
        bot_logger.debug(f"QUEUE [{task_id}]: Bot already playing in {guild_id}, yielding.")
        return

    try:
        member, sound_path = guild_sound_queues[guild_id].popleft()
        bot_logger.info(f"QUEUE [{task_id}]: Processing {member.display_name} in {guild.name}. Path: {os.path.basename(sound_path)}. Left: {len(guild_sound_queues[guild_id])}")
    except IndexError:
        bot_logger.debug(f"QUEUE [{task_id}]: Became empty unexpectedly for {guild_id}. Triggering disconnect check.")
        # Let after_play_handler logic handle this
        if vc and vc.is_connected():
             bot.loop.create_task(safe_disconnect(vc), name=f"SafeDisconnectQueueIndexError_{guild_id}")
        if guild_id in guild_play_tasks and guild_play_tasks[guild_id] is asyncio.current_task(): del guild_play_tasks[guild_id]
        return

    # Process audio (will now include trimming)
    audio_source = process_audio(sound_path, member.display_name)

    if audio_source:
        try:
            bot_logger.info(f"QUEUE PLAYBACK [{task_id}]: Playing for {member.display_name}...")
            # Use the standard after_play_handler
            vc.play(audio_source, after=lambda e: after_play_handler(e, vc))
            bot_logger.debug(f"QUEUE PLAYBACK [{task_id}]: vc.play() called for {member.display_name}.")
        except (discord.errors.ClientException, Exception) as e:
            bot_logger.error(f"QUEUE PLAYBACK ERROR [{task_id}]: {type(e).__name__}: {e}", exc_info=True)
            # Still call after_play_handler on error to process next item or disconnect
            after_play_handler(e, vc)
    else:
        bot_logger.warning(f"QUEUE PLAYBACK [{task_id}]: No valid source for {member.display_name} ({os.path.basename(sound_path)}). Skipping.")
        # Schedule next check if processing failed, using create_task to avoid blocking
        bot.loop.create_task(play_next_in_queue(guild), name=f"QueueSkip_{guild_id}")


# --- on_voice_state_update [UNCHANGED logic, interacts with modified safe_disconnect] ---
@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    # Prevent triggering for bots, channel moves, or disconnects
    if member.bot or not after.channel or before.channel == after.channel:
        return

    channel_to_join = after.channel
    guild = member.guild
    bot_logger.info(f"EVENT: {member.display_name} ({member.id}) entered {channel_to_join.name} in {guild.name}")

    # Check bot's permissions in the target channel
    bot_perms = channel_to_join.permissions_for(guild.me)
    if not bot_perms.connect or not bot_perms.speak:
        bot_logger.warning(f"Missing Connect/Speak permission in '{channel_to_join.name}'. Cannot play sound.")
        return

    sound_path: Optional[str] = None
    is_tts = False
    user_id_str = str(member.id)

    # Determine which sound to play (user preference or TTS)
    user_config = user_sound_config.get(user_id_str)
    if user_config and "join_sound" in user_config:
        filename = user_config["join_sound"]
        potential_path = os.path.join(SOUNDS_DIR, filename)
        if os.path.exists(potential_path):
            sound_path = potential_path
            bot_logger.info(f"SOUND: Using join sound: '{filename}' for {member.display_name}")
        else:
            bot_logger.warning(f"SOUND: Configured join sound '{filename}' not found. Removing broken entry for {member.display_name}, using TTS.")
            del user_config["join_sound"]
            if not user_config: # Remove user entry if it's now empty
                if user_id_str in user_sound_config: del user_sound_config[user_id_str]
            save_config()
            is_tts = True
    else:
        is_tts = True # No custom sound configured, use TTS
        bot_logger.info(f"SOUND: No custom join sound for {member.display_name}. Using TTS.")

    # Generate TTS if needed
    if is_tts:
        tts_path = os.path.join(SOUNDS_DIR, f"tts_join_{member.id}.mp3")
        bot_logger.info(f"TTS: Generating join TTS for {member.display_name} ('{os.path.basename(tts_path)}')...")
        try:
            tts_defaults = user_config.get("tts_defaults", {}) if user_config else {}
            tts_lang = tts_defaults.get("language", DEFAULT_TTS_LANGUAGE)
            bot_logger.debug(f"TTS Join using lang: {tts_lang}")
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: gTTS(text=f"{member.display_name} joined", lang=tts_lang, slow=False).save(tts_path))
            bot_logger.info(f"TTS: Saved join TTS file '{os.path.basename(tts_path)}'")
            sound_path = tts_path
        except gTTSError as e:
            bot_logger.error(f"TTS: Failed join TTS generation for {member.display_name} (lang={tts_lang}): {e}", exc_info=True)
            sound_path = None
        except Exception as e:
            bot_logger.error(f"TTS: Unexpected error during join TTS generation for {member.display_name}: {e}", exc_info=True)
            sound_path = None

    if not sound_path:
        bot_logger.error(f"Could not determine or generate a sound/TTS path for {member.display_name}. Skipping playback.")
        return

    # --- Queueing and Playback Initiation Logic ---
    guild_id = guild.id
    if guild_id not in guild_sound_queues:
        guild_sound_queues[guild_id] = deque()

    guild_sound_queues[guild_id].append((member, sound_path))
    bot_logger.info(f"QUEUE: Added join sound for {member.display_name}. Queue size: {len(guild_sound_queues[guild_id])}")

    vc = discord.utils.get(bot.voice_clients, guild=guild)

    if vc and vc.is_playing():
        bot_logger.info(f"VOICE: Bot playing in {guild.name}. Join sound queued. Playback deferred.")
        if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
             task_name = f"QueueTriggerDeferred_{guild_id}"
             if guild_sound_queues.get(guild_id):
                 guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(guild), name=task_name)
                 bot_logger.debug(f"VOICE: Created deferred play task '{task_name}'.")
             else:
                 bot_logger.debug(f"VOICE: Deferred task '{task_name}' skipped, queue emptied concurrently.")
        return

    should_start_play_task = False
    try:
        if not vc or not vc.is_connected():
            bot_logger.info(f"VOICE: Connecting to '{channel_to_join.name}' to start queue.")
            vc = await channel_to_join.connect(timeout=30.0, reconnect=True)
            bot_logger.info(f"VOICE: Connected to '{channel_to_join.name}'.")
            should_start_play_task = True
        elif vc.channel != channel_to_join:
             bot_logger.info(f"VOICE: Moving from '{vc.channel.name}' to '{channel_to_join.name}' to start queue.")
             await vc.move_to(channel_to_join)
             bot_logger.info(f"VOICE: Moved to '{channel_to_join.name}'.")
             should_start_play_task = True
        else:
             bot_logger.debug(f"VOICE: Bot already in '{channel_to_join.name}' and idle. Starting queue.")
             should_start_play_task = True

    except asyncio.TimeoutError:
        bot_logger.error(f"VOICE: Timeout connecting/moving to '{channel_to_join.name}'. Clearing queue.")
        if guild_id in guild_sound_queues: guild_sound_queues[guild_id].clear()
        vc = None
    except discord.errors.ClientException as e:
        bot_logger.warning(f"VOICE: ClientException during connect/move to '{channel_to_join.name}': {e}")
        vc = discord.utils.get(bot.voice_clients, guild=guild)
    except Exception as e:
        bot_logger.error(f"VOICE: Unexpected error connecting/moving to '{channel_to_join.name}': {e}", exc_info=True)
        if guild_id in guild_sound_queues: guild_sound_queues[guild_id].clear()
        vc = None

    if should_start_play_task and vc and vc.is_connected():
        if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
            task_name = f"QueueStart_{guild_id}"
            if guild_sound_queues.get(guild_id):
                guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(guild), name=task_name)
                bot_logger.info(f"VOICE: Started play task '{task_name}' for guild {guild_id}.")
            else:
                bot_logger.debug(f"VOICE: Start task '{task_name}' skipped, queue emptied concurrently.")
        else:
             bot_logger.debug(f"VOICE: Play task for {guild_id} already running/scheduled.")
    elif not vc or not vc.is_connected():
         bot_logger.warning(f"VOICE: Bot could not connect/move to {channel_to_join.name}, cannot start playback task.")


# --- after_play_handler [MODIFIED to call safe_disconnect consistently] ---
def after_play_handler(error: Optional[Exception], vc: discord.VoiceClient):
    # This function runs after a sound finishes playing or errors out.
    # It's crucial for processing the next sound in the queue or checking disconnect logic.
    guild_id = vc.guild.id if vc and vc.guild else None
    if error:
        bot_logger.error(f'PLAYBACK ERROR (In after_play_handler for guild {guild_id}): {error}', exc_info=error)

    if not guild_id or not vc.is_connected():
        bot_logger.warning(f"after_play_handler called with invalid/disconnected vc (Guild ID: {guild_id}). Cleaning up potential task.")
        if guild_id and guild_id in guild_play_tasks:
             play_task = guild_play_tasks.pop(guild_id, None)
             if play_task and not play_task.done():
                 play_task.cancel()
                 bot_logger.debug(f"Cancelled lingering play task for disconnected guild {guild_id}.")
        return

    bot_logger.debug(f"Playback finished/errored for {guild_id}. Triggering next queue check or disconnect check.")

    # Check if there are more sounds in the *join* queue for this guild
    is_join_queue_empty = guild_id not in guild_sound_queues or not guild_sound_queues[guild_id]

    if not is_join_queue_empty:
        bot_logger.debug(f"AFTER_PLAY: Join queue for {guild_id} not empty. Ensuring task runs to process next.")
        if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
             task_name = f"QueueCheckAfterPlay_{guild_id}"
             # Ensure queue still has items before creating task (race condition guard)
             if guild_sound_queues.get(guild_id):
                 guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(vc.guild), name=task_name)
                 bot_logger.debug(f"AFTER_PLAY: Scheduled task '{task_name}' for {guild_id} as existing task was done/missing.")
             else:
                 bot_logger.debug(f"AFTER_PLAY: Task '{task_name}' creation skipped, queue emptied concurrently.")
        else:
             bot_logger.debug(f"AFTER_PLAY: Existing play task found for {guild_id}, letting it continue.")
    else:
         # Join queue is empty. Check if we should disconnect (safe_disconnect handles 'stay' logic).
         bot_logger.debug(f"AFTER_PLAY: Join queue for {guild_id} is empty. Scheduling safe disconnect check.")
         # Schedule the disconnect check instead of calling directly
         # Pass manual_leave=False as this is the automatic check after playback
         bot.loop.create_task(safe_disconnect(vc, manual_leave=False), name=f"SafeDisconnectAfterPlay_{guild_id}")

# --- Helper Function: Check if bot should stay [NEW] ---
def should_bot_stay(guild_id: int) -> bool:
    """Checks if the bot is configured to stay in the channel for a given guild."""
    settings = guild_settings.get(str(guild_id), {})
    # Default to False if not set or explicitly False
    stay = settings.get("stay_in_channel", False)
    bot_logger.debug(f"Checked stay setting for guild {guild_id}: {stay}")
    return stay is True

# --- safe_disconnect [MODIFIED] ---
async def safe_disconnect(vc: Optional[discord.VoiceClient], *, manual_leave: bool = False):
    """
    Disconnects the bot ONLY if the join queue is empty, it's not playing,
    AND the 'stay_in_channel' setting is FALSE or not set for the guild,
    OR if manual_leave is True.
    Also cleans up lingering tasks if staying but idle.
    """
    if not vc or not vc.is_connected():
        # bot_logger.debug("Safe disconnect called but VC is already disconnected.")
        return

    guild = vc.guild
    guild_id = guild.id

    # --- Check if the bot should stay ---
    # If manually told to leave (e.g., /leave command), skip the 'stay' check.
    if not manual_leave and should_bot_stay(guild_id):
        bot_logger.debug(f"Disconnect skipped for {guild.name}: 'Stay in channel' is enabled.")
        # Clean up the task tracker if the bot is staying but idle
        is_join_queue_empty_check = guild_id not in guild_sound_queues or not guild_sound_queues[guild_id]
        is_playing_check = vc.is_playing()
        if is_join_queue_empty_check and not is_playing_check:
            if guild_id in guild_play_tasks:
                play_task = guild_play_tasks.pop(guild_id, None)
                if play_task:
                    if not play_task.done():
                        try:
                            play_task.cancel()
                            bot_logger.debug(f"STAY MODE: Cancelled lingering play task for idle bot in {guild_id}.")
                        except Exception as e_cancel:
                             bot_logger.warning(f"STAY MODE: Error cancelling lingering task for {guild_id}: {e_cancel}")
                    else:
                        bot_logger.debug(f"STAY MODE: Cleaned up completed play task tracker for idle bot in {guild_id}.")
        return # Explicitly return if staying
    # --- End Stay Check ---

    # Check join queue status again right before potentially disconnecting
    is_join_queue_empty = guild_id not in guild_sound_queues or not guild_sound_queues[guild_id]
    # Check if playing (could be a non-join sound like TTS or /playsound)
    is_playing = vc.is_playing()

    # Determine if disconnect conditions are met
    should_disconnect = manual_leave or (is_join_queue_empty and not is_playing)

    if should_disconnect:
        disconnect_reason = "Manual /leave command" if manual_leave else "Idle and queue empty"
        bot_logger.info(f"DISCONNECT: Conditions met for {guild.name} ({disconnect_reason}). Disconnecting...")
        try:
            # Explicitly stop just in case is_playing state changed quickly
            if vc.is_playing():
                log_level = logging.WARNING if not manual_leave else logging.DEBUG
                bot_logger.log(log_level, f"DISCONNECT: Called stop() during disconnect for {guild.name} (Manual: {manual_leave}).")
                vc.stop()

            await vc.disconnect(force=False) # Use force=False for graceful disconnect
            bot_logger.info(f"DISCONNECT: Bot disconnected from '{guild.name}'.")

            # Clean up the task tracker and queue for this guild after successful disconnect
            if guild_id in guild_play_tasks:
                play_task = guild_play_tasks.pop(guild_id, None)
                if play_task:
                    if not play_task.done():
                        try:
                            play_task.cancel()
                            bot_logger.debug(f"DISCONNECT: Cancelled associated play task for {guild_id}.")
                        except Exception as e_cancel:
                             bot_logger.warning(f"DISCONNECT: Error cancelling task for {guild_id}: {e_cancel}")
                    else:
                         bot_logger.debug(f"DISCONNECT: Cleaned up completed play task tracker for {guild_id}.")
            # Clear the queue as well after disconnect
            if guild_id in guild_sound_queues:
                guild_sound_queues[guild_id].clear()
                bot_logger.debug(f"DISCONNECT: Cleared sound queue for {guild_id}.")


        except Exception as e:
            bot_logger.error(f"DISCONNECT ERROR: Failed disconnect from {guild.name}: {e}", exc_info=True)
            # Don't remove task/queue on failed disconnect, state is uncertain
    else:
         bot_logger.debug(f"Disconnect skipped for {guild.name}: Manual={manual_leave}, QueueEmpty={is_join_queue_empty}, Playing={is_playing}.")


# --- Voice Client Connection/Busy Check Helper [UNCHANGED] ---
async def _ensure_voice_client_ready(interaction: discord.Interaction, target_channel: discord.VoiceChannel, action_type: str = "Playback") -> Optional[discord.VoiceClient]:
    """Helper to connect/move/check busy status and permissions. Returns VC or None."""
    guild = interaction.guild
    user = interaction.user
    guild_id = guild.id
    log_prefix = f"{action_type.upper()}:"

    if not guild:
        await interaction.followup.send("This command must be used in a server.", ephemeral=True)
        return None

    bot_perms = target_channel.permissions_for(guild.me)
    if not bot_perms.connect or not bot_perms.speak:
        await interaction.followup.send(f"❌ I don't have permission to Connect or Speak in {target_channel.mention}.", ephemeral=True)
        bot_logger.warning(f"{log_prefix} Missing Connect/Speak perms in {target_channel.name} ({guild.name}).")
        return None

    vc = discord.utils.get(bot.voice_clients, guild=guild)
    try:
        if vc and vc.is_connected():
            if vc.is_playing():
                 # Join queue takes precedence
                join_queue_active = guild_id in guild_sound_queues and guild_sound_queues[guild_id]
                msg = "⏳ Bot is currently playing join sounds. Please wait." if join_queue_active else "⏳ Bot is currently playing another sound/TTS. Please wait."
                log_msg = f"{log_prefix} Bot busy ({'join queue' if join_queue_active else 'non-join'}) in {guild.name}, user {user.name}'s request ignored."
                await interaction.followup.send(msg, ephemeral=True)
                bot_logger.info(log_msg)
                return None # Indicate busy

            elif vc.channel != target_channel:
                # Don't move if staying enabled and already connected, unless the user is in the target channel
                # Allow moving *to* the user's channel even if stay is on
                if should_bot_stay(guild_id) and user.voice and user.voice.channel == target_channel:
                     bot_logger.info(f"{log_prefix} Moving from '{vc.channel.name}' to '{target_channel.name}' (user's channel) for {user.name}.")
                     await vc.move_to(target_channel)
                     bot_logger.info(f"{log_prefix} Moved successfully.")
                elif not should_bot_stay(guild_id):
                    bot_logger.info(f"{log_prefix} Moving from '{vc.channel.name}' to '{target_channel.name}' for {user.name}.")
                    await vc.move_to(target_channel)
                    bot_logger.info(f"{log_prefix} Moved successfully.")
                else:
                    bot_logger.debug(f"{log_prefix} Not moving from '{vc.channel.name}' to '{target_channel.name}' because stay is enabled and user isn't there.")
                    # If stay is on, but the bot is in the wrong channel for the user's request,
                    # tell the user they need to join the bot's channel.
                    await interaction.followup.send(f"ℹ️ I'm currently staying in {vc.channel.mention}. Please join that channel to use this command.", ephemeral=True)
                    return None # Indicate wrong channel due to stay mode
        else:
            bot_logger.info(f"{log_prefix} Connecting to '{target_channel.name}' for {user.name}.")
            vc = await target_channel.connect(timeout=30.0, reconnect=True)
            bot_logger.info(f"{log_prefix} Connected successfully.")

        if not vc or not vc.is_connected():
             bot_logger.error(f"{log_prefix} Failed to establish voice client for {target_channel.name} after connect/move attempt.")
             await interaction.followup.send("❌ Failed to connect or move to the voice channel.", ephemeral=True)
             return None

        return vc # Success

    except asyncio.TimeoutError:
         await interaction.followup.send("❌ Connection to the voice channel timed out.", ephemeral=True)
         bot_logger.error(f"{log_prefix} Connection/Move Timeout in {guild.name} to {target_channel.name}")
         return None
    except discord.errors.ClientException as e:
        msg = "⏳ Bot is busy connecting/disconnecting. Please wait a moment." if "already connect" in str(e).lower() else "❌ Error connecting/moving. Check permissions or try again."
        await interaction.followup.send(msg, ephemeral=True)
        bot_logger.warning(f"{log_prefix} Connection/Move ClientException in {guild.name}: {e}")
        return None
    except Exception as e:
        await interaction.followup.send("❌ An unexpected error occurred while joining the voice channel.", ephemeral=True)
        bot_logger.error(f"{log_prefix} Connection/Move unexpected error in {guild.name}: {e}", exc_info=True)
        return None


# --- Single Sound Playback Logic (For Files) [UNCHANGED logic, relies on modified helpers] ---
async def play_single_sound(interaction: discord.Interaction, sound_path: str):
    """Connects (if needed), plays a single sound FILE (processed/trimmed), and uses after_play_handler."""
    user = interaction.user
    guild = interaction.guild

    if not guild or not isinstance(user, discord.Member) or not user.voice or not user.voice.channel:
        await interaction.followup.send("You need to be in a voice channel in this server to use this.", ephemeral=True)
        return

    target_channel = user.voice.channel
    if not os.path.exists(sound_path):
         await interaction.followup.send("❌ Error: The requested sound file seems to be missing on the server.", ephemeral=True)
         bot_logger.error(f"SINGLE PLAY: File not found: {sound_path}")
         return

    # Use the helper for connection, permission, busy, and stay checks
    voice_client = await _ensure_voice_client_ready(interaction, target_channel, action_type="SINGLE PLAY (File)")
    if not voice_client:
        return # Helper already sent feedback

    # Process and Play Audio FILE
    sound_basename = os.path.basename(sound_path)
    bot_logger.info(f"SINGLE PLAY (File): Processing '{sound_basename}' for {user.name}...")
    audio_source = process_audio(sound_path, user.display_name)

    if audio_source:
        if voice_client.is_playing():
             bot_logger.warning(f"SINGLE PLAY (File): VC became busy between check and play for {user.name}. Aborting.")
             await interaction.followup.send("⏳ Bot became busy just now. Please try again.", ephemeral=True)
             # Trigger the after-handler manually as play won't happen
             after_play_handler(None, voice_client)
             return

        try:
            sound_display_name = os.path.splitext(sound_basename)[0]
            bot_logger.info(f"SINGLE PLAYBACK (File): Playing '{sound_display_name}' requested by {user.display_name}...")
            voice_client.play(audio_source, after=lambda e: after_play_handler(e, voice_client))
            await interaction.followup.send(f"▶️ Playing `{sound_display_name}` (max {MAX_PLAYBACK_DURATION_MS / 1000}s)...", ephemeral=True)
        except discord.errors.ClientException as e:
            msg = "❌ Error: Bot is already playing or encountered a client issue."
            await interaction.followup.send(msg, ephemeral=True)
            bot_logger.error(f"SINGLE PLAYBACK ERROR (File - ClientException): {e}", exc_info=True)
            after_play_handler(e, voice_client)
        except Exception as e:
            await interaction.followup.send("❌ An unexpected error occurred during playback.", ephemeral=True)
            bot_logger.error(f"SINGLE PLAYBACK ERROR (File - Unexpected): {e}", exc_info=True)
            after_play_handler(e, voice_client)
    else:
        await interaction.followup.send("❌ Error: Could not process the audio file. It might be corrupted or unsupported.", ephemeral=True)
        bot_logger.error(f"SINGLE PLAYBACK (File): Failed to get audio source for '{sound_path}' requested by {user.name}")
        if voice_client and voice_client.is_connected():
            # Call handler even if processing failed, to trigger disconnect check if needed
            after_play_handler(None, voice_client)


# --- Helper Functions [UNCHANGED] ---
def sanitize_filename(name: str) -> str:
    """Removes/replaces invalid chars for filenames and limits length."""
    name = re.sub(r'[<>:"/\\|?*\.\s]+', '_', name)
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')
    return name[:50]

def _find_sound_path_in_dir(directory: str, sound_name: str) -> Optional[str]:
    """Generic helper to find a sound file by name (case-insensitive, checks extensions)."""
    if not os.path.isdir(directory): return None
    preferred_order = ['.mp3', '.wav'] + [ext for ext in ALLOWED_EXTENSIONS if ext not in ['.mp3', '.wav']]
    for name_variant in [sound_name, sanitize_filename(sound_name)]:
        try:
            for filename in os.listdir(directory):
                 base, ext = os.path.splitext(filename)
                 if ext.lower() in ALLOWED_EXTENSIONS and base.lower() == name_variant.lower():
                     return os.path.join(directory, filename)
        except OSError as e:
             bot_logger.error(f"Error listing files in {directory} during find: {e}")
             return None
    return None

def _get_sound_files_from_dir(directory: str) -> List[str]:
    """Generic helper to list sound base names from a directory."""
    sounds = []
    if os.path.isdir(directory):
        try:
            for filename in os.listdir(directory):
                filepath = os.path.join(directory, filename)
                base_name, ext = os.path.splitext(filename)
                if os.path.isfile(filepath) and ext.lower() in ALLOWED_EXTENSIONS:
                    sounds.append(base_name)
        except OSError as e:
            bot_logger.error(f"Error listing files in {directory}: {e}")
    return sounds

def get_user_sound_files(user_id: int) -> List[str]:
    """Lists base names of sound files for a specific user."""
    return _get_sound_files_from_dir(os.path.join(USER_SOUNDS_DIR, str(user_id)))

def find_user_sound_path(user_id: int, sound_name: str) -> Optional[str]:
    """Finds the full path for a user's sound by name."""
    return _find_sound_path_in_dir(os.path.join(USER_SOUNDS_DIR, str(user_id)), sound_name)

def get_public_sound_files() -> List[str]:
    """Lists base names of public sound files."""
    return _get_sound_files_from_dir(PUBLIC_SOUNDS_DIR)

def find_public_sound_path(sound_name: str) -> Optional[str]:
    """Finds the full path for a public sound by name."""
    return _find_sound_path_in_dir(PUBLIC_SOUNDS_DIR, sound_name)


# --- Autocomplete Helper [UNCHANGED] ---
async def _generic_sound_autocomplete(ctx: discord.AutocompleteContext, source_func, *args) -> List[discord.OptionChoice]:
    """Generic autocomplete handler returning OptionChoices from a list function."""
    try:
        sounds = source_func(*args)
        current_value = ctx.value.lower() if ctx.value else ""
        suggestions = sorted(
            [discord.OptionChoice(name=name, value=name)
             for name in sounds if current_value in name.lower()],
            key=lambda choice: choice.name
        )
        return suggestions[:25]
    except Exception as e:
         bot_logger.error(f"Error during autocomplete ({source_func.__name__} for user {ctx.interaction.user.id}): {e}", exc_info=True)
         return []

async def user_sound_autocomplete(ctx: discord.AutocompleteContext) -> List[discord.OptionChoice]:
    """Autocomplete for user's personal sounds."""
    return await _generic_sound_autocomplete(ctx, get_user_sound_files, ctx.interaction.user.id)

async def public_sound_autocomplete(ctx: discord.AutocompleteContext) -> List[discord.OptionChoice]:
    """Autocomplete for public sounds."""
    return await _generic_sound_autocomplete(ctx, get_public_sound_files)


# --- File Upload Validation Helper [UNCHANGED] ---
async def _validate_and_save_upload(
    ctx: discord.ApplicationContext,
    sound_file: discord.Attachment,
    target_save_path: str,
    command_name: str = "upload"
) -> Tuple[bool, Optional[str]]:
    """
    Validates attachment, saves temporarily, checks with Pydub, moves to final path.
    Returns (success_bool, error_message_or_None). Sends NO user feedback itself.
    """
    user_id = ctx.author.id
    log_prefix = f"{command_name.upper()} VALIDATION"

    file_extension = os.path.splitext(sound_file.filename)[1].lower()
    if file_extension not in ALLOWED_EXTENSIONS:
        bot_logger.warning(f"{log_prefix}: User {user_id} tried invalid extension '{file_extension}'.")
        return False, f"❌ Invalid file type (`{file_extension}`). Allowed: {', '.join(ALLOWED_EXTENSIONS)}"

    if sound_file.size > MAX_USER_SOUND_SIZE_MB * 1024 * 1024:
        bot_logger.warning(f"{log_prefix}: User {user_id} tried oversized file '{sound_file.filename}' ({sound_file.size / (1024*1024):.2f} MB).")
        return False, f"❌ File too large (`{sound_file.size / (1024*1024):.2f}` MB). Max: {MAX_USER_SOUND_SIZE_MB}MB."

    if not sound_file.content_type or not sound_file.content_type.startswith('audio/'):
        bot_logger.warning(f"{log_prefix}: Content-Type '{sound_file.content_type}' for '{sound_file.filename}' (user: {user_id}) not 'audio/*'. Proceeding.")

    temp_save_filename = f"temp_{command_name}_{user_id}_{os.urandom(4).hex()}{file_extension}"
    temp_save_path = os.path.join(USER_SOUNDS_DIR, temp_save_filename)

    async def cleanup_temp():
        if os.path.exists(temp_save_path):
            try: os.remove(temp_save_path); bot_logger.debug(f"Cleaned up temp: {temp_save_path}")
            except Exception as del_e: bot_logger.warning(f"Failed temp cleanup '{temp_save_path}': {del_e}")

    try:
        await sound_file.save(temp_save_path)
        bot_logger.info(f"{log_prefix}: Saved temp file for {user_id}: '{temp_save_path}'")

        try:
            bot_logger.debug(f"{log_prefix}: Pydub decode check: '{temp_save_path}'")
            _ = AudioSegment.from_file(temp_save_path, format=file_extension.strip('.'))
            bot_logger.info(f"{log_prefix}: Pydub validation OK for '{temp_save_path}'")

            try:
                target_dir = os.path.dirname(target_save_path)
                ensure_dir(target_dir)
                os.replace(temp_save_path, target_save_path)
                bot_logger.info(f"{log_prefix}: Final file saved (atomic): '{target_save_path}'")
                return True, None # Success

            except OSError as rep_e:
                bot_logger.warning(f"{log_prefix}: os.replace failed ('{rep_e}'), trying shutil.move for '{temp_save_path}' -> '{target_save_path}'.")
                try:
                    shutil.move(temp_save_path, target_save_path)
                    bot_logger.info(f"{log_prefix}: Final file saved (fallback move): '{target_save_path}'")
                    return True, None # Success
                except Exception as move_e:
                    bot_logger.error(f"{log_prefix}: FAILED final save (replace: {rep_e}, move: {move_e})", exc_info=True)
                    await cleanup_temp()
                    return False, "❌ Error saving sound after validation."

        except CouldntDecodeError as decode_error:
            bot_logger.error(f"{log_prefix}: FAILED (Pydub Decode - user: {user_id}, file: '{sound_file.filename}'): {decode_error}", exc_info=True)
            await cleanup_temp()
            return False, f"❌ **Audio Validation Failed!** Could not process `{sound_file.filename}`. Ensure valid audio ({', '.join(ALLOWED_EXTENSIONS)}) and FFmpeg is accessible."
        except Exception as validate_e:
            bot_logger.error(f"{log_prefix}: FAILED (Pydub check error - user: {user_id}, file: '{sound_file.filename}'): {validate_e}", exc_info=True)
            await cleanup_temp()
            return False, "❌ **Audio Validation Failed!** Unexpected error during processing."

    except discord.HTTPException as e:
        bot_logger.error(f"{log_prefix}: Error downloading temp file for {user_id} ('{sound_file.filename}'): {e}", exc_info=True)
        await cleanup_temp()
        return False, "❌ Error downloading sound from Discord."
    except Exception as e:
        bot_logger.error(f"{log_prefix}: Unexpected error during temp save/validate for {user_id} ('{sound_file.filename}'): {e}", exc_info=True)
        await cleanup_temp()
        return False, "❌ Unexpected server error during file handling."


# --- Slash Commands ---

# === Join Sound Commands [UNCHANGED] ===
@bot.slash_command(name="setjoinsound", description="Upload your custom join sound. Replaces existing.")
@commands.cooldown(1, 15, commands.BucketType.user)
async def setjoinsound(
    ctx: discord.ApplicationContext,
    sound_file: discord.Option(discord.Attachment, description=f"Sound ({', '.join(ALLOWED_EXTENSIONS)}). Max {MAX_USER_SOUND_SIZE_MB}MB.", required=True) # type: ignore
):
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id_str = str(author.id)
    bot_logger.info(f"COMMAND: /setjoinsound by {author.name} ({user_id_str}), file: '{sound_file.filename}'")

    file_extension = os.path.splitext(sound_file.filename)[1].lower()
    final_save_filename = f"joinsound_{user_id_str}{file_extension}"
    final_save_path = os.path.join(SOUNDS_DIR, final_save_filename)

    user_config = user_sound_config.get(user_id_str, {})
    old_config_filename = user_config.get("join_sound")

    success, error_msg = await _validate_and_save_upload(ctx, sound_file, final_save_path, command_name="setjoinsound")

    if success:
        bot_logger.info(f"Join sound validation successful for {author.name}, saved to '{final_save_path}'")
        if old_config_filename and old_config_filename != final_save_filename:
            old_path = os.path.join(SOUNDS_DIR, old_config_filename)
            if os.path.exists(old_path):
                try: os.remove(old_path); bot_logger.info(f"Removed previous join sound: '{old_path}'")
                except Exception as e: bot_logger.warning(f"Could not remove previous join sound '{old_path}': {e}")

        user_config["join_sound"] = final_save_filename
        user_sound_config[user_id_str] = user_config
        save_config()
        bot_logger.info(f"Updated join sound config for {author.name} to '{final_save_filename}'")
        await ctx.followup.send(f"✅ Success! Your join sound is set to `{sound_file.filename}`.", ephemeral=True)
    else:
        await ctx.followup.send(error_msg or "❌ An unknown error occurred during validation.", ephemeral=True)


@bot.slash_command(name="removejoinsound", description="Remove your custom join sound, revert to TTS.")
@commands.cooldown(1, 5, commands.BucketType.user)
async def removejoinsound(ctx: discord.ApplicationContext):
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id_str = str(author.id)
    bot_logger.info(f"COMMAND: /removejoinsound by {author.name} ({user_id_str})")

    user_config = user_sound_config.get(user_id_str)
    if user_config and "join_sound" in user_config:
        filename_to_remove = user_config.pop("join_sound")
        bot_logger.info(f"Removing join sound config for {author.name} (was '{filename_to_remove}')")

        if not user_config:
            if user_id_str in user_sound_config: del user_sound_config[user_id_str]
            bot_logger.info(f"Removed empty user config for {author.name} after join sound removal.")
        save_config()

        file_path_to_remove = os.path.join(SOUNDS_DIR, filename_to_remove)
        tts_join_file_path = os.path.join(SOUNDS_DIR, f"tts_join_{user_id_str}.mp3")
        removed_custom, removed_tts = False, False

        for path_to_remove, log_name in [(file_path_to_remove, "custom join sound"), (tts_join_file_path, "cached join TTS")]:
            if os.path.exists(path_to_remove):
                try:
                    os.remove(path_to_remove)
                    bot_logger.info(f"Deleted file: '{path_to_remove}' ({log_name})")
                    if path_to_remove == file_path_to_remove: removed_custom = True
                    if path_to_remove == tts_join_file_path: removed_tts = True
                except OSError as e: bot_logger.warning(f"Could not delete file '{path_to_remove}' ({log_name}): {e}")
            elif path_to_remove == file_path_to_remove:
                 bot_logger.warning(f"Configured join sound '{filename_to_remove}' not found at '{path_to_remove}' during removal.")

        msg = "🗑️ Custom join sound removed."
        if removed_tts: msg += " Cleaned up cached join TTS."
        msg += " Default TTS will now be used."
        await ctx.followup.send(msg, ephemeral=True)
    else:
        await ctx.followup.send("🤷 You don't have a custom join sound configured.", ephemeral=True)


# === User Command Sound / Soundboard Commands [UNCHANGED] ===
@bot.slash_command(name="uploadsound", description=f"Upload a sound (personal/public). Limit: {MAX_USER_SOUNDS_PER_USER} personal.")
@commands.cooldown(2, 20, commands.BucketType.user)
async def uploadsound(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Short name (letters, numbers, underscore). Will be sanitized.", required=True), # type: ignore
    sound_file: discord.Option(discord.Attachment, description=f"Sound ({', '.join(ALLOWED_EXTENSIONS)}). Max {MAX_USER_SOUND_SIZE_MB}MB.", required=True), # type: ignore
    make_public: discord.Option(bool, description="Make available for everyone? (Default: False)", default=False) # type: ignore
):
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id = author.id
    bot_logger.info(f"COMMAND: /uploadsound by {author.name} ({user_id}), name: '{name}', public: {make_public}, file: '{sound_file.filename}'")

    clean_name = sanitize_filename(name)
    if not clean_name:
        await ctx.followup.send("❌ Provide valid name (letters, numbers, underscore).", ephemeral=True); return
    followup_prefix = f"ℹ️ Name sanitized to `{clean_name}`.\n" if clean_name != name else ""

    file_extension = os.path.splitext(sound_file.filename)[1].lower()
    final_filename = f"{clean_name}{file_extension}"

    if make_public:
        target_dir = PUBLIC_SOUNDS_DIR
        if find_public_sound_path(clean_name):
            await ctx.followup.send(f"{followup_prefix}❌ Public sound `{clean_name}` exists.", ephemeral=True); return
        replacing_personal = False
        scope = "public"
    else:
        target_dir = os.path.join(USER_SOUNDS_DIR, str(user_id))
        ensure_dir(target_dir)
        existing_personal = find_user_sound_path(user_id, clean_name)
        replacing_personal = existing_personal is not None
        if not replacing_personal and len(get_user_sound_files(user_id)) >= MAX_USER_SOUNDS_PER_USER:
             await ctx.followup.send(f"{followup_prefix}❌ Max {MAX_USER_SOUNDS_PER_USER} personal sounds reached.", ephemeral=True); return
        scope = "personal"

    final_path = os.path.join(target_dir, final_filename)
    success, error_msg = await _validate_and_save_upload(ctx, sound_file, final_path, command_name="uploadsound")

    if success:
        bot_logger.info(f"Sound validation successful for {author.name}, saved to '{final_path}' ({scope})")
        if replacing_personal and not make_public:
            old_path = find_user_sound_path(user_id, clean_name) # Re-check after save
            if old_path and old_path != final_path: # Different extension
                 try: os.remove(old_path); bot_logger.info(f"Removed old personal '{os.path.basename(old_path)}' for {user_id} on replace.")
                 except Exception as e: bot_logger.warning(f"Could not remove old '{old_path}' during replace: {e}")

        action = "updated" if replacing_personal and not make_public else "uploaded"
        play_cmd = "playpublic" if make_public else "playsound"
        list_cmd = "publicsounds" if make_public else "mysounds"
        msg = f"{followup_prefix}✅ Success! Sound `{clean_name}` {action} as {scope}.\n"
        msg += f"Use `/{play_cmd} name:{clean_name}`"
        if not make_public: msg += f", `/{list_cmd}`, `/soundpanel`, or `/publishsound name:{clean_name}`."
        else: msg += f" or `/{list_cmd}`."
        await ctx.followup.send(msg, ephemeral=True)
    else:
        await ctx.followup.send(f"{followup_prefix}{error_msg or '❌ Unknown validation error.'}", ephemeral=True)


@bot.slash_command(name="mysounds", description="Lists your personal uploaded sounds.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def mysounds(ctx: discord.ApplicationContext):
    await ctx.defer(ephemeral=True)
    author = ctx.author
    bot_logger.info(f"COMMAND: /mysounds by {author.name} ({author.id})")
    user_sounds = get_user_sound_files(author.id)

    if not user_sounds:
        await ctx.followup.send("No personal sounds yet. Use `/uploadsound`!", ephemeral=True); return

    sorted_sounds = sorted(user_sounds, key=str.lower)
    parts = []
    length = 0
    limit = 1900
    for name in sorted_sounds:
        line = f"- `{name}`"
        if length + len(line) + 1 > limit: parts.append("... (list truncated)"); break
        parts.append(line); length += len(line) + 1
    list_str = "\n".join(parts)

    embed = discord.Embed(
        title=f"{author.display_name}'s Sounds ({len(sorted_sounds)}/{MAX_USER_SOUNDS_PER_USER})",
        description=f"Use `/playsound`, `/soundpanel`, or `/publishsound`.\n\n{list_str}",
        color=discord.Color.blurple()
    ).set_footer(text="Use /deletesound to remove.")
    await ctx.followup.send(embed=embed, ephemeral=True)


@bot.slash_command(name="deletesound", description="Deletes one of your PERSONAL sounds.")
@commands.cooldown(1, 5, commands.BucketType.user)
async def deletesound(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Name of the personal sound to delete.", required=True, autocomplete=user_sound_autocomplete) # type: ignore
):
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id = author.id
    bot_logger.info(f"COMMAND: /deletesound by {author.name} ({user_id}), target: '{name}'")

    sound_path = find_user_sound_path(user_id, name)
    sound_base_name = name
    if not sound_path:
        sanitized = sanitize_filename(name)
        if sanitized != name: sound_path = find_user_sound_path(user_id, sanitized); sound_base_name = sanitized

    if not sound_path:
        await ctx.followup.send(f"❌ Personal sound `{name}` not found. Use `/mysounds`.", ephemeral=True); return

    user_dir_abs = os.path.abspath(os.path.join(USER_SOUNDS_DIR, str(user_id)))
    resolved_path_abs = os.path.abspath(sound_path)
    if not resolved_path_abs.startswith(user_dir_abs + os.sep):
         bot_logger.critical(f"CRITICAL SECURITY ALERT: Path traversal in /deletesound. User: {user_id}, Input: '{name}', Path: '{resolved_path_abs}'")
         await ctx.followup.send("❌ Internal security error.", ephemeral=True); return

    try:
        deleted_filename = os.path.basename(sound_path)
        os.remove(sound_path)
        bot_logger.info(f"Deleted PERSONAL sound '{deleted_filename}' for user {user_id}.")
        await ctx.followup.send(f"🗑️ Personal sound `{sound_base_name}` deleted.", ephemeral=True)
    except OSError as e:
        bot_logger.error(f"Failed to delete personal sound '{sound_path}' for {user_id}: {e}", exc_info=True)
        await ctx.followup.send(f"❌ Failed to delete `{sound_base_name}`: {type(e).__name__}.", ephemeral=True)
    except Exception as e:
        bot_logger.error(f"Unexpected error deleting personal sound '{sound_path}' for {user_id}: {e}", exc_info=True)
        await ctx.followup.send(f"❌ Unexpected error deleting `{sound_base_name}`.", ephemeral=True)


@bot.slash_command(name="playsound", description="Plays one of your PERSONAL sounds in your current VC.")
@commands.cooldown(1, 4, commands.BucketType.user)
async def playsound(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Name of the personal sound to play.", required=True, autocomplete=user_sound_autocomplete) # type: ignore
):
    await ctx.defer() # Defer publicly
    author = ctx.author
    bot_logger.info(f"COMMAND: /playsound by {author.name} ({author.id}), request: '{name}'")

    sound_path = find_user_sound_path(author.id, name)
    if not sound_path:
        sanitized = sanitize_filename(name)
        if sanitized != name: sound_path = find_user_sound_path(author.id, sanitized)

    if not sound_path:
        await ctx.followup.send(f"❌ Personal sound `{name}` not found. Use `/mysounds` or `/soundpanel`.", ephemeral=True); return

    await play_single_sound(ctx.interaction, sound_path)


# --- Sound Panel View [UNCHANGED] ---
class UserSoundboardView(discord.ui.View):
    def __init__(self, user_id: int, *, timeout: Optional[float] = 600.0): # 10 min timeout
        super().__init__(timeout=timeout)
        self.user_id = user_id
        self.message: Optional[discord.Message] = None
        self.populate_buttons()

    def populate_buttons(self):
        user_dir = os.path.join(USER_SOUNDS_DIR, str(self.user_id))
        bot_logger.debug(f"Populating panel for user {self.user_id} from: {user_dir}")
        if not os.path.isdir(user_dir):
            self.add_item(discord.ui.Button(label="No sounds uploaded!", style=discord.ButtonStyle.secondary, disabled=True, custom_id=f"usersb_no_sounds_{self.user_id}"))
            return

        sounds_found, button_row = 0, 0
        max_buttons_per_row, max_rows = 5, 5
        max_buttons_total = max_buttons_per_row * max_rows
        try: files_in_dir = sorted(os.listdir(user_dir), key=str.lower)
        except OSError as e:
            bot_logger.error(f"Error listing user dir '{user_dir}' for panel: {e}")
            self.add_item(discord.ui.Button(label="Error Reading Sounds", style=discord.ButtonStyle.danger, disabled=True, custom_id=f"usersb_error_{self.user_id}"))
            return

        for filename in files_in_dir:
            if sounds_found >= max_buttons_total:
                bot_logger.warning(f"Button limit ({max_buttons_total}) reached for user {self.user_id}. File '{filename}' skipped.")
                break
            filepath = os.path.join(user_dir, filename)
            if os.path.isfile(filepath):
                base_name, ext = os.path.splitext(filename)
                if ext.lower() in ALLOWED_EXTENSIONS:
                    label = base_name.replace("_", " ")[:80]
                    custom_id = f"usersb_play:{filename}" # Use filename with extension
                    if len(custom_id) > 100:
                        bot_logger.warning(f"Skipping sound '{filename}' for {self.user_id} panel: custom_id too long.")
                        continue
                    button = discord.ui.Button(label=label, style=discord.ButtonStyle.secondary, custom_id=custom_id, row=button_row)
                    button.callback = self.user_soundboard_button_callback
                    self.add_item(button)
                    sounds_found += 1
                    if sounds_found % max_buttons_per_row == 0: button_row += 1

        if sounds_found == 0:
             bot_logger.info(f"No valid sounds found for panel user {self.user_id} in '{user_dir}'.")
             self.add_item(discord.ui.Button(label="No sounds uploaded yet!", style=discord.ButtonStyle.secondary, disabled=True, custom_id=f"usersb_no_sounds_{self.user_id}"))

    async def user_soundboard_button_callback(self, interaction: discord.Interaction):
        custom_id = interaction.data["custom_id"]
        user = interaction.user
        bot_logger.info(f"USER PANEL: Button '{custom_id}' clicked by {user.name} on panel for {self.user_id}")
        await interaction.response.defer(ephemeral=True)

        if not custom_id.startswith("usersb_play:"):
            bot_logger.error(f"Invalid custom_id from user panel: '{custom_id}'")
            await interaction.followup.send("❌ Internal error: Invalid button.", ephemeral=True); return

        sound_filename = custom_id.split(":", 1)[1]
        sound_path = os.path.join(USER_SOUNDS_DIR, str(self.user_id), sound_filename)
        await play_single_sound(interaction, sound_path)

    async def on_timeout(self):
        if self.message:
            bot_logger.debug(f"User sound panel timed out for {self.user_id} (message: {self.message.id})")
            owner_name = f"User {self.user_id}"
            try:
                 panel_owner = await self.message.guild.fetch_member(self.user_id) if self.message.guild else await bot.fetch_user(self.user_id)
                 if panel_owner: owner_name = panel_owner.display_name
            except Exception as e: bot_logger.warning(f"Could not fetch panel owner {self.user_id} for timeout: {e}")

            for item in self.children:
                if hasattr(item, 'disabled'): item.disabled = True
            try: await self.message.edit(content=f"🔊 **{owner_name}'s Personal Panel (Expired)**", view=self)
            except discord.HTTPException as e: bot_logger.warning(f"Failed to edit expired panel {self.message.id} for {self.user_id}: {e}")
        else: bot_logger.debug(f"User panel timed out for {self.user_id} but no message ref.")


@bot.slash_command(name="soundpanel", description="Displays buttons to play YOUR personal sounds in your VC.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def soundpanel(ctx: discord.ApplicationContext):
    await ctx.defer() # Defer publicly
    author = ctx.author
    bot_logger.info(f"COMMAND: /soundpanel invoked by {author.name} ({author.id})")
    view = UserSoundboardView(user_id=author.id, timeout=600.0)
    has_playable_buttons = any(
        isinstance(item, discord.ui.Button) and not item.disabled and item.custom_id and item.custom_id.startswith("usersb_play:")
        for item in view.children
    )

    if not has_playable_buttons:
         await ctx.followup.send("No personal sounds uploaded or error generating panel. Use `/uploadsound`!", ephemeral=True); return

    msg_content = f"🔊 **{author.display_name}'s Personal Sound Panel** - Click to play!"
    try:
        message = await ctx.followup.send(msg_content, view=view)
        view.message = message
    except Exception as e:
        bot_logger.error(f"Failed to send soundpanel for user {author.id}: {e}", exc_info=True)
        try: await ctx.followup.send("❌ Failed to create the sound panel.", ephemeral=True)
        except Exception: pass


# === Public Sound Commands [UNCHANGED] ===
@bot.slash_command(name="publishsound", description="Make one of your personal sounds public for everyone.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def publishsound(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Name of YOUR personal sound to make public.", required=True, autocomplete=user_sound_autocomplete) # type: ignore
):
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id = author.id
    bot_logger.info(f"COMMAND: /publishsound by {author.name} ({user_id}), target: '{name}'")

    user_path = find_user_sound_path(user_id, name)
    base_name = name
    if not user_path:
        sanitized = sanitize_filename(name)
        if sanitized != name: user_path = find_user_sound_path(user_id, sanitized); base_name = sanitized

    if not user_path:
        await ctx.followup.send(f"❌ Personal sound `{name}` not found.", ephemeral=True); return

    source_filename = os.path.basename(user_path)
    public_path = os.path.join(PUBLIC_SOUNDS_DIR, source_filename)
    target_base, _ = os.path.splitext(source_filename)

    if find_public_sound_path(target_base):
        await ctx.followup.send(f"❌ Public sound `{target_base}` already exists.", ephemeral=True); return

    try:
        ensure_dir(PUBLIC_SOUNDS_DIR)
        shutil.copy2(user_path, public_path)
        bot_logger.info(f"SOUND PUBLISHED: Copied '{user_path}' to '{public_path}' by {author.name}.")
        await ctx.followup.send(f"✅ Sound `{base_name}` (as `{target_base}`) is now public!\nUse `/playpublic name:{target_base}`.", ephemeral=True)
    except Exception as e:
        bot_logger.error(f"Failed to copy user sound '{user_path}' to public '{public_path}': {e}", exc_info=True)
        await ctx.followup.send(f"❌ Failed to publish `{base_name}`: {type(e).__name__}.", ephemeral=True)

@bot.slash_command(name="removepublic", description="[Admin Only] Remove a sound from the public collection.")
@commands.has_permissions(manage_guild=True)
@commands.cooldown(1, 5, commands.BucketType.guild)
async def removepublic(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Name of the public sound to remove.", required=True, autocomplete=public_sound_autocomplete) # type: ignore
):
    await ctx.defer(ephemeral=True)
    admin = ctx.author
    bot_logger.info(f"COMMAND: /removepublic by admin {admin.name} in guild {ctx.guild.id}, target: '{name}'")

    public_path = find_public_sound_path(name)
    base_name = name
    if not public_path:
        sanitized = sanitize_filename(name)
        if sanitized != name: public_path = find_public_sound_path(sanitized); base_name = sanitized

    if not public_path:
        await ctx.followup.send(f"❌ Public sound `{name}` not found. Use `/publicsounds`.", ephemeral=True); return

    try:
        deleted_filename = os.path.basename(public_path)
        os.remove(public_path)
        bot_logger.info(f"ADMIN ACTION: Deleted public sound '{deleted_filename}' by {admin.name}.")
        await ctx.followup.send(f"🗑️ Public sound `{base_name}` deleted.", ephemeral=True)
    except Exception as e:
        bot_logger.error(f"Admin {admin.name} failed to delete public sound '{public_path}': {e}", exc_info=True)
        await ctx.followup.send(f"❌ Failed to delete public `{base_name}`: {type(e).__name__}.", ephemeral=True)

@removepublic.error
async def removepublic_error(ctx: discord.ApplicationContext, error: discord.DiscordException):
    if isinstance(error, commands.MissingPermissions):
        bot_logger.warning(f"User {ctx.author.name} tried /removepublic without Manage Guild permission.")
        await ctx.respond("🚫 You need `Manage Server` permission.", ephemeral=True)
    else: await on_application_command_error(ctx, error) # Pass other errors to global handler


@bot.slash_command(name="publicsounds", description="Lists all available public sounds.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def publicsounds(ctx: discord.ApplicationContext):
    await ctx.defer(ephemeral=True)
    bot_logger.info(f"COMMAND: /publicsounds by {ctx.author.name}")
    public_sounds = get_public_sound_files()

    if not public_sounds:
        await ctx.followup.send("No public sounds yet. Use `/publishsound` or ask an Admin!", ephemeral=True); return

    sorted_sounds = sorted(public_sounds, key=str.lower)
    parts = []
    length = 0
    limit = 1900
    for name in sorted_sounds:
        line = f"- `{name}`"
        if length + len(line) + 1 > limit: parts.append("... (list truncated)"); break
        parts.append(line); length += len(line) + 1
    list_str = "\n".join(parts)

    embed = discord.Embed(
        title=f"📢 Public Sounds ({len(sorted_sounds)})",
        description=f"Use `/playpublic name:<sound_name>`.\n\n{list_str}",
        color=discord.Color.green()
    ).set_footer(text="Admins use /removepublic.")
    await ctx.followup.send(embed=embed, ephemeral=True)


@bot.slash_command(name="playpublic", description="Plays a public sound in your current voice channel.")
@commands.cooldown(1, 4, commands.BucketType.user)
async def playpublic(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Name of the public sound to play.", required=True, autocomplete=public_sound_autocomplete) # type: ignore
):
    await ctx.defer() # Defer publicly
    author = ctx.author
    bot_logger.info(f"COMMAND: /playpublic by {author.name}, request: '{name}'")

    public_path = find_public_sound_path(name)
    if not public_path:
        sanitized = sanitize_filename(name)
        if sanitized != name: public_path = find_public_sound_path(sanitized)

    if not public_path:
        await ctx.followup.send(f"❌ Public sound `{name}` not found. Use `/publicsounds`.", ephemeral=True); return

    await play_single_sound(ctx.interaction, public_path)


# === TTS Defaults Commands [UNCHANGED] ===
@bot.slash_command(name="setttsdefaults", description="Set your preferred default TTS language and speed for /tts.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def setttsdefaults(
    ctx: discord.ApplicationContext,
    language: discord.Option(str, description="Your preferred default language/accent.", required=True, choices=TTS_LANGUAGE_CHOICES), # type: ignore
    slow: discord.Option(bool, description="Should the TTS speak slowly by default?", required=True) # type: ignore
):
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id_str = str(author.id)
    bot_logger.info(f"COMMAND: /setttsdefaults by {author.name}, lang: {language}, slow: {slow}")

    user_config = user_sound_config.setdefault(user_id_str, {})
    user_config['tts_defaults'] = {'language': language, 'slow': slow}
    save_config()

    lang_name = language
    for choice in TTS_LANGUAGE_CHOICES:
        if choice.value == language: lang_name = choice.name; break

    await ctx.followup.send(
        f"✅ TTS defaults updated!\n"
        f"• Language: {lang_name} (`{language}`)\n"
        f"• Speed: {'Slow' if slow else 'Normal'}\n\n"
        f"Used by `/tts` when options aren't specified.",
        ephemeral=True
    )

@bot.slash_command(name="removettsdefaults", description="Remove your custom TTS language/speed defaults.")
@commands.cooldown(1, 5, commands.BucketType.user)
async def removettsdefaults(ctx: discord.ApplicationContext):
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id_str = str(author.id)
    bot_logger.info(f"COMMAND: /removettsdefaults by {author.name}")

    user_config = user_sound_config.get(user_id_str)
    if user_config and 'tts_defaults' in user_config:
        del user_config['tts_defaults']
        bot_logger.info(f"Removed TTS defaults for {author.name}")
        if not user_config:
            if user_id_str in user_sound_config: del user_sound_config[user_id_str]
            bot_logger.info(f"Removed empty user config for {author.name}.")
        save_config()
        await ctx.followup.send(
            f"🗑️ Custom TTS defaults removed.\nBot defaults will be used.",
            ephemeral=True
        )
    else:
        await ctx.followup.send("🤷 No custom TTS defaults configured.", ephemeral=True)


# === TTS Command [UNCHANGED logic, interacts with modified helpers] ===
@bot.slash_command(name="tts", description="Make the bot say something using Text-to-Speech.")
@commands.cooldown(1, 6, commands.BucketType.user)
async def tts(
    ctx: discord.ApplicationContext,
    message: discord.Option(str, description=f"Text to speak (max {MAX_TTS_LENGTH} chars).", required=True), # type: ignore
    language: discord.Option(str, description="Override TTS language.", required=False, choices=TTS_LANGUAGE_CHOICES), # type: ignore
    slow: discord.Option(bool, description="Override slow speech.", required=False) # type: ignore
):
    await ctx.defer(ephemeral=True)
    user = ctx.author
    guild = ctx.guild
    user_id_str = str(user.id)
    bot_logger.info(f"COMMAND: /tts by {user.name}, lang: {language}, slow: {slow}, msg: '{message[:50]}...'")

    if not guild or not isinstance(user, discord.Member) or not user.voice or not user.voice.channel:
        await ctx.followup.send("Must be in a VC to use TTS.", ephemeral=True); return
    if len(message) > MAX_TTS_LENGTH:
         await ctx.followup.send(f"❌ Message too long! Max {MAX_TTS_LENGTH} chars.", ephemeral=True); return
    if not message.strip():
         await ctx.followup.send("❌ Provide text to say.", ephemeral=True); return

    target_channel = user.voice.channel
    user_config = user_sound_config.get(user_id_str, {})
    saved_defaults = user_config.get("tts_defaults", {})
    final_language = language if language is not None else saved_defaults.get('language', DEFAULT_TTS_LANGUAGE)
    final_slow = slow if slow is not None else saved_defaults.get('slow', DEFAULT_TTS_SLOW)
    lang_source = "explicit" if language is not None else ("saved" if 'language' in saved_defaults else "default")
    slow_source = "explicit" if slow is not None else ("saved" if 'slow' in saved_defaults else "default")
    bot_logger.info(f"TTS Final: lang={final_language}({lang_source}), slow={final_slow}({slow_source}) for {user.name}")

    audio_source: Optional[discord.PCMAudio] = None
    pcm_fp = io.BytesIO()

    try:
        bot_logger.info(f"TTS: Generating audio for '{user.name}' (lang={final_language}, slow={final_slow})")
        tts_instance = gTTS(text=message, lang=final_language, slow=final_slow)
        loop = asyncio.get_running_loop()

        def process_tts_sync():
            mp3_fp = io.BytesIO()
            tts_instance.write_to_fp(mp3_fp); mp3_fp.seek(0)
            if mp3_fp.getbuffer().nbytes == 0: raise ValueError("gTTS gen empty")
            bot_logger.debug(f"TTS: MP3 in memory ({mp3_fp.getbuffer().nbytes} bytes)")
            seg = AudioSegment.from_file(mp3_fp, format="mp3")
            bot_logger.debug(f"TTS: Loaded MP3 (duration: {len(seg)}ms)")
            if len(seg) > MAX_PLAYBACK_DURATION_MS:
                bot_logger.info(f"TTS: Trimming from {len(seg)}ms to {MAX_PLAYBACK_DURATION_MS}ms.")
                seg = seg[:MAX_PLAYBACK_DURATION_MS]
            seg = seg.set_frame_rate(48000).set_channels(2)
            seg.export(pcm_fp, format="s16le"); pcm_fp.seek(0)
            if pcm_fp.getbuffer().nbytes == 0: raise ValueError("Pydub export empty")
            bot_logger.debug(f"TTS: PCM in memory ({pcm_fp.getbuffer().nbytes} bytes)")

        await loop.run_in_executor(None, process_tts_sync)
        audio_source = discord.PCMAudio(pcm_fp)
        bot_logger.info(f"TTS: PCMAudio source created for {user.name}.")

    except gTTSError as e:
        msg = f"❌ TTS Error: Lang '{final_language}' invalid?" if "Language not found" in str(e) else f"❌ TTS Gen Error: {e}"
        await ctx.followup.send(msg, ephemeral=True)
        bot_logger.error(f"TTS Gen Error (gTTS) for {user.name} (lang={final_language}): {e}", exc_info=True)
        pcm_fp.close(); return
    except (ImportError, ValueError, FileNotFoundError, Exception) as e:
        err_type = type(e).__name__
        msg = f"❌ Error processing TTS ({err_type})."
        if isinstance(e, FileNotFoundError) and 'ffmpeg' in str(e).lower():
             msg = "❌ Error: FFmpeg needed for TTS processing."
        elif isinstance(e, ValueError): msg = f"❌ Error processing TTS: {e}"
        await ctx.followup.send(msg, ephemeral=True)
        bot_logger.error(f"TTS: Failed gen/process for {user.name}: {e}", exc_info=True)
        pcm_fp.close(); return

    if not audio_source:
        await ctx.followup.send("❌ Failed to prepare TTS audio.", ephemeral=True)
        bot_logger.error("TTS: Audio source None after processing block."); pcm_fp.close(); return

    # Use helper for connection, permission, busy, stay checks
    voice_client = await _ensure_voice_client_ready(ctx.interaction, target_channel, action_type="TTS")
    if not voice_client: pcm_fp.close(); return # Helper failed/sent feedback

    if voice_client.is_playing():
         bot_logger.warning(f"TTS: VC busy between check and play for {user.name}.")
         await ctx.followup.send("⏳ Bot became busy. Try again.", ephemeral=True)
         after_play_handler(None, voice_client); pcm_fp.close(); return

    try:
        bot_logger.info(f"TTS PLAYBACK: Playing TTS requested by {user.display_name}...")
        # Close PCM buffer in 'after' callback
        voice_client.play(audio_source, after=lambda e: (after_play_handler(e, voice_client), pcm_fp.close()))

        speed_str = "(slow)" if final_slow else ""
        lang_name = final_language
        for choice in TTS_LANGUAGE_CHOICES:
            if choice.value == final_language: lang_name = choice.name; break
        display_msg = message[:150] + ('...' if len(message) > 150 else '')
        await ctx.followup.send(f"🗣️ Now saying in **{lang_name}** {speed_str} (max {MAX_PLAYBACK_DURATION_MS/1000}s): \"{display_msg}\"", ephemeral=True)

    except discord.errors.ClientException as e:
        msg = "❌ Error: Bot already playing or client issue."
        await ctx.followup.send(msg, ephemeral=True)
        bot_logger.error(f"TTS PLAYBACK ERROR (ClientException): {e}", exc_info=True)
        after_play_handler(e, voice_client); pcm_fp.close()
    except Exception as e:
        await ctx.followup.send("❌ Unexpected error during TTS playback.", ephemeral=True)
        bot_logger.error(f"TTS PLAYBACK ERROR (Unexpected): {e}", exc_info=True)
        after_play_handler(e, voice_client); pcm_fp.close()

# === Stay/Leave Commands [NEW] ===

@bot.slash_command(name="togglestay", description="[Admin Only] Toggle whether the bot stays in VC when idle.")
@commands.has_permissions(manage_guild=True)
@commands.cooldown(1, 5, commands.BucketType.guild)
async def togglestay(ctx: discord.ApplicationContext):
    """Toggles the 'stay_in_channel' setting for the current guild."""
    await ctx.defer(ephemeral=True) # Admin actions best kept private
    guild_id_str = str(ctx.guild_id)
    admin = ctx.author
    bot_logger.info(f"COMMAND: /togglestay by admin {admin.name} ({admin.id}) in guild {guild_id_str}")

    # Get current setting, default to False if not present
    current_setting = guild_settings.get(guild_id_str, {}).get("stay_in_channel", False)
    new_setting = not current_setting

    # Update the setting using setdefault to create guild entry if needed
    guild_settings.setdefault(guild_id_str, {})['stay_in_channel'] = new_setting
    save_guild_settings() # Persist the change

    status_message = "ENABLED ✅" if new_setting else "DISABLED ❌"
    await ctx.followup.send(f"Bot 'Stay in Channel' feature is now **{status_message}** for this server.", ephemeral=True)
    bot_logger.info(f"Guild {guild_id_str} 'stay_in_channel' set to {new_setting} by {admin.name}")

@togglestay.error
async def togglestay_error(ctx: discord.ApplicationContext, error: discord.DiscordException):
    """Error handler specifically for /togglestay permissions."""
    if isinstance(error, commands.MissingPermissions):
        bot_logger.warning(f"User {ctx.author.name} tried /togglestay without Manage Guild permission.")
        await ctx.respond("🚫 You need the `Manage Server` permission to use this command.", ephemeral=True)
    elif isinstance(error, commands.CommandOnCooldown):
         await ctx.respond(f"⏳ This command is on cooldown. Try again in {error.retry_after:.1f}s.", ephemeral=True)
    else:
        # Pass other errors (like cooldowns, bot perms) to the global handler
        await on_application_command_error(ctx, error)


@bot.slash_command(name="leave", description="Make the bot leave its current voice channel.")
@commands.cooldown(1, 5, commands.BucketType.user)
async def leave(ctx: discord.ApplicationContext):
    """Forces the bot to leave the voice channel in the current guild."""
    await ctx.defer(ephemeral=True) # Keep confirmation private
    guild = ctx.guild
    user = ctx.author
    bot_logger.info(f"COMMAND: /leave invoked by {user.name} ({user.id}) in guild {guild.id}")

    vc = discord.utils.get(bot.voice_clients, guild=guild)

    if vc and vc.is_connected():
        bot_logger.info(f"LEAVE: Manually disconnecting from {vc.channel.name} in {guild.name}...")
        # Call safe_disconnect with manual_leave=True to bypass the 'stay' check
        await safe_disconnect(vc, manual_leave=True)
        await ctx.followup.send("👋 Leaving the voice channel.", ephemeral=True)
    else:
        bot_logger.info(f"LEAVE: Request by {user.name}, but bot not connected in {guild.name}.")
        await ctx.followup.send("🤷 I'm not currently in a voice channel in this server.", ephemeral=True)


# --- Error Handler for Application Commands [UNCHANGED] ---
@bot.event
async def on_application_command_error(ctx: discord.ApplicationContext, error: discord.DiscordException):
    """Global handler for slash command errors."""
    cmd_name = ctx.command.qualified_name if ctx.command else "Unknown"
    user_name = f"{ctx.author.name}({ctx.author.id})" if ctx.author else "Unknown"
    log_prefix = f"CMD ERROR (/{cmd_name}, user: {user_name}):"

    async def send_error_response(message: str, log_level=logging.WARNING):
        bot_logger.log(log_level, f"{log_prefix} {message} (ErrType: {type(error).__name__}, Details: {error})")
        try:
            if ctx.interaction.response.is_done(): await ctx.followup.send(message, ephemeral=True)
            else: await ctx.respond(message, ephemeral=True)
        except discord.NotFound: bot_logger.warning(f"{log_prefix} Interaction expired before error response.")
        except discord.Forbidden: bot_logger.error(f"{log_prefix} Missing perms to send error response in {ctx.channel.name}.")
        except Exception as e_resp: bot_logger.error(f"{log_prefix} Failed to send error response: {e_resp}")

    if isinstance(error, commands.CommandOnCooldown):
        await send_error_response(f"⏳ Command on cooldown. Wait {error.retry_after:.1f}s.")
    elif isinstance(error, commands.MissingPermissions):
        perms = ', '.join(f"`{p}`" for p in error.missing_permissions)
        await send_error_response(f"🚫 Missing permissions: {perms}", log_level=logging.WARNING)
    elif isinstance(error, commands.BotMissingPermissions):
        perms = ', '.join(f"`{p}`" for p in error.missing_permissions)
        await send_error_response(f"🚫 I lack permissions: {perms}.", log_level=logging.ERROR)
    elif isinstance(error, commands.CheckFailure):
        await send_error_response("🚫 Permission check failed.")
    elif isinstance(error, discord.errors.ApplicationCommandInvokeError):
        original = error.original
        bot_logger.error(f"{log_prefix} Error in command code: {original}", exc_info=original)
        user_msg = "❌ Internal error running command."
        if isinstance(original, FileNotFoundError) and 'ffmpeg' in str(original).lower():
             user_msg = "❌ Internal Error: FFmpeg needed but not found."
        elif isinstance(original, CouldntDecodeError):
             user_msg = "❌ Internal Error: Failed decoding audio."
        elif isinstance(original, gTTSError): user_msg = f"❌ Internal TTS Error: {original}"
        elif isinstance(original, discord.errors.Forbidden): user_msg = "❌ Internal Error: Permission issue during execution."
        await send_error_response(user_msg, log_level=logging.ERROR)
    else:
        bot_logger.error(f"{log_prefix} Unexpected DiscordException: {error}", exc_info=error)
        await send_error_response(f"❌ Unexpected error ({type(error).__name__}). Try later.", log_level=logging.ERROR)


# --- Run the Bot [UNCHANGED] ---
if __name__ == "__main__":
    if not PYDUB_AVAILABLE:
        bot_logger.critical("Pydub library missing. Install: pip install pydub ffmpeg")
        exit(1)
    if not BOT_TOKEN:
        bot_logger.critical("BOT_TOKEN missing in environment/.env.")
        exit(1)

    opus_loaded = discord.opus.is_loaded()
    if not opus_loaded:
        bot_logger.warning("Default Opus load failed. Trying explicit paths...")
        opus_paths = ["libopus.so.0", "libopus.so", "opus", "libopus-0.dll", "opus.dll", "/usr/local/lib/libopus.so.0", "/opt/homebrew/opt/opus/lib/libopus.0.dylib"]
        for path in opus_paths:
            try:
                discord.opus.load_opus(path)
                if discord.opus.is_loaded():
                    bot_logger.info(f"Opus loaded successfully from: {path}")
                    opus_loaded = True; break
            except OSError: bot_logger.debug(f"Opus load failed (Not found): {path}")
            except Exception as e: bot_logger.warning(f"Opus load failed (Error: {e}): {path}")
        if not opus_loaded:
            bot_logger.critical("CRITICAL: Opus library NOT loaded. Voice WILL FAIL.")
            # Consider exit(1) here

    try:
        bot_logger.info("Attempting bot startup...")
        bot.run(BOT_TOKEN)
    except discord.errors.LoginFailure:
        bot_logger.critical("CRITICAL STARTUP ERROR: Login Failure. Bad BOT_TOKEN?")
    except discord.errors.PrivilegedIntentsRequired as e:
        bot_logger.critical(f"CRITICAL STARTUP ERROR: Missing Privileged Intents: {e}")
    except Exception as e:
        log_level = logging.CRITICAL if not opus_loaded and "opus" in str(e).lower() else logging.ERROR
        bot_logger.log(log_level, f"FATAL RUNTIME ERROR during bot execution: {e}", exc_info=True)
        exit(1)