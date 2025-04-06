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
TARGET_LOUDNESS_DBFS = -14.0
MAX_USER_SOUND_SIZE_MB = 5
MAX_USER_SOUNDS_PER_USER = 25
ALLOWED_EXTENSIONS = ['.mp3', '.wav', '.ogg', '.m4a', '.aac']
MAX_TTS_LENGTH = 250 # Max characters for TTS command
DEFAULT_TTS_LANGUAGE = 'en' # Bot's default if no user pref/override
DEFAULT_TTS_SLOW = False    # Bot's default if no user pref/override

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
# User config can now store join sound and tts defaults:
# { "user_id_str": { "join_sound": "filename.mp3", "tts_defaults": {"language": "fr", "slow": true} } }
user_sound_config: Dict[str, Dict[str, Any]] = {}
guild_sound_queues = {}
guild_play_tasks = {}

# --- Config/Dir Functions ---
def load_config():
    global user_sound_config
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f: user_sound_config = json.load(f)
            # Convert old format (just filename string) to new format (dict)
            upgraded_count = 0
            for user_id, data in list(user_sound_config.items()):
                if isinstance(data, str): # Old format detected
                    user_sound_config[user_id] = {"join_sound": data}
                    upgraded_count += 1
            if upgraded_count > 0:
                bot_logger.info(f"Upgraded {upgraded_count} old user configs to new format.")
                save_config() # Save the upgraded format immediately
            bot_logger.info(f"Loaded {len(user_sound_config)} configs from {CONFIG_FILE}")
        except (json.JSONDecodeError, Exception) as e:
             bot_logger.error(f"Error loading {CONFIG_FILE}: {e}", exc_info=True)
             user_sound_config = {}
    else:
        user_sound_config = {}
        bot_logger.info(f"{CONFIG_FILE} not found. Starting fresh.")

def save_config():
     try:
        with open(CONFIG_FILE, 'w') as f: json.dump(user_sound_config, f, indent=4)
        bot_logger.debug(f"Saved {len(user_sound_config)} configs to {CONFIG_FILE}")
     except Exception as e:
         bot_logger.error(f"Error saving {CONFIG_FILE}: {e}", exc_info=True)

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
    bot_logger.info(f"Py-cord: {discord.__version__}, Norm Target: {TARGET_LOUDNESS_DBFS}dBFS")
    bot_logger.info(f"Allowed: {', '.join(ALLOWED_EXTENSIONS)}, Max TTS: {MAX_TTS_LENGTH}")
    bot_logger.info(f"Dirs: {os.path.abspath(SOUNDS_DIR)}, {os.path.abspath(USER_SOUNDS_DIR)}, {os.path.abspath(PUBLIC_SOUNDS_DIR)}")
    bot_logger.info("Sound Bot is operational.")

# --- Audio Processing Helper [UNCHANGED] ---
def process_audio(sound_path: str, member_display_name: str = "User") -> Optional[discord.PCMAudio]:
    """Loads, normalizes, and prepares audio returning a PCMAudio source or None."""
    if not PYDUB_AVAILABLE or not os.path.exists(sound_path):
        bot_logger.error(f"AUDIO: Pydub missing or File not found: '{sound_path}'")
        return None

    audio_source = None
    basename = os.path.basename(sound_path)
    try:
        bot_logger.debug(f"AUDIO: Loading '{basename}'...")
        ext = os.path.splitext(sound_path)[1].lower().strip('. ') or 'mp3'
        audio_segment = AudioSegment.from_file(sound_path, format=ext)

        peak_dbfs = audio_segment.max_dBFS
        if not math.isinf(peak_dbfs) and peak_dbfs > -90.0:
            change_in_dbfs = TARGET_LOUDNESS_DBFS - peak_dbfs
            bot_logger.info(f"AUDIO: Normalizing '{basename}'. Peak:{peak_dbfs:.2f} Target:{TARGET_LOUDNESS_DBFS:.2f} Gain:{change_in_dbfs:.2f} dB.")
            if change_in_dbfs < 0: audio_segment = audio_segment.apply_gain(change_in_dbfs)
            else: bot_logger.info(f"AUDIO: Skipping positive gain for '{basename}'.")
        elif math.isinf(peak_dbfs): bot_logger.warning(f"AUDIO: Cannot normalize silent '{basename}'.")
        else: bot_logger.warning(f"AUDIO: Skipping normalization for quiet '{basename}'. Peak: {peak_dbfs:.2f}")

        audio_segment = audio_segment.set_frame_rate(48000).set_channels(2)
        pcm_data_io = io.BytesIO()
        audio_segment.export(pcm_data_io, format="s16le")
        pcm_data_io.seek(0)

        if pcm_data_io.getbuffer().nbytes > 0:
            audio_source = discord.PCMAudio(pcm_data_io)
            bot_logger.debug(f"AUDIO: Successfully processed '{basename}'")
        else: bot_logger.error(f"AUDIO: Exported raw audio for '{basename}' is empty!")

    except CouldntDecodeError: bot_logger.error(f"AUDIO: Pydub CouldntDecodeError for '{basename}'. FFmpeg installed/PATH? File corrupt?", exc_info=True)
    except FileNotFoundError: bot_logger.error(f"AUDIO: File not found during processing: '{sound_path}'")
    except Exception as e: bot_logger.error(f"AUDIO: Unexpected error processing '{basename}': {e}", exc_info=True)
    return audio_source

# --- Core Join Sound Queue Logic [UNCHANGED] ---
async def play_next_in_queue(guild: discord.Guild):
    guild_id = guild.id
    task_id = asyncio.current_task().get_name() if asyncio.current_task() else 'Unknown'
    bot_logger.debug(f"QUEUE CHECK [{task_id}]: Guild {guild_id}")

    if guild_id not in guild_sound_queues or not guild_sound_queues[guild_id]:
        bot_logger.debug(f"QUEUE [{task_id}]: Empty/Non-existent for {guild_id}. Disconnecting.")
        await safe_disconnect(discord.utils.get(bot.voice_clients, guild=guild))
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
        bot_logger.debug(f"QUEUE [{task_id}]: Became empty unexpectedly for {guild_id}.")
        await safe_disconnect(vc)
        if guild_id in guild_play_tasks and guild_play_tasks[guild_id] is asyncio.current_task(): del guild_play_tasks[guild_id]
        return

    audio_source = process_audio(sound_path, member.display_name)
    if audio_source:
        try:
            bot_logger.info(f"QUEUE PLAYBACK [{task_id}]: Playing for {member.display_name}...")
            vc.play(audio_source, after=lambda e: after_play_handler(e, vc))
            bot_logger.debug(f"QUEUE PLAYBACK [{task_id}]: vc.play() called for {member.display_name}.")
        except (discord.errors.ClientException, Exception) as e:
            bot_logger.error(f"QUEUE PLAYBACK ERROR [{task_id}]: {type(e).__name__}: {e}", exc_info=True)
            bot.loop.create_task(play_next_in_queue(guild), name=f"QueueRetry_{guild_id}")
    else:
        bot_logger.warning(f"QUEUE PLAYBACK [{task_id}]: No valid source for {member.display_name} ({os.path.basename(sound_path)}). Skipping.")
        bot.loop.create_task(play_next_in_queue(guild), name=f"QueueSkip_{guild_id}")

# --- on_voice_state_update [Modified for new config structure] ---
@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot or not after.channel or before.channel == after.channel: return

    channel_to_join = after.channel
    guild = member.guild
    bot_logger.info(f"EVENT: {member.display_name} ({member.id}) entered {channel_to_join.name} in {guild.name}")

    bot_perms = channel_to_join.permissions_for(guild.me)
    if not bot_perms.connect or not bot_perms.speak:
        bot_logger.warning(f"Missing Connect/Speak permission in '{channel_to_join.name}'.")
        return

    sound_path: Optional[str] = None
    is_tts = False
    user_id_str = str(member.id)

    # --- MODIFIED PART ---
    user_config = user_sound_config.get(user_id_str)
    if user_config and "join_sound" in user_config:
        filename = user_config["join_sound"]
        potential_path = os.path.join(SOUNDS_DIR, filename)
        if os.path.exists(potential_path):
            sound_path = potential_path
            bot_logger.info(f"SOUND: Using join sound: '{filename}' for {member.display_name}")
        else:
            bot_logger.warning(f"SOUND: Configured join sound '{filename}' not found. Removing broken entry, using TTS.")
            del user_config["join_sound"]
            # If the user config dict is now empty, remove the user entirely
            if not user_config:
                del user_sound_config[user_id_str]
            save_config()
            is_tts = True
    else:
        is_tts = True
        bot_logger.info(f"SOUND: No custom join sound for {member.display_name}. Using TTS.")
    # --- END MODIFIED PART ---

    if is_tts:
        tts_path = os.path.join(SOUNDS_DIR, f"tts_join_{member.id}.mp3")
        bot_logger.info(f"TTS: Generating join TTS for {member.display_name} ('{tts_path}')...")
        try:
            # Get user's preferred TTS language for join sound, default to bot's default
            tts_defaults = user_config.get("tts_defaults", {}) if user_config else {}
            tts_lang = tts_defaults.get("language", DEFAULT_TTS_LANGUAGE)
            # Join sounds are never slow
            bot_logger.debug(f"TTS Join using lang: {tts_lang}")

            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: gTTS(text=f"{member.display_name} joined", lang=tts_lang).save(tts_path))
            bot_logger.info(f"TTS: Saved join TTS file '{tts_path}'")
            sound_path = tts_path
        except Exception as e:
            bot_logger.error(f"TTS: Failed join TTS generation for {member.display_name}: {e}", exc_info=True)
            sound_path = None # Fallback to silence if TTS fails

    if not sound_path:
        bot_logger.error(f"Could not determine/generate join sound/TTS path for {member.display_name}. Skipping.")
        return

    # Rest of the function remains unchanged...
    guild_id = guild.id
    if guild_id not in guild_sound_queues: guild_sound_queues[guild_id] = deque()
    guild_sound_queues[guild_id].append((member, sound_path))
    bot_logger.info(f"QUEUE: Added join sound for {member.display_name}. Queue size: {len(guild_sound_queues[guild_id])}")

    vc = discord.utils.get(bot.voice_clients, guild=guild)

    if vc and vc.is_playing():
        bot_logger.info(f"VOICE: Bot playing in {guild.name}. Join sound queued. Deferred.")
        if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
             task_name = f"QueueTriggerDeferred_{guild_id}"
             guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(guild), name=task_name)
             bot_logger.debug(f"VOICE: Created deferred play task '{task_name}'.")
        return

    should_start_play_task = False
    try:
        if not vc or not vc.is_connected():
            bot_logger.info(f"VOICE: Connecting to '{channel_to_join.name}' for queue.")
            vc = await channel_to_join.connect(timeout=30.0, reconnect=True)
            bot_logger.info(f"VOICE: Connected to '{channel_to_join.name}'.")
            should_start_play_task = True
        elif vc.channel != channel_to_join:
             bot_logger.info(f"VOICE: Moving to '{channel_to_join.name}' for queue.")
             await vc.move_to(channel_to_join)
             bot_logger.info(f"VOICE: Moved to '{channel_to_join.name}'.")
             should_start_play_task = True
        else:
             bot_logger.debug(f"VOICE: Bot already in '{channel_to_join.name}' and idle.")
             should_start_play_task = True

    except (asyncio.TimeoutError, discord.errors.ClientException, Exception) as e:
        bot_logger.error(f"VOICE: {type(e).__name__} during connect/move to '{channel_to_join.name}': {e}", exc_info=True)
        if guild_id in guild_sound_queues: guild_sound_queues[guild_id].clear()

    if should_start_play_task and vc and vc.is_connected():
        if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
            task_name = f"QueueStart_{guild_id}"
            guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(guild), name=task_name)
            bot_logger.info(f"VOICE: Started play task '{task_name}' for guild {guild_id}.")
        else:
             bot_logger.debug(f"VOICE: Play task for {guild_id} already running.")


# --- after_play_handler [UNCHANGED] ---
def after_play_handler(error: Optional[Exception], vc: discord.VoiceClient):
    guild_id = vc.guild.id if vc and vc.guild else None
    if error: bot_logger.error(f'PLAYBACK ERROR (In after_play_handler for guild {guild_id}): {error}', exc_info=error)
    if not guild_id or not vc.is_connected():
        bot_logger.warning(f"after_play_handler called with invalid/disconnected vc (ID: {guild_id}).")
        if guild_id and guild_id in guild_play_tasks:
             play_task = guild_play_tasks.pop(guild_id, None)
             if play_task and not play_task.done(): play_task.cancel()
        return

    bot_logger.debug(f"Playback finished for {guild_id}. Triggering queue check.")
    is_join_queue_empty = guild_id not in guild_sound_queues or not guild_sound_queues[guild_id]

    if not is_join_queue_empty:
        bot_logger.debug(f"AFTER_PLAY: Join queue for {guild_id} not empty. Ensuring task runs.")
        if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
             task_name = f"QueueCheckAfterPlay_{guild_id}"
             guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(vc.guild), name=task_name)
             bot_logger.debug(f"AFTER_PLAY: Scheduled task '{task_name}' for {guild_id}.")
        else: bot_logger.debug(f"AFTER_PLAY: Task for {guild_id} already exists.")
    else:
         bot_logger.debug(f"AFTER_PLAY: Join queue for {guild_id} is empty. Attempting safe disconnect.")
         bot.loop.create_task(safe_disconnect(vc), name=f"SafeDisconnectAfterPlay_{guild_id}")

# --- safe_disconnect [UNCHANGED] ---
async def safe_disconnect(vc: Optional[discord.VoiceClient]):
    if not vc or not vc.is_connected(): return

    guild = vc.guild
    guild_id = guild.id
    is_join_queue_empty = guild_id not in guild_sound_queues or not guild_sound_queues[guild_id]
    is_playing = vc.is_playing()

    if is_join_queue_empty and not is_playing:
        bot_logger.info(f"DISCONNECT: Conditions met for {guild_id}. Disconnecting...")
        try:
            if vc.is_playing(): # Should be false, but double-check
                bot_logger.warning(f"DISCONNECT: Called stop() during safe_disconnect for {guild.name}.")
                vc.stop()
            await vc.disconnect(force=False)
            bot_logger.info(f"DISCONNECT: Bot disconnected from '{guild.name}'.")
            if guild_id in guild_play_tasks:
                 play_task = guild_play_tasks.pop(guild_id, None)
                 if play_task:
                     if not play_task.done(): play_task.cancel()
                     bot_logger.debug(f"DISCONNECT: Cleaned up play task tracker for {guild_id}.")
        except Exception as e:
            bot_logger.error(f"DISCONNECT ERROR: Failed disconnect from {guild.name}: {e}", exc_info=True)
    else:
         bot_logger.debug(f"Disconnect skipped for {guild.name}: Queue empty={is_join_queue_empty}, Playing={is_playing}.")

# --- NEW: Voice Client Connection/Busy Check Helper [UNCHANGED] ---
async def _ensure_voice_client_ready(interaction: discord.Interaction, target_channel: discord.VoiceChannel, action_type: str = "Playback") -> Optional[discord.VoiceClient]:
    """Helper to connect/move/check busy status and permissions. Returns VC or None."""
    guild = interaction.guild
    user = interaction.user
    guild_id = guild.id
    log_prefix = f"{action_type.upper()}:"

    bot_perms = target_channel.permissions_for(guild.me)
    if not bot_perms.connect or not bot_perms.speak:
        await interaction.followup.send(f"❌ I don't have permission to Connect or Speak in {target_channel.mention}.", ephemeral=True)
        return None

    vc = discord.utils.get(bot.voice_clients, guild=guild)
    try:
        if vc and vc.is_connected():
            if vc.is_playing():
                # Prioritize join queue
                if guild_id in guild_sound_queues and guild_sound_queues[guild_id]:
                    msg = "⏳ Bot is currently playing join sounds. Please wait."
                    log_msg = f"{log_prefix} Bot busy with join queue in {guild.name}, user {user.name} ignored."
                else:
                    msg = "⏳ Bot is currently playing another sound/TTS. Please wait."
                    log_msg = f"{log_prefix} Bot busy in {guild.name}, user {user.name} ignored."
                await interaction.followup.send(msg, ephemeral=True)
                bot_logger.info(log_msg)
                return None # Indicate busy
            elif vc.channel != target_channel:
                bot_logger.info(f"{log_prefix} Moving from '{vc.channel.name}' to '{target_channel.name}' for {user.name}.")
                await vc.move_to(target_channel)
                bot_logger.info(f"{log_prefix} Moved successfully.")
        else:
            bot_logger.info(f"{log_prefix} Connecting to '{target_channel.name}' for {user.name}.")
            vc = await target_channel.connect(timeout=30.0, reconnect=True)
            bot_logger.info(f"{log_prefix} Connected successfully.")

        if not vc or not vc.is_connected():
             bot_logger.error(f"{log_prefix} Failed to establish voice client for {target_channel.name}.")
             await interaction.followup.send("❌ Failed to connect/move to the voice channel.", ephemeral=True)
             return None
        return vc # Success

    except asyncio.TimeoutError:
         await interaction.followup.send("❌ Connection to the voice channel timed out.", ephemeral=True)
         bot_logger.error(f"{log_prefix} Connection/Move Timeout in {guild.name}")
         return None
    except discord.errors.ClientException as e:
        msg = "⏳ Bot is busy connecting/disconnecting. Please wait." if "already connect" in str(e).lower() else "❌ Error connecting/moving. Check permissions?"
        await interaction.followup.send(msg, ephemeral=True)
        bot_logger.warning(f"{log_prefix} Connection/Move ClientException in {guild.name}: {e}")
        return None
    except Exception as e:
        await interaction.followup.send("❌ An unexpected error occurred joining the voice channel.", ephemeral=True)
        bot_logger.error(f"{log_prefix} Connection/Move unexpected error in {guild.name}: {e}", exc_info=True)
        return None

# --- Single Sound Playback Logic (For Files) [UNCHANGED] ---
async def play_single_sound(interaction: discord.Interaction, sound_path: str):
    """Connects (if needed), plays a single sound FILE, and uses after_play_handler."""
    user = interaction.user
    guild = interaction.guild

    if not guild or not user.voice or not user.voice.channel:
        await interaction.followup.send("This command only works in a server where you're in a voice channel.", ephemeral=True)
        return

    target_channel = user.voice.channel
    if not os.path.exists(sound_path):
         await interaction.followup.send("❌ Error: The sound file seems to be missing.", ephemeral=True)
         bot_logger.error(f"SINGLE PLAY: File not found: {sound_path}")
         return

    # Use the helper for connection/busy checks
    voice_client = await _ensure_voice_client_ready(interaction, target_channel, action_type="SINGLE PLAY (File)")
    if not voice_client:
        return # Helper already sent feedback

    # Process and Play Audio FILE
    bot_logger.info(f"SINGLE PLAY (File): Processing '{os.path.basename(sound_path)}' for {user.name}...")
    audio_source = process_audio(sound_path, user.display_name)

    if audio_source:
        if voice_client.is_playing(): # Final check before playing
             bot_logger.warning(f"SINGLE PLAY (File): VC became busy between check and play for {user.name}. Aborting.")
             await interaction.followup.send("⏳ Bot became busy just now. Please try again.", ephemeral=True)
             after_play_handler(None, voice_client) # Ensure handler runs
             return

        try:
            sound_basename = os.path.basename(sound_path)
            bot_logger.info(f"SINGLE PLAYBACK (File): Playing '{sound_basename}' requested by {user.display_name}...")
            voice_client.play(audio_source, after=lambda e: after_play_handler(e, voice_client))
            await interaction.followup.send(f"▶️ Playing `{os.path.splitext(sound_basename)[0]}`...", ephemeral=True)
        except (discord.errors.ClientException, Exception) as e:
            msg = "❌ Error: Already playing or client issue." if isinstance(e, discord.errors.ClientException) else "❌ Unexpected playback error."
            await interaction.followup.send(msg, ephemeral=True)
            bot_logger.error(f"SINGLE PLAYBACK ERROR (File - {type(e).__name__}): {e}", exc_info=True)
            after_play_handler(e, voice_client) # Still call handler
    else:
        await interaction.followup.send("❌ Error: Could not process the audio file. Check logs.", ephemeral=True)
        bot_logger.error(f"SINGLE PLAYBACK (File): Failed to get audio source for '{sound_path}'")
        if voice_client and voice_client.is_connected(): after_play_handler(None, voice_client) # Call handler even on failure

# --- Helper Functions [UNCHANGED except autocomplete] ---
def sanitize_filename(name: str) -> str:
    name = re.sub(r'[<>:"/\\|?*\.\s]+', '_', name)
    name = re.sub(r'_+', '_', name).strip('_')
    return name[:50]

def _find_sound_path_in_dir(directory: str, sound_name: str) -> Optional[str]:
    """Generic sound finder helper."""
    if not os.path.isdir(directory): return None
    preferred_order = ['.mp3', '.wav'] + [ext for ext in ALLOWED_EXTENSIONS if ext not in ['.mp3', '.wav']]
    for ext in preferred_order:
        for name_variant in [sound_name, sanitize_filename(sound_name)]:
             potential_path = os.path.join(directory, f"{name_variant}{ext}")
             if os.path.exists(potential_path): return potential_path
    return None

def _get_sound_files_from_dir(directory: str) -> List[str]:
    """Generic sound lister helper."""
    sounds = []
    if os.path.isdir(directory):
        try:
            for filename in os.listdir(directory):
                filepath = os.path.join(directory, filename)
                base_name, ext = os.path.splitext(filename)
                if os.path.isfile(filepath) and ext.lower() in ALLOWED_EXTENSIONS:
                    sounds.append(base_name)
        except OSError as e: bot_logger.error(f"Error listing files in {directory}: {e}")
    return sounds

def get_user_sound_files(user_id: int) -> List[str]:
    return _get_sound_files_from_dir(os.path.join(USER_SOUNDS_DIR, str(user_id)))

def find_user_sound_path(user_id: int, sound_name: str) -> Optional[str]:
    path = _find_sound_path_in_dir(os.path.join(USER_SOUNDS_DIR, str(user_id)), sound_name)
    #if not path: bot_logger.debug(f"Sound '{sound_name}' not found for user {user_id}")
    return path

def get_public_sound_files() -> List[str]:
    return _get_sound_files_from_dir(PUBLIC_SOUNDS_DIR)

def find_public_sound_path(sound_name: str) -> Optional[str]:
    path = _find_sound_path_in_dir(PUBLIC_SOUNDS_DIR, sound_name)
    #if not path: bot_logger.debug(f"Public sound '{sound_name}' not found")
    return path

# --- MODIFIED: Autocomplete Helper handles potential errors better ---
async def _generic_sound_autocomplete(ctx: discord.AutocompleteContext, source_func, *args) -> List[discord.OptionChoice]:
    """Generic autocomplete handler returning OptionChoices."""
    try:
        sounds = source_func(*args)
        current_value = ctx.value.lower() if ctx.value else ""
        # Create OptionChoice objects for better display/handling if needed later
        suggestions = sorted(
            [discord.OptionChoice(name=name, value=name)
             for name in sounds if current_value in name.lower()],
            key=lambda choice: choice.name # Sort by name
        )
        return suggestions[:25]
    except Exception as e:
         bot_logger.error(f"Error during autocomplete ({source_func.__name__} for user {ctx.interaction.user.id}): {e}", exc_info=True)
         return [] # Return empty list on error

async def user_sound_autocomplete(ctx: discord.AutocompleteContext) -> List[discord.OptionChoice]:
    return await _generic_sound_autocomplete(ctx, get_user_sound_files, ctx.interaction.user.id)

async def public_sound_autocomplete(ctx: discord.AutocompleteContext) -> List[discord.OptionChoice]:
    return await _generic_sound_autocomplete(ctx, get_public_sound_files)


# --- NEW: File Upload Validation Helper [UNCHANGED] ---
async def _validate_and_save_upload(
    ctx: discord.ApplicationContext, # For logging user ID
    sound_file: discord.Attachment,
    target_save_path: str,
    command_name: str = "upload"
) -> Tuple[bool, Optional[str]]:
    """
    Validates attachment (type, size), saves temporarily, checks with Pydub,
    moves to final path, and cleans up. Returns (success_bool, error_message_or_None).
    Sends NO feedback itself.
    """
    user_id = ctx.author.id
    log_prefix = f"{command_name.upper()} VALIDATION"

    # Basic Validation
    file_extension = os.path.splitext(sound_file.filename)[1].lower()
    if file_extension not in ALLOWED_EXTENSIONS:
        return False, f"❌ Invalid file type (`{file_extension}`). Allowed: {', '.join(ALLOWED_EXTENSIONS)}"
    if sound_file.size > MAX_USER_SOUND_SIZE_MB * 1024 * 1024:
        return False, f"❌ File too large (`{sound_file.size / (1024*1024):.2f}` MB). Max: {MAX_USER_SOUND_SIZE_MB}MB."
    if not sound_file.content_type or not sound_file.content_type.startswith('audio/'):
        bot_logger.warning(f"{log_prefix}: Content-Type '{sound_file.content_type}' for '{sound_file.filename}' not 'audio/*'. Proceeding on ext.")

    # Temporary Save and Pydub Check
    temp_save_filename = f"temp_{command_name}_{user_id}_{os.urandom(4).hex()}{file_extension}"
    # Save temp files in a consistent place, maybe USER_SOUNDS_DIR root
    temp_save_path = os.path.join(USER_SOUNDS_DIR, temp_save_filename) # Or SOUNDS_DIR?

    async def cleanup_temp():
        if os.path.exists(temp_save_path):
            try: os.remove(temp_save_path); bot_logger.debug(f"Cleaned up temp: {temp_save_path}")
            except Exception as del_e: bot_logger.warning(f"Failed cleanup {temp_save_path}: {del_e}")

    try:
        await sound_file.save(temp_save_path)
        bot_logger.info(f"{log_prefix}: Saved temporary file: '{temp_save_path}'")

        try:
            bot_logger.debug(f"{log_prefix}: Attempting Pydub decode: '{temp_save_path}'")
            _ = AudioSegment.from_file(temp_save_path, format=file_extension.strip('.'))
            bot_logger.info(f"{log_prefix}: Pydub validation successful for '{temp_save_path}'")

            # Move to final location
            try:
                os.replace(temp_save_path, target_save_path)
                bot_logger.info(f"{log_prefix}: Final file saved: '{target_save_path}'")
                return True, None # Success
            except OSError as rep_e:
                try:
                    shutil.move(temp_save_path, target_save_path)
                    bot_logger.info(f"{log_prefix}: Final file saved (move fallback): '{target_save_path}'")
                    return True, None # Success
                except Exception as move_e:
                    bot_logger.error(f"{log_prefix}: Failed final save (replace: {rep_e}, move: {move_e})", exc_info=True)
                    await cleanup_temp()
                    return False, "❌ Error saving the sound file after validation."

        except CouldntDecodeError as decode_error:
            bot_logger.error(f"{log_prefix}: FAILED (Pydub Decode Error - user: {user_id}, file: '{sound_file.filename}'): {decode_error}", exc_info=True)
            await cleanup_temp()
            return False, (f"❌ **Audio Validation Failed!** Could not process `{sound_file.filename}`.\n"
                            f"Ensure valid audio ({', '.join(ALLOWED_EXTENSIONS)}) & FFmpeg installed.")
        except Exception as validate_e:
            bot_logger.error(f"{log_prefix}: FAILED (Unexpected Pydub check - user: {user_id}, file: '{sound_file.filename}'): {validate_e}", exc_info=True)
            await cleanup_temp()
            return False, "❌ **Audio Validation Failed!** Unexpected error during processing."

    except discord.HTTPException as e:
        bot_logger.error(f"{log_prefix}: Error downloading temp file from Discord for {user_id}: {e}", exc_info=True)
        await cleanup_temp()
        return False, "❌ Error downloading the sound file from Discord."
    except Exception as e:
        bot_logger.error(f"{log_prefix}: Unexpected error during save/validate for {user_id}: {e}", exc_info=True)
        await cleanup_temp()
        return False, "❌ An unexpected server error occurred during file handling."

# --- Slash Commands ---

# === Join Sound Commands [Modified for new config structure] ===
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
    final_save_filename = f"{user_id_str}{file_extension}"
    final_save_path = os.path.join(SOUNDS_DIR, final_save_filename)

    # --- MODIFIED PART ---
    # Get existing config or create empty dict
    user_config = user_sound_config.get(user_id_str, {})
    old_config_filename = user_config.get("join_sound")
    # --- END MODIFIED PART ---

    # Use validation helper
    success, error_msg = await _validate_and_save_upload(ctx, sound_file, final_save_path, command_name="setjoinsound")

    if success:
        # --- MODIFIED PART ---
        # Remove old sound file if config existed and filename differs
        if old_config_filename and old_config_filename != final_save_filename:
            old_path = os.path.join(SOUNDS_DIR, old_config_filename)
            if os.path.exists(old_path):
                try: os.remove(old_path); bot_logger.info(f"Removed previous join sound file: '{old_path}'")
                except Exception as e: bot_logger.warning(f"Could not remove previous join sound file '{old_path}': {e}")

        # Update config *after* successful save
        user_config["join_sound"] = final_save_filename # Set the join_sound key
        user_sound_config[user_id_str] = user_config # Put the potentially updated dict back
        # --- END MODIFIED PART ---
        save_config()
        bot_logger.info(f"Updated join sound config for {author.name} ({user_id_str}) to '{final_save_filename}'")
        await ctx.followup.send(f"✅ Success! Your join sound set to `{sound_file.filename}`.", ephemeral=True)
    else:
        # Validation helper already logged details
        await ctx.followup.send(error_msg or "❌ An unknown error occurred during validation.", ephemeral=True)


@bot.slash_command(name="removejoinsound", description="Remove your custom join sound, revert to TTS.")
@commands.cooldown(1, 5, commands.BucketType.user)
async def removejoinsound(ctx: discord.ApplicationContext):
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id_str = str(author.id)
    bot_logger.info(f"COMMAND: /removejoinsound by {author.name} ({user_id_str})")

    # --- MODIFIED PART ---
    user_config = user_sound_config.get(user_id_str)
    if user_config and "join_sound" in user_config:
        filename = user_config.pop("join_sound") # Remove key from user's dict
        bot_logger.info(f"Removed join sound config for {author.name}")

        # If the user config dict is now empty, remove the user key entirely
        if not user_config:
            del user_sound_config[user_id_str]
        # Otherwise, the dict (now without join_sound) remains in user_sound_config

        save_config()
    # --- END MODIFIED PART ---

        file_path = os.path.join(SOUNDS_DIR, filename)
        tts_join_file = os.path.join(SOUNDS_DIR, f"tts_join_{user_id_str}.mp3") # Also clean up potential TTS join file

        for path_to_remove in [file_path, tts_join_file]:
            if os.path.exists(path_to_remove):
                try: os.remove(path_to_remove); bot_logger.info(f"Deleted file: '{path_to_remove}'")
                except OSError as e: bot_logger.warning(f"Could not delete file '{path_to_remove}': {e}")
            elif path_to_remove == file_path: # Log only if the main configured file was missing
                 bot_logger.warning(f"Configured join sound '{filename}' not found at '{file_path}' during removal.")

        await ctx.followup.send("🗑️ Custom join sound removed. Default TTS will be used for joins.", ephemeral=True)
    else:
        await ctx.followup.send("🤷 You don't have a custom join sound configured.", ephemeral=True)


# === User Command Sound / Soundboard Commands [UNCHANGED Upload/List/Delete/Play/Panel] ===
@bot.slash_command(name="uploadsound", description=f"Upload a sound (personal/public). Limit: {MAX_USER_SOUNDS_PER_USER}.")
@commands.cooldown(2, 20, commands.BucketType.user)
async def uploadsound(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Short name (letters, numbers, underscore).", required=True), # type: ignore
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

    file_extension = os.path.splitext(sound_file.filename)[1].lower()
    final_save_filename = f"{clean_name}{file_extension}"
    followup_message_prefix = f"ℹ️ Name sanitized to `{clean_name}`.\n" if clean_name != name else ""

    target_dir = PUBLIC_SOUNDS_DIR if make_public else os.path.join(USER_SOUNDS_DIR, str(user_id))
    if not make_public: ensure_dir(target_dir) # Ensure user dir exists only if needed

    # Pre-checks based on scope
    if make_public:
        if find_public_sound_path(clean_name):
            await ctx.followup.send(f"{followup_message_prefix}❌ Public sound `{clean_name}` already exists.", ephemeral=True); return
        is_replacing = False
    else:
        existing_path = find_user_sound_path(user_id, clean_name)
        is_replacing = existing_path is not None
        if not is_replacing and len(get_user_sound_files(user_id)) >= MAX_USER_SOUNDS_PER_USER:
             await ctx.followup.send(f"{followup_message_prefix}❌ Max {MAX_USER_SOUNDS_PER_USER} personal sounds reached.", ephemeral=True); return

    final_save_path = os.path.join(target_dir, final_save_filename)

    # Use validation helper
    success, error_msg = await _validate_and_save_upload(ctx, sound_file, final_save_path, command_name="uploadsound")

    if success:
        # Remove conflicting personal file if replacing with different extension
        if is_replacing and not make_public:
            existing_personal_path = find_user_sound_path(user_id, clean_name) # Find again to be sure
            if existing_personal_path and existing_personal_path != final_save_path:
                 try: os.remove(existing_personal_path); bot_logger.info(f"Removed existing personal sound '{os.path.basename(existing_personal_path)}' due to extension change.")
                 except Exception as e: bot_logger.warning(f"Could not remove conflicting personal sound '{existing_personal_path}': {e}")

        scope = "public" if make_public else "personal"
        action = "updated" if (is_replacing and not make_public) else "uploaded"
        play_cmd = "playpublic" if make_public else "playsound"
        list_cmd = "publicsounds" if make_public else "mysounds"

        msg = f"{followup_message_prefix}✅ Success! Sound `{clean_name}` {action} as {scope}.\n"
        msg += f"Use `/{play_cmd} name:{clean_name}`"
        msg += "." if make_public else f", `/{list_cmd}`, or `/soundpanel`."
        if not make_public: msg += f"\nUse `/publishsound name:{clean_name}` to make public later."

        await ctx.followup.send(msg, ephemeral=True)
    else:
        await ctx.followup.send(f"{followup_message_prefix}{error_msg or '❌ Unknown validation error.'}", ephemeral=True)

@bot.slash_command(name="mysounds", description="Lists your personal uploaded sounds.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def mysounds(ctx: discord.ApplicationContext):
    await ctx.defer(ephemeral=True)
    author = ctx.author
    bot_logger.info(f"COMMAND: /mysounds by {author.name} ({author.id})")
    user_sounds = get_user_sound_files(author.id)

    if not user_sounds:
        await ctx.followup.send("No personal sounds yet. Use `/uploadsound`!", ephemeral=True); return

    sorted_sounds = sorted(user_sounds)
    sound_list_str = "\n".join([f"- `{name}`" for name in sorted_sounds])
    limit = 1900
    if len(sound_list_str) > limit: sound_list_str = sound_list_str[:sound_list_str.rfind('\n', 0, limit)] + "\n... (truncated)"

    embed = discord.Embed(
        title=f"{author.display_name}'s Sounds ({len(sorted_sounds)}/{MAX_USER_SOUNDS_PER_USER})",
        description=f"Use `/playsound`, `/soundpanel`, or `/publishsound`.\n\n{sound_list_str}",
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
    if not sound_path and sanitize_filename(name) != name: # Check sanitized name if exact fails
         sound_path = find_user_sound_path(user_id, sanitize_filename(name))
         if sound_path: sound_base_name = sanitize_filename(name)

    if not sound_path:
        await ctx.followup.send(f"❌ Personal sound `{name}` not found. Use `/mysounds`.", ephemeral=True); return

    # Security check (redundant with find_user_sound_path structure, but belt-and-suspenders)
    user_dir_abs = os.path.abspath(os.path.join(USER_SOUNDS_DIR, str(user_id)))
    if not os.path.abspath(sound_path).startswith(user_dir_abs):
         bot_logger.error(f"CRITICAL SECURITY: /deletesound path traversal attempt. User: {user_id}, Path: '{sound_path}'")
         await ctx.followup.send("❌ Internal error.", ephemeral=True); return

    try:
        deleted_filename = os.path.basename(sound_path)
        os.remove(sound_path)
        bot_logger.info(f"Deleted PERSONAL sound '{deleted_filename}' for user {user_id}.")
        await ctx.followup.send(f"🗑️ Personal sound `{sound_base_name}` deleted.", ephemeral=True)
    except (OSError, Exception) as e:
        bot_logger.error(f"Failed to delete personal sound '{sound_path}' for {user_id}: {e}", exc_info=True)
        await ctx.followup.send(f"❌ Failed to delete `{sound_base_name}` ({type(e).__name__}).", ephemeral=True)

@bot.slash_command(name="playsound", description="Plays one of your PERSONAL sounds.")
@commands.cooldown(1, 4, commands.BucketType.user)
async def playsound(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Name of the personal sound to play.", required=True, autocomplete=user_sound_autocomplete) # type: ignore
):
    await ctx.defer() # Public defer needed as followup is ephemeral in helper
    author = ctx.author
    bot_logger.info(f"COMMAND: /playsound by {author.name} ({author.id}), request: '{name}'")

    sound_path = find_user_sound_path(author.id, name)
    if not sound_path and sanitize_filename(name) != name: # Check sanitized
         sound_path = find_user_sound_path(author.id, sanitize_filename(name))

    if not sound_path:
        await ctx.followup.send(f"❌ Personal sound `{name}` not found. Use `/mysounds`.", ephemeral=True); return

    await play_single_sound(ctx.interaction, sound_path)

# --- Sound Panel View [UNCHANGED] ---
class UserSoundboardView(discord.ui.View):
    def __init__(self, user_id: int, *, timeout: Optional[float] = 300.0):
        super().__init__(timeout=timeout)
        self.user_id = user_id
        self.message: Optional[discord.Message] = None
        self.populate_buttons()

    def populate_buttons(self):
        user_dir = os.path.join(USER_SOUNDS_DIR, str(self.user_id))
        bot_logger.debug(f"Populating panel for {self.user_id} from: {user_dir}")
        if not os.path.isdir(user_dir):
            self.add_item(discord.ui.Button(label="No sounds yet!", style=discord.ButtonStyle.secondary, disabled=True))
            return

        sounds_found, button_row = 0, 0
        max_buttons_per_row, max_rows = 5, 5
        max_buttons_total = max_buttons_per_row * max_rows

        try: files_in_dir = sorted(os.listdir(user_dir))
        except OSError as e:
            bot_logger.error(f"Error listing user dir '{user_dir}': {e}")
            self.add_item(discord.ui.Button(label="Error Reading Sounds", style=discord.ButtonStyle.danger, disabled=True))
            return

        for filename in files_in_dir:
            if sounds_found >= max_buttons_total:
                bot_logger.warning(f"Reached button limit ({max_buttons_total}) for user {self.user_id}.")
                if button_row < max_rows: self.add_item(discord.ui.Button(label="...", style=discord.ButtonStyle.secondary, disabled=True, row=button_row))
                break

            filepath = os.path.join(user_dir, filename)
            if os.path.isfile(filepath):
                base_name, ext = os.path.splitext(filename)
                if ext.lower() in ALLOWED_EXTENSIONS:
                    label = base_name.replace("_", " ")[:80]
                    custom_id = f"usersb_play:{filename}" # User-specific prefix

                    if len(custom_id) > 100:
                        bot_logger.warning(f"Skipping user sound '{filename}' ({self.user_id}): custom_id too long.")
                        continue

                    button = discord.ui.Button(label=label, style=discord.ButtonStyle.secondary, custom_id=custom_id, row=button_row)
                    button.callback = self.user_soundboard_button_callback
                    self.add_item(button)
                    sounds_found += 1
                    if sounds_found % max_buttons_per_row == 0: button_row += 1
                # else: bot_logger.debug(f"Skipping non-audio in user dir {self.user_id}: '{filename}'") # Too verbose maybe

        if sounds_found == 0:
             bot_logger.info(f"No valid sound files found for {self.user_id} in '{user_dir}'.")
             self.add_item(discord.ui.Button(label="No sounds yet!", style=discord.ButtonStyle.secondary, disabled=True))

    async def user_soundboard_button_callback(self, interaction: discord.Interaction):
        custom_id = interaction.data["custom_id"]
        user = interaction.user
        bot_logger.info(f"USER PANEL: Button '{custom_id}' by {user.name} ({user.id}) on panel for {self.user_id}")
        await interaction.response.defer(ephemeral=True) # Defer privately first

        if not custom_id.startswith("usersb_play:"):
            bot_logger.error(f"Invalid custom_id from user panel: '{custom_id}'")
            await interaction.followup.send("❌ Internal error: Invalid button.", ephemeral=True); return

        sound_filename = custom_id.split(":", 1)[1]
        sound_path = os.path.join(USER_SOUNDS_DIR, str(self.user_id), sound_filename)
        # Use the generic play function, interaction object handles feedback
        await play_single_sound(interaction, sound_path)

    async def on_timeout(self):
        if self.message:
            bot_logger.debug(f"User panel view timed out for {self.user_id} (msg {self.message.id})")
            owner_name = f"User {self.user_id}" # Default
            try: # Try to get a better name
                 if self.message.guild: panel_owner = await self.message.guild.fetch_member(self.user_id)
                 else: panel_owner = await bot.fetch_user(self.user_id)
                 if panel_owner: owner_name = panel_owner.display_name
            except (discord.NotFound, discord.Forbidden, AttributeError): pass

            for item in self.children: item.disabled = True
            try: await self.message.edit(content=f"🔊 **{owner_name}'s Personal Panel (Expired)**", view=self)
            except discord.HTTPException: pass # Ignore errors editing old message
        else: bot_logger.debug(f"User panel view timed out for {self.user_id} (no message ref).")

@bot.slash_command(name="soundpanel", description="Displays buttons to play YOUR personal sounds.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def soundpanel(ctx: discord.ApplicationContext):
    await ctx.defer() # Defer publicly, view content is user-specific
    author = ctx.author
    bot_logger.info(f"COMMAND: /soundpanel invoked by {author.name} ({author.id})")

    view = UserSoundboardView(user_id=author.id, timeout=600.0) # 10 min

    # Check if any buttons were actually added and are playable
    has_playable_buttons = any(isinstance(item, discord.ui.Button) and not item.disabled and item.custom_id and item.custom_id.startswith("usersb_play:") for item in view.children)

    if not has_playable_buttons:
         await ctx.followup.send("No personal sounds found! Use `/uploadsound`.", ephemeral=True); return

    msg_content = f"🔊 **{author.display_name}'s Personal Sound Panel** - Click to play!"
    try:
        message = await ctx.followup.send(msg_content, view=view)
        view.message = message # Store reference for timeout edit
    except Exception as e:
        bot_logger.error(f"Failed to send soundpanel for {author.id}: {e}", exc_info=True)
        try: await ctx.followup.send("❌ Failed to create sound panel.", ephemeral=True)
        except: pass # Ignore if followup fails


# === Public Sound Commands [UNCHANGED] ===
@bot.slash_command(name="publishsound", description="Make one of your personal sounds public.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def publishsound(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Name of YOUR sound to make public.", required=True, autocomplete=user_sound_autocomplete) # type: ignore
):
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id = author.id
    bot_logger.info(f"COMMAND: /publishsound by {author.name} ({user_id}), target: '{name}'")

    user_sound_path = find_user_sound_path(user_id, name)
    sound_base_name = name
    if not user_sound_path and sanitize_filename(name) != name: # Check sanitized
         user_sound_path = find_user_sound_path(user_id, sanitize_filename(name))
         if user_sound_path: sound_base_name = sanitize_filename(name)

    if not user_sound_path:
        await ctx.followup.send(f"❌ Personal sound `{name}` not found.", ephemeral=True); return

    source_filename = os.path.basename(user_sound_path)
    public_path = os.path.join(PUBLIC_SOUNDS_DIR, source_filename)
    target_base_name, _ = os.path.splitext(source_filename)

    if find_public_sound_path(target_base_name): # Check if public name exists
        await ctx.followup.send(f"❌ Public sound `{target_base_name}` already exists.", ephemeral=True); return

    try:
        shutil.copy2(user_sound_path, public_path) # copy2 preserves metadata
        bot_logger.info(f"SOUND PUBLISHED: Copied '{user_sound_path}' to '{public_path}' by {author.name}.")
        await ctx.followup.send(f"✅ Sound `{sound_base_name}` is now public!\nUse `/playpublic name:{target_base_name}`.", ephemeral=True)
    except (OSError, Exception) as e:
        bot_logger.error(f"Failed copy '{user_sound_path}' to public: {e}", exc_info=True)
        await ctx.followup.send(f"❌ Failed to publish `{sound_base_name}` ({type(e).__name__}).", ephemeral=True)

@bot.slash_command(name="removepublic", description="[Admin] Remove a sound from the public collection.")
@commands.has_permissions(manage_guild=True)
@commands.cooldown(1, 5, commands.BucketType.guild)
async def removepublic(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Name of the public sound to remove.", required=True, autocomplete=public_sound_autocomplete) # type: ignore
):
    await ctx.defer(ephemeral=True)
    admin = ctx.author
    bot_logger.info(f"COMMAND: /removepublic by admin {admin.name} ({admin.id}), target: '{name}'")

    public_path = find_public_sound_path(name)
    sound_base_name = name
    if not public_path and sanitize_filename(name) != name: # Check sanitized
        public_path = find_public_sound_path(sanitize_filename(name))
        if public_path: sound_base_name = sanitize_filename(name)

    if not public_path:
        await ctx.followup.send(f"❌ Public sound `{name}` not found.", ephemeral=True); return

    try:
        deleted_filename = os.path.basename(public_path)
        os.remove(public_path)
        bot_logger.info(f"ADMIN ACTION: Deleted public sound '{deleted_filename}' by {admin.name}.")
        await ctx.followup.send(f"🗑️ Public sound `{sound_base_name}` deleted.", ephemeral=True)
    except (OSError, Exception) as e:
        bot_logger.error(f"Failed delete public '{public_path}': {e}", exc_info=True)
        await ctx.followup.send(f"❌ Failed to delete `{sound_base_name}` ({type(e).__name__}).", ephemeral=True)

@bot.slash_command(name="publicsounds", description="Lists all available public sounds.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def publicsounds(ctx: discord.ApplicationContext):
    await ctx.defer(ephemeral=True)
    bot_logger.info(f"COMMAND: /publicsounds by {ctx.author.name} ({ctx.author.id})")
    public_sounds = get_public_sound_files()

    if not public_sounds:
        await ctx.followup.send("No public sounds yet. Use `/uploadsound` (public) or `/publishsound`!", ephemeral=True); return

    sorted_sounds = sorted(public_sounds)
    sound_list_str = "\n".join([f"- `{name}`" for name in sorted_sounds])
    limit = 1900
    if len(sound_list_str) > limit: sound_list_str = sound_list_str[:sound_list_str.rfind('\n', 0, limit)] + "\n... (truncated)"

    embed = discord.Embed(
        title=f"📢 Public Sounds ({len(sorted_sounds)})",
        description=f"Use `/playpublic name:<sound_name>`.\n\n{sound_list_str}",
        color=discord.Color.green()
    ).set_footer(text="Admins use /removepublic.")
    await ctx.followup.send(embed=embed, ephemeral=True)

@bot.slash_command(name="playpublic", description="Plays a public sound in your voice channel.")
@commands.cooldown(1, 4, commands.BucketType.user)
async def playpublic(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Name of the public sound to play.", required=True, autocomplete=public_sound_autocomplete) # type: ignore
):
    await ctx.defer() # Public defer
    author = ctx.author
    bot_logger.info(f"COMMAND: /playpublic by {author.name} ({author.id}), request: '{name}'")

    public_path = find_public_sound_path(name)
    if not public_path and sanitize_filename(name) != name: # Check sanitized
        public_path = find_public_sound_path(sanitize_filename(name))

    if not public_path:
        await ctx.followup.send(f"❌ Public sound `{name}` not found. Use `/publicsounds`.", ephemeral=True); return

    await play_single_sound(ctx.interaction, public_path)


# === NEW: TTS Defaults Commands ===
@bot.slash_command(name="setttsdefaults", description="Set your preferred default TTS language and speed.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def setttsdefaults(
    ctx: discord.ApplicationContext,
    language: discord.Option(str, description="Your preferred default language/accent.", required=True, choices=TTS_LANGUAGE_CHOICES), # type: ignore
    slow: discord.Option(bool, description="Speak slowly by default?", required=True) # type: ignore
):
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id_str = str(author.id)
    bot_logger.info(f"COMMAND: /setttsdefaults by {author.name} ({user_id_str}), lang: {language}, slow: {slow}")

    # Get user's config dict, creating it if it doesn't exist
    user_config = user_sound_config.setdefault(user_id_str, {})

    # Set or update the tts_defaults key
    user_config['tts_defaults'] = {'language': language, 'slow': slow}

    save_config()
    lang_name = next((choice.name for choice in TTS_LANGUAGE_CHOICES if choice.value == language), language) # Get friendly name
    await ctx.followup.send(
        f"✅ Your TTS defaults are now:\n"
        f"- Language: **{lang_name}** (`{language}`)\n"
        f"- Slow: **{slow}**\n"
        f"These will be used for `/tts` unless you specify options.",
        ephemeral=True
    )

@bot.slash_command(name="removettsdefaults", description="Remove your custom TTS language/speed defaults.")
@commands.cooldown(1, 5, commands.BucketType.user)
async def removettsdefaults(ctx: discord.ApplicationContext):
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id_str = str(author.id)
    bot_logger.info(f"COMMAND: /removettsdefaults by {author.name} ({user_id_str})")

    user_config = user_sound_config.get(user_id_str)

    if user_config and 'tts_defaults' in user_config:
        del user_config['tts_defaults']
        bot_logger.info(f"Removed TTS defaults for {author.name}")

        # If the user config dict is now empty (no join sound either), remove user key
        if not user_config:
            del user_sound_config[user_id_str]
            bot_logger.info(f"Removed empty user config entry for {author.name}")

        save_config()
        await ctx.followup.send(
            f"🗑️ Your custom TTS defaults have been removed.\n"
            f"The bot will now use default language (`{DEFAULT_TTS_LANGUAGE}`) and speed (`{DEFAULT_TTS_SLOW}`) unless you specify options in `/tts`.",
            ephemeral=True
        )
    else:
        await ctx.followup.send("🤷 You don't have any custom TTS defaults set.", ephemeral=True)


# === TTS Command [MODIFIED for Defaults] ===
@bot.slash_command(name="tts", description="Make the bot say something using Text-to-Speech.")
@commands.cooldown(1, 6, commands.BucketType.user)
async def tts(
    ctx: discord.ApplicationContext,
    message: discord.Option(str, description=f"Text to speak (max {MAX_TTS_LENGTH} chars).", required=True), # type: ignore
    language: discord.Option(str, description="Override language (uses your default otherwise).", required=False, choices=TTS_LANGUAGE_CHOICES), # type: ignore
    slow: discord.Option(bool, description="Override slow speech (uses your default otherwise).", required=False) # type: ignore
):
    await ctx.defer(ephemeral=True)
    user = ctx.author
    guild = ctx.guild
    user_id_str = str(user.id)
    # Log requested overrides (will be None if not provided)
    bot_logger.info(f"COMMAND: /tts by {user.name} ({user_id_str}), explicit lang: {language}, explicit slow: {slow}")

    # Initial Validations
    if not guild or not user.voice or not user.voice.channel:
        await ctx.followup.send("Must be in a server voice channel.", ephemeral=True); return
    if len(message) > MAX_TTS_LENGTH:
         await ctx.followup.send(f"❌ Message > {MAX_TTS_LENGTH} chars.", ephemeral=True); return
    if not message.strip():
         await ctx.followup.send("❌ Provide text to speak.", ephemeral=True); return

    target_channel = user.voice.channel

    # --- Determine final TTS settings ---
    user_config = user_sound_config.get(user_id_str, {})
    saved_defaults = user_config.get("tts_defaults", {})

    # Use explicit option if provided, otherwise saved default, otherwise bot default
    final_language = language if language is not None else saved_defaults.get('language', DEFAULT_TTS_LANGUAGE)
    final_slow = slow if slow is not None else saved_defaults.get('slow', DEFAULT_TTS_SLOW)

    source_info = "explicit" if language is not None else ("saved" if 'language' in saved_defaults else "default")
    source_info_slow = "explicit" if slow is not None else ("saved" if 'slow' in saved_defaults else "default")
    bot_logger.info(f"TTS Final Settings for {user.name}: lang={final_language} ({source_info}), slow={final_slow} ({source_info_slow})")
    # --- End settings determination ---

    # Generate TTS In Memory
    audio_source: Optional[discord.PCMAudio] = None
    pcm_fp = io.BytesIO() # Only need PCM BytesIO now
    try:
        bot_logger.info(f"TTS: Generating for '{user.name}' (lang={final_language}, slow={final_slow}): '{message[:50]}...'")
        tts_instance = gTTS(text=message, lang=final_language, slow=final_slow)

        # Use run_in_executor for both gTTS and Pydub processing
        loop = asyncio.get_running_loop()
        def process_tts_sync():
            mp3_fp = io.BytesIO()
            tts_instance.write_to_fp(mp3_fp)
            mp3_fp.seek(0)
            if mp3_fp.getbuffer().nbytes == 0: raise ValueError("gTTS yielded empty data.")
            bot_logger.debug(f"TTS: Generated MP3 in memory ({mp3_fp.getbuffer().nbytes} bytes)")
            audio_segment = AudioSegment.from_file(mp3_fp, format="mp3").set_frame_rate(48000).set_channels(2)
            audio_segment.export(pcm_fp, format="s16le")
            pcm_fp.seek(0)
            if pcm_fp.getbuffer().nbytes == 0: raise ValueError("Pydub export yielded empty PCM.")
            bot_logger.debug(f"TTS: Converted to PCM in memory ({pcm_fp.getbuffer().nbytes} bytes)")

        await loop.run_in_executor(None, process_tts_sync)
        audio_source = discord.PCMAudio(pcm_fp)
        bot_logger.info(f"TTS: Successfully created PCMAudio source for {user.name}.")

    except gTTSError as e:
        msg = f"❌ TTS Error: Language '{final_language}' unsupported?" if "Language not found" in str(e) else f"❌ TTS Error: {e}"
        await ctx.followup.send(msg, ephemeral=True)
        bot_logger.error(f"TTS Generation Error for {user.name} (lang={final_language}): {e}", exc_info=True)
        return
    except (ImportError, ValueError, Exception) as e: # Catch Pydub/other errors
        err_type = type(e).__name__
        await ctx.followup.send(f"❌ Error during TTS processing ({err_type}).", ephemeral=True)
        bot_logger.error(f"TTS: Failed generation/processing for {user.name}: {e}", exc_info=True)
        return
    # No finally needed for pcm_fp, PCMAudio takes ownership

    if not audio_source:
        await ctx.followup.send("❌ Failed to prepare TTS audio.", ephemeral=True)
        bot_logger.error("TTS: Failed to create audio source after processing.")
        return

    # Use the helper for connection/busy checks
    voice_client = await _ensure_voice_client_ready(ctx.interaction, target_channel, action_type="TTS")
    if not voice_client:
        # If helper failed, need to clean up PCM buffer manually as PCMAudio object might not have been fully used
        pcm_fp.close()
        return # Helper already sent feedback

    # Play TTS Audio
    if voice_client.is_playing(): # Final check
         bot_logger.warning(f"TTS: VC became busy between check and play for {user.name}. Aborting.")
         await ctx.followup.send("⏳ Bot became busy. Try again.", ephemeral=True)
         after_play_handler(None, voice_client)
         pcm_fp.close() # Clean up buffer if play fails
         return

    try:
        bot_logger.info(f"TTS PLAYBACK: Playing TTS requested by {user.display_name}...")
        # Pass pcm_fp to close automatically after playback using a lambda in after
        voice_client.play(audio_source, after=lambda e: (after_play_handler(e, voice_client), pcm_fp.close()))

        # Feedback message showing the *actual* settings used
        speed_str = "(slow)" if final_slow else ""
        lang_name = next((choice.name for choice in TTS_LANGUAGE_CHOICES if choice.value == final_language), final_language)
        await ctx.followup.send(f"🗣️ Saying in **{lang_name}** {speed_str}: \"{message[:100]}{'...' if len(message) > 100 else ''}\"", ephemeral=True)
    except (discord.errors.ClientException, Exception) as e:
        msg = "❌ Error: Already playing or client issue." if isinstance(e, discord.errors.ClientException) else "❌ Unexpected playback error."
        await ctx.followup.send(msg, ephemeral=True)
        bot_logger.error(f"TTS PLAYBACK ERROR ({type(e).__name__}): {e}", exc_info=True)
        after_play_handler(e, voice_client)
        pcm_fp.close() # Clean up buffer on error


# --- Error Handler for Application Commands [UNCHANGED] ---
@bot.event
async def on_application_command_error(ctx: discord.ApplicationContext, error: discord.DiscordException):
    """Handles errors raised during slash command execution."""
    cmd_name = ctx.command.qualified_name if ctx.command else "Unknown"
    user_name = ctx.author.name if ctx.author else "Unknown"
    log_prefix = f"CMD ERROR (/{cmd_name}, user: {user_name}):"

    async def send_error(msg: str, log_level=logging.WARNING):
        bot_logger.log(log_level, f"{log_prefix} {msg} (Error: {error})")
        try:
            # Check if response already sent before trying to send another
            is_done = ctx.interaction.response.is_done()
            if not is_done: await ctx.respond(msg, ephemeral=True)
            else: await ctx.followup.send(msg, ephemeral=True)
        except discord.NotFound: pass # Interaction expired
        except discord.Forbidden: bot_logger.error(f"{log_prefix} Cannot send error response (Forbidden).")
        except Exception as e_resp: bot_logger.error(f"{log_prefix} Failed sending error response: {e_resp}")

    if isinstance(error, commands.CommandOnCooldown):
        await send_error(f"⏳ Cooldown. Try again in {error.retry_after:.1f}s.")
    elif isinstance(error, commands.MissingPermissions):
        perms = ', '.join(f'`{p}`' for p in error.missing_permissions)
        await send_error(f"🚫 You need permissions: {perms}", log_level=logging.WARNING)
    elif isinstance(error, commands.BotMissingPermissions):
        perms = ', '.join(f'`{p}`' for p in error.missing_permissions)
        await send_error(f"🚫 I need permissions: {perms}", log_level=logging.ERROR)
    elif isinstance(error, commands.CheckFailure):
        await send_error("🚫 You can't use this command.")
    elif isinstance(error, discord.errors.ApplicationCommandInvokeError):
        original = error.original
        bot_logger.error(f"{log_prefix} Invoke Error: {original}", exc_info=original)
        # Specific user-facing messages for common underlying errors
        if isinstance(original, FileNotFoundError): msg = "❌ Error: A required file was not found."
        elif isinstance(original, CouldntDecodeError): msg = "❌ Error: Could not process an audio file."
        elif isinstance(original, gTTSError): msg = f"❌ Error generating TTS: {original}"
        else: msg = "❌ An internal error occurred."
        await send_error(msg, log_level=logging.ERROR)
    else:
        # Generic handler for other discord.DiscordException errors
        await send_error(f"❌ An unexpected error occurred ({type(error).__name__}).", log_level=logging.ERROR)


# --- Run the Bot [UNCHANGED] ---
if __name__ == "__main__":
    if not PYDUB_AVAILABLE or not BOT_TOKEN:
        bot_logger.critical("Pydub unavailable or BOT_TOKEN missing. Cannot start.")
        exit(1)

    opus_loaded = discord.opus.is_loaded()
    if not opus_loaded:
        bot_logger.warning("Opus not loaded by default. Attempting explicit load...")
        # Common paths; adjust if necessary for your environment
        opus_paths = ["libopus.so.0", "opus", "libopus-0.dll", "/opt/homebrew/opt/opus/lib/libopus.0.dylib"]
        for path in opus_paths:
            try:
                discord.opus.load_opus(path)
                if discord.opus.is_loaded():
                    bot_logger.info(f"Opus loaded successfully via: {path}")
                    opus_loaded = True; break
            except Exception: pass # Ignore load errors for specific paths
        if not opus_loaded:
            bot_logger.critical("Opus library STILL not loaded. Voice WILL FAIL. Install libopus.")
            # Decide whether to exit or proceed with broken voice (current choice: proceed with warning)

    try:
        bot_logger.info("Starting bot...")
        bot.run(BOT_TOKEN)
    except (discord.errors.LoginFailure, discord.errors.PrivilegedIntentsRequired) as e:
        bot_logger.critical(f"CRITICAL STARTUP ERROR: {type(e).__name__}: {e}")
    except Exception as e:
        is_opus_related = "opus" in str(e).lower() and isinstance(e, discord.errors.DiscordException) and not opus_loaded
        level = logging.CRITICAL if is_opus_related else logging.ERROR
        bot_logger.log(level, f"FATAL RUNTIME ERROR: {e}", exc_info=True)