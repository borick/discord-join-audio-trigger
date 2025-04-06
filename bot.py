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
MAX_PLAYBACK_DURATION_MS = 10 * 1000 # <<<--- NEW: Max duration in milliseconds (10 seconds)

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
    bot_logger.info(f"Playback limited to first {MAX_PLAYBACK_DURATION_MS / 1000} seconds.") # <<<--- NEW Log
    bot_logger.info(f"Dirs: {os.path.abspath(SOUNDS_DIR)}, {os.path.abspath(USER_SOUNDS_DIR)}, {os.path.abspath(PUBLIC_SOUNDS_DIR)}")
    bot_logger.info("Sound Bot is operational.")

# --- Audio Processing Helper [MODIFIED] ---
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

        # --- NEW: Trim audio to MAX_PLAYBACK_DURATION_MS ---
        if len(audio_segment) > MAX_PLAYBACK_DURATION_MS:
            bot_logger.info(f"AUDIO: Trimming '{basename}' from {len(audio_segment)}ms to first {MAX_PLAYBACK_DURATION_MS}ms.")
            audio_segment = audio_segment[:MAX_PLAYBACK_DURATION_MS]
        else:
            bot_logger.debug(f"AUDIO: '{basename}' is {len(audio_segment)}ms (<= {MAX_PLAYBACK_DURATION_MS}ms), no trimming needed.")
        # --- END NEW ---

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
        # --- End Normalization ---

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

    # Process audio (will now include trimming)
    audio_source = process_audio(sound_path, member.display_name)

    if audio_source:
        try:
            bot_logger.info(f"QUEUE PLAYBACK [{task_id}]: Playing for {member.display_name}...")
            vc.play(audio_source, after=lambda e: after_play_handler(e, vc))
            bot_logger.debug(f"QUEUE PLAYBACK [{task_id}]: vc.play() called for {member.display_name}.")
        except (discord.errors.ClientException, Exception) as e:
            bot_logger.error(f"QUEUE PLAYBACK ERROR [{task_id}]: {type(e).__name__}: {e}", exc_info=True)
            # Schedule next check even if play fails
            bot.loop.create_task(play_next_in_queue(guild), name=f"QueueRetry_{guild_id}")
    else:
        bot_logger.warning(f"QUEUE PLAYBACK [{task_id}]: No valid source for {member.display_name} ({os.path.basename(sound_path)}). Skipping.")
        # Schedule next check if processing failed
        bot.loop.create_task(play_next_in_queue(guild), name=f"QueueSkip_{guild_id}")

# --- on_voice_state_update [UNCHANGED logic, relies on modified process_audio] ---
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
        # Optionally notify someone, but likely spammy.
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
            # Configured sound file is missing! Clean up config and fallback to TTS.
            bot_logger.warning(f"SOUND: Configured join sound '{filename}' not found. Removing broken entry for {member.display_name}, using TTS.")
            del user_config["join_sound"]
            if not user_config: # Remove user entry if it's now empty
                del user_sound_config[user_id_str]
            save_config()
            is_tts = True
    else:
        is_tts = True # No custom sound configured, use TTS
        bot_logger.info(f"SOUND: No custom join sound for {member.display_name}. Using TTS.")

    # Generate TTS if needed
    if is_tts:
        # Use a consistent filename pattern for join TTS
        tts_path = os.path.join(SOUNDS_DIR, f"tts_join_{member.id}.mp3")
        bot_logger.info(f"TTS: Generating join TTS for {member.display_name} ('{os.path.basename(tts_path)}')...")
        try:
            # Determine language: user pref -> bot default
            tts_defaults = user_config.get("tts_defaults", {}) if user_config else {}
            tts_lang = tts_defaults.get("language", DEFAULT_TTS_LANGUAGE)
            # Join sounds are never slow (gTTS default is False)
            bot_logger.debug(f"TTS Join using lang: {tts_lang}")

            # Perform TTS generation in a separate thread to avoid blocking
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: gTTS(text=f"{member.display_name} joined", lang=tts_lang, slow=False).save(tts_path))
            bot_logger.info(f"TTS: Saved join TTS file '{os.path.basename(tts_path)}'")
            sound_path = tts_path # Use the generated file
        except gTTSError as e:
            bot_logger.error(f"TTS: Failed join TTS generation for {member.display_name} (lang={tts_lang}): {e}", exc_info=True)
            sound_path = None # Fallback to silence if TTS fails critically
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

    # Add the member and sound path to this guild's queue
    guild_sound_queues[guild_id].append((member, sound_path))
    bot_logger.info(f"QUEUE: Added join sound for {member.display_name}. Queue size: {len(guild_sound_queues[guild_id])}")

    # Get the current voice client for this guild, if any
    vc = discord.utils.get(bot.voice_clients, guild=guild)

    # If bot is already playing, the 'after' callback will handle the queue.
    # We just need to ensure a task *exists* if it died unexpectedly.
    if vc and vc.is_playing():
        bot_logger.info(f"VOICE: Bot playing in {guild.name}. Join sound queued. Playback deferred.")
        # Ensure a play task is running or scheduled if the current one finishes/errors
        if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
             task_name = f"QueueTriggerDeferred_{guild_id}"
             # Ensure task only starts if queue isn't empty *after* potential concurrent processing
             if guild_sound_queues.get(guild_id):
                 guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(guild), name=task_name)
                 bot_logger.debug(f"VOICE: Created deferred play task '{task_name}'.")
             else:
                 bot_logger.debug(f"VOICE: Deferred task '{task_name}' skipped, queue emptied concurrently.")
        return # Don't try to connect/move if already playing

    # If bot is NOT playing, try to connect/move and start the queue task
    should_start_play_task = False
    try:
        if not vc or not vc.is_connected():
            # Bot is not connected, connect to the user's channel
            bot_logger.info(f"VOICE: Connecting to '{channel_to_join.name}' to start queue.")
            vc = await channel_to_join.connect(timeout=30.0, reconnect=True) # Set timeout
            bot_logger.info(f"VOICE: Connected to '{channel_to_join.name}'.")
            should_start_play_task = True # Start playing immediately after connect
        elif vc.channel != channel_to_join:
            # Bot is connected elsewhere, move to the user's channel
             bot_logger.info(f"VOICE: Moving from '{vc.channel.name}' to '{channel_to_join.name}' to start queue.")
             await vc.move_to(channel_to_join)
             bot_logger.info(f"VOICE: Moved to '{channel_to_join.name}'.")
             should_start_play_task = True # Start playing immediately after move
        else:
             # Bot is already in the correct channel and idle
             bot_logger.debug(f"VOICE: Bot already in '{channel_to_join.name}' and idle. Starting queue.")
             should_start_play_task = True # Start playing

    except asyncio.TimeoutError:
        bot_logger.error(f"VOICE: Timeout connecting/moving to '{channel_to_join.name}'. Clearing queue.")
        if guild_id in guild_sound_queues: guild_sound_queues[guild_id].clear() # Clear queue on failure
        vc = None # Ensure vc is None if connect failed
    except discord.errors.ClientException as e:
        # Handle cases like "already connecting" or permission issues during connect/move
        bot_logger.warning(f"VOICE: ClientException during connect/move to '{channel_to_join.name}': {e}")
        # Don't clear queue here, might be a temporary state
        vc = discord.utils.get(bot.voice_clients, guild=guild) # Re-fetch VC state
    except Exception as e:
        bot_logger.error(f"VOICE: Unexpected error connecting/moving to '{channel_to_join.name}': {e}", exc_info=True)
        if guild_id in guild_sound_queues: guild_sound_queues[guild_id].clear()
        vc = None

    # Start the playback task if connection/move was successful (or already there) and needed
    if should_start_play_task and vc and vc.is_connected():
        # Check if a task is already running for this guild (e.g., from a rapid join/leave)
        if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
            task_name = f"QueueStart_{guild_id}"
            # Final check: ensure queue still has items before starting task
            if guild_sound_queues.get(guild_id):
                guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(guild), name=task_name)
                bot_logger.info(f"VOICE: Started play task '{task_name}' for guild {guild_id}.")
            else:
                bot_logger.debug(f"VOICE: Start task '{task_name}' skipped, queue emptied concurrently.")
        else:
             bot_logger.debug(f"VOICE: Play task for {guild_id} already running/scheduled.")
    elif not vc or not vc.is_connected():
         bot_logger.warning(f"VOICE: Bot could not connect/move to {channel_to_join.name}, cannot start playback task.")


# --- after_play_handler [UNCHANGED] ---
def after_play_handler(error: Optional[Exception], vc: discord.VoiceClient):
    # This function runs after a sound finishes playing or errors out.
    # It's crucial for processing the next sound in the queue or disconnecting.
    guild_id = vc.guild.id if vc and vc.guild else None
    if error:
        bot_logger.error(f'PLAYBACK ERROR (In after_play_handler for guild {guild_id}): {error}', exc_info=error)

    # Ensure VC is still valid and connected before proceeding
    if not guild_id or not vc.is_connected():
        bot_logger.warning(f"after_play_handler called with invalid/disconnected vc (Guild ID: {guild_id}). Cleaning up potential task.")
        # Clean up task tracker if VC is gone
        if guild_id and guild_id in guild_play_tasks:
             play_task = guild_play_tasks.pop(guild_id, None)
             if play_task and not play_task.done():
                 play_task.cancel()
                 bot_logger.debug(f"Cancelled lingering play task for disconnected guild {guild_id}.")
        return

    bot_logger.debug(f"Playback finished/errored for {guild_id}. Triggering next queue check.")

    # Check if there are more sounds in the *join* queue for this guild
    is_join_queue_empty = guild_id not in guild_sound_queues or not guild_sound_queues[guild_id]

    if not is_join_queue_empty:
        bot_logger.debug(f"AFTER_PLAY: Join queue for {guild_id} not empty. Ensuring task runs to process next.")
        # Ensure a task exists and is running/scheduled. If the previous task errored,
        # it might be 'done', so we need to potentially create a new one.
        if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
             task_name = f"QueueCheckAfterPlay_{guild_id}"
             guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(vc.guild), name=task_name)
             bot_logger.debug(f"AFTER_PLAY: Scheduled task '{task_name}' for {guild_id} as existing task was done/missing.")
        else:
             # Task already exists and is presumably waiting or running play_next_in_queue
             bot_logger.debug(f"AFTER_PLAY: Existing play task found for {guild_id}, letting it continue.")
             # No action needed here, the existing task loop will call play_next_in_queue again.
    else:
         # Join queue is empty. Check if we should disconnect.
         bot_logger.debug(f"AFTER_PLAY: Join queue for {guild_id} is empty. Scheduling safe disconnect check.")
         # Schedule the disconnect check instead of calling directly to avoid potential recursion/blocking issues
         bot.loop.create_task(safe_disconnect(vc), name=f"SafeDisconnectAfterPlay_{guild_id}")

# --- safe_disconnect [UNCHANGED] ---
async def safe_disconnect(vc: Optional[discord.VoiceClient]):
    # Disconnects the bot ONLY if the join queue is empty and it's not currently playing anything else.
    if not vc or not vc.is_connected():
        # bot_logger.debug("Safe disconnect called but VC is already disconnected.")
        return

    guild = vc.guild
    guild_id = guild.id
    # Check join queue status again right before potentially disconnecting
    is_join_queue_empty = guild_id not in guild_sound_queues or not guild_sound_queues[guild_id]
    # Check if playing (could be a non-join sound like TTS or /playsound)
    is_playing = vc.is_playing()

    if is_join_queue_empty and not is_playing:
        bot_logger.info(f"DISCONNECT: Conditions met for {guild.name} (Queue Empty: {is_join_queue_empty}, Playing: {is_playing}). Disconnecting...")
        try:
            # Explicitly stop just in case, though is_playing should be False
            if vc.is_playing():
                bot_logger.warning(f"DISCONNECT: Called stop() during safe_disconnect despite is_playing() initially false for {guild.name}.")
                vc.stop()

            await vc.disconnect(force=False) # Use force=False for graceful disconnect
            bot_logger.info(f"DISCONNECT: Bot disconnected from '{guild.name}'.")

            # Clean up the task tracker for this guild after successful disconnect
            if guild_id in guild_play_tasks:
                 play_task = guild_play_tasks.pop(guild_id, None)
                 if play_task:
                     if not play_task.done():
                         play_task.cancel()
                         bot_logger.debug(f"DISCONNECT: Cancelled associated play task for {guild_id}.")
                     else:
                          bot_logger.debug(f"DISCONNECT: Cleaned up completed play task tracker for {guild_id}.")

        except Exception as e:
            bot_logger.error(f"DISCONNECT ERROR: Failed disconnect from {guild.name}: {e}", exc_info=True)
            # Don't remove task tracker on failed disconnect, state is uncertain
    else:
         bot_logger.debug(f"Disconnect skipped for {guild.name}: Queue empty={is_join_queue_empty}, Playing={is_playing}.")


# --- Voice Client Connection/Busy Check Helper [UNCHANGED] ---
async def _ensure_voice_client_ready(interaction: discord.Interaction, target_channel: discord.VoiceChannel, action_type: str = "Playback") -> Optional[discord.VoiceClient]:
    """Helper to connect/move/check busy status and permissions. Returns VC or None."""
    guild = interaction.guild
    user = interaction.user
    guild_id = guild.id
    log_prefix = f"{action_type.upper()}:"

    if not guild: # Should not happen with slash commands in guilds
        await interaction.followup.send("This command must be used in a server.", ephemeral=True)
        return None

    # 1. Check Permissions
    bot_perms = target_channel.permissions_for(guild.me)
    if not bot_perms.connect or not bot_perms.speak:
        await interaction.followup.send(f"❌ I don't have permission to Connect or Speak in {target_channel.mention}.", ephemeral=True)
        bot_logger.warning(f"{log_prefix} Missing Connect/Speak perms in {target_channel.name} ({guild.name}).")
        return None

    vc = discord.utils.get(bot.voice_clients, guild=guild)
    try:
        # 2. Check if Connected and Busy
        if vc and vc.is_connected():
            if vc.is_playing():
                # Prioritize join queue sounds over commands
                if guild_id in guild_sound_queues and guild_sound_queues[guild_id]:
                    msg = "⏳ Bot is currently playing join sounds. Please wait."
                    log_msg = f"{log_prefix} Bot busy with join queue in {guild.name}, user {user.name}'s request ignored."
                else:
                    # Bot is playing something else (likely another command's sound/TTS)
                    msg = "⏳ Bot is currently playing another sound/TTS. Please wait."
                    log_msg = f"{log_prefix} Bot busy (non-join) in {guild.name}, user {user.name}'s request ignored."
                await interaction.followup.send(msg, ephemeral=True)
                bot_logger.info(log_msg)
                return None # Indicate busy

            # 3. Check if in Correct Channel
            elif vc.channel != target_channel:
                bot_logger.info(f"{log_prefix} Moving from '{vc.channel.name}' to '{target_channel.name}' for {user.name}.")
                await vc.move_to(target_channel)
                bot_logger.info(f"{log_prefix} Moved successfully.")
                # VC is now ready after move
        else:
            # 4. Connect if Not Connected
            bot_logger.info(f"{log_prefix} Connecting to '{target_channel.name}' for {user.name}.")
            vc = await target_channel.connect(timeout=30.0, reconnect=True)
            bot_logger.info(f"{log_prefix} Connected successfully.")
            # VC is now ready after connect

        # 5. Final Verification (Should be redundant but safe)
        if not vc or not vc.is_connected():
             bot_logger.error(f"{log_prefix} Failed to establish voice client for {target_channel.name} after connect/move attempt.")
             await interaction.followup.send("❌ Failed to connect or move to the voice channel.", ephemeral=True)
             return None

        return vc # Success, return the ready VoiceClient

    # --- Error Handling for Connect/Move ---
    except asyncio.TimeoutError:
         await interaction.followup.send("❌ Connection to the voice channel timed out.", ephemeral=True)
         bot_logger.error(f"{log_prefix} Connection/Move Timeout in {guild.name} to {target_channel.name}")
         return None
    except discord.errors.ClientException as e:
        # Handle common client errors like "already connecting/disconnecting"
        msg = "⏳ Bot is busy connecting/disconnecting. Please wait a moment." if "already connect" in str(e).lower() else "❌ Error connecting/moving. Check permissions or try again."
        await interaction.followup.send(msg, ephemeral=True)
        bot_logger.warning(f"{log_prefix} Connection/Move ClientException in {guild.name}: {e}")
        return None
    except Exception as e:
        await interaction.followup.send("❌ An unexpected error occurred while joining the voice channel.", ephemeral=True)
        bot_logger.error(f"{log_prefix} Connection/Move unexpected error in {guild.name}: {e}", exc_info=True)
        return None


# --- Single Sound Playback Logic (For Files) [UNCHANGED logic, relies on modified process_audio] ---
async def play_single_sound(interaction: discord.Interaction, sound_path: str):
    """Connects (if needed), plays a single sound FILE (processed/trimmed), and uses after_play_handler."""
    user = interaction.user
    guild = interaction.guild

    # Basic checks: Must be in a guild, user must be in VC
    if not guild or not isinstance(user, discord.Member) or not user.voice or not user.voice.channel:
        await interaction.followup.send("You need to be in a voice channel in this server to use this.", ephemeral=True)
        return

    target_channel = user.voice.channel
    # Check if the sound file actually exists before proceeding
    if not os.path.exists(sound_path):
         await interaction.followup.send("❌ Error: The requested sound file seems to be missing on the server.", ephemeral=True)
         bot_logger.error(f"SINGLE PLAY: File not found: {sound_path}")
         return

    # Use the helper for connection, permission, and busy checks
    # Pass the interaction object directly now
    voice_client = await _ensure_voice_client_ready(interaction, target_channel, action_type="SINGLE PLAY (File)")
    if not voice_client:
        # Helper already sent feedback to the user if it returned None
        return

    # Process and Play Audio FILE (process_audio now includes trimming)
    sound_basename = os.path.basename(sound_path)
    bot_logger.info(f"SINGLE PLAY (File): Processing '{sound_basename}' for {user.name}...")
    audio_source = process_audio(sound_path, user.display_name)

    if audio_source:
        # Final check before playing, just in case state changed very quickly
        if voice_client.is_playing():
             bot_logger.warning(f"SINGLE PLAY (File): VC became busy between check and play for {user.name}. Aborting.")
             await interaction.followup.send("⏳ Bot became busy just now. Please try again.", ephemeral=True)
             # Trigger the after-handler manually as play won't happen
             after_play_handler(None, voice_client)
             return

        try:
            sound_display_name = os.path.splitext(sound_basename)[0] # Get name without extension
            bot_logger.info(f"SINGLE PLAYBACK (File): Playing '{sound_display_name}' requested by {user.display_name}...")
            # Use the standard after_play_handler
            voice_client.play(audio_source, after=lambda e: after_play_handler(e, voice_client))
            # Send confirmation *after* successfully calling play
            await interaction.followup.send(f"▶️ Playing `{sound_display_name}` (max {MAX_PLAYBACK_DURATION_MS / 1000}s)...", ephemeral=True)
        except discord.errors.ClientException as e:
            # Handle "Already playing" which shouldn't happen after checks, but safety first
            msg = "❌ Error: Bot is already playing or encountered a client issue."
            await interaction.followup.send(msg, ephemeral=True)
            bot_logger.error(f"SINGLE PLAYBACK ERROR (File - ClientException): {e}", exc_info=True)
            # Call handler even on play failure to potentially disconnect or process queue
            after_play_handler(e, voice_client)
        except Exception as e:
            # Catch other unexpected playback errors
            await interaction.followup.send("❌ An unexpected error occurred during playback.", ephemeral=True)
            bot_logger.error(f"SINGLE PLAYBACK ERROR (File - Unexpected): {e}", exc_info=True)
            after_play_handler(e, voice_client) # Call handler
    else:
        # process_audio failed (logged details already)
        await interaction.followup.send("❌ Error: Could not process the audio file. It might be corrupted or unsupported.", ephemeral=True)
        bot_logger.error(f"SINGLE PLAYBACK (File): Failed to get audio source for '{sound_path}' requested by {user.name}")
        # If processing failed but we are connected, ensure the handler runs to potentially disconnect
        if voice_client and voice_client.is_connected():
            after_play_handler(None, voice_client) # No error passed, but trigger check


# --- Helper Functions [UNCHANGED] ---
def sanitize_filename(name: str) -> str:
    """Removes/replaces invalid chars for filenames and limits length."""
    # Remove or replace invalid characters: < > : " / \ | ? * and excessive whitespace/dots
    name = re.sub(r'[<>:"/\\|?*\.\s]+', '_', name)
    # Collapse multiple underscores
    name = re.sub(r'_+', '_', name)
    # Remove leading/trailing underscores
    name = name.strip('_')
    # Limit length (conservative)
    return name[:50]

def _find_sound_path_in_dir(directory: str, sound_name: str) -> Optional[str]:
    """Generic helper to find a sound file by name (case-insensitive, checks extensions)."""
    if not os.path.isdir(directory): return None
    # Try common extensions first for slight optimization
    preferred_order = ['.mp3', '.wav'] + [ext for ext in ALLOWED_EXTENSIONS if ext not in ['.mp3', '.wav']]
    # Check both exact and sanitized name just in case
    for name_variant in [sound_name, sanitize_filename(sound_name)]:
         # Case-insensitive check by iterating through directory
        try:
            for filename in os.listdir(directory):
                 base, ext = os.path.splitext(filename)
                 if ext.lower() in ALLOWED_EXTENSIONS and base.lower() == name_variant.lower():
                     return os.path.join(directory, filename) # Found match
        except OSError as e:
             bot_logger.error(f"Error listing files in {directory} during find: {e}")
             return None # Abort search on directory error
    return None # Not found

def _get_sound_files_from_dir(directory: str) -> List[str]:
    """Generic helper to list sound base names from a directory."""
    sounds = []
    if os.path.isdir(directory):
        try:
            for filename in os.listdir(directory):
                filepath = os.path.join(directory, filename)
                base_name, ext = os.path.splitext(filename)
                # Check if it's a file and has an allowed extension
                if os.path.isfile(filepath) and ext.lower() in ALLOWED_EXTENSIONS:
                    sounds.append(base_name) # Add the name without extension
        except OSError as e:
            bot_logger.error(f"Error listing files in {directory}: {e}")
    return sounds

def get_user_sound_files(user_id: int) -> List[str]:
    """Lists base names of sound files for a specific user."""
    return _get_sound_files_from_dir(os.path.join(USER_SOUNDS_DIR, str(user_id)))

def find_user_sound_path(user_id: int, sound_name: str) -> Optional[str]:
    """Finds the full path for a user's sound by name."""
    path = _find_sound_path_in_dir(os.path.join(USER_SOUNDS_DIR, str(user_id)), sound_name)
    #if not path: bot_logger.debug(f"Sound '{sound_name}' not found for user {user_id}")
    return path

def get_public_sound_files() -> List[str]:
    """Lists base names of public sound files."""
    return _get_sound_files_from_dir(PUBLIC_SOUNDS_DIR)

def find_public_sound_path(sound_name: str) -> Optional[str]:
    """Finds the full path for a public sound by name."""
    path = _find_sound_path_in_dir(PUBLIC_SOUNDS_DIR, sound_name)
    #if not path: bot_logger.debug(f"Public sound '{sound_name}' not found")
    return path

# --- Autocomplete Helper [UNCHANGED] ---
async def _generic_sound_autocomplete(ctx: discord.AutocompleteContext, source_func, *args) -> List[discord.OptionChoice]:
    """Generic autocomplete handler returning OptionChoices from a list function."""
    try:
        # Call the provided function (e.g., get_user_sound_files)
        sounds = source_func(*args)
        current_value = ctx.value.lower() if ctx.value else ""

        # Filter sounds based on current input and create OptionChoice objects
        suggestions = sorted(
            [discord.OptionChoice(name=name, value=name) # Use sound name for both name and value
             for name in sounds if current_value in name.lower()],
            key=lambda choice: choice.name # Sort alphabetically by name
        )
        # Return up to 25 suggestions (Discord limit)
        return suggestions[:25]
    except Exception as e:
         # Log errors during autocomplete but return empty list to avoid user disruption
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
    ctx: discord.ApplicationContext, # For logging user ID and interaction context
    sound_file: discord.Attachment,
    target_save_path: str, # The final destination path INCLUDING filename
    command_name: str = "upload"
) -> Tuple[bool, Optional[str]]:
    """
    Validates attachment (type, size), saves temporarily, checks integrity with Pydub,
    moves to final path if valid, and cleans up temp files.
    Returns (success_bool, error_message_or_None).
    Sends NO user feedback itself. Relies on caller to send messages.
    """
    user_id = ctx.author.id
    log_prefix = f"{command_name.upper()} VALIDATION"

    # 1. Basic Validation (Extension, Size, Content-Type)
    file_extension = os.path.splitext(sound_file.filename)[1].lower()
    if file_extension not in ALLOWED_EXTENSIONS:
        bot_logger.warning(f"{log_prefix}: User {user_id} tried invalid extension '{file_extension}' for '{sound_file.filename}'.")
        return False, f"❌ Invalid file type (`{file_extension}`). Allowed: {', '.join(ALLOWED_EXTENSIONS)}"

    if sound_file.size > MAX_USER_SOUND_SIZE_MB * 1024 * 1024:
        bot_logger.warning(f"{log_prefix}: User {user_id} tried oversized file '{sound_file.filename}' ({sound_file.size / (1024*1024):.2f} MB).")
        return False, f"❌ File too large (`{sound_file.size / (1024*1024):.2f}` MB). Max: {MAX_USER_SOUND_SIZE_MB}MB."

    # Check content type, but only warn if it looks wrong, rely primarily on extension & Pydub
    if not sound_file.content_type or not sound_file.content_type.startswith('audio/'):
        bot_logger.warning(f"{log_prefix}: Content-Type '{sound_file.content_type}' for '{sound_file.filename}' (user: {user_id}) not 'audio/*'. Proceeding based on extension.")

    # 2. Temporary Save and Pydub Check
    # Create a unique temporary filename to avoid collisions
    temp_save_filename = f"temp_{command_name}_{user_id}_{os.urandom(4).hex()}{file_extension}"
    # Place temp files in a known location for potential cleanup (e.g., root of USER_SOUNDS_DIR)
    temp_save_path = os.path.join(USER_SOUNDS_DIR, temp_save_filename) # Store temp files within the user sounds area

    # --- Cleanup Function ---
    async def cleanup_temp():
        if os.path.exists(temp_save_path):
            try:
                os.remove(temp_save_path)
                bot_logger.debug(f"Cleaned up temporary file: {temp_save_path}")
            except Exception as del_e:
                bot_logger.warning(f"Failed to clean up temporary file '{temp_save_path}': {del_e}")
    # --- End Cleanup ---

    try:
        # Save the attachment to the temporary path
        await sound_file.save(temp_save_path)
        bot_logger.info(f"{log_prefix}: Saved temporary file for {user_id}: '{temp_save_path}'")

        # Validate with Pydub by attempting to load it
        try:
            bot_logger.debug(f"{log_prefix}: Attempting Pydub decode: '{temp_save_path}'")
            # We just need to load it to see if it's valid; discard the result
            _ = AudioSegment.from_file(temp_save_path, format=file_extension.strip('.'))
            bot_logger.info(f"{log_prefix}: Pydub validation successful for '{temp_save_path}'")

            # 3. Move to Final Location if Pydub check passes
            try:
                # Ensure the target directory exists before moving
                target_dir = os.path.dirname(target_save_path)
                ensure_dir(target_dir) # Create if it doesn't exist

                # Use os.replace for atomic move where possible, fallback to shutil.move
                os.replace(temp_save_path, target_save_path)
                bot_logger.info(f"{log_prefix}: Final file saved (atomic replace): '{target_save_path}'")
                # No cleanup needed here, temp file is now the final file
                return True, None # Success!

            except OSError as rep_e: # os.replace might fail across filesystems/permissions
                bot_logger.warning(f"{log_prefix}: os.replace failed ('{rep_e}'), attempting shutil.move for '{temp_save_path}' -> '{target_save_path}'.")
                try:
                    shutil.move(temp_save_path, target_save_path)
                    bot_logger.info(f"{log_prefix}: Final file saved (shutil.move fallback): '{target_save_path}'")
                    return True, None # Success!
                except Exception as move_e:
                    bot_logger.error(f"{log_prefix}: FAILED final save (replace error: {rep_e}, move error: {move_e})", exc_info=True)
                    await cleanup_temp() # Clean up temp file on move failure
                    return False, "❌ Error saving the sound file after validation. Please try again."

        # --- Pydub Validation Error Handling ---
        except CouldntDecodeError as decode_error:
            bot_logger.error(f"{log_prefix}: FAILED (Pydub Decode Error - user: {user_id}, file: '{sound_file.filename}'): {decode_error}", exc_info=True)
            await cleanup_temp()
            return False, (f"❌ **Audio Validation Failed!** Could not process `{sound_file.filename}`.\n"
                            f"Please ensure it's a valid audio file ({', '.join(ALLOWED_EXTENSIONS)}) and that the bot has FFmpeg accessible.")
        except Exception as validate_e:
            # Catch unexpected errors during the Pydub check phase
            bot_logger.error(f"{log_prefix}: FAILED (Unexpected Pydub check error - user: {user_id}, file: '{sound_file.filename}'): {validate_e}", exc_info=True)
            await cleanup_temp()
            return False, "❌ **Audio Validation Failed!** An unexpected error occurred during audio processing."

    # --- Temporary Save Error Handling ---
    except discord.HTTPException as e:
        # Error downloading the file from Discord's servers
        bot_logger.error(f"{log_prefix}: Error downloading temp file from Discord for {user_id} ('{sound_file.filename}'): {e}", exc_info=True)
        await cleanup_temp() # Attempt cleanup even if save failed (maybe partial file?)
        return False, "❌ Error downloading the sound file from Discord. Please try again."
    except Exception as e:
        # Catch other errors during the initial save process
        bot_logger.error(f"{log_prefix}: Unexpected error during temp save/validate for {user_id} ('{sound_file.filename}'): {e}", exc_info=True)
        await cleanup_temp()
        return False, "❌ An unexpected server error occurred during file handling."


# --- Slash Commands ---

# === Join Sound Commands [UNCHANGED logic, uses modified _validate_and_save_upload] ===
@bot.slash_command(name="setjoinsound", description="Upload your custom join sound. Replaces existing.")
@commands.cooldown(1, 15, commands.BucketType.user) # Cooldown: 1 use per 15 sec per user
async def setjoinsound(
    ctx: discord.ApplicationContext,
    sound_file: discord.Option(discord.Attachment, description=f"Sound ({', '.join(ALLOWED_EXTENSIONS)}). Max {MAX_USER_SOUND_SIZE_MB}MB.", required=True) # type: ignore
):
    await ctx.defer(ephemeral=True) # Acknowledge interaction privately
    author = ctx.author
    user_id_str = str(author.id)
    bot_logger.info(f"COMMAND: /setjoinsound by {author.name} ({user_id_str}), file: '{sound_file.filename}'")

    # Determine final filename and path (using user ID to avoid collisions)
    file_extension = os.path.splitext(sound_file.filename)[1].lower()
    final_save_filename = f"joinsound_{user_id_str}{file_extension}" # Unique filename pattern
    final_save_path = os.path.join(SOUNDS_DIR, final_save_filename)

    # Get existing config to check for old file later
    user_config = user_sound_config.get(user_id_str, {})
    old_config_filename = user_config.get("join_sound")

    # Use the validation and save helper function
    success, error_msg = await _validate_and_save_upload(ctx, sound_file, final_save_path, command_name="setjoinsound")

    if success:
        # Validation and save successful, now update config and clean up old file
        bot_logger.info(f"Join sound validation successful for {author.name}, saved to '{final_save_path}'")

        # Remove the *previous* sound file if it existed and had a different name/extension
        if old_config_filename and old_config_filename != final_save_filename:
            old_path = os.path.join(SOUNDS_DIR, old_config_filename)
            if os.path.exists(old_path):
                try:
                    os.remove(old_path)
                    bot_logger.info(f"Removed previous join sound file: '{old_path}'")
                except Exception as e:
                    bot_logger.warning(f"Could not remove previous join sound file '{old_path}': {e}")

        # Update or create the user's configuration
        user_config["join_sound"] = final_save_filename # Set/update the join_sound key
        user_sound_config[user_id_str] = user_config    # Store the updated config back
        save_config()                                    # Persist changes to disk

        bot_logger.info(f"Updated join sound config for {author.name} ({user_id_str}) to '{final_save_filename}'")
        await ctx.followup.send(f"✅ Success! Your join sound is set to `{sound_file.filename}`.", ephemeral=True)
    else:
        # Validation or save failed, helper function logged details
        # Send the error message provided by the helper function
        await ctx.followup.send(error_msg or "❌ An unknown error occurred during validation.", ephemeral=True)


@bot.slash_command(name="removejoinsound", description="Remove your custom join sound, revert to TTS.")
@commands.cooldown(1, 5, commands.BucketType.user)
async def removejoinsound(ctx: discord.ApplicationContext):
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id_str = str(author.id)
    bot_logger.info(f"COMMAND: /removejoinsound by {author.name} ({user_id_str})")

    # Check if the user actually has a join sound configured
    user_config = user_sound_config.get(user_id_str)
    if user_config and "join_sound" in user_config:
        # Get the filename before removing the config entry
        filename_to_remove = user_config.pop("join_sound") # Remove the key from user's dict
        bot_logger.info(f"Removing join sound config for {author.name} (was '{filename_to_remove}')")

        # If the user config dict is now empty (no TTS defaults either), remove the user key entirely
        if not user_config:
            del user_sound_config[user_id_str]
            bot_logger.info(f"Removed empty user config entry for {author.name} after join sound removal.")
        # Otherwise, the dict (now without join_sound, but possibly with TTS defaults) remains.

        save_config() # Save the updated configuration

        # Attempt to delete the associated sound file
        file_path_to_remove = os.path.join(SOUNDS_DIR, filename_to_remove)
        # Also try to clean up potential old TTS join file for this user
        tts_join_file_path = os.path.join(SOUNDS_DIR, f"tts_join_{user_id_str}.mp3")

        removed_custom = False
        removed_tts = False

        for path_to_remove, log_name in [(file_path_to_remove, "custom join sound"), (tts_join_file_path, "cached join TTS")]:
            if os.path.exists(path_to_remove):
                try:
                    os.remove(path_to_remove)
                    bot_logger.info(f"Deleted file: '{path_to_remove}' ({log_name})")
                    if path_to_remove == file_path_to_remove: removed_custom = True
                    if path_to_remove == tts_join_file_path: removed_tts = True
                except OSError as e:
                    bot_logger.warning(f"Could not delete file '{path_to_remove}' ({log_name}): {e}")
            elif path_to_remove == file_path_to_remove: # Log only if the main configured file was expected but missing
                 bot_logger.warning(f"Configured join sound '{filename_to_remove}' not found at '{file_path_to_remove}' during removal.")

        msg = "🗑️ Custom join sound removed."
        if removed_tts: msg += " Cleaned up cached join TTS."
        msg += " Default TTS will now be used for joins."
        await ctx.followup.send(msg, ephemeral=True)
    else:
        # User didn't have a join sound set
        await ctx.followup.send("🤷 You don't have a custom join sound configured.", ephemeral=True)


# === User Command Sound / Soundboard Commands [UNCHANGED logic, uses helpers] ===
@bot.slash_command(name="uploadsound", description=f"Upload a sound (personal/public). Limit: {MAX_USER_SOUNDS_PER_USER} personal.")
@commands.cooldown(2, 20, commands.BucketType.user) # Cooldown: 2 uses per 20 sec per user
async def uploadsound(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Short name (letters, numbers, underscore). Will be sanitized.", required=True), # type: ignore
    sound_file: discord.Option(discord.Attachment, description=f"Sound ({', '.join(ALLOWED_EXTENSIONS)}). Max {MAX_USER_SOUND_SIZE_MB}MB.", required=True), # type: ignore
    make_public: discord.Option(bool, description="Make available for everyone? (Default: False)", default=False) # type: ignore
):
    await ctx.defer(ephemeral=True) # Acknowledge privately
    author = ctx.author
    user_id = author.id
    bot_logger.info(f"COMMAND: /uploadsound by {author.name} ({user_id}), name: '{name}', public: {make_public}, file: '{sound_file.filename}'")

    # Sanitize the user-provided name
    clean_name = sanitize_filename(name)
    if not clean_name:
        await ctx.followup.send("❌ Please provide a valid name (letters, numbers, underscore allowed before sanitization).", ephemeral=True); return

    # Inform user if name was changed
    followup_message_prefix = f"ℹ️ Name sanitized to `{clean_name}`.\n" if clean_name != name else ""

    # Determine target directory and perform pre-checks
    file_extension = os.path.splitext(sound_file.filename)[1].lower()
    final_save_filename = f"{clean_name}{file_extension}"

    if make_public:
        target_dir = PUBLIC_SOUNDS_DIR
        # Check if a public sound with this (sanitized) name already exists (any extension)
        if find_public_sound_path(clean_name):
            await ctx.followup.send(f"{followup_message_prefix}❌ A public sound named `{clean_name}` already exists.", ephemeral=True); return
        is_replacing_personal = False # Not replacing personal if making public
        scope_description = "public"
    else:
        # Personal sound
        target_dir = os.path.join(USER_SOUNDS_DIR, str(user_id))
        ensure_dir(target_dir) # Ensure user's personal directory exists

        # Check if replacing an existing personal sound (ignoring extension initially)
        existing_personal_path = find_user_sound_path(user_id, clean_name)
        is_replacing_personal = existing_personal_path is not None

        # Check personal sound limit only if NOT replacing
        if not is_replacing_personal and len(get_user_sound_files(user_id)) >= MAX_USER_SOUNDS_PER_USER:
             await ctx.followup.send(f"{followup_message_prefix}❌ You have reached the maximum of {MAX_USER_SOUNDS_PER_USER} personal sounds. Delete one using `/deletesound`.", ephemeral=True); return
        scope_description = "personal"

    # Define the final save path
    final_save_path = os.path.join(target_dir, final_save_filename)

    # Use the validation helper function
    success, error_msg = await _validate_and_save_upload(ctx, sound_file, final_save_path, command_name="uploadsound")

    if success:
        bot_logger.info(f"Sound validation successful for {author.name}, saved to '{final_save_path}' ({scope_description})")

        # If replacing a personal sound with a file of a *different* extension, remove the old one.
        # The validation helper already overwrites if the extension is the same.
        if is_replacing_personal and not make_public:
            # Re-check existing path after successful save to get the exact old path
            old_personal_path = find_user_sound_path(user_id, clean_name)
            # Ensure the found path isn't the one we just saved (it shouldn't be if extensions differ)
            if old_personal_path and old_personal_path != final_save_path:
                 try:
                     os.remove(old_personal_path)
                     bot_logger.info(f"Removed existing personal sound '{os.path.basename(old_personal_path)}' for {user_id} due to extension change during replace.")
                 except Exception as e:
                     bot_logger.warning(f"Could not remove conflicting personal sound '{old_personal_path}' during replacement: {e}")

        # Determine feedback message based on action
        action = "updated" if is_replacing_personal and not make_public else "uploaded"
        play_cmd = "playpublic" if make_public else "playsound"
        list_cmd = "publicsounds" if make_public else "mysounds"

        msg = f"{followup_message_prefix}✅ Success! Sound `{clean_name}` {action} as {scope_description}.\n"
        msg += f"Use `/{play_cmd} name:{clean_name}`"
        if not make_public:
            msg += f", `/{list_cmd}`, or `/soundpanel`."
            msg += f"\nUse `/publishsound name:{clean_name}` to make it public later."
        else:
            msg += f" or `/{list_cmd}`."

        await ctx.followup.send(msg, ephemeral=True)
    else:
        # Validation failed, send the error message from the helper
        await ctx.followup.send(f"{followup_message_prefix}{error_msg or '❌ Unknown validation error.'}", ephemeral=True)


@bot.slash_command(name="mysounds", description="Lists your personal uploaded sounds.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def mysounds(ctx: discord.ApplicationContext):
    await ctx.defer(ephemeral=True) # Private response
    author = ctx.author
    bot_logger.info(f"COMMAND: /mysounds by {author.name} ({author.id})")

    # Get the list of sound base names for the user
    user_sounds = get_user_sound_files(author.id)

    if not user_sounds:
        await ctx.followup.send("You haven't uploaded any personal sounds yet. Use `/uploadsound`!", ephemeral=True); return

    # Sort and format the list for display
    sorted_sounds = sorted(user_sounds, key=str.lower) # Case-insensitive sort
    sound_list_parts = []
    current_length = 0
    char_limit = 1900 # Keep well below embed description limit

    for name in sorted_sounds:
        line = f"- `{name}`"
        if current_length + len(line) + 1 > char_limit:
            sound_list_parts.append("... (list truncated)")
            break
        sound_list_parts.append(line)
        current_length += len(line) + 1 # +1 for newline

    sound_list_str = "\n".join(sound_list_parts)

    # Create and send the embed
    embed = discord.Embed(
        title=f"{author.display_name}'s Sounds ({len(sorted_sounds)}/{MAX_USER_SOUNDS_PER_USER})",
        description=f"Use `/playsound name:<sound>`, `/soundpanel`, or `/publishsound name:<sound>`.\n\n{sound_list_str}",
        color=discord.Color.blurple() # Or author.color
    ).set_footer(text="Use /deletesound name:<sound> to remove.")

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

    # Try to find the sound path using the provided name (case-insensitive check handled by helper)
    sound_path = find_user_sound_path(user_id, name)

    # If not found, maybe the user entered the sanitized name? Check that too.
    # This is less likely if autocomplete is used, but handles manual input.
    sound_base_name = name # Keep original input for feedback
    if not sound_path:
        sanitized = sanitize_filename(name)
        if sanitized != name: # Only check sanitized if it's different
            sound_path = find_user_sound_path(user_id, sanitized)
            if sound_path:
                 sound_base_name = sanitized # Update base name if found via sanitized version
                 bot_logger.debug(f"Found sound for deletion using sanitized name '{sanitized}' for user {user_id}.")

    if not sound_path:
        await ctx.followup.send(f"❌ Personal sound `{name}` not found. Use `/mysounds` to see your sounds.", ephemeral=True); return

    # Security check (important!): Ensure the path is actually within the user's directory.
    # This prevents potential path traversal if find_user_sound_path logic had a flaw.
    user_dir_abs = os.path.abspath(os.path.join(USER_SOUNDS_DIR, str(user_id)))
    resolved_sound_path_abs = os.path.abspath(sound_path)
    if not resolved_sound_path_abs.startswith(user_dir_abs + os.sep): # Check it starts with the user dir path + separator
         bot_logger.critical(f"CRITICAL SECURITY ALERT: Path traversal attempt in /deletesound. User: {user_id}, Input: '{name}', Resolved Path: '{resolved_sound_path_abs}', Expected Dir: '{user_dir_abs}'")
         await ctx.followup.send("❌ Internal security error. Action prevented.", ephemeral=True); return

    # Attempt to delete the file
    try:
        deleted_filename = os.path.basename(sound_path) # Get actual filename with extension
        os.remove(sound_path)
        bot_logger.info(f"Deleted PERSONAL sound '{deleted_filename}' for user {user_id} (requested as '{name}').")
        await ctx.followup.send(f"🗑️ Personal sound `{sound_base_name}` deleted successfully.", ephemeral=True)
    except OSError as e:
        bot_logger.error(f"Failed to delete personal sound '{sound_path}' for {user_id}: {e}", exc_info=True)
        await ctx.followup.send(f"❌ Failed to delete `{sound_base_name}`. Error: {type(e).__name__}. Check bot permissions or if the file exists.", ephemeral=True)
    except Exception as e:
        bot_logger.error(f"Unexpected error deleting personal sound '{sound_path}' for {user_id}: {e}", exc_info=True)
        await ctx.followup.send(f"❌ An unexpected error occurred while trying to delete `{sound_base_name}`.", ephemeral=True)


@bot.slash_command(name="playsound", description="Plays one of your PERSONAL sounds in your current VC.")
@commands.cooldown(1, 4, commands.BucketType.user) # Allow slightly faster playback
async def playsound(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Name of the personal sound to play.", required=True, autocomplete=user_sound_autocomplete) # type: ignore
):
    # Defer publicly because the final feedback ("Playing...") is ephemeral within play_single_sound
    # but the command itself isn't inherently private. If play fails, followup is ephemeral.
    await ctx.defer()
    author = ctx.author
    bot_logger.info(f"COMMAND: /playsound by {author.name} ({author.id}), request: '{name}'")

    # Find the path for the user's sound
    sound_path = find_user_sound_path(author.id, name)
    sound_base_name = name
    # Try sanitized name if exact match failed (less likely with autocomplete)
    if not sound_path:
        sanitized = sanitize_filename(name)
        if sanitized != name:
            sound_path = find_user_sound_path(author.id, sanitized)
            if sound_path: sound_base_name = sanitized

    if not sound_path:
        await ctx.followup.send(f"❌ Personal sound `{name}` not found. Use `/mysounds` or `/soundpanel`.", ephemeral=True); return

    # Call the generic playback function, passing the interaction context
    await play_single_sound(ctx.interaction, sound_path)


# --- Sound Panel View [UNCHANGED] ---
class UserSoundboardView(discord.ui.View):
    def __init__(self, user_id: int, *, timeout: Optional[float] = 300.0): # Default timeout 5 minutes
        super().__init__(timeout=timeout)
        self.user_id = user_id # Store the ID of the user this panel belongs to
        self.message: Optional[discord.Message] = None # To store the message later for editing on timeout
        self.populate_buttons() # Add buttons upon initialization

    def populate_buttons(self):
        user_dir = os.path.join(USER_SOUNDS_DIR, str(self.user_id))
        bot_logger.debug(f"Populating sound panel for user {self.user_id} from directory: {user_dir}")

        if not os.path.isdir(user_dir):
            # Add a disabled button indicating no sounds if the directory doesn't exist
            self.add_item(discord.ui.Button(label="No sounds uploaded yet!", style=discord.ButtonStyle.secondary, disabled=True, custom_id=f"usersb_no_sounds_{self.user_id}"))
            bot_logger.debug(f"User directory not found for {self.user_id}, adding 'no sounds' button.")
            return

        sounds_found, button_row = 0, 0
        max_buttons_per_row, max_rows = 5, 5 # Discord limits: 5 components per row, 5 rows total (25 components)
        max_buttons_total = max_buttons_per_row * max_rows

        try:
            # Sort files alphabetically for consistent panel layout
            files_in_dir = sorted(os.listdir(user_dir), key=str.lower)
        except OSError as e:
            bot_logger.error(f"Error listing user directory '{user_dir}' for panel: {e}")
            self.add_item(discord.ui.Button(label="Error Reading Sounds", style=discord.ButtonStyle.danger, disabled=True, custom_id=f"usersb_error_{self.user_id}"))
            return

        for filename in files_in_dir:
            if sounds_found >= max_buttons_total:
                bot_logger.warning(f"Reached button limit ({max_buttons_total}) for user panel {self.user_id}. File '{filename}' and subsequent skipped.")
                # Optionally add a '...' button if we hit the limit before the last row
                # if button_row < max_rows:
                #     self.add_item(discord.ui.Button(label="...", style=discord.ButtonStyle.secondary, disabled=True, row=button_row))
                break # Stop adding buttons

            filepath = os.path.join(user_dir, filename)
            # Ensure it's a file and has an allowed audio extension
            if os.path.isfile(filepath):
                base_name, ext = os.path.splitext(filename)
                if ext.lower() in ALLOWED_EXTENSIONS:
                    # Create button label from filename (strip extension, replace underscores)
                    # Limit label length for cleaner display (Discord limits button labels too)
                    label = base_name.replace("_", " ")[:80] # Max 80 chars for label
                    # Create a unique custom_id for the button including the filename
                    # Prefix helps identify the button type in the callback
                    custom_id = f"usersb_play:{filename}" # Use filename with extension here

                    # Discord custom_id limit is 100 chars. Check length.
                    if len(custom_id) > 100:
                        bot_logger.warning(f"Skipping sound '{filename}' for user {self.user_id} panel: resulting custom_id '{custom_id}' is too long (> 100 chars).")
                        continue # Skip this button

                    # Create the button
                    button = discord.ui.Button(
                        label=label,
                        style=discord.ButtonStyle.secondary, # Use secondary style for less visual clutter
                        custom_id=custom_id,
                        row=button_row # Assign button to the current row
                    )
                    # Assign the shared callback function to this button
                    button.callback = self.user_soundboard_button_callback
                    self.add_item(button) # Add the button to the view
                    sounds_found += 1

                    # Move to the next row if the current row is full
                    if sounds_found % max_buttons_per_row == 0:
                        button_row += 1
                # else: bot_logger.debug(f"Skipping non-audio file in user dir {self.user_id}: '{filename}'") # Potentially verbose
            # else: bot_logger.debug(f"Skipping non-file item in user dir {self.user_id}: '{filename}'") # Potentially verbose

        if sounds_found == 0:
             # If after checking all files, none were valid sounds, add the 'no sounds' button
             bot_logger.info(f"No valid sound files found for panel user {self.user_id} in '{user_dir}'.")
             self.add_item(discord.ui.Button(label="No sounds uploaded yet!", style=discord.ButtonStyle.secondary, disabled=True, custom_id=f"usersb_no_sounds_{self.user_id}"))

    async def user_soundboard_button_callback(self, interaction: discord.Interaction):
        # This function handles clicks on ANY sound button in this specific view instance.
        custom_id = interaction.data["custom_id"] # Get the ID of the button that was clicked
        user = interaction.user # User who clicked the button
        bot_logger.info(f"USER PANEL: Button '{custom_id}' clicked by {user.name} ({user.id}) on panel originally created for user {self.user_id}")

        # Important: Check if the user clicking is the one the panel was generated for?
        # For now, allowing anyone in the channel to click seems reasonable for a soundboard.
        # if interaction.user.id != self.user_id:
        #     await interaction.response.send_message("This is not your sound panel!", ephemeral=True)
        #     return

        # Defer the response ephemerally first. play_single_sound will send further feedback.
        await interaction.response.defer(ephemeral=True)

        # Parse the custom_id to get the filename
        if not custom_id.startswith("usersb_play:"):
            bot_logger.error(f"Invalid custom_id received from user panel button: '{custom_id}'")
            await interaction.followup.send("❌ Internal error: Invalid button action.", ephemeral=True); return

        sound_filename = custom_id.split(":", 1)[1]
        # Construct the full path to the sound file using the panel's owner ID and the filename
        sound_path = os.path.join(USER_SOUNDS_DIR, str(self.user_id), sound_filename)

        # Use the generic play function. It handles VC checks, permissions, playback, etc.
        # Pass the interaction object from the button click.
        await play_single_sound(interaction, sound_path)

    async def on_timeout(self):
        # This is called when the view times out (no interaction for the timeout duration).
        if self.message: # Check if we have a reference to the original message
            bot_logger.debug(f"User sound panel view timed out for user {self.user_id} (message ID: {self.message.id})")

            # Try to get the display name of the panel's owner for the timeout message
            owner_name = f"User {self.user_id}" # Default fallback name
            try:
                 # Fetch member if in guild, otherwise fetch user (for DMs, though panel unlikely there)
                 if self.message.guild:
                     panel_owner = await self.message.guild.fetch_member(self.user_id)
                 else:
                     panel_owner = await bot.fetch_user(self.user_id)
                 if panel_owner: owner_name = panel_owner.display_name
            except (discord.NotFound, discord.Forbidden, AttributeError) as e:
                 bot_logger.warning(f"Could not fetch panel owner {self.user_id} for timeout message: {e}")

            # Disable all buttons in the view
            for item in self.children:
                if isinstance(item, discord.ui.Button): # Or any component with 'disabled'
                    item.disabled = True

            # Edit the original message to show it's expired
            try:
                await self.message.edit(content=f"🔊 **{owner_name}'s Personal Panel (Expired)**", view=self)
            except discord.HTTPException as e:
                # Ignore errors editing (e.g., message deleted, permissions lost)
                bot_logger.warning(f"Failed to edit expired sound panel message {self.message.id} for user {self.user_id}: {e}")
        else:
            # This shouldn't normally happen if view.message is set correctly
            bot_logger.debug(f"User sound panel view timed out for user {self.user_id} but no message reference was stored.")

@bot.slash_command(name="soundpanel", description="Displays buttons to play YOUR personal sounds in your VC.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def soundpanel(ctx: discord.ApplicationContext):
    # Defer publicly initially, as the panel itself is visible to others.
    # Interactions (button clicks) will be handled ephemerally by the view/play function.
    await ctx.defer()
    author = ctx.author
    bot_logger.info(f"COMMAND: /soundpanel invoked by {author.name} ({author.id})")

    # Create an instance of the soundboard view for the invoking user
    # Increase timeout for panels, e.g., 10 minutes (600 seconds)
    view = UserSoundboardView(user_id=author.id, timeout=600.0)

    # Check if the view actually has any playable buttons after populating
    # (i.e., user has sounds and no errors occurred)
    has_playable_buttons = any(
        isinstance(item, discord.ui.Button) and not item.disabled and item.custom_id and item.custom_id.startswith("usersb_play:")
        for item in view.children
    )

    if not has_playable_buttons:
         # If no buttons are playable (no sounds, or error during population), inform the user ephemerally.
         await ctx.followup.send("You don't have any personal sounds uploaded or there was an error generating the panel. Use `/uploadsound` first!", ephemeral=True); return

    # If buttons exist, send the panel message
    msg_content = f"🔊 **{author.display_name}'s Personal Sound Panel** - Click a button to play a sound!"
    try:
        # Send the message with the view attached
        message = await ctx.followup.send(msg_content, view=view)
        # Store the message reference in the view object so it can be edited on timeout
        view.message = message
    except Exception as e:
        bot_logger.error(f"Failed to send soundpanel for user {author.id}: {e}", exc_info=True)
        # Try to send an ephemeral error message if the main followup failed
        try: await ctx.followup.send("❌ Failed to create the sound panel.", ephemeral=True)
        except Exception: pass # Ignore errors sending the error message


# === Public Sound Commands [UNCHANGED] ===
@bot.slash_command(name="publishsound", description="Make one of your personal sounds public for everyone.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def publishsound(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Name of YOUR personal sound to make public.", required=True, autocomplete=user_sound_autocomplete) # type: ignore
):
    await ctx.defer(ephemeral=True) # Private acknowledgement and response
    author = ctx.author
    user_id = author.id
    bot_logger.info(f"COMMAND: /publishsound by {author.name} ({user_id}), target: '{name}'")

    # Find the user's personal sound path
    user_sound_path = find_user_sound_path(user_id, name)
    sound_base_name = name # Keep original input for feedback
    if not user_sound_path:
        sanitized = sanitize_filename(name)
        if sanitized != name:
            user_sound_path = find_user_sound_path(user_id, sanitized)
            if user_sound_path: sound_base_name = sanitized

    if not user_sound_path:
        await ctx.followup.send(f"❌ Personal sound `{name}` not found. Use `/mysounds`.", ephemeral=True); return

    # Determine the target public path and name
    source_filename = os.path.basename(user_sound_path)
    public_path = os.path.join(PUBLIC_SOUNDS_DIR, source_filename)
    target_base_name, _ = os.path.splitext(source_filename) # Public name is based on the actual filename

    # Check if a public sound with the same base name already exists
    if find_public_sound_path(target_base_name):
        await ctx.followup.send(f"❌ A public sound named `{target_base_name}` already exists. Choose a different name or ask an admin to remove it.", ephemeral=True); return

    # Copy the file from the user's directory to the public directory
    try:
        ensure_dir(PUBLIC_SOUNDS_DIR) # Ensure public dir exists
        shutil.copy2(user_sound_path, public_path) # copy2 preserves metadata like modification time
        bot_logger.info(f"SOUND PUBLISHED: Copied '{user_sound_path}' to '{public_path}' by {author.name}.")
        await ctx.followup.send(f"✅ Your sound `{sound_base_name}` (as `{target_base_name}`) is now public!\nUse `/playpublic name:{target_base_name}`.", ephemeral=True)
    except (OSError, Exception) as e:
        bot_logger.error(f"Failed to copy user sound '{user_sound_path}' to public directory '{public_path}': {e}", exc_info=True)
        await ctx.followup.send(f"❌ Failed to publish `{sound_base_name}`. Error: {type(e).__name__}.", ephemeral=True)

@bot.slash_command(name="removepublic", description="[Admin Only] Remove a sound from the public collection.")
@commands.has_permissions(manage_guild=True) # Restrict to users with "Manage Server" permission
@commands.cooldown(1, 5, commands.BucketType.guild) # Guild-wide cooldown
async def removepublic(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Name of the public sound to remove.", required=True, autocomplete=public_sound_autocomplete) # type: ignore
):
    await ctx.defer(ephemeral=True) # Admin actions usually best kept private initially
    admin = ctx.author
    bot_logger.info(f"COMMAND: /removepublic by admin {admin.name} ({admin.id}) in guild {ctx.guild.id}, target: '{name}'")

    # Find the public sound path
    public_path = find_public_sound_path(name)
    sound_base_name = name
    if not public_path:
        sanitized = sanitize_filename(name)
        if sanitized != name:
            public_path = find_public_sound_path(sanitized)
            if public_path: sound_base_name = sanitized

    if not public_path:
        await ctx.followup.send(f"❌ Public sound `{name}` not found. Use `/publicsounds`.", ephemeral=True); return

    # Attempt to delete the file
    try:
        deleted_filename = os.path.basename(public_path)
        os.remove(public_path)
        bot_logger.info(f"ADMIN ACTION: Deleted public sound '{deleted_filename}' by {admin.name}.")
        await ctx.followup.send(f"🗑️ Public sound `{sound_base_name}` deleted successfully.", ephemeral=True)
    except (OSError, Exception) as e:
        bot_logger.error(f"Admin {admin.name} failed to delete public sound '{public_path}': {e}", exc_info=True)
        await ctx.followup.send(f"❌ Failed to delete public sound `{sound_base_name}`. Error: {type(e).__name__}.", ephemeral=True)

@removepublic.error # Error handler specifically for removepublic
async def removepublic_error(ctx: discord.ApplicationContext, error: discord.DiscordException):
    # Handle permission errors specifically for this admin command
    if isinstance(error, commands.MissingPermissions):
        bot_logger.warning(f"User {ctx.author.name} ({ctx.author.id}) tried /removepublic without Manage Guild permission.")
        await ctx.respond("🚫 You need the `Manage Server` permission to use this command.", ephemeral=True)
    else:
        # For other errors (like cooldowns, etc.), let the global handler manage it
        # This avoids duplicating error handling logic. We can call it directly:
        await on_application_command_error(ctx, error)


@bot.slash_command(name="publicsounds", description="Lists all available public sounds.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def publicsounds(ctx: discord.ApplicationContext):
    await ctx.defer(ephemeral=True) # List can be long, keep it private
    bot_logger.info(f"COMMAND: /publicsounds by {ctx.author.name} ({ctx.author.id})")

    # Get the list of public sound base names
    public_sounds = get_public_sound_files()

    if not public_sounds:
        await ctx.followup.send("There are no public sounds available yet. Admins can add some or users can use `/publishsound`!", ephemeral=True); return

    # Sort and format the list
    sorted_sounds = sorted(public_sounds, key=str.lower)
    sound_list_parts = []
    current_length = 0
    char_limit = 1900 # Keep well below embed description limit

    for name in sorted_sounds:
        line = f"- `{name}`"
        if current_length + len(line) + 1 > char_limit:
            sound_list_parts.append("... (list truncated)")
            break
        sound_list_parts.append(line)
        current_length += len(line) + 1

    sound_list_str = "\n".join(sound_list_parts)

    # Create and send embed
    embed = discord.Embed(
        title=f"📢 Public Sounds ({len(sorted_sounds)})",
        description=f"Use `/playpublic name:<sound_name>` to play one.\n\n{sound_list_str}",
        color=discord.Color.green()
    ).set_footer(text="Admins can use /removepublic name:<sound_name>.")

    await ctx.followup.send(embed=embed, ephemeral=True)


@bot.slash_command(name="playpublic", description="Plays a public sound in your current voice channel.")
@commands.cooldown(1, 4, commands.BucketType.user) # Same cooldown as personal playsound
async def playpublic(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Name of the public sound to play.", required=True, autocomplete=public_sound_autocomplete) # type: ignore
):
    # Defer publicly, feedback handled within play_single_sound
    await ctx.defer()
    author = ctx.author
    bot_logger.info(f"COMMAND: /playpublic by {author.name} ({author.id}), request: '{name}'")

    # Find the public sound path
    public_path = find_public_sound_path(name)
    sound_base_name = name
    if not public_path:
        sanitized = sanitize_filename(name)
        if sanitized != name:
            public_path = find_public_sound_path(sanitized)
            if public_path: sound_base_name = sanitized

    if not public_path:
        await ctx.followup.send(f"❌ Public sound `{name}` not found. Use `/publicsounds` to see available sounds.", ephemeral=True); return

    # Call the generic playback function
    await play_single_sound(ctx.interaction, public_path)


# === TTS Defaults Commands [UNCHANGED] ===
@bot.slash_command(name="setttsdefaults", description="Set your preferred default TTS language and speed for /tts.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def setttsdefaults(
    ctx: discord.ApplicationContext,
    language: discord.Option(str, description="Your preferred default language/accent for TTS.", required=True, choices=TTS_LANGUAGE_CHOICES), # type: ignore
    slow: discord.Option(bool, description="Should the TTS speak slowly by default?", required=True) # type: ignore
):
    await ctx.defer(ephemeral=True) # Settings are personal
    author = ctx.author
    user_id_str = str(author.id)
    bot_logger.info(f"COMMAND: /setttsdefaults by {author.name} ({user_id_str}), lang: {language}, slow: {slow}")

    # Get the user's config dict, creating it if it doesn't exist using setdefault
    user_config = user_sound_config.setdefault(user_id_str, {})

    # Set or update the tts_defaults key within the user's config
    user_config['tts_defaults'] = {'language': language, 'slow': slow}

    save_config() # Persist the changes

    # Find the friendly display name for the chosen language
    lang_name = language # Default to code if name not found (shouldn't happen with choices)
    for choice in TTS_LANGUAGE_CHOICES:
        if choice.value == language:
            lang_name = choice.name
            break

    await ctx.followup.send(
        f"✅ Your TTS defaults have been updated!\n"
        f"• **Default Language:** {lang_name} (`{language}`)\n"
        f"• **Default Speed:** {'Slow' if slow else 'Normal'}\n\n"
        f"These settings will be used when you use `/tts` without specifying the `language` or `slow` options.",
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

    # Check if the user has config and if 'tts_defaults' exists within it
    if user_config and 'tts_defaults' in user_config:
        del user_config['tts_defaults'] # Remove the defaults key
        bot_logger.info(f"Removed TTS defaults config for {author.name}")

        # If the user config dict is now empty (no join sound either), remove the user key entirely
        if not user_config:
            if user_id_str in user_sound_config: # Check existence before deleting
                del user_sound_config[user_id_str]
                bot_logger.info(f"Removed empty user config entry for {author.name} after TTS default removal.")

        save_config() # Save changes

        await ctx.followup.send(
            f"🗑️ Your custom TTS defaults have been removed.\n"
            f"The bot will now use its standard defaults (Language: `{DEFAULT_TTS_LANGUAGE}`, Slow: `{DEFAULT_TTS_SLOW}`) unless you specify options in `/tts`.",
            ephemeral=True
        )
    else:
        # User didn't have any defaults set
        await ctx.followup.send("🤷 You don't have any custom TTS defaults configured.", ephemeral=True)


# === TTS Command [MODIFIED for trimming] ===
@bot.slash_command(name="tts", description="Make the bot say something using Text-to-Speech.")
@commands.cooldown(1, 6, commands.BucketType.user) # Cooldown slightly longer due to generation/playback
async def tts(
    ctx: discord.ApplicationContext,
    message: discord.Option(str, description=f"The text you want the bot to speak (max {MAX_TTS_LENGTH} chars).", required=True), # type: ignore
    language: discord.Option(str, description="Override TTS language (uses your default if set, else bot default).", required=False, choices=TTS_LANGUAGE_CHOICES), # type: ignore
    slow: discord.Option(bool, description="Override slow speech (uses your default if set, else bot default).", required=False) # type: ignore
):
    # Defer ephemerally first. If successful, followup will show message being spoken.
    await ctx.defer(ephemeral=True)
    user = ctx.author
    guild = ctx.guild
    user_id_str = str(user.id)
    # Log requested overrides (will be None if not provided by user)
    bot_logger.info(f"COMMAND: /tts by {user.name} ({user_id_str}), explicit lang: {language}, explicit slow: {slow}, msg: '{message[:50]}...'")

    # --- Initial Validations ---
    if not guild or not isinstance(user, discord.Member) or not user.voice or not user.voice.channel:
        await ctx.followup.send("You must be in a voice channel in this server to use TTS.", ephemeral=True); return

    if len(message) > MAX_TTS_LENGTH:
         await ctx.followup.send(f"❌ Message too long! Maximum is {MAX_TTS_LENGTH} characters.", ephemeral=True); return
    if not message.strip():
         await ctx.followup.send("❌ Please provide some text for the bot to say.", ephemeral=True); return

    target_channel = user.voice.channel

    # --- Determine Final TTS Settings (Defaults Logic) ---
    user_config = user_sound_config.get(user_id_str, {})
    saved_defaults = user_config.get("tts_defaults", {})

    # Priority: Explicit Option > Saved Default > Bot Default
    final_language = language if language is not None else saved_defaults.get('language', DEFAULT_TTS_LANGUAGE)
    final_slow = slow if slow is not None else saved_defaults.get('slow', DEFAULT_TTS_SLOW)

    # Log the source of the final settings for debugging
    lang_source = "explicit" if language is not None else ("saved" if 'language' in saved_defaults else "bot default")
    slow_source = "explicit" if slow is not None else ("saved" if 'slow' in saved_defaults else "bot default")
    bot_logger.info(f"TTS Final Settings for {user.name}: lang={final_language} ({lang_source}), slow={final_slow} ({slow_source})")
    # --- End Settings Determination ---

    # --- Generate TTS Audio In Memory ---
    audio_source: Optional[discord.PCMAudio] = None
    pcm_fp = io.BytesIO() # BytesIO buffer to hold the final PCM data

    try:
        bot_logger.info(f"TTS: Generating audio for '{user.name}' (lang={final_language}, slow={final_slow})")
        # Create gTTS instance with final parameters
        tts_instance = gTTS(text=message, lang=final_language, slow=final_slow)

        # --- Use run_in_executor for Blocking IO (gTTS write + Pydub load/export) ---
        loop = asyncio.get_running_loop()
        def process_tts_sync():
            # 1. Write gTTS output (MP3) to an in-memory buffer
            mp3_fp = io.BytesIO()
            tts_instance.write_to_fp(mp3_fp)
            mp3_fp.seek(0) # Rewind buffer to the beginning
            # Check if gTTS actually produced data
            if mp3_fp.getbuffer().nbytes == 0:
                raise ValueError("gTTS generation resulted in empty audio data.")
            bot_logger.debug(f"TTS: Generated MP3 in memory ({mp3_fp.getbuffer().nbytes} bytes)")

            # 2. Load the MP3 data using Pydub
            audio_segment = AudioSegment.from_file(mp3_fp, format="mp3")
            bot_logger.debug(f"TTS: Loaded MP3 segment from memory (duration: {len(audio_segment)}ms)")

            # --- NEW: Trim TTS audio segment ---
            if len(audio_segment) > MAX_PLAYBACK_DURATION_MS:
                bot_logger.info(f"TTS: Trimming generated TTS from {len(audio_segment)}ms to first {MAX_PLAYBACK_DURATION_MS}ms.")
                audio_segment = audio_segment[:MAX_PLAYBACK_DURATION_MS]
            # --- END NEW ---

            # 3. Set desired Discord format (48kHz, stereo)
            audio_segment = audio_segment.set_frame_rate(48000).set_channels(2)

            # 4. Export the processed audio to the PCM buffer
            audio_segment.export(pcm_fp, format="s16le") # Signed 16-bit little-endian PCM
            pcm_fp.seek(0) # Rewind the PCM buffer

            # Check if Pydub export produced data
            if pcm_fp.getbuffer().nbytes == 0:
                raise ValueError("Pydub export resulted in empty PCM data.")
            bot_logger.debug(f"TTS: Converted to PCM in memory ({pcm_fp.getbuffer().nbytes} bytes)")
            # mp3_fp closes automatically when this function scope ends

        # Run the synchronous processing function in an executor thread
        await loop.run_in_executor(None, process_tts_sync)

        # If processing succeeded, create the PCMAudio source from the PCM buffer
        audio_source = discord.PCMAudio(pcm_fp) # PCMAudio takes ownership of the buffer
        bot_logger.info(f"TTS: Successfully created PCMAudio source for {user.name}.")

    except gTTSError as e:
        # Handle specific gTTS errors (like invalid language)
        msg = f"❌ TTS Error: Language '{final_language}' might be unsupported or invalid." if "Language not found" in str(e) else f"❌ TTS Generation Error: {e}"
        await ctx.followup.send(msg, ephemeral=True)
        bot_logger.error(f"TTS Generation Error (gTTS) for {user.name} (lang={final_language}): {e}", exc_info=True)
        pcm_fp.close() # Ensure buffer is closed on error
        return
    except (ImportError, ValueError, FileNotFoundError, Exception) as e: # Catch Pydub/FFmpeg issues or other errors
        # FileNotFoundError can happen if FFmpeg isn't found by pydub
        # ValueError can happen from empty data checks
        err_type = type(e).__name__
        if isinstance(e, FileNotFoundError) and 'ffmpeg' in str(e).lower():
             msg = "❌ Error during TTS processing: FFmpeg executable not found. Please ensure it's installed and in the system PATH."
        elif isinstance(e, ValueError):
             msg = f"❌ Error during TTS processing: {e}" # e.g., "empty audio data"
        else:
            msg = f"❌ An unexpected error occurred during TTS audio processing ({err_type})."
        await ctx.followup.send(msg, ephemeral=True)
        bot_logger.error(f"TTS: Failed generation/processing for {user.name}: {e}", exc_info=True)
        pcm_fp.close() # Ensure buffer is closed on error
        return

    # --- Playback ---
    if not audio_source: # Should not happen if errors were caught, but safety check
        await ctx.followup.send("❌ Failed to prepare TTS audio for playback.", ephemeral=True)
        bot_logger.error("TTS: Audio source was None after processing block, despite no caught exceptions.")
        pcm_fp.close() # Ensure buffer is closed
        return

    # Use the helper for connection, permission, and busy checks
    voice_client = await _ensure_voice_client_ready(ctx.interaction, target_channel, action_type="TTS")
    if not voice_client:
        # Helper failed (and sent feedback). PCMAudio object might exist but wasn't played.
        # The PCMAudio object holds the pcm_fp, and its __del__ should close it,
        # but explicitly closing is safer if the object might not be garbage collected immediately.
        pcm_fp.close()
        return

    # Final check before playing
    if voice_client.is_playing():
         bot_logger.warning(f"TTS: VC became busy between check and play for {user.name}. Aborting.")
         await ctx.followup.send("⏳ The bot became busy just before speaking. Please try again.", ephemeral=True)
         after_play_handler(None, voice_client) # Ensure handler runs for potential disconnect
         pcm_fp.close() # Clean up buffer if play fails here
         return

    # Play the generated TTS Audio
    try:
        bot_logger.info(f"TTS PLAYBACK: Playing TTS requested by {user.display_name}...")
        # The 'after' callback ensures the standard handler runs AND the PCM buffer is closed
        # Note: PCMAudio might try to close it too, but closing a BytesIO twice is safe.
        voice_client.play(audio_source, after=lambda e: (after_play_handler(e, voice_client), pcm_fp.close()))

        # Send confirmation message showing *actual* settings used and the message text
        speed_str = "(slow)" if final_slow else ""
        # Get friendly language name
        lang_name = final_language
        for choice in TTS_LANGUAGE_CHOICES:
            if choice.value == final_language: lang_name = choice.name; break

        # Truncate message for display in confirmation
        display_message = message[:150] + ('...' if len(message) > 150 else '')
        await ctx.followup.send(f"🗣️ Now saying in **{lang_name}** {speed_str} (max {MAX_PLAYBACK_DURATION_MS/1000}s): \"{display_message}\"", ephemeral=True)

    except discord.errors.ClientException as e:
        # Handle "Already playing" etc.
        msg = "❌ Error: Bot is already playing or encountered a client issue."
        await ctx.followup.send(msg, ephemeral=True)
        bot_logger.error(f"TTS PLAYBACK ERROR (ClientException): {e}", exc_info=True)
        after_play_handler(e, voice_client) # Call handler
        pcm_fp.close() # Ensure buffer closed on error
    except Exception as e:
        # Catch other unexpected playback errors
        await ctx.followup.send("❌ An unexpected error occurred during TTS playback.", ephemeral=True)
        bot_logger.error(f"TTS PLAYBACK ERROR (Unexpected): {e}", exc_info=True)
        after_play_handler(e, voice_client) # Call handler
        pcm_fp.close() # Ensure buffer closed on error


# --- Error Handler for Application Commands [UNCHANGED] ---
@bot.event
async def on_application_command_error(ctx: discord.ApplicationContext, error: discord.DiscordException):
    """Global handler for slash command errors."""
    # Extract command name and user if possible
    cmd_name = ctx.command.qualified_name if ctx.command else "Unknown Command"
    user_name = f"{ctx.author.name} ({ctx.author.id})" if ctx.author else "Unknown User"
    log_prefix = f"CMD ERROR (/{cmd_name}, user: {user_name}):"

    # Helper to send error message, checking if interaction is already responded/deferred
    async def send_error_response(message: str, log_level=logging.WARNING):
        bot_logger.log(log_level, f"{log_prefix} {message} (Error Type: {type(error).__name__}, Details: {error})")
        try:
            if ctx.interaction.response.is_done():
                # If already responded or deferred, use followup
                await ctx.followup.send(message, ephemeral=True)
            else:
                # Otherwise, use respond
                await ctx.respond(message, ephemeral=True)
        except discord.NotFound:
            bot_logger.warning(f"{log_prefix} Interaction expired before error response could be sent.")
        except discord.Forbidden:
            bot_logger.error(f"{log_prefix} Missing permissions to send error response in {ctx.channel.name}.")
        except Exception as e_resp:
            bot_logger.error(f"{log_prefix} Failed to send error response: {e_resp}")

    # --- Handle Specific Error Types ---
    if isinstance(error, commands.CommandOnCooldown):
        await send_error_response(f"⏳ This command is on cooldown. Please wait {error.retry_after:.1f} seconds.")

    elif isinstance(error, commands.MissingPermissions):
        perms_needed = ', '.join(f"`{p}`" for p in error.missing_permissions)
        await send_error_response(f"🚫 You lack the required permissions to use this command: {perms_needed}", log_level=logging.WARNING)

    elif isinstance(error, commands.BotMissingPermissions):
        perms_needed = ', '.join(f"`{p}`" for p in error.missing_permissions)
        await send_error_response(f"🚫 I lack the required permissions to perform this action: {perms_needed}. Please grant them in channel/server settings.", log_level=logging.ERROR)

    elif isinstance(error, commands.CheckFailure):
        # Generic check failure (like custom checks or has_permissions failing outside of specific types above)
        await send_error_response("🚫 You do not have permission to use this command or failed a check.")

    elif isinstance(error, discord.errors.ApplicationCommandInvokeError):
        # Error occurred *within* the command's code
        original_error = error.original
        bot_logger.error(f"{log_prefix} An error occurred within the command code: {original_error}", exc_info=original_error)

        # Provide more specific feedback for common underlying errors if possible
        if isinstance(original_error, FileNotFoundError) and 'ffmpeg' in str(original_error).lower():
             user_msg = "❌ Internal Error: The bot requires FFmpeg for audio processing, but it wasn't found."
        elif isinstance(original_error, CouldntDecodeError):
             user_msg = "❌ Internal Error: Failed to decode an audio file. It might be corrupted or an unsupported format/codec."
        elif isinstance(original_error, gTTSError):
            user_msg = f"❌ Internal Error generating TTS: {original_error}"
        elif isinstance(original_error, discord.errors.Forbidden):
             user_msg = "❌ Internal Error: The bot encountered a permissions issue while executing the command."
        else:
            user_msg = "❌ An unexpected internal error occurred while running this command. Please report this if it persists."

        await send_error_response(user_msg, log_level=logging.ERROR)

    else:
        # Handle other Discord API or library errors not specifically caught above
        bot_logger.error(f"{log_prefix} An unexpected DiscordException occurred: {error}", exc_info=error)
        await send_error_response(f"❌ An unexpected error occurred ({type(error).__name__}). Please try again later.", log_level=logging.ERROR)


# --- Run the Bot [UNCHANGED] ---
if __name__ == "__main__":
    # Critical Pre-checks
    if not PYDUB_AVAILABLE:
        bot_logger.critical("Pydub library is not available (failed import). The bot cannot function without it.")
        bot_logger.critical("Install it using: pip install pydub")
        bot_logger.critical("Ensure FFmpeg is also installed and accessible in your system's PATH.")
        exit(1)
    if not BOT_TOKEN:
        bot_logger.critical("BOT_TOKEN environment variable not found. Create a .env file or set the environment variable.")
        exit(1)

    # Opus Check (Essential for Discord Voice)
    opus_loaded = discord.opus.is_loaded()
    if not opus_loaded:
        bot_logger.warning("Default Opus load failed. Attempting explicit load paths...")
        # Common library names/paths - Adjust if your system differs significantly
        # Order might matter depending on system preference
        opus_paths = [
            "libopus.so.0",       # Linux (Debian/Ubuntu typical)
            "libopus.so",         # Linux (Generic)
            "opus",               # macOS (Homebrew link name) or some Linux installs
            "libopus-0.dll",      # Windows (Older name?)
            "opus.dll",           # Windows (Newer name?)
            "/usr/local/lib/libopus.so.0", # Common custom install path
            "/opt/homebrew/opt/opus/lib/libopus.0.dylib", # macOS Apple Silicon Homebrew path
            # Add more potential paths if needed
        ]
        for path in opus_paths:
            try:
                discord.opus.load_opus(path)
                if discord.opus.is_loaded():
                    bot_logger.info(f"Opus library loaded successfully from: {path}")
                    opus_loaded = True; break # Stop searching once loaded
            except OSError: # Library not found at this path
                 bot_logger.debug(f"Opus load failed for path: {path} (Not found)")
            except Exception as e: # Other load errors (e.g., wrong architecture)
                 bot_logger.warning(f"Opus load failed for path: {path} (Error: {e})")

        if not opus_loaded:
            bot_logger.critical("CRITICAL: Opus library could NOT be loaded from any known path.")
            bot_logger.critical("Voice communication WILL FAIL.")
            bot_logger.critical("Please install the Opus library (e.g., 'sudo apt install libopus-dev', 'brew install opus', or download DLLs for Windows) and ensure discord.py can find it.")
            # Consider exiting here if Opus is absolutely mandatory: exit(1)
            # Or let it run with broken voice, logging the critical failure. (Current choice)

    # Start the bot
    try:
        bot_logger.info("Attempting to start the bot...")
        bot.run(BOT_TOKEN)
    except discord.errors.LoginFailure:
        bot_logger.critical("CRITICAL STARTUP ERROR: Login Failure. Check if the BOT_TOKEN is correct.")
    except discord.errors.PrivilegedIntentsRequired as e:
        bot_logger.critical(f"CRITICAL STARTUP ERROR: Missing Privileged Intents. Enable them in the Developer Portal: {e}")
    except Exception as e:
        # Catch-all for other potential startup errors
        log_level = logging.CRITICAL if not opus_loaded and "opus" in str(e).lower() else logging.ERROR
        bot_logger.log(log_level, f"FATAL RUNTIME ERROR during bot startup or execution: {e}", exc_info=True)
        exit(1) # Exit on fatal runtime errors