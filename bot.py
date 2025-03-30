# bot.py

import discord
from discord.ext import commands
import os
import json
import asyncio
from gtts import gTTS
import logging
import io # Required for BytesIO
import math # For checking infinite values in dBFS
from collections import deque # Efficient queue structure
import re # For cleaning filenames
from typing import List, Optional # For type hinting

# Load environment variables first
from dotenv import load_dotenv
load_dotenv()

# Import pydub safely
try:
    from pydub import AudioSegment
    from pydub.exceptions import CouldntDecodeError
    PYDUB_AVAILABLE = True
except ImportError:
    # Basic logging setup just for this critical error if the main one fails
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')
    logging.critical("CRITICAL: Pydub library not found. Please install it: pip install pydub ffmpeg")
    PYDUB_AVAILABLE = False

# --- Configuration ---
BOT_TOKEN = os.getenv('BOT_TOKEN')
SOUNDS_DIR = "sounds" # For join sounds
USER_SOUNDS_DIR = "usersounds" # For user-uploaded command sounds / personal soundboards
# SOUNDBOARD_DIR = "soundboard" # No longer used for the primary user panel functionality
CONFIG_FILE = "user_sounds.json" # For join sound mappings (user_id -> filename)
TARGET_LOUDNESS_DBFS = -14.0
MAX_USER_SOUND_SIZE_MB = 5
MAX_USER_SOUNDS_PER_USER = 25 # Increased limit slightly as these are now the panel sounds too
ALLOWED_EXTENSIONS = ['.mp3', '.wav', '.ogg', '.m4a', '.aac'] # Allowed extensions for uploads

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')
discord_logger = logging.getLogger('discord')
discord_logger.setLevel(logging.WARNING)
bot_logger = logging.getLogger('JoinSoundBot')
bot_logger.setLevel(logging.INFO)

# --- Validate Critical Config ---
if not BOT_TOKEN:
    bot_logger.critical("CRITICAL ERROR: Bot token (BOT_TOKEN) not found in environment variables or .env file.")
    exit()
if not PYDUB_AVAILABLE:
    bot_logger.critical("CRITICAL ERROR: Pydub library failed to import. Cannot process audio.")
    exit()

# --- Intents ---
intents = discord.Intents.default()
intents.voice_states = True
intents.guilds = True
# intents.message_content = False # Still not needed

# --- Bot Definition ---
bot = discord.Bot(intents=intents)

# --- Data Storage & Helpers ---
user_sound_config = {} # Maps user ID (str) to their custom *join* sound filename (str)
guild_sound_queues = {}
guild_play_tasks = {}

def load_config():
    global user_sound_config
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                user_sound_config = json.load(f)
            bot_logger.info(f"Loaded {len(user_sound_config)} join sound configs from {CONFIG_FILE}")
        except json.JSONDecodeError as e:
             bot_logger.error(f"Error decoding JSON from {CONFIG_FILE}: {e}", exc_info=True)
             user_sound_config = {}
        except Exception as e:
             bot_logger.error(f"Error loading join sound config {CONFIG_FILE}: {e}", exc_info=True)
             user_sound_config = {}
    else:
        user_sound_config = {}
        bot_logger.info(f"Join sound config file {CONFIG_FILE} not found. Starting fresh.")

def save_config():
     try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(user_sound_config, f, indent=4)
        bot_logger.debug(f"Saved {len(user_sound_config)} join sound configs to {CONFIG_FILE}")
     except Exception as e:
         bot_logger.error(f"Error saving join sound config to {CONFIG_FILE}: {e}", exc_info=True)

# --- Create Directories ---
def ensure_dir(dir_path: str):
    if not os.path.exists(dir_path):
        try:
            os.makedirs(dir_path)
            bot_logger.info(f"Created directory: {dir_path}")
        except Exception as e:
            bot_logger.critical(f"CRITICAL: Could not create directory '{dir_path}': {e}", exc_info=True)
            if dir_path in [SOUNDS_DIR, USER_SOUNDS_DIR]: # Removed SOUNDBOARD_DIR
                exit(f"Failed to create essential directory: {dir_path}")

ensure_dir(SOUNDS_DIR)
ensure_dir(USER_SOUNDS_DIR)
# ensure_dir(SOUNDBOARD_DIR) # No longer strictly needed by core logic

# --- Bot Events ---
@bot.event
async def on_ready():
    bot_logger.info(f'Logged in as {bot.user.name} ({bot.user.id})')
    load_config()
    bot_logger.info('------')
    bot_logger.info(f"Py-cord Version: {discord.__version__}")
    bot_logger.info(f"Audio Normalization Target: {TARGET_LOUDNESS_DBFS} dBFS")
    bot_logger.info(f"Allowed Upload Extensions: {', '.join(ALLOWED_EXTENSIONS)}")
    bot_logger.info(f"Join sound directory: {os.path.abspath(SOUNDS_DIR)}")
    bot_logger.info(f"User sounds directory: {os.path.abspath(USER_SOUNDS_DIR)}")
    # bot_logger.info(f"Soundboard directory: {os.path.abspath(SOUNDBOARD_DIR)}") # Commented out
    bot_logger.info("Join Sound Bot is operational.")


# --- Audio Processing Helper ---
# [NO CHANGES NEEDED IN process_audio]
def process_audio(sound_path: str, member_display_name: str = "User") -> Optional[discord.PCMAudio]:
    """Loads, normalizes, and prepares audio returning a PCMAudio source or None."""
    if not PYDUB_AVAILABLE:
        bot_logger.error("Pydub not available, cannot process audio.")
        return None
    if not os.path.exists(sound_path):
        bot_logger.error(f"AUDIO: File not found during processing attempt: '{sound_path}'")
        return None

    audio_source = None
    try:
        bot_logger.debug(f"AUDIO: Loading '{os.path.basename(sound_path)}'...")
        file_extension = os.path.splitext(sound_path)[1].lower().strip('. ')
        if not file_extension:
             bot_logger.warning(f"AUDIO: No extension found for {sound_path}, assuming mp3.")
             file_extension = 'mp3'

        audio_segment = AudioSegment.from_file(sound_path, format=file_extension)

        # --- Normalization ---
        peak_dbfs = audio_segment.max_dBFS
        if not math.isinf(peak_dbfs) and peak_dbfs > -90.0: # Avoid processing complete silence
            change_in_dbfs = TARGET_LOUDNESS_DBFS - peak_dbfs
            bot_logger.info(f"AUDIO: Normalizing '{os.path.basename(sound_path)}' for {member_display_name}. Peak:{peak_dbfs:.2f} Target:{TARGET_LOUDNESS_DBFS:.2f} Gain:{change_in_dbfs:.2f} dB.")
            if change_in_dbfs < 0:
                 audio_segment = audio_segment.apply_gain(change_in_dbfs)
            else:
                 bot_logger.info(f"AUDIO: Skipping positive gain ({change_in_dbfs:.2f}dB) for '{os.path.basename(sound_path)}'.")
        elif math.isinf(peak_dbfs):
            bot_logger.warning(f"AUDIO: Cannot normalize silent sound ('{os.path.basename(sound_path)}'). Peak is -inf.")
        else:
            bot_logger.warning(f"AUDIO: Skipping normalization for very quiet sound ('{os.path.basename(sound_path)}'). Peak: {peak_dbfs:.2f} below -90 dBFS.")

        # --- Resampling and Channel Conversion (Discord prefers 48kHz stereo) ---
        audio_segment = audio_segment.set_frame_rate(48000).set_channels(2)

        # --- Export to Raw PCM for Discord ---
        pcm_data_io = io.BytesIO()
        audio_segment.export(pcm_data_io, format="s16le")
        pcm_data_io.seek(0)

        if pcm_data_io.getbuffer().nbytes > 0:
            audio_source = discord.PCMAudio(pcm_data_io)
            bot_logger.debug(f"AUDIO: Successfully processed '{os.path.basename(sound_path)}'")
        else:
            bot_logger.error(f"AUDIO: Exported raw audio data for '{os.path.basename(sound_path)}' is empty!")

    except CouldntDecodeError:
         bot_logger.error(f"AUDIO: Pydub CouldntDecodeError for '{os.path.basename(sound_path)}'. Is FFmpeg installed and in PATH? Is the file corrupted or an unsupported format?", exc_info=True)
    except FileNotFoundError:
        bot_logger.error(f"AUDIO: File not found during processing: '{sound_path}'")
    except Exception as e:
        bot_logger.error(f"AUDIO: Unexpected error processing '{os.path.basename(sound_path)}' for {member_display_name}: {e}", exc_info=True)

    return audio_source


# --- Core Join Sound Queue Logic (Queue Processor) ---
# [NO CHANGES NEEDED IN play_next_in_queue, on_voice_state_update, after_play_handler, safe_disconnect]
async def play_next_in_queue(guild: discord.Guild):
    """Processes the join sound queue for a given guild."""
    guild_id = guild.id
    task_id = asyncio.current_task().get_name() if asyncio.current_task() else 'Unknown Task'
    bot_logger.debug(f"QUEUE CHECK [{task_id}]: Checking queue for guild {guild_id}")

    if guild_id not in guild_sound_queues or not guild_sound_queues[guild_id]:
        bot_logger.debug(f"QUEUE [{task_id}]: Empty or non-existent for guild {guild_id}. Attempting disconnect.")
        await safe_disconnect(discord.utils.get(bot.voice_clients, guild=guild))
        if guild_id in guild_play_tasks and guild_play_tasks[guild_id] is asyncio.current_task():
             del guild_play_tasks[guild_id]
             bot_logger.debug(f"QUEUE [{task_id}]: Removed self from play tasks for guild {guild_id}.")
        return

    voice_client = discord.utils.get(bot.voice_clients, guild=guild)
    if not voice_client or not voice_client.is_connected():
        bot_logger.warning(f"QUEUE [{task_id}]: Play task running for {guild_id}, but bot is not connected. Clearing queue.")
        if guild_id in guild_sound_queues: guild_sound_queues[guild_id].clear()
        if guild_id in guild_play_tasks and guild_play_tasks[guild_id] is asyncio.current_task(): del guild_play_tasks[guild_id]
        return

    if voice_client.is_playing():
        bot_logger.debug(f"QUEUE [{task_id}]: Bot is already playing in guild {guild_id}, play_next_in_queue will yield.")
        return

    try:
        member, sound_path = guild_sound_queues[guild_id].popleft()
        bot_logger.info(f"QUEUE [{task_id}]: Processing join sound for {member.display_name} in {guild.name}. Path: {os.path.basename(sound_path)}. Remaining: {len(guild_sound_queues[guild_id])}")
    except IndexError:
        bot_logger.debug(f"QUEUE [{task_id}]: Became empty unexpectedly for guild {guild_id} after play check.")
        await safe_disconnect(voice_client)
        if guild_id in guild_play_tasks and guild_play_tasks[guild_id] is asyncio.current_task(): del guild_play_tasks[guild_id]
        return

    audio_source = process_audio(sound_path, member.display_name)

    if audio_source:
        try:
            bot_logger.info(f"QUEUE PLAYBACK [{task_id}]: Playing join sound for {member.display_name}...")
            voice_client.play(audio_source, after=lambda e: after_play_handler(e, voice_client)) # Pass voice_client
            bot_logger.debug(f"QUEUE PLAYBACK [{task_id}]: vc.play() called for join sound of {member.display_name}.")
        except discord.errors.ClientException as e:
            bot_logger.error(f"QUEUE PLAYBACK ERROR [{task_id}] (ClientException): Bot potentially already playing or disconnected unexpectedly. {e}", exc_info=True)
            bot.loop.create_task(play_next_in_queue(guild), name=f"QueueRetry_{guild_id}")
        except Exception as e:
            bot_logger.error(f"QUEUE PLAYBACK ERROR [{task_id}] (Unexpected): {e}", exc_info=True)
            bot.loop.create_task(play_next_in_queue(guild), name=f"QueueRetry_{guild_id}")
    else:
        bot_logger.warning(f"QUEUE PLAYBACK [{task_id}]: No valid audio source for {member.display_name}'s join sound ({os.path.basename(sound_path)}). Skipping.")
        bot.loop.create_task(play_next_in_queue(guild), name=f"QueueSkip_{guild_id}")

@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot: return

    if after.channel is not None and before.channel != after.channel:
        channel_to_join = after.channel
        guild = member.guild
        bot_logger.info(f"EVENT: {member.display_name} ({member.id}) entered voice channel {channel_to_join.name} ({channel_to_join.id}) in guild {guild.name} ({guild.id})")

        bot_perms = channel_to_join.permissions_for(guild.me)
        if not bot_perms.connect or not bot_perms.speak:
            bot_logger.warning(f"Missing Connect ({bot_perms.connect}) or Speak ({bot_perms.speak}) permission in '{channel_to_join.name}'. Cannot play join sound for {member.display_name}.")
            return

        sound_path: Optional[str] = None
        is_tts = False
        user_id_str = str(member.id)

        if user_id_str in user_sound_config:
            sound_filename = user_sound_config[user_id_str]
            potential_path = os.path.join(SOUNDS_DIR, sound_filename)
            if os.path.exists(potential_path):
                sound_path = potential_path
                bot_logger.info(f"SOUND: Using configured join sound: '{sound_filename}' for {member.display_name}")
            else:
                bot_logger.warning(f"SOUND: Configured join sound file '{sound_filename}' for user {user_id_str} not found at '{potential_path}'. Removing broken config entry and falling back to TTS.")
                del user_sound_config[user_id_str]
                save_config()
                is_tts = True
        else:
            is_tts = True
            bot_logger.info(f"SOUND: No custom join sound config found for {member.display_name} ({user_id_str}). Using TTS.")

        if is_tts:
            tts_filename = f"tts_{member.id}.mp3"
            tts_path = os.path.join(SOUNDS_DIR, tts_filename)
            if not os.path.exists(tts_path):
                bot_logger.info(f"TTS: Generating for {member.display_name} ('{tts_path}')...")
                tts_text = f"{member.display_name} joined"
                try:
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, lambda: gTTS(text=tts_text, lang='en').save(tts_path))
                    bot_logger.info(f"TTS: Saved '{tts_path}'")
                    sound_path = tts_path
                except Exception as e:
                    bot_logger.error(f"TTS: Failed generation for {member.display_name}: {e}", exc_info=True)
                    sound_path = None
            else:
                 bot_logger.info(f"TTS: Using existing file: '{tts_path}'")
                 sound_path = tts_path

        if not sound_path:
            bot_logger.error(f"Could not determine or generate a join sound/TTS path for {member.display_name}. Skipping queue add.")
            return

        guild_id = guild.id
        if guild_id not in guild_sound_queues:
            guild_sound_queues[guild_id] = deque()

        queue_item = (member, sound_path)
        guild_sound_queues[guild_id].append(queue_item)
        bot_logger.info(f"QUEUE: Added join sound for {member.display_name} to queue for guild {guild.name}. Queue size: {len(guild_sound_queues[guild_id])}")

        voice_client = discord.utils.get(bot.voice_clients, guild=guild)

        if voice_client and voice_client.is_playing():
            bot_logger.info(f"VOICE: Bot is currently playing in {guild.name}. Join sound for {member.display_name} queued. Connection/play deferred.")
            if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
                 task_name = f"QueueTrigger_{guild_id}"
                 guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(guild), name=task_name)
                 bot_logger.debug(f"VOICE: Created deferred play task '{task_name}' due to active playback.")
            return

        should_start_play_task = False
        try:
            if not voice_client or not voice_client.is_connected():
                bot_logger.info(f"VOICE: Connecting to '{channel_to_join.name}' to start join sound queue processing.")
                voice_client = await channel_to_join.connect(timeout=30.0, reconnect=True)
                bot_logger.info(f"VOICE: Successfully connected to '{channel_to_join.name}'.")
                should_start_play_task = True
            elif voice_client.channel != channel_to_join:
                 bot_logger.info(f"VOICE: Moving from '{voice_client.channel.name}' to '{channel_to_join.name}' to process join sound queue.")
                 await voice_client.move_to(channel_to_join)
                 bot_logger.info(f"VOICE: Successfully moved to '{channel_to_join.name}'.")
                 should_start_play_task = True
            else:
                 bot_logger.debug(f"VOICE: Bot already connected in '{channel_to_join.name}' and not playing.")
                 should_start_play_task = True

        except asyncio.TimeoutError:
            bot_logger.error(f"VOICE: Connection to '{channel_to_join.name}' timed out.")
            if guild_id in guild_sound_queues: guild_sound_queues[guild_id].clear()
        except discord.errors.ClientException as e:
            bot_logger.error(f"VOICE: ClientException during connect/move to '{channel_to_join.name}': {e}", exc_info=True)
            if guild_id in guild_sound_queues: guild_sound_queues[guild_id].clear()
        except Exception as e:
             bot_logger.error(f"VOICE: Unexpected error during connect/move to '{channel_to_join.name}': {e}", exc_info=True)
             if guild_id in guild_sound_queues: guild_sound_queues[guild_id].clear()

        if should_start_play_task and voice_client and voice_client.is_connected():
            if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
                task_name = f"QueueStart_{guild_id}"
                guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(guild), name=task_name)
                bot_logger.info(f"VOICE: Started play task '{task_name}' for guild {guild_id}.")
            else:
                 bot_logger.debug(f"VOICE: Play task for guild {guild_id} already exists and is not done.")

def after_play_handler(error: Optional[Exception], voice_client: discord.VoiceClient):
    """Callback registered in voice_client.play(). Runs after ANY sound finishes."""
    guild_id = voice_client.guild.id if voice_client and voice_client.guild else None
    if error:
        bot_logger.error(f'PLAYBACK ERROR (In after_play_handler for guild {guild_id}): {error}', exc_info=error)

    if not guild_id:
        bot_logger.warning("after_play_handler called with invalid/disconnected voice_client or no guild.")
        return

    bot_logger.debug(f"Playback finished for guild {guild_id}. Triggering queue check.")

    if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
         task_name = f"QueueCheckAfterPlay_{guild_id}"
         guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(voice_client.guild), name=task_name)
         bot_logger.debug(f"AFTER_PLAY: Scheduled task '{task_name}' for guild {guild_id}.")
    else:
         bot_logger.debug(f"AFTER_PLAY: Task for guild {guild_id} already exists, not creating duplicate check task.")

async def safe_disconnect(voice_client: Optional[discord.VoiceClient]):
    """Safely disconnects if connected, not playing, AND join queue is empty."""
    if not voice_client or not voice_client.is_connected():
        return

    guild = voice_client.guild
    guild_id = guild.id

    is_queue_empty = guild_id not in guild_sound_queues or not guild_sound_queues[guild_id]
    is_playing = voice_client.is_playing()

    if is_queue_empty and not is_playing:
        bot_logger.info(f"DISCONNECT: Conditions met for guild {guild_id} (Queue empty, not playing). Disconnecting...")
        try:
            if voice_client.is_playing(): voice_client.stop()
            await voice_client.disconnect(force=False)
            bot_logger.info(f"DISCONNECT: Bot disconnected from '{guild.name}'.")
            if guild_id in guild_play_tasks:
                 del guild_play_tasks[guild_id]
                 bot_logger.debug(f"DISCONNECT: Removed play task tracker for guild {guild_id}.")
        except Exception as e:
            bot_logger.error(f"DISCONNECT ERROR: Failed to disconnect from {guild.name}: {e}", exc_info=True)
    else:
         bot_logger.debug(f"Disconnect skipped for guild {guild.name}: Queue empty={is_queue_empty}, Playing={is_playing}.")


# --- Single Sound Playback Logic ---
# [NO CHANGES NEEDED IN play_single_sound - it handles paths correctly]
async def play_single_sound(interaction: discord.Interaction, sound_path: str):
    """Connects (if needed), plays a single sound, and uses after_play_handler."""
    # NOTE: interaction.user is the user who clicked the button or ran /playsound
    user = interaction.user
    guild = interaction.guild

    # Use followup for responses as the initial response was likely deferred
    if not guild:
        await interaction.followup.send("This command only works in a server.", ephemeral=True)
        return

    if not user.voice or not user.voice.channel:
        await interaction.followup.send("You need to be in a voice channel to use this sound.", ephemeral=True)
        return

    target_channel = user.voice.channel
    guild_id = guild.id

    bot_perms = target_channel.permissions_for(guild.me)
    if not bot_perms.connect or not bot_perms.speak:
        await interaction.followup.send(f"❌ I don't have permission to Connect or Speak in {target_channel.mention}.", ephemeral=True)
        return

    if not os.path.exists(sound_path):
         await interaction.followup.send("❌ Error: The sound file seems to be missing or was deleted.", ephemeral=True)
         bot_logger.error(f"SINGLE PLAY: File not found: {sound_path}")
         return

    voice_client = discord.utils.get(bot.voice_clients, guild=guild)

    try:
        if voice_client and voice_client.is_connected():
            if voice_client.is_playing():
                await interaction.followup.send("⏳ Bot is currently playing another sound. Please wait a moment.", ephemeral=True)
                bot_logger.info(f"SINGLE PLAY: Bot busy in {guild.name}, user {user.name} tried to play '{os.path.basename(sound_path)}'. Request ignored.")
                return
            elif voice_client.channel != target_channel:
                bot_logger.info(f"SINGLE PLAY: Moving from '{voice_client.channel.name}' to '{target_channel.name}' for {user.name}.")
                await voice_client.move_to(target_channel)
                bot_logger.info(f"SINGLE PLAY: Moved successfully.")
        else:
            bot_logger.info(f"SINGLE PLAY: Connecting to '{target_channel.name}' for {user.name}.")
            voice_client = await target_channel.connect(timeout=30.0, reconnect=True)
            bot_logger.info(f"SINGLE PLAY: Connected successfully.")

        if not voice_client or not voice_client.is_connected():
             bot_logger.error(f"SINGLE PLAY: Failed to establish voice client for {target_channel.name}")
             await interaction.followup.send("❌ Failed to connect to the voice channel.", ephemeral=True)
             return

    except asyncio.TimeoutError:
         await interaction.followup.send("❌ Connection to the voice channel timed out.", ephemeral=True)
         bot_logger.error(f"SINGLE PLAY: Connection/Move Timeout in {guild.name}")
         return
    except discord.errors.ClientException as e:
        await interaction.followup.send("❌ Error connecting/moving voice channel. Maybe check permissions?", ephemeral=True)
        bot_logger.error(f"SINGLE PLAY: Connection/Move ClientException in {guild.name}: {e}", exc_info=True)
        return
    except Exception as e:
        await interaction.followup.send("❌ An unexpected error occurred trying to join the voice channel.", ephemeral=True)
        bot_logger.error(f"SINGLE PLAY: Connection/Move unexpected error in {guild.name}: {e}", exc_info=True)
        return

    bot_logger.info(f"SINGLE PLAY: Processing '{os.path.basename(sound_path)}' for {user.name}...")
    # Use the display name of the user who initiated the action for logging clarity
    audio_source = process_audio(sound_path, user.display_name)

    if audio_source:
        if voice_client.is_playing():
             bot_logger.warning(f"SINGLE PLAY: Voice client became busy between check and play call for {user.name}. Aborting playback.")
             await interaction.followup.send("⏳ Bot became busy just now. Please try again.", ephemeral=True)
             return

        try:
            sound_basename = os.path.basename(sound_path)
            bot_logger.info(f"SINGLE PLAYBACK: Playing '{sound_basename}' requested by {user.display_name}...")
            voice_client.play(audio_source, after=lambda e: after_play_handler(e, voice_client))
            # Confirmation message - ephemeral is good here
            await interaction.followup.send(f"▶️ Playing `{os.path.splitext(sound_basename)[0]}`...", ephemeral=True)
        except discord.errors.ClientException as e:
            await interaction.followup.send("❌ Error: Already playing audio or another client issue occurred.", ephemeral=True)
            bot_logger.error(f"SINGLE PLAYBACK ERROR (ClientException): {e}", exc_info=True)
            after_play_handler(e, voice_client)
        except Exception as e:
            await interaction.followup.send("❌ An unexpected error occurred during playback.", ephemeral=True)
            bot_logger.error(f"SINGLE PLAYBACK ERROR (Unexpected): {e}", exc_info=True)
            after_play_handler(e, voice_client)
    else:
        await interaction.followup.send("❌ Error: Could not process the audio file. Check bot logs.", ephemeral=True)
        bot_logger.error(f"SINGLE PLAYBACK: Failed to get audio source for '{sound_path}'")
        if voice_client and voice_client.is_connected():
            after_play_handler(None, voice_client)

# --- Helper: Sanitize Filename ---
# [NO CHANGES NEEDED]
def sanitize_filename(name: str) -> str:
    """Removes disallowed characters for filenames and limits length."""
    name = re.sub(r'[<>:"/\\|?*\.\s]+', '_', name)
    name = re.sub(r'_+', '_', name) # Collapse multiple underscores
    name = name.strip('_')
    return name[:50]

# --- Helper: Get User Sound Files ---
# [NO CHANGES NEEDED - already works with USER_SOUNDS_DIR]
def get_user_sound_files(user_id: int) -> List[str]:
    """Returns a list of sound basenames (without ext) for a user's command sounds."""
    user_dir = os.path.join(USER_SOUNDS_DIR, str(user_id))
    sounds = []
    if os.path.isdir(user_dir):
        try:
            for filename in os.listdir(user_dir):
                filepath = os.path.join(user_dir, filename)
                base_name, ext = os.path.splitext(filename)
                if os.path.isfile(filepath) and ext.lower() in ALLOWED_EXTENSIONS:
                    sounds.append(base_name) # Return name without extension
        except OSError as e:
            bot_logger.error(f"Error listing files in user sound directory {user_dir}: {e}")
    return sounds

# --- Helper: Find User Sound Path ---
# [NO CHANGES NEEDED - already works with USER_SOUNDS_DIR]
def find_user_sound_path(user_id: int, sound_name: str) -> Optional[str]:
    """Finds the full path for a user's command sound by name, checking allowed extensions."""
    user_dir = os.path.join(USER_SOUNDS_DIR, str(user_id))
    if not os.path.isdir(user_dir):
        return None
    preferred_order = ['.mp3', '.wav'] + [ext for ext in ALLOWED_EXTENSIONS if ext not in ['.mp3', '.wav']]
    for ext in preferred_order:
        potential_path_exact = os.path.join(user_dir, f"{sound_name}{ext}")
        if os.path.exists(potential_path_exact):
            return potential_path_exact
    bot_logger.debug(f"Sound '{sound_name}' not found for user {user_id} in {user_dir} with extensions {ALLOWED_EXTENSIONS}")
    return None

# --- Autocomplete Functions ---
# [NO CHANGES NEEDED - already works with USER_SOUNDS_DIR]
async def user_sound_autocomplete(ctx: discord.AutocompleteContext) -> List[str]:
    """Provides autocomplete suggestions for the user's uploaded command sounds."""
    user_id = ctx.interaction.user.id
    try:
        user_sounds = get_user_sound_files(user_id)
        current_value = ctx.value.lower() if ctx.value else ""
        suggestions = [
            name for name in user_sounds if current_value in name.lower()
        ]
        suggestions.sort()
        return suggestions[:25]
    except Exception as e:
         bot_logger.error(f"Error during autocomplete for user {user_id}: {e}", exc_info=True)
         return []

# --- Slash Commands ---

# === Join Sound Commands ===
# [NO CHANGES NEEDED IN setjoinsound, removejoinsound]
@bot.slash_command(
    name="setjoinsound",
    description="Upload your custom join sound (MP3, WAV etc). Replaces any existing one."
)
@commands.cooldown(1, 15, commands.BucketType.user)
async def setjoinsound(
    ctx: discord.ApplicationContext,
    sound_file: discord.Option(
        discord.Attachment,
        description=f"Sound file ({', '.join(ALLOWED_EXTENSIONS)}). Max {MAX_USER_SOUND_SIZE_MB}MB.",
        required=True
    ) # type: ignore
):
    """Handles uploading and setting a user's custom join sound."""
    await ctx.defer(ephemeral=True) # Respond privately
    author = ctx.author
    bot_logger.info(f"COMMAND: /setjoinsound invoked by {author.name} ({author.id}), file: '{sound_file.filename}'")
    user_id_str = str(author.id)

    # --- Validation ---
    file_extension = os.path.splitext(sound_file.filename)[1].lower()
    if file_extension not in ALLOWED_EXTENSIONS:
        await ctx.followup.send(f"❌ Invalid file type (`{file_extension}`). Allowed: {', '.join(ALLOWED_EXTENSIONS)}", ephemeral=True)
        return

    if not sound_file.content_type or not sound_file.content_type.startswith('audio/'):
         bot_logger.warning(f"Content-Type '{sound_file.content_type}' for '{sound_file.filename}' is not 'audio/*'. Proceeding based on extension '{file_extension}'.")

    if sound_file.size > MAX_USER_SOUND_SIZE_MB * 1024 * 1024:
        await ctx.followup.send(f"❌ File is too large (`{sound_file.size / (1024*1024):.2f}` MB). Maximum size is {MAX_USER_SOUND_SIZE_MB}MB.", ephemeral=True)
        return

    temp_save_filename = f"temp_joinvalidate_{user_id_str}{file_extension}"
    temp_save_path = os.path.join(SOUNDS_DIR, temp_save_filename)
    final_save_filename = f"{user_id_str}{file_extension}" # Final name for join sound file
    final_save_path = os.path.join(SOUNDS_DIR, final_save_filename)

    async def cleanup_temp():
        if os.path.exists(temp_save_path):
            try:
                os.remove(temp_save_path)
                bot_logger.debug(f"Cleaned up temporary file: {temp_save_path}")
            except Exception as del_e:
                bot_logger.warning(f"Failed to cleanup temporary file {temp_save_path}: {del_e}")

    try:
        await sound_file.save(temp_save_path)
        bot_logger.info(f"Saved temporary join sound for validation: '{temp_save_path}'")

        try:
            bot_logger.debug(f"Attempting Pydub decode validation: '{temp_save_path}'")
            _ = AudioSegment.from_file(temp_save_path, format=file_extension.strip('.'))
            bot_logger.info(f"Pydub validation successful for join sound: '{temp_save_path}'")

            if user_id_str in user_sound_config:
                old_config_filename = user_sound_config[user_id_str]
                if old_config_filename != final_save_filename:
                    old_path = os.path.join(SOUNDS_DIR, old_config_filename)
                    if os.path.exists(old_path):
                        try:
                            os.remove(old_path)
                            bot_logger.info(f"Removed previous join sound file due to overwrite: '{old_path}'")
                        except Exception as e:
                            bot_logger.warning(f"Could not remove previous join sound file '{old_path}' during overwrite: {e}")

            try:
                os.replace(temp_save_path, final_save_path)
                bot_logger.info(f"Final join sound saved: '{final_save_path}'")
            except OSError as rep_e:
                bot_logger.error(f"Failed to replace/rename '{temp_save_path}' to '{final_save_path}': {rep_e}.", exc_info=True)
                await cleanup_temp()
                await ctx.followup.send("❌ Error saving the sound file. Please try again.", ephemeral=True)
                return

            user_sound_config[user_id_str] = final_save_filename
            save_config()
            bot_logger.info(f"Updated join sound config for {author.name} ({user_id_str}) to use '{final_save_filename}'")
            await ctx.followup.send(f"✅ Success! Your join sound has been set to `{sound_file.filename}`.", ephemeral=True)

        except CouldntDecodeError as decode_error:
            bot_logger.error(f"JOIN SOUND VALIDATION FAILED (Pydub Decode Error - user: {author.id}, file: '{sound_file.filename}'): {decode_error}", exc_info=True)
            await cleanup_temp()
            await ctx.followup.send(f"❌ **Audio Validation Failed!**\nCould not process `{sound_file.filename}`.\n"
                                    f"Ensure it's a valid audio file ({', '.join(ALLOWED_EXTENSIONS)}) and not corrupted.\n"
                                    f"*(Make sure FFmpeg is installed and accessible by the bot)*", ephemeral=True)
        except Exception as validate_e:
            bot_logger.error(f"JOIN SOUND VALIDATION FAILED (Unexpected during Pydub check - user: {author.id}, file: '{sound_file.filename}'): {validate_e}", exc_info=True)
            await cleanup_temp()
            await ctx.followup.send(f"❌ **Audio Validation Failed!** An unexpected error occurred during audio processing.", ephemeral=True)

    except discord.HTTPException as e:
        bot_logger.error(f"Error downloading temp join sound file from Discord for {author.id}: {e}", exc_info=True)
        await cleanup_temp()
        await ctx.followup.send("❌ Error downloading the sound file from Discord. Please try again.", ephemeral=True)
    except Exception as e:
        bot_logger.error(f"Unexpected error in /setjoinsound command for {author.id}: {e}", exc_info=True)
        await cleanup_temp()
        await ctx.followup.send("❌ An unexpected server error occurred.", ephemeral=True)

@bot.slash_command(
    name="removejoinsound",
    description="Remove your custom join sound and revert to default TTS."
)
@commands.cooldown(1, 5, commands.BucketType.user)
async def removejoinsound(ctx: discord.ApplicationContext):
    """Handles removing a user's custom join sound."""
    await ctx.defer(ephemeral=True)
    author = ctx.author
    bot_logger.info(f"COMMAND: /removejoinsound invoked by {author.name} ({author.id})")
    user_id_str = str(author.id)

    if user_id_str in user_sound_config:
        filename_to_remove = user_sound_config[user_id_str]
        file_path_to_remove = os.path.join(SOUNDS_DIR, filename_to_remove)

        del user_sound_config[user_id_str]
        save_config()
        bot_logger.info(f"Removed join sound config entry for {author.name} ({user_id_str})")

        if os.path.exists(file_path_to_remove):
            try:
                os.remove(file_path_to_remove)
                bot_logger.info(f"Deleted join sound file: '{file_path_to_remove}'")
            except OSError as e:
                bot_logger.warning(f"Could not delete join sound file '{file_path_to_remove}': {e}")
        else:
            bot_logger.warning(f"Join sound file '{filename_to_remove}' for user {user_id_str} was configured but not found at '{file_path_to_remove}' during removal.")

        await ctx.followup.send("🗑️ Your custom join sound has been removed. The default TTS will be used next time you join.", ephemeral=True)
    else:
        await ctx.followup.send("🤷 You don't currently have a custom join sound configured.", ephemeral=True)


# === User Command Sound / Soundboard Commands ===
# NOTE: /uploadsound, /mysounds, /deletesound now also manage the sounds for the /soundpanel

@bot.slash_command(
    name="uploadsound",
    description=f"Upload a sound for your personal sound panel (/soundpanel) (Max {MAX_USER_SOUNDS_PER_USER})." # Updated description
)
@commands.cooldown(2, 20, commands.BucketType.user)
async def uploadsound(
    ctx: discord.ApplicationContext,
    name: discord.Option(
        str,
        description="Choose a short name for this sound (letters, numbers, underscore).",
        required=True
    ), # type: ignore
    sound_file: discord.Option(
        discord.Attachment,
        description=f"Sound file ({', '.join(ALLOWED_EXTENSIONS)}). Max {MAX_USER_SOUND_SIZE_MB}MB.",
        required=True
    ) # type: ignore
):
    """Handles uploading a named sound for a user's personal collection."""
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id = author.id
    bot_logger.info(f"COMMAND: /uploadsound invoked by {author.name} ({user_id}), trying name: '{name}', file: '{sound_file.filename}'")

    clean_name = sanitize_filename(name)
    if not clean_name:
        await ctx.followup.send("❌ Please provide a valid name using only letters, numbers, or underscores.", ephemeral=True)
        return
    if clean_name != name:
         bot_logger.warning(f"Sanitized sound name for user {user_id}: '{name}' -> '{clean_name}'")
         # Notify user about sanitization
         await ctx.interaction.edit_original_response(content=f"ℹ️ Your sound name was sanitized to `{clean_name}` for compatibility.") # Edit initial deferral


    file_extension = os.path.splitext(sound_file.filename)[1].lower()
    if file_extension not in ALLOWED_EXTENSIONS:
        await ctx.followup.send(f"❌ Invalid file type (`{file_extension}`). Allowed: {', '.join(ALLOWED_EXTENSIONS)}", ephemeral=True)
        return
    if not sound_file.content_type or not sound_file.content_type.startswith('audio/'):
         bot_logger.warning(f"Content-Type '{sound_file.content_type}' for '{sound_file.filename}' not 'audio/*'. Proceeding based on extension.")
    if sound_file.size > MAX_USER_SOUND_SIZE_MB * 1024 * 1024:
        await ctx.followup.send(f"❌ File too large (`{sound_file.size / (1024*1024):.2f}` MB). Max: {MAX_USER_SOUND_SIZE_MB}MB.", ephemeral=True)
        return

    user_dir = os.path.join(USER_SOUNDS_DIR, str(user_id))
    ensure_dir(user_dir)
    current_sounds = get_user_sound_files(user_id) # Gets names without extension

    existing_sound_path = find_user_sound_path(user_id, clean_name)
    is_replacing = existing_sound_path is not None

    if not is_replacing and len(current_sounds) >= MAX_USER_SOUNDS_PER_USER:
         await ctx.followup.send(f"❌ You have reached the maximum limit of {MAX_USER_SOUNDS_PER_USER} sounds. Use `/deletesound` to remove some before adding new ones.", ephemeral=True)
         return

    temp_save_filename = f"temp_cmdvalidate_{user_id}_{clean_name}{file_extension}"
    temp_save_path = os.path.join(USER_SOUNDS_DIR, temp_save_filename) # Store temp outside user dir
    final_save_filename = f"{clean_name}{file_extension}"
    final_save_path = os.path.join(user_dir, final_save_filename)

    async def cleanup_temp_user():
        if os.path.exists(temp_save_path):
            try: os.remove(temp_save_path); bot_logger.debug(f"Cleaned up {temp_save_path}")
            except Exception as del_e: bot_logger.warning(f"Failed cleanup {temp_save_path}: {del_e}")

    try:
        await sound_file.save(temp_save_path)
        bot_logger.info(f"Saved temporary command sound for validation: '{temp_save_path}'")

        try:
            bot_logger.debug(f"Attempting Pydub decode validation: '{temp_save_path}'")
            _ = AudioSegment.from_file(temp_save_path, format=file_extension.strip('.'))
            bot_logger.info(f"Pydub validation successful for command sound: '{temp_save_path}'")

            if is_replacing and existing_sound_path and existing_sound_path != final_save_path:
                try:
                    os.remove(existing_sound_path)
                    bot_logger.info(f"Removed existing sound '{os.path.basename(existing_sound_path)}' for user {user_id} due to overwrite with new extension.")
                except Exception as e:
                    bot_logger.warning(f"Could not remove conflicting existing sound file '{existing_sound_path}': {e}")

            try:
                os.replace(temp_save_path, final_save_path)
                bot_logger.info(f"Final command sound saved for user {user_id}: '{final_save_path}'")
            except OSError as rep_e:
                bot_logger.error(f"Failed to replace/rename '{temp_save_path}' to '{final_save_path}': {rep_e}.", exc_info=True)
                await cleanup_temp_user()
                await ctx.followup.send("❌ Error saving the sound file to your collection.", ephemeral=True)
                return

            action_word = "updated" if is_replacing else "uploaded"
            followup_message = f"✅ Success! Sound `{clean_name}` {action_word}. Use `/playsound name:{clean_name}` or `/soundpanel`."
            if clean_name != name: # Add sanitization note if needed
                followup_message = f"ℹ️ Your sound name was sanitized to `{clean_name}` for compatibility.\n" + followup_message

            await ctx.followup.send(followup_message, ephemeral=True)


        except CouldntDecodeError as decode_error:
            bot_logger.error(f"COMMAND SOUND VALIDATION FAILED (Pydub Decode Error - user: {user_id}, file: '{sound_file.filename}'): {decode_error}", exc_info=True)
            await cleanup_temp_user()
            await ctx.followup.send(f"❌ **Audio Validation Failed!** Could not process `{sound_file.filename}`.", ephemeral=True)
        except Exception as validate_e:
            bot_logger.error(f"COMMAND SOUND VALIDATION FAILED (Unexpected - user: {user_id}, file: '{sound_file.filename}'): {validate_e}", exc_info=True)
            await cleanup_temp_user()
            await ctx.followup.send(f"❌ **Audio Validation Failed!** Unexpected error during processing.", ephemeral=True)

    except discord.HTTPException as e:
        bot_logger.error(f"Error downloading temp command sound file for {user_id}: {e}", exc_info=True)
        await cleanup_temp_user()
        await ctx.followup.send("❌ Error downloading the sound file from Discord.", ephemeral=True)
    except Exception as e:
        bot_logger.error(f"Error in /uploadsound for {user_id}: {e}", exc_info=True)
        await cleanup_temp_user()
        await ctx.followup.send("❌ An unexpected server error occurred.", ephemeral=True)


@bot.slash_command(
    name="mysounds",
    description="Lists your uploaded sounds (used for /playsound and /soundpanel)." # Updated description
)
@commands.cooldown(1, 10, commands.BucketType.user)
async def mysounds(ctx: discord.ApplicationContext):
    """Displays a list of the user's uploaded command sounds."""
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id = author.id
    bot_logger.info(f"COMMAND: /mysounds invoked by {author.name} ({user_id})")
    user_sounds = get_user_sound_files(user_id)

    if not user_sounds:
        await ctx.followup.send("You haven't uploaded any sounds yet. Use `/uploadsound` to add some!", ephemeral=True)
        return

    sorted_sounds = sorted(user_sounds)
    sound_list_str = "\n".join([f"- `{name}`" for name in sorted_sounds])

    if len(sound_list_str) > 4000:
         sound_list_str = sound_list_str[:4000] + "\n... (list truncated)"

    embed = discord.Embed(
        title=f"{author.display_name}'s Sounds ({len(sorted_sounds)}/{MAX_USER_SOUNDS_PER_USER})",
        description=f"Use `/playsound name:<sound_name>` or `/soundpanel` to play.\n\n{sound_list_str}",
        color=discord.Color.blurple()
    )
    embed.set_footer(text="Use /deletesound to remove sounds.")

    await ctx.followup.send(embed=embed, ephemeral=True)


@bot.slash_command(
    name="deletesound",
    description="Deletes one of your uploaded sounds by name." # Updated description
)
@commands.cooldown(1, 5, commands.BucketType.user)
async def deletesound(
    ctx: discord.ApplicationContext,
    name: discord.Option(
        str,
        description="The name of the sound to delete (use /mysounds to see names).",
        required=True,
        autocomplete=user_sound_autocomplete
    ) # type: ignore
):
    """Handles deleting one of the user's command sounds."""
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id = author.id
    bot_logger.info(f"COMMAND: /deletesound invoked by {author.name} ({user_id}), trying to delete name: '{name}'")

    sound_path = find_user_sound_path(user_id, name)
    clean_name_for_msg = name # Store original requested name for messages

    if not sound_path:
        clean_name = sanitize_filename(name)
        if clean_name != name:
            sound_path = find_user_sound_path(user_id, clean_name)
            clean_name_for_msg = clean_name # Use the name that was actually found

    if not sound_path:
        await ctx.followup.send(f"❌ Sound named `{name}` not found in your collection. Use `/mysounds` to check available names.", ephemeral=True)
        return

    try:
        deleted_filename = os.path.basename(sound_path)
        os.remove(sound_path)
        bot_logger.info(f"Deleted user sound '{deleted_filename}' ({sound_path}) for user {user_id}.")
        await ctx.followup.send(f"🗑️ Sound `{clean_name_for_msg}` (file: `{deleted_filename}`) deleted successfully.", ephemeral=True)
    except OSError as e:
        bot_logger.error(f"Failed to delete user sound file '{sound_path}' for user {user_id}: {e}", exc_info=True)
        await ctx.followup.send(f"❌ Failed to delete sound `{clean_name_for_msg}` due to a file system error.", ephemeral=True)
    except Exception as e:
         bot_logger.error(f"Unexpected error during sound deletion for user {user_id}, path '{sound_path}': {e}", exc_info=True)
         await ctx.followup.send(f"❌ An unexpected error occurred while trying to delete `{clean_name_for_msg}`.", ephemeral=True)


@bot.slash_command(
    name="playsound",
    description="Plays one of your uploaded sounds in your current voice channel."
)
@commands.cooldown(1, 4, commands.BucketType.user)
async def playsound(
    ctx: discord.ApplicationContext,
    name: discord.Option(
        str,
        description="The name of the sound to play (use /mysounds to see names).",
        required=True,
        autocomplete=user_sound_autocomplete
    ) # type: ignore
):
    """Handles playing a user's named command sound."""
    await ctx.defer() # Public defer initially okay
    author = ctx.author
    user_id = author.id
    bot_logger.info(f"COMMAND: /playsound invoked by {author.name} ({user_id}), requesting sound name: '{name}'")

    sound_path = find_user_sound_path(user_id, name)
    clean_name_for_msg = name

    if not sound_path:
        clean_name = sanitize_filename(name)
        if clean_name != name:
             sound_path = find_user_sound_path(user_id, clean_name)
             clean_name_for_msg = clean_name # Use found name for messages

    if not sound_path:
        await ctx.followup.send(f"❌ Sound named `{name}` not found. Use `/mysounds` to see your uploads.", ephemeral=True)
        return

    # Call the generic single play function
    await play_single_sound(ctx.interaction, sound_path)


# === User Sound Panel ===

# Renamed from SoundboardView
class UserSoundboardView(discord.ui.View):
    """A View containing buttons to play sounds from the specific user's directory."""
    def __init__(self, user_id: int, *, timeout: Optional[float] = 300.0):
        super().__init__(timeout=timeout)
        self.user_id = user_id # Store the ID of the user this panel is for
        self.message: Optional[discord.Message] = None
        self.populate_buttons()

    def populate_buttons(self):
        """Scans the user's sound directory and adds buttons."""
        user_dir = os.path.join(USER_SOUNDS_DIR, str(self.user_id))
        bot_logger.debug(f"Populating user sound panel buttons for user {self.user_id} from: {user_dir}")

        if not os.path.isdir(user_dir):
            bot_logger.warning(f"User sound directory '{user_dir}' not found for user {self.user_id}.")
            # Add a single disabled button indicating no sounds
            button = discord.ui.Button(label="No sounds uploaded yet!", style=discord.ButtonStyle.secondary, disabled=True, row=0)
            self.add_item(button)
            return

        sounds_found = 0
        button_row = 0
        max_buttons_per_row = 5
        max_rows = 5
        max_buttons_total = max_buttons_per_row * max_rows # 25 button limit

        try:
            # Sort files by name for consistent order
            files_in_dir = sorted(os.listdir(user_dir))
        except OSError as e:
            bot_logger.error(f"Error listing user sound directory '{user_dir}': {e}")
            button = discord.ui.Button(label="Error Reading Sounds", style=discord.ButtonStyle.danger, disabled=True, row=0)
            self.add_item(button)
            return

        for filename in files_in_dir:
            if sounds_found >= max_buttons_total:
                bot_logger.warning(f"Reached maximum sound button limit ({max_buttons_total}) for user {self.user_id}. Skipping remaining files.")
                # Optionally add a message indicating truncation
                if button_row < max_rows: # Only add if there's space
                     info_button = discord.ui.Button(label="...", style=discord.ButtonStyle.secondary, disabled=True, row=button_row)
                     self.add_item(info_button)
                break

            filepath = os.path.join(user_dir, filename)
            if os.path.isfile(filepath):
                base_name, ext = os.path.splitext(filename)
                if ext.lower() in ALLOWED_EXTENSIONS:
                    # Use base name for label, full filename for ID
                    button_label = base_name.replace("_", " ")[:80]
                    # Prefix changed to avoid potential collisions if old panel exists
                    button_custom_id = f"usersb_play:{filename}"

                    if len(button_custom_id) > 100:
                        bot_logger.warning(f"Skipping user sound file '{filename}' (user {self.user_id}) because its custom_id ('{button_custom_id}') would exceed 100 characters.")
                        continue

                    button = discord.ui.Button(
                        label=button_label,
                        style=discord.ButtonStyle.secondary,
                        custom_id=button_custom_id,
                        row=button_row
                    )
                    # Assign the callback method
                    button.callback = self.user_soundboard_button_callback # Use the renamed callback
                    self.add_item(button)
                    sounds_found += 1

                    if sounds_found % max_buttons_per_row == 0:
                        button_row += 1
                        if button_row >= max_rows:
                             # No need for separate warning here, outer loop handles max_buttons_total
                             break
                else:
                    bot_logger.debug(f"Skipping non-audio file in user dir {self.user_id}: '{filename}'")

        if sounds_found == 0:
             bot_logger.info(f"No valid sound files found for user {self.user_id} in '{user_dir}'.")
             button = discord.ui.Button(label="No sounds uploaded yet!", style=discord.ButtonStyle.secondary, disabled=True, row=0)
             self.add_item(button)

    # Renamed callback
    async def user_soundboard_button_callback(self, interaction: discord.Interaction):
        """Callback executed when a user soundboard button is pressed."""
        custom_id = interaction.data["custom_id"]
        # user who clicked the button
        interacting_user = interaction.user
        bot_logger.info(f"USER SOUND PANEL: Button '{custom_id}' pressed by {interacting_user.name} ({interacting_user.id}) on panel for user {self.user_id}")

        # Defer ephemerally - play_single_sound will send the actual feedback
        # Removed thinking=True
        await interaction.response.defer(ephemeral=True)

        if not custom_id.startswith("usersb_play:"):
            bot_logger.error(f"Invalid custom_id format from user sound panel button: '{custom_id}'")
            await interaction.followup.send("❌ Internal error: Invalid button data.", ephemeral=True)
            return

        sound_filename = custom_id.split(":", 1)[1]
        # Construct path using the user ID stored in the view
        sound_path = os.path.join(USER_SOUNDS_DIR, str(self.user_id), sound_filename)

        # Call the generic single play function, passing the interaction object
        # It will check the INTERACTING user's voice state and permissions
        await play_single_sound(interaction, sound_path)

    async def on_timeout(self):
        """Called when the view times out."""
        bot_logger.debug(f"User sound panel view timed out for user {self.user_id} (message {self.message.id if self.message else 'Unknown'})")
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True

        if self.message:
            try:
                await self.message.edit(content=f"🔊 **{self.message.author.display_name}'s Sound Panel (Expired)**", view=self) # Indicate owner
            except discord.NotFound:
                bot_logger.debug(f"User sound panel message {self.message.id} not found on timeout.")
            except discord.Forbidden:
                 bot_logger.warning(f"Missing permissions to edit user sound panel message {self.message.id} on timeout.")
            except Exception as e:
                bot_logger.warning(f"Failed to edit user sound panel message {self.message.id} on timeout: {e}", exc_info=True)


@bot.slash_command(
    name="soundpanel",
    description="Displays buttons to play YOUR uploaded sounds." # Updated description
)
@commands.cooldown(1, 10, commands.BucketType.user) # Cooldown per user now more appropriate
@commands.has_permissions(use_application_commands=True)
async def soundpanel(ctx: discord.ApplicationContext):
    """Sends the user's personal sound panel message."""
    # Defer publicly, the panel itself is visible
    await ctx.defer()
    author = ctx.author
    user_id = author.id
    bot_logger.info(f"COMMAND: /soundpanel invoked by {author.name} ({user_id}) in channel {ctx.channel_id}")

    # Create the view instance, passing the invoking user's ID
    view = UserSoundboardView(user_id=user_id, timeout=600.0) # 10 min timeout

    # Check if buttons were actually added
    if not view.children or all(getattr(item, 'disabled', False) for item in view.children):
         # If no sounds or only error/placeholder buttons were added
         await ctx.followup.send("You haven't uploaded any sounds yet! Use `/uploadsound` to add sounds to your panel.", ephemeral=True)
         return

    # Send the panel message
    message_content = f"🔊 **{author.display_name}'s Sound Panel** - Click a button to play!"
    message = await ctx.followup.send(message_content, view=view)
    # Store message reference for timeout editing
    view.message = message


# --- Error Handler for Application Commands ---
# [NO CHANGES NEEDED]
@bot.event
async def on_application_command_error(ctx: discord.ApplicationContext, error: discord.DiscordException):
    """Handles errors raised during slash command execution."""
    command_name = ctx.command.qualified_name if ctx.command else "Unknown Command"

    if isinstance(error, commands.CommandOnCooldown):
        retry_after = error.retry_after
        message = f"⏳ This command (`/{command_name}`) is on cooldown. Please try again in {retry_after:.1f} seconds."
        if not ctx.interaction.response.is_done():
            await ctx.respond(message, ephemeral=True)
        else:
            await ctx.followup.send(message, ephemeral=True)

    elif isinstance(error, commands.MissingPermissions):
        perms_list = "\n".join([f"- `{perm}`" for perm in error.missing_permissions])
        message = f"🚫 You do not have the required permissions to use `/{command_name}`.\nMissing:\n{perms_list}"
        if not ctx.interaction.response.is_done():
            await ctx.respond(message, ephemeral=True)
        else:
            await ctx.followup.send(message, ephemeral=True)

    elif isinstance(error, commands.BotMissingPermissions):
         perms_list = "\n".join([f"- `{perm}`" for perm in error.missing_permissions])
         message = f"🚫 I don't have the required permissions to execute `/{command_name}`.\nPlease ensure I have:\n{perms_list}"
         try:
             if not ctx.interaction.response.is_done(): await ctx.respond(message, ephemeral=True)
             else: await ctx.followup.send(message, ephemeral=True)
         except discord.Forbidden:
              bot_logger.error(f"Cannot inform user about missing bot permissions for '/{command_name}' in channel {ctx.channel_id}.")

    else:
        bot_logger.error(f"Unhandled error in application command '/{command_name}':", exc_info=error)
        error_message = f"❌ An unexpected error occurred while running `/{command_name}`. The issue has been logged."
        try:
            if not ctx.interaction.response.is_done():
                await ctx.respond(error_message, ephemeral=True)
            else:
                await ctx.followup.send(error_message, ephemeral=True)
        except Exception as e_resp:
            bot_logger.error(f"Failed to send error response message to user for command '/{command_name}': {e_resp}", exc_info=e_resp)


# --- Run the Bot ---
if __name__ == "__main__":
    if not PYDUB_AVAILABLE:
        bot_logger.critical("Pydub library is not available. Install it ('pip install pydub') and ensure FFmpeg is in your PATH. Bot cannot start.")
        exit(1)
    if not BOT_TOKEN:
        bot_logger.critical("BOT_TOKEN environment variable not set. Create a .env file or set the environment variable. Bot cannot start.")
        exit(1)

    if not discord.opus.is_loaded():
         bot_logger.warning("="*30 + "\nOpus library not loaded. Voice functionality WILL FAIL.\n"
                         "Ensure libopus is installed on your system:\n"
                         "  Debian/Ubuntu: sudo apt update && sudo apt install libopus0\n"
                         "  Fedora: sudo dnf install opus\n"
                         "  Arch: sudo pacman -S opus\n"
                         "  macOS (Homebrew): brew install opus\n"
                         "  Windows: Usually bundled with ffmpeg builds, ensure ffmpeg is in PATH.\n" + "="*30)

    try:
        bot_logger.info("Attempting to start the bot...")
        bot.run(BOT_TOKEN)
    except discord.errors.LoginFailure:
        bot_logger.critical("="*30 + "\nLOGIN FAILURE: Invalid BOT_TOKEN provided. Check your token.\n" + "="*30)
    except discord.errors.PrivilegedIntentsRequired as e:
        bot_logger.critical("="*30 + f"\nINTENT ERROR: Required intents are missing: {e.shard_id}\n"
                         "Ensure 'Server Members Intent' and 'Voice State Intent' are enabled in the Discord Developer Portal.\n" + "="*30)
    except discord.errors.OpusNotLoaded:
         bot_logger.critical("="*30 + "\nOPUS ERROR: Opus library failed to load during runtime startup.\n" + "="*30)
    except Exception as e:
        bot_logger.critical(f"FATAL RUNTIME ERROR during bot startup: {e}", exc_info=True)