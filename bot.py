# bot.py

import discord
from discord.ext import commands
import os
import json
import asyncio
from gtts import gTTS # Make sure this is imported
import logging
import io # Required for BytesIO
import math # For checking infinite values in dBFS
from collections import deque # Efficient queue structure
import re # For cleaning filenames
from typing import List, Optional # For type hinting
import shutil # For copying files

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
# <<< NEW CONFIG >>>
MAX_TTS_LENGTH = 250 # Max characters for TTS command

TTS_LANGUAGE_CHOICES = [
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
    # Add more supported codes from gTTS documentation if desired
]

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')
discord_logger = logging.getLogger('discord')
discord_logger.setLevel(logging.WARNING)
bot_logger = logging.getLogger('SoundBot')
bot_logger.setLevel(logging.INFO)

# --- Validate Critical Config ---
if not BOT_TOKEN:
    bot_logger.critical("CRITICAL ERROR: Bot token (BOT_TOKEN) not found.")
    exit()
if not PYDUB_AVAILABLE:
    bot_logger.critical("CRITICAL ERROR: Pydub library failed to import.")
    exit()

# --- Intents ---
intents = discord.Intents.default()
intents.voice_states = True
intents.guilds = True
intents.message_content = False # Generally don't need message content for slash commands

# --- Bot Definition ---
bot = discord.Bot(intents=intents)

# --- Data Storage & Helpers ---
user_sound_config = {}
guild_sound_queues = {}
guild_play_tasks = {}

# --- Config/Dir Functions [UNCHANGED] ---
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

def ensure_dir(dir_path: str):
    if not os.path.exists(dir_path):
        try:
            os.makedirs(dir_path)
            bot_logger.info(f"Created directory: {dir_path}")
        except Exception as e:
            bot_logger.critical(f"CRITICAL: Could not create directory '{dir_path}': {e}", exc_info=True)
            if dir_path in [SOUNDS_DIR, USER_SOUNDS_DIR, PUBLIC_SOUNDS_DIR]:
                exit(f"Failed to create essential directory: {dir_path}")

ensure_dir(SOUNDS_DIR)
ensure_dir(USER_SOUNDS_DIR)
ensure_dir(PUBLIC_SOUNDS_DIR)


# --- Bot Events ---
@bot.event
async def on_ready():
    bot_logger.info(f'Logged in as {bot.user.name} ({bot.user.id})')
    load_config()
    bot_logger.info('------')
    bot_logger.info(f"Py-cord Version: {discord.__version__}")
    bot_logger.info(f"Audio Normalization Target: {TARGET_LOUDNESS_DBFS} dBFS")
    bot_logger.info(f"Allowed Upload Extensions: {', '.join(ALLOWED_EXTENSIONS)}")
    bot_logger.info(f"Max TTS Length: {MAX_TTS_LENGTH} chars")
    bot_logger.info(f"Join sound directory: {os.path.abspath(SOUNDS_DIR)}")
    bot_logger.info(f"User sounds directory: {os.path.abspath(USER_SOUNDS_DIR)}")
    bot_logger.info(f"Public sounds directory: {os.path.abspath(PUBLIC_SOUNDS_DIR)}")
    bot_logger.info("Sound Bot is operational.")


# --- Audio Processing Helper ---
# [NO CHANGES NEEDED IN process_audio - it works on file paths]
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
        if not file_extension:
             bot_logger.warning(f"AUDIO: Invalid empty extension for {sound_path}, assuming mp3.")
             file_extension = 'mp3' # Default again

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
        audio_segment.export(pcm_data_io, format="s16le") # s16le = PCM signed 16-bit little-endian
        pcm_data_io.seek(0) # Reset pointer to the beginning for reading

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


# --- Core Join Sound Queue Logic [UNCHANGED] ---
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

    # Process the join sound file using the existing helper
    audio_source = process_audio(sound_path, member.display_name)

    if audio_source:
        try:
            bot_logger.info(f"QUEUE PLAYBACK [{task_id}]: Playing join sound for {member.display_name}...")
            # Crucially, pass the voice_client to the handler
            voice_client.play(audio_source, after=lambda e: after_play_handler(e, voice_client))
            bot_logger.debug(f"QUEUE PLAYBACK [{task_id}]: vc.play() called for join sound of {member.display_name}.")
        except discord.errors.ClientException as e:
            bot_logger.error(f"QUEUE PLAYBACK ERROR [{task_id}] (ClientException): Bot potentially already playing or disconnected unexpectedly. {e}", exc_info=True)
            # Reschedule queue check rather than potentially losing the item
            bot.loop.create_task(play_next_in_queue(guild), name=f"QueueRetry_{guild_id}")
        except Exception as e:
            bot_logger.error(f"QUEUE PLAYBACK ERROR [{task_id}] (Unexpected): {e}", exc_info=True)
            bot.loop.create_task(play_next_in_queue(guild), name=f"QueueRetry_{guild_id}")
    else:
        bot_logger.warning(f"QUEUE PLAYBACK [{task_id}]: No valid audio source for {member.display_name}'s join sound ({os.path.basename(sound_path)}). Skipping.")
        # Trigger next check immediately if processing failed
        bot.loop.create_task(play_next_in_queue(guild), name=f"QueueSkip_{guild_id}")


# --- on_voice_state_update [UNCHANGED] ---
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
                is_tts = True # Fallback to TTS
        else:
            is_tts = True
            bot_logger.info(f"SOUND: No custom join sound config found for {member.display_name} ({user_id_str}). Using TTS.")

        # Generate TTS for join event if needed
        if is_tts:
            # Use a predictable, temporary filename for join TTS
            tts_filename = f"tts_join_{member.id}.mp3" # Keep distinct from user-triggered TTS if needed
            tts_path = os.path.join(SOUNDS_DIR, tts_filename) # Store in main sounds dir for simplicity

            # Check if file exists and maybe reuse? Or always regenerate? Always regenerating is simpler.
            bot_logger.info(f"TTS: Generating join TTS for {member.display_name} ('{tts_path}')...")
            tts_text = f"{member.display_name} joined"
            try:
                # Use run_in_executor for blocking gTTS call
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, lambda: gTTS(text=tts_text, lang='en').save(tts_path))
                bot_logger.info(f"TTS: Saved join TTS file '{tts_path}'")
                sound_path = tts_path
            except Exception as e:
                bot_logger.error(f"TTS: Failed join TTS generation for {member.display_name}: {e}", exc_info=True)
                sound_path = None # Generation failed, can't play anything

        if not sound_path:
            bot_logger.error(f"Could not determine or generate a join sound/TTS path for {member.display_name}. Skipping queue add.")
            return

        # --- Queueing Logic ---
        guild_id = guild.id
        if guild_id not in guild_sound_queues:
            guild_sound_queues[guild_id] = deque()

        queue_item = (member, sound_path)
        guild_sound_queues[guild_id].append(queue_item)
        bot_logger.info(f"QUEUE: Added join sound for {member.display_name} to queue for guild {guild.name}. Queue size: {len(guild_sound_queues[guild_id])}")

        voice_client = discord.utils.get(bot.voice_clients, guild=guild)

        # If bot is already playing something (join sound, command sound, TTS), just queue it.
        # The play_next_in_queue task (if running) or after_play_handler will pick it up.
        if voice_client and voice_client.is_playing():
            bot_logger.info(f"VOICE: Bot is currently playing in {guild.name}. Join sound for {member.display_name} queued. Connection/play deferred.")
            # Ensure a task exists to process the queue if the current playback finishes
            if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
                 task_name = f"QueueTriggerDeferred_{guild_id}"
                 guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(guild), name=task_name)
                 bot_logger.debug(f"VOICE: Created deferred play task '{task_name}' due to active playback.")
            return # Don't try to connect/move if already playing

        # If bot is NOT playing, attempt connection/move and start the queue task
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
                 should_start_play_task = True # Should start task after moving too
            else:
                 bot_logger.debug(f"VOICE: Bot already connected in '{channel_to_join.name}' and not playing.")
                 should_start_play_task = True # Start task if idle in correct channel

        except asyncio.TimeoutError:
            bot_logger.error(f"VOICE: Connection to '{channel_to_join.name}' timed out.")
            if guild_id in guild_sound_queues: guild_sound_queues[guild_id].clear() # Clear queue on failure
        except discord.errors.ClientException as e:
            bot_logger.error(f"VOICE: ClientException during connect/move to '{channel_to_join.name}': {e}", exc_info=True)
            if guild_id in guild_sound_queues: guild_sound_queues[guild_id].clear()
        except Exception as e:
             bot_logger.error(f"VOICE: Unexpected error during connect/move to '{channel_to_join.name}': {e}", exc_info=True)
             if guild_id in guild_sound_queues: guild_sound_queues[guild_id].clear()

        # Start the play task only if connection/move was successful AND needed
        if should_start_play_task and voice_client and voice_client.is_connected():
            # Only start a new task if one isn't already running for this guild
            if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
                task_name = f"QueueStart_{guild_id}"
                guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(guild), name=task_name)
                bot_logger.info(f"VOICE: Started play task '{task_name}' for guild {guild_id}.")
            else:
                 bot_logger.debug(f"VOICE: Play task for guild {guild_id} already exists and is not done.")


# --- after_play_handler [UNCHANGED BUT CRITICAL] ---
def after_play_handler(error: Optional[Exception], voice_client: discord.VoiceClient):
    """Callback registered in voice_client.play(). Runs after ANY sound finishes."""
    guild_id = voice_client.guild.id if voice_client and voice_client.guild else None
    if error:
        bot_logger.error(f'PLAYBACK ERROR (In after_play_handler for guild {guild_id}): {error}', exc_info=error)

    if not guild_id or not voice_client.is_connected(): # Check connection status too
        bot_logger.warning(f"after_play_handler called with invalid/disconnected voice_client or no guild (ID: {guild_id}). Cannot process further.")
        # Clean up task if somehow it's still referenced
        if guild_id and guild_id in guild_play_tasks:
             play_task = guild_play_tasks.pop(guild_id, None)
             if play_task and not play_task.done(): play_task.cancel()
        return

    bot_logger.debug(f"Playback finished for guild {guild_id}. Triggering queue check.")

    # Check the JOIN SOUND queue specifically
    is_join_queue_empty = guild_id not in guild_sound_queues or not guild_sound_queues[guild_id]

    if not is_join_queue_empty:
        bot_logger.debug(f"AFTER_PLAY: Join queue for guild {guild_id} is not empty. Ensuring play task runs.")
        # Ensure a task exists to play the next join sound
        if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
             task_name = f"QueueCheckAfterPlay_{guild_id}"
             guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(voice_client.guild), name=task_name)
             bot_logger.debug(f"AFTER_PLAY: Scheduled task '{task_name}' for guild {guild_id} (queue not empty).")
        else:
             bot_logger.debug(f"AFTER_PLAY: Task for guild {guild_id} already exists, not creating duplicate check task.")
    else:
         # If the join queue IS empty, attempt safe disconnect.
         # safe_disconnect itself will check if the bot is playing something ELSE (like another TTS or sound)
         # before actually disconnecting.
         bot_logger.debug(f"AFTER_PLAY: Join queue for guild {guild_id} is empty. Attempting safe disconnect.")
         # Schedule the disconnect check instead of calling directly to avoid blocking the handler
         bot.loop.create_task(safe_disconnect(voice_client), name=f"SafeDisconnectAfterPlay_{guild_id}")


# --- safe_disconnect [UNCHANGED BUT CRITICAL] ---
async def safe_disconnect(voice_client: Optional[discord.VoiceClient]):
    """Safely disconnects if connected, not playing, AND join queue is empty."""
    if not voice_client or not voice_client.is_connected():
        # bot_logger.debug("Safe disconnect called but client not connected.")
        return # Already disconnected or invalid client

    guild = voice_client.guild
    guild_id = guild.id

    # Double-check conditions right before disconnecting
    is_join_queue_empty = guild_id not in guild_sound_queues or not guild_sound_queues[guild_id]
    is_playing = voice_client.is_playing() # Check the current playing state

    if is_join_queue_empty and not is_playing:
        bot_logger.info(f"DISCONNECT: Conditions met for guild {guild_id} (Join queue empty, not playing). Disconnecting...")
        try:
            # Ensure stop is called just in case, though is_playing should be false
            if voice_client.is_playing():
                bot_logger.warning(f"DISCONNECT: Called stop() during safe_disconnect for {guild.name}, though is_playing was expected false.")
                voice_client.stop()

            await voice_client.disconnect(force=False) # Use force=False for graceful disconnect
            bot_logger.info(f"DISCONNECT: Bot disconnected from '{guild.name}'.")

            # Clean up the task tracker for this guild after successful disconnect
            if guild_id in guild_play_tasks:
                 play_task = guild_play_tasks.pop(guild_id, None)
                 if play_task and not play_task.done():
                     play_task.cancel()
                     bot_logger.debug(f"DISCONNECT: Cancelled and removed play task tracker for guild {guild_id}.")
                 elif play_task:
                     bot_logger.debug(f"DISCONNECT: Removed finished play task tracker for guild {guild_id}.")

        except Exception as e:
            bot_logger.error(f"DISCONNECT ERROR: Failed to disconnect from {guild.name}: {e}", exc_info=True)
    else:
         bot_logger.debug(f"Disconnect skipped for guild {guild.name}: Join Queue empty={is_join_queue_empty}, Playing={is_playing}.")


# --- Single Sound Playback Logic (For Files) [UNCHANGED] ---
async def play_single_sound(interaction: discord.Interaction, sound_path: str):
    """Connects (if needed), plays a single sound FILE, and uses after_play_handler."""
    user = interaction.user
    guild = interaction.guild

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
         await interaction.followup.send("❌ Error: The sound file seems to be missing.", ephemeral=True)
         bot_logger.error(f"SINGLE PLAY: File not found: {sound_path}")
         return

    voice_client = discord.utils.get(bot.voice_clients, guild=guild)

    # --- Connection/Busy Check Logic ---
    try:
        if voice_client and voice_client.is_connected():
            if voice_client.is_playing():
                # Check if it's the join sound queue playing - prioritize join sounds
                if guild_id in guild_sound_queues and guild_sound_queues[guild_id]:
                    await interaction.followup.send("⏳ Bot is currently playing join sounds. Please wait.", ephemeral=True)
                    bot_logger.info(f"SINGLE PLAY (File): Bot busy with join queue in {guild.name}, user {user.name} tried to play '{os.path.basename(sound_path)}'. Request ignored.")
                else:
                    # It's likely playing another single sound or TTS
                    await interaction.followup.send("⏳ Bot is currently playing another sound/TTS. Please wait.", ephemeral=True)
                    bot_logger.info(f"SINGLE PLAY (File): Bot busy in {guild.name}, user {user.name} tried to play '{os.path.basename(sound_path)}'. Request ignored.")
                return # Exit if bot is busy
            elif voice_client.channel != target_channel:
                bot_logger.info(f"SINGLE PLAY (File): Moving from '{voice_client.channel.name}' to '{target_channel.name}' for {user.name}.")
                await voice_client.move_to(target_channel)
                bot_logger.info(f"SINGLE PLAY (File): Moved successfully.")
        else:
             # Connect if not connected
            bot_logger.info(f"SINGLE PLAY (File): Connecting to '{target_channel.name}' for {user.name}.")
            voice_client = await target_channel.connect(timeout=30.0, reconnect=True)
            bot_logger.info(f"SINGLE PLAY (File): Connected successfully.")

        # Re-verify client state after potential connection/move
        if not voice_client or not voice_client.is_connected():
             bot_logger.error(f"SINGLE PLAY (File): Failed to establish voice client for {target_channel.name} after connect/move attempt.")
             await interaction.followup.send("❌ Failed to connect/move to the voice channel.", ephemeral=True)
             return

    except asyncio.TimeoutError:
         await interaction.followup.send("❌ Connection to the voice channel timed out.", ephemeral=True)
         bot_logger.error(f"SINGLE PLAY (File): Connection/Move Timeout in {guild.name}")
         return
    except discord.errors.ClientException as e:
        if "already connecting" in str(e).lower() or "already disconnecting" in str(e).lower():
             await interaction.followup.send("⏳ Bot is busy connecting/disconnecting. Please wait a moment.", ephemeral=True)
             bot_logger.warning(f"SINGLE PLAY (File): Connection/Move failed in {guild.name}, already busy: {e}")
        else:
            await interaction.followup.send("❌ Error connecting/moving voice channel. Maybe check permissions?", ephemeral=True)
            bot_logger.error(f"SINGLE PLAY (File): Connection/Move ClientException in {guild.name}: {e}", exc_info=True)
        return
    except Exception as e:
        await interaction.followup.send("❌ An unexpected error occurred trying to join the voice channel.", ephemeral=True)
        bot_logger.error(f"SINGLE PLAY (File): Connection/Move unexpected error in {guild.name}: {e}", exc_info=True)
        return

    # --- Process and Play Audio FILE ---
    bot_logger.info(f"SINGLE PLAY (File): Processing '{os.path.basename(sound_path)}' for {user.name}...")
    audio_source = process_audio(sound_path, user.display_name) # Use file processing helper

    if audio_source:
        if voice_client.is_playing(): # Final check before playing
             bot_logger.warning(f"SINGLE PLAY (File): Voice client became busy between check and play call for {user.name}. Aborting playback.")
             await interaction.followup.send("⏳ Bot became busy just now. Please try again.", ephemeral=True)
             # Ensure handler runs to potentially disconnect if queue is empty now
             after_play_handler(None, voice_client)
             return

        try:
            sound_basename = os.path.basename(sound_path)
            bot_logger.info(f"SINGLE PLAYBACK (File): Playing '{sound_basename}' requested by {user.display_name}...")
            # Use the standard after_play_handler
            voice_client.play(audio_source, after=lambda e: after_play_handler(e, voice_client))
            await interaction.followup.send(f"▶️ Playing `{os.path.splitext(sound_basename)[0]}`...", ephemeral=True) # Keep feedback minimal
        except discord.errors.ClientException as e:
            await interaction.followup.send("❌ Error: Already playing audio or another client issue occurred.", ephemeral=True)
            bot_logger.error(f"SINGLE PLAYBACK ERROR (File - ClientException): {e}", exc_info=True)
            after_play_handler(e, voice_client) # Still call handler
        except Exception as e:
            await interaction.followup.send("❌ An unexpected error occurred during playback.", ephemeral=True)
            bot_logger.error(f"SINGLE PLAYBACK ERROR (File - Unexpected): {e}", exc_info=True)
            after_play_handler(e, voice_client) # Still call handler
    else:
        await interaction.followup.send("❌ Error: Could not process the audio file. Check bot logs.", ephemeral=True)
        bot_logger.error(f"SINGLE PLAYBACK (File): Failed to get audio source for '{sound_path}'")
        if voice_client and voice_client.is_connected(): # Call handler even on failure
            after_play_handler(None, voice_client)


# --- Helper Functions [UNCHANGED] ---
# sanitize_filename, get_user_sound_files, find_user_sound_path
# get_public_sound_files, find_public_sound_path
# user_sound_autocomplete, public_sound_autocomplete
# ... (Keep all these helper functions exactly the same) ...
def sanitize_filename(name: str) -> str:
    """Removes disallowed characters for filenames and limits length."""
    name = re.sub(r'[<>:"/\\|?*\.\s]+', '_', name)
    name = re.sub(r'_+', '_', name) # Collapse multiple underscores
    name = name.strip('_')
    return name[:50]

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
        sanitized = sanitize_filename(sound_name)
        if sanitized != sound_name:
            potential_path_sanitized = os.path.join(user_dir, f"{sanitized}{ext}")
            if os.path.exists(potential_path_sanitized):
                 return potential_path_sanitized

    bot_logger.debug(f"Sound '{sound_name}' not found for user {user_id} in {user_dir} with extensions {ALLOWED_EXTENSIONS}")
    return None

def get_public_sound_files() -> List[str]:
    """Returns a list of sound basenames (without ext) from the public sounds directory."""
    sounds = []
    if os.path.isdir(PUBLIC_SOUNDS_DIR):
        try:
            for filename in os.listdir(PUBLIC_SOUNDS_DIR):
                filepath = os.path.join(PUBLIC_SOUNDS_DIR, filename)
                base_name, ext = os.path.splitext(filename)
                if os.path.isfile(filepath) and ext.lower() in ALLOWED_EXTENSIONS:
                    sounds.append(base_name) # Return name without extension
        except OSError as e:
            bot_logger.error(f"Error listing files in public sound directory {PUBLIC_SOUNDS_DIR}: {e}")
    return sounds

def find_public_sound_path(sound_name: str) -> Optional[str]:
    """Finds the full path for a public sound by name, checking allowed extensions."""
    if not os.path.isdir(PUBLIC_SOUNDS_DIR):
        return None
    preferred_order = ['.mp3', '.wav'] + [ext for ext in ALLOWED_EXTENSIONS if ext not in ['.mp3', '.wav']]
    for ext in preferred_order:
        potential_path_exact = os.path.join(PUBLIC_SOUNDS_DIR, f"{sound_name}{ext}")
        if os.path.exists(potential_path_exact):
            return potential_path_exact
        sanitized = sanitize_filename(sound_name)
        if sanitized != sound_name:
            potential_path_sanitized = os.path.join(PUBLIC_SOUNDS_DIR, f"{sanitized}{ext}")
            if os.path.exists(potential_path_sanitized):
                 return potential_path_sanitized

    bot_logger.debug(f"Public sound '{sound_name}' not found in {PUBLIC_SOUNDS_DIR} with extensions {ALLOWED_EXTENSIONS}")
    return None

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
         bot_logger.error(f"Error during user sound autocomplete for user {user_id}: {e}", exc_info=True)
         return []

async def public_sound_autocomplete(ctx: discord.AutocompleteContext) -> List[str]:
    """Provides autocomplete suggestions for public sounds."""
    try:
        public_sounds = get_public_sound_files()
        current_value = ctx.value.lower() if ctx.value else ""
        suggestions = [
            name for name in public_sounds if current_value in name.lower()
        ]
        suggestions.sort()
        return suggestions[:25]
    except Exception as e:
         bot_logger.error(f"Error during public sound autocomplete: {e}", exc_info=True)
         return []


# --- Slash Commands ---

# === Join Sound Commands [UNCHANGED] ===
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

            # Remove old sound *file* if config existed and filename differs
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
                try:
                    shutil.move(temp_save_path, final_save_path)
                    bot_logger.info(f"Final join sound saved (using move fallback): '{final_save_path}'")
                except Exception as move_e:
                    bot_logger.error(f"Failed to save final join sound (replace failed: {rep_e}, move failed: {move_e})", exc_info=True)
                    await cleanup_temp()
                    await ctx.followup.send("❌ Error saving the sound file. Please try again.", ephemeral=True)
                    return

            # Update config *after* successful save
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
            # Also remove any generated join TTS file for this user
            tts_join_file = os.path.join(SOUNDS_DIR, f"tts_join_{user_id_str}.mp3")
            if os.path.exists(tts_join_file):
                 try: os.remove(tts_join_file); bot_logger.info(f"Removed old join TTS file: {tts_join_file}")
                 except Exception: pass
        else:
            bot_logger.warning(f"Join sound file '{filename_to_remove}' for user {user_id_str} was configured but not found at '{file_path_to_remove}' during removal.")

        await ctx.followup.send("🗑️ Your custom join sound has been removed. The default TTS will be used next time you join.", ephemeral=True)
    else:
        await ctx.followup.send("🤷 You don't currently have a custom join sound configured.", ephemeral=True)


# === User Command Sound / Soundboard Commands [UNCHANGED] ===
# /uploadsound, /mysounds, /deletesound, /playsound
# UserSoundboardView, /soundpanel
# ... (Keep all these commands and the view class exactly the same) ...
@bot.slash_command(
    name="uploadsound",
    description=f"Upload a sound (personal or public). Personal sound limit: {MAX_USER_SOUNDS_PER_USER}."
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
    ), # type: ignore
    make_public: discord.Option(
        bool,
        description="Make this sound available for everyone to use? (Default: False)",
        required=False,
        default=False
    ) # type: ignore
):
    """Handles uploading a named sound, either personal or public."""
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id = author.id
    bot_logger.info(f"COMMAND: /uploadsound invoked by {author.name} ({user_id}), name: '{name}', public: {make_public}, file: '{sound_file.filename}'")

    clean_name = sanitize_filename(name)
    if not clean_name:
        await ctx.followup.send("❌ Please provide a valid name using only letters, numbers, or underscores.", ephemeral=True)
        return

    file_extension = os.path.splitext(sound_file.filename)[1].lower()
    if file_extension not in ALLOWED_EXTENSIONS:
        await ctx.followup.send(f"❌ Invalid file type (`{file_extension}`). Allowed: {', '.join(ALLOWED_EXTENSIONS)}", ephemeral=True)
        return
    if not sound_file.content_type or not sound_file.content_type.startswith('audio/'):
         bot_logger.warning(f"Content-Type '{sound_file.content_type}' for '{sound_file.filename}' not 'audio/*'. Proceeding based on extension.")
    if sound_file.size > MAX_USER_SOUND_SIZE_MB * 1024 * 1024:
        await ctx.followup.send(f"❌ File too large (`{sound_file.size / (1024*1024):.2f}` MB). Max: {MAX_USER_SOUND_SIZE_MB}MB.", ephemeral=True)
        return

    is_replacing = False
    target_dir = ""
    final_save_filename = f"{clean_name}{file_extension}"

    if make_public:
        target_dir = PUBLIC_SOUNDS_DIR
        existing_public_path = find_public_sound_path(clean_name)
        if existing_public_path:
            bot_logger.warning(f"Public upload rejected for '{clean_name}' by {user_id}. Name already exists at '{existing_public_path}'.")
            await ctx.followup.send(f"❌ A public sound named `{clean_name}` already exists. Please choose a different name or ask an admin to manage the existing sound.", ephemeral=True)
            return
        is_replacing = False
    else:
        target_dir = os.path.join(USER_SOUNDS_DIR, str(user_id))
        ensure_dir(target_dir)
        current_personal_sounds = get_user_sound_files(user_id)
        existing_personal_path = find_user_sound_path(user_id, clean_name)
        is_replacing = existing_personal_path is not None

        if not is_replacing and len(current_personal_sounds) >= MAX_USER_SOUNDS_PER_USER:
             await ctx.followup.send(f"❌ You have reached the maximum limit of {MAX_USER_SOUNDS_PER_USER} personal sounds. Use `/deletesound` or upload as public.", ephemeral=True)
             return

    final_save_path = os.path.join(target_dir, final_save_filename)
    temp_save_filename = f"temp_upload_{user_id}_{clean_name}{file_extension}"
    temp_save_path = os.path.join(USER_SOUNDS_DIR, temp_save_filename)

    followup_message_prefix = ""
    if clean_name != name:
         bot_logger.warning(f"Sanitized sound name for user {user_id}: '{name}' -> '{clean_name}'")
         followup_message_prefix = f"ℹ️ Your sound name was sanitized to `{clean_name}` for compatibility.\n"

    async def cleanup_temp_upload():
        if os.path.exists(temp_save_path):
            try: os.remove(temp_save_path); bot_logger.debug(f"Cleaned up {temp_save_path}")
            except Exception as del_e: bot_logger.warning(f"Failed cleanup {temp_save_path}: {del_e}")

    try:
        await sound_file.save(temp_save_path)
        bot_logger.info(f"Saved temporary sound for validation: '{temp_save_path}' (public={make_public})")

        try:
            bot_logger.debug(f"Attempting Pydub decode validation: '{temp_save_path}'")
            _ = AudioSegment.from_file(temp_save_path, format=file_extension.strip('.'))
            bot_logger.info(f"Pydub validation successful for: '{temp_save_path}'")

            if is_replacing and not make_public:
                existing_personal_path = find_user_sound_path(user_id, clean_name)
                if existing_personal_path and existing_personal_path != final_save_path:
                    try:
                        os.remove(existing_personal_path)
                        bot_logger.info(f"Removed existing personal sound '{os.path.basename(existing_personal_path)}' for user {user_id} due to overwrite with new extension.")
                    except Exception as e:
                        bot_logger.warning(f"Could not remove conflicting existing personal sound file '{existing_personal_path}': {e}")

            try:
                os.replace(temp_save_path, final_save_path)
                bot_logger.info(f"Final sound saved {'publicly' if make_public else 'personally'} for user {user_id}: '{final_save_path}'")
            except OSError as rep_e:
                try:
                    shutil.move(temp_save_path, final_save_path)
                    bot_logger.info(f"Final sound saved (using move fallback) {'publicly' if make_public else 'personally'} for user {user_id}: '{final_save_path}'")
                except Exception as move_e:
                    bot_logger.error(f"Failed to save final sound (replace failed: {rep_e}, move failed: {move_e})", exc_info=True)
                    await cleanup_temp_upload()
                    await ctx.followup.send(f"{followup_message_prefix}❌ Error saving the sound file.", ephemeral=True)
                    return

            scope = "public" if make_public else "personal"
            action_word = "updated" if (is_replacing and not make_public) else "uploaded"
            play_command = "playpublic" if make_public else "playsound"
            list_command = "publicsounds" if make_public else "mysounds"

            followup_message = f"{followup_message_prefix}✅ Success! Sound `{clean_name}` {action_word} as a {scope} sound.\n"
            if make_public:
                 followup_message += f"Use `/{play_command} name:{clean_name}` to play or `/{list_command}` to list."
            else:
                 followup_message += f"Use `/{play_command} name:{clean_name}`, `/{list_command}`, or `/soundpanel`."
                 followup_message += f"\nYou can make it public later using `/publishsound name:{clean_name}`."
            await ctx.followup.send(followup_message, ephemeral=True)

        except CouldntDecodeError as decode_error:
            bot_logger.error(f"UPLOAD SOUND VALIDATION FAILED (Pydub Decode Error - user: {user_id}, file: '{sound_file.filename}', public: {make_public}): {decode_error}", exc_info=True)
            await cleanup_temp_upload()
            await ctx.followup.send(f"{followup_message_prefix}❌ **Audio Validation Failed!** Could not process `{sound_file.filename}`.", ephemeral=True)
        except Exception as validate_e:
            bot_logger.error(f"UPLOAD SOUND VALIDATION FAILED (Unexpected - user: {user_id}, file: '{sound_file.filename}', public: {make_public}): {validate_e}", exc_info=True)
            await cleanup_temp_upload()
            await ctx.followup.send(f"{followup_message_prefix}❌ **Audio Validation Failed!** Unexpected error during processing.", ephemeral=True)

    except discord.HTTPException as e:
        bot_logger.error(f"Error downloading temp sound file for {user_id} (public={make_public}): {e}", exc_info=True)
        await cleanup_temp_upload()
        await ctx.followup.send(f"{followup_message_prefix}❌ Error downloading the sound file from Discord.", ephemeral=True)
    except Exception as e:
        bot_logger.error(f"Error in /uploadsound for {user_id} (public={make_public}): {e}", exc_info=True)
        await cleanup_temp_upload()
        await ctx.followup.send(f"{followup_message_prefix}❌ An unexpected server error occurred.", ephemeral=True)

@bot.slash_command(
    name="mysounds",
    description="Lists your personal uploaded sounds."
)
@commands.cooldown(1, 10, commands.BucketType.user)
async def mysounds(ctx: discord.ApplicationContext):
    """Displays a list of the user's personal sounds."""
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id = author.id
    bot_logger.info(f"COMMAND: /mysounds invoked by {author.name} ({user_id})")
    user_sounds = get_user_sound_files(user_id) # Only lists from user dir

    if not user_sounds:
        await ctx.followup.send("You haven't uploaded any personal sounds yet. Use `/uploadsound`!", ephemeral=True)
        return

    sorted_sounds = sorted(user_sounds)
    sound_list_str = "\n".join([f"- `{name}`" for name in sorted_sounds])
    output_limit = 1900

    if len(sound_list_str) > output_limit:
         cutoff_point = sound_list_str.rfind('\n', 0, output_limit)
         if cutoff_point != -1:
             sound_list_str = sound_list_str[:cutoff_point] + "\n... (list truncated)"
         else:
             sound_list_str = sound_list_str[:output_limit] + "... (list truncated)"

    embed = discord.Embed(
        title=f"{author.display_name}'s Personal Sounds ({len(sorted_sounds)}/{MAX_USER_SOUNDS_PER_USER})",
        description=f"Use `/playsound name:<sound_name>` or `/soundpanel` to play.\n"
                    f"Use `/publishsound name:<sound_name>` to make one public.\n\n{sound_list_str}",
        color=discord.Color.blurple()
    )
    embed.set_footer(text="Use /deletesound to remove sounds from this personal list.")

    await ctx.followup.send(embed=embed, ephemeral=True)

@bot.slash_command(
    name="deletesound",
    description="Deletes one of your PERSONAL uploaded sounds by name."
)
@commands.cooldown(1, 5, commands.BucketType.user)
async def deletesound(
    ctx: discord.ApplicationContext,
    name: discord.Option(
        str,
        description="The name of the personal sound to delete (use /mysounds to see names).",
        required=True,
        autocomplete=user_sound_autocomplete
    ) # type: ignore
):
    """Handles deleting one of the user's PERSONAL sounds."""
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id = author.id
    bot_logger.info(f"COMMAND: /deletesound invoked by {author.name} ({user_id}), target personal sound name: '{name}'")

    sound_path = find_user_sound_path(user_id, name)
    sound_base_name = name

    if not sound_path:
        clean_name_try = sanitize_filename(name)
        if clean_name_try != name:
            sound_path = find_user_sound_path(user_id, clean_name_try)
            if sound_path:
                 sound_base_name = clean_name_try

    if not sound_path:
        await ctx.followup.send(f"❌ Personal sound named `{name}` not found in your collection. Use `/mysounds`.", ephemeral=True)
        return

    user_dir_abs = os.path.abspath(os.path.join(USER_SOUNDS_DIR, str(user_id)))
    sound_path_abs = os.path.abspath(sound_path)
    if not sound_path_abs.startswith(user_dir_abs):
         bot_logger.error(f"CRITICAL SECURITY: /deletesound attempted path traversal. User: {user_id}, Path: '{sound_path}'")
         await ctx.followup.send(f"❌ An internal error occurred. Cannot delete sound.", ephemeral=True)
         return

    try:
        deleted_filename = os.path.basename(sound_path)
        os.remove(sound_path)
        bot_logger.info(f"Deleted PERSONAL sound '{deleted_filename}' ({sound_path}) for user {user_id}.")
        await ctx.followup.send(f"🗑️ Personal sound `{sound_base_name}` (file: `{deleted_filename}`) deleted successfully.", ephemeral=True)
    except OSError as e:
        bot_logger.error(f"Failed to delete personal sound file '{sound_path}' for user {user_id}: {e}", exc_info=True)
        await ctx.followup.send(f"❌ Failed to delete personal sound `{sound_base_name}` due to a file system error.", ephemeral=True)
    except Exception as e:
         bot_logger.error(f"Unexpected error during personal sound deletion for user {user_id}, path '{sound_path}': {e}", exc_info=True)
         await ctx.followup.send(f"❌ An unexpected error occurred while trying to delete personal sound `{sound_base_name}`.", ephemeral=True)

@bot.slash_command(
    name="playsound",
    description="Plays one of your PERSONAL sounds in your current voice channel."
)
@commands.cooldown(1, 4, commands.BucketType.user)
async def playsound(
    ctx: discord.ApplicationContext,
    name: discord.Option(
        str,
        description="The name of the personal sound to play (use /mysounds).",
        required=True,
        autocomplete=user_sound_autocomplete
    ) # type: ignore
):
    """Handles playing a user's personal sound file."""
    await ctx.defer() # Public defer okay, feedback is ephemeral in play_single_sound
    author = ctx.author
    user_id = author.id
    bot_logger.info(f"COMMAND: /playsound invoked by {author.name} ({user_id}), requesting personal sound name: '{name}'")

    sound_path = find_user_sound_path(user_id, name)
    sound_base_name = name

    if not sound_path:
        clean_name_try = sanitize_filename(name)
        if clean_name_try != name:
             sound_path = find_user_sound_path(user_id, clean_name_try)
             if sound_path:
                 sound_base_name = clean_name_try # Use sanitized name if found that way

    if not sound_path:
        await ctx.followup.send(f"❌ Personal sound named `{name}` not found. Use `/mysounds`.", ephemeral=True)
        return

    # Use the dedicated function for playing sound files
    await play_single_sound(ctx.interaction, sound_path)


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
            button = discord.ui.Button(label="No personal sounds yet!", style=discord.ButtonStyle.secondary, disabled=True, row=0)
            self.add_item(button)
            return

        sounds_found = 0
        button_row = 0
        max_buttons_per_row = 5
        max_rows = 5
        max_buttons_total = max_buttons_per_row * max_rows # 25 button limit

        try:
            files_in_dir = sorted(os.listdir(user_dir))
        except OSError as e:
            bot_logger.error(f"Error listing user sound directory '{user_dir}': {e}")
            button = discord.ui.Button(label="Error Reading Sounds", style=discord.ButtonStyle.danger, disabled=True, row=0)
            self.add_item(button)
            return

        for filename in files_in_dir:
            if sounds_found >= max_buttons_total:
                bot_logger.warning(f"Reached maximum sound button limit ({max_buttons_total}) for user {self.user_id}. Skipping remaining files.")
                if button_row < max_rows:
                     info_button = discord.ui.Button(label="...", style=discord.ButtonStyle.secondary, disabled=True, custom_id=f"usersb_truncated:{self.user_id}", row=button_row)
                     self.add_item(info_button)
                break

            filepath = os.path.join(user_dir, filename)
            if os.path.isfile(filepath):
                base_name, ext = os.path.splitext(filename)
                if ext.lower() in ALLOWED_EXTENSIONS:
                    button_label = base_name.replace("_", " ")[:80]
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
                    button.callback = self.user_soundboard_button_callback
                    self.add_item(button)
                    sounds_found += 1

                    if sounds_found % max_buttons_per_row == 0 and sounds_found > 0:
                        button_row += 1
                        if button_row >= max_rows: pass
                else:
                    bot_logger.debug(f"Skipping non-audio file in user dir {self.user_id}: '{filename}'")

        if sounds_found == 0:
             bot_logger.info(f"No valid sound files found for user {self.user_id} in '{user_dir}'.")
             button = discord.ui.Button(label="No personal sounds yet!", style=discord.ButtonStyle.secondary, disabled=True, row=0)
             self.add_item(button)

    async def user_soundboard_button_callback(self, interaction: discord.Interaction):
        """Callback executed when a user soundboard button is pressed."""
        custom_id = interaction.data["custom_id"]
        interacting_user = interaction.user
        bot_logger.info(f"USER SOUND PANEL: Button '{custom_id}' pressed by {interacting_user.name} ({interacting_user.id}) on panel for user {self.user_id}")

        await interaction.response.defer(ephemeral=True)

        if not custom_id.startswith("usersb_play:"):
            bot_logger.error(f"Invalid custom_id format from user sound panel button: '{custom_id}'")
            await interaction.followup.send("❌ Internal error: Invalid button data.", ephemeral=True)
            return

        sound_filename = custom_id.split(":", 1)[1]
        sound_path = os.path.join(USER_SOUNDS_DIR, str(self.user_id), sound_filename)

        # Use the generic play_single_sound for files
        await play_single_sound(interaction, sound_path)

    async def on_timeout(self):
        """Called when the view times out."""
        if self.message:
            bot_logger.debug(f"User sound panel view timed out for user {self.user_id} (message {self.message.id})")
            try:
                # Fetch panel owner name
                if self.message.guild:
                     panel_owner = await self.message.guild.fetch_member(self.user_id)
                     owner_name = panel_owner.display_name if panel_owner else f"User {self.user_id}"
                else: # Fallback for DM context or missing guild
                     owner_name = f"User {self.user_id}"
            except (discord.NotFound, discord.Forbidden, AttributeError):
                 owner_name = f"User {self.user_id}" # Fallback if fetch fails

            for item in self.children:
                if isinstance(item, discord.ui.Button):
                    item.disabled = True
            try:
                await self.message.edit(content=f"🔊 **{owner_name}'s Personal Sound Panel (Expired)**", view=self)
            except discord.NotFound: pass
            except discord.Forbidden: pass
            except Exception as e: bot_logger.warning(f"Failed to edit user sound panel message {self.message.id} on timeout: {e}")
        else:
             bot_logger.debug(f"User sound panel view timed out for user {self.user_id} but message reference was lost.")

@bot.slash_command(
    name="soundpanel",
    description="Displays buttons to play YOUR personal sounds."
)
@commands.cooldown(1, 10, commands.BucketType.user)
async def soundpanel(ctx: discord.ApplicationContext):
    """Sends the user's personal sound panel message."""
    await ctx.defer() # Public defer okay, view is specific
    author = ctx.author
    user_id = author.id
    bot_logger.info(f"COMMAND: /soundpanel invoked by {author.name} ({user_id}) in channel {ctx.channel_id}")

    view = UserSoundboardView(user_id=user_id, timeout=600.0) # 10 min timeout

    has_playable_buttons = any(
        isinstance(item, discord.ui.Button) and not item.disabled and item.custom_id and item.custom_id.startswith("usersb_play:")
        for item in view.children
    )

    if not has_playable_buttons:
         # Use followup since we deferred
         await ctx.followup.send("You haven't uploaded any personal sounds yet! Use `/uploadsound`.", ephemeral=True)
         return

    message_content = f"🔊 **{author.display_name}'s Personal Sound Panel** - Click a button to play!"
    try:
        message = await ctx.followup.send(message_content, view=view)
        view.message = message # Store message reference for timeout editing
    except Exception as e:
        bot_logger.error(f"Failed to send soundpanel message for user {user_id}: {e}", exc_info=True)
        try: await ctx.followup.send("❌ Failed to create the sound panel message.", ephemeral=True)
        except: pass # Ignore if followup also fails


# === Public Sound Commands [UNCHANGED] ===
# /publishsound, /removepublic, /publicsounds, /playpublic
# ... (Keep all these commands exactly the same) ...
@bot.slash_command(
    name="publishsound",
    description="Make one of your personal sounds public for everyone to use."
)
@commands.cooldown(1, 10, commands.BucketType.user)
async def publishsound(
    ctx: discord.ApplicationContext,
    name: discord.Option(
        str,
        description="The name of YOUR personal sound to make public (use /mysounds).",
        required=True,
        autocomplete=user_sound_autocomplete
    ) # type: ignore
):
    """Copies a user's personal sound to the public directory if the name isn't taken."""
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id = author.id
    bot_logger.info(f"COMMAND: /publishsound invoked by {author.name} ({user_id}), target sound name: '{name}'")

    user_sound_path = find_user_sound_path(user_id, name)
    sound_base_name = name

    if not user_sound_path:
        clean_name_try = sanitize_filename(name)
        if clean_name_try != name:
             user_sound_path = find_user_sound_path(user_id, clean_name_try)
             if user_sound_path:
                 sound_base_name = clean_name_try

    if not user_sound_path:
        await ctx.followup.send(f"❌ Personal sound named `{name}` not found in your collection. Use `/mysounds`.", ephemeral=True)
        return

    source_filename = os.path.basename(user_sound_path)
    public_filename = source_filename
    public_sound_path = os.path.join(PUBLIC_SOUNDS_DIR, public_filename)
    target_base_name, _ = os.path.splitext(public_filename)

    existing_public_path = find_public_sound_path(target_base_name)
    if existing_public_path:
        bot_logger.warning(f"Publish rejected for '{target_base_name}' by {user_id}. Public name already exists.")
        await ctx.followup.send(f"❌ Cannot publish. A public sound named `{target_base_name}` already exists. Choose a different name for your sound or ask an admin.", ephemeral=True)
        return

    try:
        shutil.copy2(user_sound_path, public_sound_path) # copy2 preserves metadata
        bot_logger.info(f"SOUND PUBLISHED: Copied '{user_sound_path}' to '{public_sound_path}' by {author.name} ({user_id}).")
        await ctx.followup.send(f"✅ Sound `{sound_base_name}` (file: `{public_filename}`) is now public!\n"
                                f"Anyone can play it using `/playpublic name:{target_base_name}`.", ephemeral=True)
    except OSError as e:
        bot_logger.error(f"Failed to copy sound '{user_sound_path}' to public dir '{public_sound_path}' for publishing: {e}", exc_info=True)
        await ctx.followup.send(f"❌ Failed to publish sound `{sound_base_name}` due to a file system error.", ephemeral=True)
    except Exception as e:
        bot_logger.error(f"Unexpected error during /publishsound for sound '{user_sound_path}': {e}", exc_info=True)
        await ctx.followup.send(f"❌ An unexpected error occurred while publishing sound `{sound_base_name}`.", ephemeral=True)

@bot.slash_command(
    name="removepublic",
    description="[Admin] Remove a sound from the public collection."
)
@commands.has_permissions(manage_guild=True)
@commands.cooldown(1, 5, commands.BucketType.guild) # Changed to guild bucket for admin commands
async def removepublic(
    ctx: discord.ApplicationContext,
    name: discord.Option(
        str,
        description="The name of the public sound to remove (use /publicsounds to see names).",
        required=True,
        autocomplete=public_sound_autocomplete
    ) # type: ignore
):
    """Deletes a sound from the public directory."""
    await ctx.defer(ephemeral=True)
    admin_user = ctx.author
    bot_logger.info(f"COMMAND: /removepublic invoked by admin {admin_user.name} ({admin_user.id}), target public sound name: '{name}'")

    public_sound_path = find_public_sound_path(name)
    sound_base_name = name

    if not public_sound_path:
        clean_name_try = sanitize_filename(name)
        if clean_name_try != name:
            public_sound_path = find_public_sound_path(clean_name_try)
            if public_sound_path:
                sound_base_name = clean_name_try

    if not public_sound_path:
        await ctx.followup.send(f"❌ Public sound named `{name}` not found. Use `/publicsounds` to check names.", ephemeral=True)
        return

    try:
        deleted_filename = os.path.basename(public_sound_path)
        os.remove(public_sound_path)
        bot_logger.info(f"ADMIN ACTION: Deleted public sound '{deleted_filename}' ({public_sound_path}) by {admin_user.name} ({admin_user.id}).")
        await ctx.followup.send(f"🗑️ Public sound `{sound_base_name}` (file: `{deleted_filename}`) deleted successfully.", ephemeral=True)
    except OSError as e:
        bot_logger.error(f"Failed to delete public sound file '{public_sound_path}': {e}", exc_info=True)
        await ctx.followup.send(f"❌ Failed to delete public sound `{sound_base_name}` due to a file system error.", ephemeral=True)
    except Exception as e:
         bot_logger.error(f"Unexpected error during public sound deletion '{public_sound_path}': {e}", exc_info=True)
         await ctx.followup.send(f"❌ An unexpected error occurred while trying to delete public sound `{sound_base_name}`.", ephemeral=True)

@bot.slash_command(
    name="publicsounds",
    description="Lists all available public sounds."
)
@commands.cooldown(1, 10, commands.BucketType.user)
async def publicsounds(ctx: discord.ApplicationContext):
    """Displays a list of public sounds."""
    await ctx.defer(ephemeral=True)
    bot_logger.info(f"COMMAND: /publicsounds invoked by {ctx.author.name} ({ctx.author.id})")
    public_sounds = get_public_sound_files() # Gets base names

    if not public_sounds:
        await ctx.followup.send("There are no public sounds available yet. Upload one with the `make_public` option or use `/publishsound`!", ephemeral=True)
        return

    sorted_sounds = sorted(public_sounds)
    sound_list_str = "\n".join([f"- `{name}`" for name in sorted_sounds])
    output_limit = 1900

    if len(sound_list_str) > output_limit:
         cutoff_point = sound_list_str.rfind('\n', 0, output_limit)
         if cutoff_point != -1:
             sound_list_str = sound_list_str[:cutoff_point] + "\n... (list truncated)"
         else:
             sound_list_str = sound_list_str[:output_limit] + "... (list truncated)"

    embed = discord.Embed(
        title=f"📢 Public Sounds ({len(sorted_sounds)})",
        description=f"Use `/playpublic name:<sound_name>` to play one.\n\n{sound_list_str}",
        color=discord.Color.green()
    )
    embed.set_footer(text="Admins can use /removepublic to manage these sounds.")

    await ctx.followup.send(embed=embed, ephemeral=True)

@bot.slash_command(
    name="playpublic",
    description="Plays a public sound in your current voice channel."
)
@commands.cooldown(1, 4, commands.BucketType.user)
async def playpublic(
    ctx: discord.ApplicationContext,
    name: discord.Option(
        str,
        description="The name of the public sound to play (use /publicsounds).",
        required=True,
        autocomplete=public_sound_autocomplete
    ) # type: ignore
):
    """Handles playing a public sound file."""
    await ctx.defer() # Public defer okay
    author = ctx.author
    bot_logger.info(f"COMMAND: /playpublic invoked by {author.name} ({author.id}), requesting public sound name: '{name}'")

    public_sound_path = find_public_sound_path(name)
    sound_base_name = name

    if not public_sound_path:
        clean_name_try = sanitize_filename(name)
        if clean_name_try != name:
            public_sound_path = find_public_sound_path(clean_name_try)
            if public_sound_path:
                sound_base_name = clean_name_try

    if not public_sound_path:
        await ctx.followup.send(f"❌ Public sound named `{name}` not found. Use `/publicsounds`.", ephemeral=True)
        return

    # Use the generic play_single_sound for files
    await play_single_sound(ctx.interaction, public_sound_path)


@bot.slash_command(
    name="tts",
    description="Make the bot say something using Text-to-Speech in your channel."
)
@commands.cooldown(1, 6, commands.BucketType.user)
async def tts(
    ctx: discord.ApplicationContext,
    message: discord.Option(
        str,
        description=f"The text you want the bot to speak (max {MAX_TTS_LENGTH} chars).",
        required=True
    ), # type: ignore
    language: discord.Option(
        str,
        description="Language/accent for the TTS voice (default: English US).",
        required=False,
        default='en', # Default language
        choices=TTS_LANGUAGE_CHOICES # Use the defined choices
    ), # type: ignore
    slow: discord.Option(
        bool,
        description="Speak slowly? (Default: False)",
        required=False,
        default=False # Default speed
    ) # type: ignore
):
    """Generates TTS audio from text with language/speed options and plays it."""
    await ctx.defer(ephemeral=True)
    user = ctx.author
    guild = ctx.guild
    # <<< Log chosen options >>>
    bot_logger.info(f"COMMAND: /tts invoked by {user.name} ({user.id}), lang: {language}, slow: {slow}")

    # --- Initial Validations [UNCHANGED] ---
    if not guild:
        await ctx.followup.send("This command only works in a server.", ephemeral=True)
        return
    if not user.voice or not user.voice.channel:
        await ctx.followup.send("You need to be in a voice channel to use this command.", ephemeral=True)
        return
    if len(message) > MAX_TTS_LENGTH:
         await ctx.followup.send(f"❌ Message is too long ({len(message)} chars). Maximum is {MAX_TTS_LENGTH} characters.", ephemeral=True)
         return
    if not message.strip():
         await ctx.followup.send("❌ Please provide some text to speak.", ephemeral=True)
         return

    target_channel = user.voice.channel
    guild_id = guild.id

    bot_perms = target_channel.permissions_for(guild.me)
    if not bot_perms.connect or not bot_perms.speak:
        await ctx.followup.send(f"❌ I don't have permission to Connect or Speak in {target_channel.mention}.", ephemeral=True)
        return

    # --- Generate TTS In Memory ---
    audio_source: Optional[discord.PCMAudio] = None
    mp3_fp = io.BytesIO()
    pcm_fp = io.BytesIO()
    try:
        # <<< Log options being used for generation >>>
        bot_logger.info(f"TTS: Generating for '{user.name}' (lang={language}, slow={slow}) in guild {guild_id}: '{message[:50]}...'")
        # <<< Use the language and slow parameters from the command options >>>
        tts_instance = gTTS(text=message, lang=language, slow=slow)

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, tts_instance.write_to_fp, mp3_fp)
        mp3_fp.seek(0)
        bot_logger.debug(f"TTS: Generated MP3 in memory ({mp3_fp.getbuffer().nbytes} bytes)")

        if mp3_fp.getbuffer().nbytes == 0:
             raise ValueError("gTTS resulted in empty audio data.")

        # --- Process In-Memory Audio with Pydub [UNCHANGED relative to previous TTS] ---
        bot_logger.debug("TTS: Processing in-memory MP3 with Pydub...")
        audio_segment = AudioSegment.from_file(mp3_fp, format="mp3")
        audio_segment = audio_segment.set_frame_rate(48000).set_channels(2)
        audio_segment.export(pcm_fp, format="s16le")
        pcm_fp.seek(0)
        bot_logger.debug(f"TTS: Converted to PCM in memory ({pcm_fp.getbuffer().nbytes} bytes)")

        if pcm_fp.getbuffer().nbytes > 0:
            audio_source = discord.PCMAudio(pcm_fp)
            bot_logger.info(f"TTS: Successfully created PCMAudio source for {user.name}.")
        else:
            raise ValueError("Pydub export resulted in empty PCM data.")

    except gTTS.gTTSError as e: # Specific gTTS error handling
        bot_logger.error(f"TTS Generation Error for {user.name} (lang={language}): {e}", exc_info=True)
        # Check if it's likely an invalid language code error
        if "Language not found" in str(e) or "lang" in str(e).lower():
             await ctx.followup.send(f"❌ Error: The language code '{language}' seems unsupported by the TTS engine.", ephemeral=True)
        else:
            await ctx.followup.send(f"❌ Error generating TTS: {e}", ephemeral=True)
        return
    except ImportError as e:
        bot_logger.critical(f"TTS Error: Missing library: {e}", exc_info=True)
        await ctx.followup.send("❌ Error: A required library for TTS is missing on the bot.", ephemeral=True)
        return
    except Exception as e:
        bot_logger.error(f"TTS: Failed to generate or process TTS for {user.name}: {e}", exc_info=True)
        await ctx.followup.send(f"❌ Error during TTS processing: {e}", ephemeral=True)
        return
    finally:
        mp3_fp.close()

    if not audio_source:
        bot_logger.error("TTS: Failed to create audio source after processing.")
        await ctx.followup.send("❌ Failed to prepare TTS audio for playback.", ephemeral=True)
        return

    # --- Voice Connection & Playback Logic [UNCHANGED relative to previous TTS] ---
    voice_client = discord.utils.get(bot.voice_clients, guild=guild)
    try:
        # ... (Connection/Move/Busy Check Logic - exactly the same as before) ...
        if voice_client and voice_client.is_connected():
            if voice_client.is_playing():
                if guild_id in guild_sound_queues and guild_sound_queues[guild_id]:
                    await ctx.followup.send("⏳ Bot is currently playing join sounds. Please wait.", ephemeral=True)
                    bot_logger.info(f"TTS: Bot busy with join queue in {guild.name}, user {user.name} tried TTS. Request ignored.")
                else:
                    await ctx.followup.send("⏳ Bot is currently playing another sound/TTS. Please wait.", ephemeral=True)
                    bot_logger.info(f"TTS: Bot busy in {guild.name}, user {user.name} tried TTS. Request ignored.")
                return
            elif voice_client.channel != target_channel:
                bot_logger.info(f"TTS: Moving from '{voice_client.channel.name}' to '{target_channel.name}' for {user.name}.")
                await voice_client.move_to(target_channel)
                bot_logger.info(f"TTS: Moved successfully.")
        else:
            bot_logger.info(f"TTS: Connecting to '{target_channel.name}' for {user.name}.")
            voice_client = await target_channel.connect(timeout=30.0, reconnect=True)
            bot_logger.info(f"TTS: Connected successfully.")

        if not voice_client or not voice_client.is_connected():
             bot_logger.error(f"TTS: Failed to establish voice client for {target_channel.name} after connect/move attempt.")
             await ctx.followup.send("❌ Failed to connect/move to the voice channel.", ephemeral=True)
             return

    except asyncio.TimeoutError:
         await ctx.followup.send("❌ Connection to the voice channel timed out.", ephemeral=True)
         bot_logger.error(f"TTS: Connection/Move Timeout in {guild.name}")
         return
    except discord.errors.ClientException as e:
        if "already connecting" in str(e).lower() or "already disconnecting" in str(e).lower():
             await ctx.followup.send("⏳ Bot is busy connecting/disconnecting. Please wait a moment.", ephemeral=True)
             bot_logger.warning(f"TTS: Connection/Move failed in {guild.name}, already busy: {e}")
        else:
            await ctx.followup.send("❌ Error connecting/moving voice channel. Maybe check permissions?", ephemeral=True)
            bot_logger.error(f"TTS: Connection/Move ClientException in {guild.name}: {e}", exc_info=True)
        return
    except Exception as e:
        await ctx.followup.send("❌ An unexpected error occurred trying to join the voice channel.", ephemeral=True)
        bot_logger.error(f"TTS: Connection/Move unexpected error in {guild.name}: {e}", exc_info=True)
        return

    # --- Play TTS Audio [UNCHANGED relative to previous TTS] ---
    if voice_client.is_playing(): # Final check
         bot_logger.warning(f"TTS: Voice client became busy between check and play call for {user.name}. Aborting playback.")
         await ctx.followup.send("⏳ Bot became busy just now. Please try again.", ephemeral=True)
         after_play_handler(None, voice_client)
         return

    try:
        bot_logger.info(f"TTS PLAYBACK: Playing TTS requested by {user.display_name}...")
        voice_client.play(audio_source, after=lambda e: after_play_handler(e, voice_client))
        # Give slightly more informative feedback including the language/speed used
        speed_str = "(slow)" if slow else "(normal speed)"
        await ctx.followup.send(f"🗣️ Saying in `{language}` {speed_str}: \"{message[:100]}{'...' if len(message) > 100 else ''}\"", ephemeral=True)
    except discord.errors.ClientException as e:
        await ctx.followup.send("❌ Error: Already playing audio or another client issue occurred.", ephemeral=True)
        bot_logger.error(f"TTS PLAYBACK ERROR (ClientException): {e}", exc_info=True)
        after_play_handler(e, voice_client)
    except Exception as e:
        await ctx.followup.send("❌ An unexpected error occurred during TTS playback.", ephemeral=True)
        bot_logger.error(f"TTS PLAYBACK ERROR (Unexpected): {e}", exc_info=True)
        after_play_handler(e, voice_client)

# --- Error Handler for Application Commands [UNCHANGED] ---
@bot.event
async def on_application_command_error(ctx: discord.ApplicationContext, error: discord.DiscordException):
    """Handles errors raised during slash command execution."""
    command_name = ctx.command.qualified_name if ctx.command else "Unknown Command"
    user_name = ctx.author.name if ctx.author else "Unknown User"

    if isinstance(error, commands.CommandOnCooldown):
        retry_after = error.retry_after
        message = f"⏳ This command (`/{command_name}`) is on cooldown. Please try again in {retry_after:.1f} seconds."
        try:
            if not ctx.interaction.response.is_done(): await ctx.respond(message, ephemeral=True)
            else: await ctx.followup.send(message, ephemeral=True)
        except discord.NotFound: pass
        except Exception as e_resp: bot_logger.error(f"Failed to send cooldown error response for /{command_name}: {e_resp}")

    elif isinstance(error, commands.MissingPermissions):
        perms_list = "\n".join([f"- `{perm}`" for perm in error.missing_permissions])
        message = f"🚫 You do not have the required permissions to use `/{command_name}`.\nMissing:\n{perms_list}"
        bot_logger.warning(f"Permission Error: User {user_name} missing permissions for /{command_name}: {error.missing_permissions}")
        try:
            if not ctx.interaction.response.is_done(): await ctx.respond(message, ephemeral=True)
            else: await ctx.followup.send(message, ephemeral=True)
        except discord.NotFound: pass
        except Exception as e_resp: bot_logger.error(f"Failed to send MissingPermissions error response for /{command_name}: {e_resp}")

    elif isinstance(error, commands.BotMissingPermissions):
         perms_list = "\n".join([f"- `{perm}`" for perm in error.missing_permissions])
         message = f"🚫 I don't have the required permissions to execute `/{command_name}`.\nPlease ensure I have:\n{perms_list}"
         bot_logger.error(f"Bot Permission Error: Missing permissions for /{command_name}: {error.missing_permissions}")
         try:
             if not ctx.interaction.response.is_done(): await ctx.respond(message, ephemeral=True)
             else: await ctx.followup.send(message, ephemeral=True)
         except discord.Forbidden: bot_logger.error(f"Cannot inform user about missing bot perms for '/{command_name}'")
         except discord.NotFound: pass
         except Exception as e_resp: bot_logger.error(f"Failed to send BotMissingPermissions error response for /{command_name}: {e_resp}")

    elif isinstance(error, commands.CheckFailure):
        message = f"🚫 You do not meet the requirements to use the command `/{command_name}`."
        bot_logger.warning(f"Check Failure Error: User {user_name} failed checks for /{command_name}.")
        try:
            if not ctx.interaction.response.is_done(): await ctx.respond(message, ephemeral=True)
            else: await ctx.followup.send(message, ephemeral=True)
        except discord.NotFound: pass
        except Exception as e_resp: bot_logger.error(f"Failed to send CheckFailure error response for /{command_name}: {e_resp}")

    elif isinstance(error, discord.errors.ApplicationCommandInvokeError):
         original = error.original
         bot_logger.error(f"Error invoking application command '/{command_name}' by {user_name}: {original}", exc_info=original)
         if isinstance(original, FileNotFoundError): error_message = f"❌ Error running `/{command_name}`: A required file was not found."
         elif isinstance(original, CouldntDecodeError): error_message = f"❌ Error running `/{command_name}`: Could not process an audio file."
         # <<< ADDED TTS specific user error >>>
         elif isinstance(original, gTTS.gTTSError): error_message = f"❌ Error generating TTS: {original}"
         else: error_message = f"❌ An internal error occurred while running `/{command_name}`."
         try:
             if not ctx.interaction.response.is_done(): await ctx.respond(error_message, ephemeral=True)
             else: await ctx.followup.send(error_message, ephemeral=True)
         except discord.NotFound: pass
         except Exception as e_resp: bot_logger.error(f"Failed to send Invoke Error response message for /{command_name}: {e_resp}")

    else:
        bot_logger.error(f"Unhandled error in application command '/{command_name}' by {user_name}:", exc_info=error)
        error_message = f"❌ An unexpected error occurred running `/{command_name}`."
        try:
            if not ctx.interaction.response.is_done(): await ctx.respond(error_message, ephemeral=True)
            else: await ctx.followup.send(error_message, ephemeral=True)
        except discord.NotFound: pass
        except Exception as e_resp: bot_logger.error(f"Failed to send generic error response message for /{command_name}: {e_resp}")


# --- Run the Bot [UNCHANGED] ---
if __name__ == "__main__":
    if not PYDUB_AVAILABLE:
        bot_logger.critical("Pydub library is not available. Install it ('pip install pydub') and ensure FFmpeg is in your PATH. Bot cannot start.")
        exit(1)
    if not BOT_TOKEN:
        bot_logger.critical("BOT_TOKEN environment variable not set. Create a .env file or set the environment variable. Bot cannot start.")
        exit(1)

    # --- Opus Loading Check ---
    opus_loaded_successfully = discord.opus.is_loaded()
    if not opus_loaded_successfully:
        bot_logger.warning("Opus library not loaded by default. Attempting explicit load...")
        # Add paths relevant to your system if needed
        opus_paths_to_try = ["libopus.so.0", "opus", "libopus-0.dll", "/opt/homebrew/opt/opus/lib/libopus.0.dylib"] # Added potential M1 Mac path
        for opus_path in opus_paths_to_try:
             try:
                 if os.path.exists(opus_path) or opus_path == "opus": # Check existence for specific files
                     discord.opus.load_opus(opus_path)
                     opus_loaded_successfully = discord.opus.is_loaded()
                     if opus_loaded_successfully:
                         bot_logger.info(f"Opus library loaded successfully via: {opus_path}")
                         break
                 else:
                      bot_logger.debug(f"Opus path '{opus_path}' not found, skipping load attempt.")
             except Exception as e:
                 bot_logger.debug(f"Failed to load opus using path '{opus_path}': {e}")
                 opus_loaded_successfully = False

        if not opus_loaded_successfully:
             bot_logger.critical("="*30 + "\nOpus library STILL not loaded. Voice functionality WILL FAIL.\nInstall libopus and ensure it's findable (check PATH/LD_LIBRARY_PATH).\n" + "="*30)
             # Decide whether to exit or continue with broken voice
             # exit(1) # Exit if voice is critical

    # --- Start the Bot ---
    try:
        bot_logger.info("Attempting to start the bot...")
        bot.run(BOT_TOKEN)
    except discord.errors.LoginFailure:
        bot_logger.critical("="*30 + "\nLOGIN FAILURE: Invalid BOT_TOKEN provided.\n" + "="*30)
    except discord.errors.PrivilegedIntentsRequired as e:
        bot_logger.critical("="*30 + f"\nINTENT ERROR: Required intents are missing: {e.shard_id}\nEnsure Voice State/Server Members intents are enabled.\n" + "="*30)
    except Exception as e:
        if "opus" in str(e).lower() and isinstance(e, discord.errors.DiscordException) and not opus_loaded_successfully:
             bot_logger.critical(f"FATAL RUNTIME ERROR likely related to Opus (Opus failed to load earlier): {e}", exc_info=True)
        else:
             bot_logger.critical(f"FATAL RUNTIME ERROR during bot execution: {e}", exc_info=True)