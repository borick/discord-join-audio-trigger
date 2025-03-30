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
USER_SOUNDS_DIR = "usersounds" # For user-uploaded command sounds
SOUNDBOARD_DIR = "soundboard" # For soundboard sounds
CONFIG_FILE = "user_sounds.json" # For join sound mappings (user_id -> filename)
TARGET_LOUDNESS_DBFS = -14.0
MAX_USER_SOUND_SIZE_MB = 5
MAX_USER_SOUNDS_PER_USER = 10 # Limit how many command sounds a user can upload
ALLOWED_EXTENSIONS = ['.mp3', '.wav', '.ogg', '.m4a', '.aac'] # Allowed extensions for uploads

# --- Logging Setup ---
# Configure root logger and discord logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')
discord_logger = logging.getLogger('discord')
discord_logger.setLevel(logging.WARNING) # Reduce discord lib noise unless debugging

# Configure bot-specific logger
bot_logger = logging.getLogger('JoinSoundBot')
bot_logger.setLevel(logging.INFO) # Set bot's logging level

# --- Validate Critical Config ---
if not BOT_TOKEN:
    bot_logger.critical("CRITICAL ERROR: Bot token (BOT_TOKEN) not found in environment variables or .env file.")
    exit()
if not PYDUB_AVAILABLE:
    bot_logger.critical("CRITICAL ERROR: Pydub library failed to import. Cannot process audio.")
    exit()

# --- Intents ---
intents = discord.Intents.default()
intents.voice_states = True # Required for voice channel events
intents.guilds = True # Required for guild information access
# intents.message_content = False # Not needed for slash commands / voice

# --- Bot Definition ---
bot = discord.Bot(intents=intents)

# --- Data Storage & Helpers ---
user_sound_config = {} # Maps user ID (str) to their custom *join* sound filename (str)
# Dictionary to hold sound queues per guild {guild_id: deque([(member, sound_path), ...])}
guild_sound_queues = {}
# Dictionary to track if a play_next task is running per guild {guild_id: asyncio.Task}
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
             user_sound_config = {} # Reset to empty on load error
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
            # If essential directories fail, we might need to exit
            if dir_path in [SOUNDS_DIR, USER_SOUNDS_DIR, SOUNDBOARD_DIR]:
                exit(f"Failed to create essential directory: {dir_path}")

ensure_dir(SOUNDS_DIR)
ensure_dir(USER_SOUNDS_DIR)
ensure_dir(SOUNDBOARD_DIR)

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
    bot_logger.info(f"User command sounds directory: {os.path.abspath(USER_SOUNDS_DIR)}")
    bot_logger.info(f"Soundboard directory: {os.path.abspath(SOUNDBOARD_DIR)}")
    bot_logger.info("Join Sound Bot is operational.")
    # Command registration happens automatically via decorators by Pycord >= 2.0
    # Explicit syncing is usually only needed for specific debugging or guild commands.
    # try:
    #     # Use bot.sync_commands() if you need explicit global sync on startup
    #     # await bot.sync_commands()
    #     bot_logger.info("Application commands synced (implicitly or explicitly).")
    # except Exception as e:
    #     bot_logger.error(f"Error during command sync: {e}", exc_info=True)


# --- Audio Processing Helper ---
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
             # Ensure the assumed format is actually allowed by pydub/ffmpeg
             # format='mp3' might work for pydub.from_file

        audio_segment = AudioSegment.from_file(sound_path, format=file_extension)

        # --- Normalization ---
        peak_dbfs = audio_segment.max_dBFS
        if not math.isinf(peak_dbfs) and peak_dbfs > -90.0: # Avoid processing complete silence
            change_in_dbfs = TARGET_LOUDNESS_DBFS - peak_dbfs
            bot_logger.info(f"AUDIO: Normalizing '{os.path.basename(sound_path)}' for {member_display_name}. Peak:{peak_dbfs:.2f} Target:{TARGET_LOUDNESS_DBFS:.2f} Gain:{change_in_dbfs:.2f} dB.")
            # Apply gain using pydub's normalize (more robust) or apply_gain
            # audio_segment = audio_segment.normalize() # This targets -0 dBFS, maybe too loud
            # Let's stick to apply_gain for TARGET_LOUDNESS_DBFS
            if change_in_dbfs < 0: # Only apply gain if it's reducing volume
                 audio_segment = audio_segment.apply_gain(change_in_dbfs)
            else:
                 bot_logger.info(f"AUDIO: Skipping positive gain ({change_in_dbfs:.2f}dB) for '{os.path.basename(sound_path)}'.")
        elif math.isinf(peak_dbfs):
            bot_logger.warning(f"AUDIO: Cannot normalize silent sound ('{os.path.basename(sound_path)}'). Peak is -inf.")
        else: # Very quiet sound
            bot_logger.warning(f"AUDIO: Skipping normalization for very quiet sound ('{os.path.basename(sound_path)}'). Peak: {peak_dbfs:.2f} below -90 dBFS.")

        # --- Resampling and Channel Conversion (Discord prefers 48kHz stereo) ---
        audio_segment = audio_segment.set_frame_rate(48000).set_channels(2)

        # --- Export to Raw PCM for Discord ---
        pcm_data_io = io.BytesIO()
        # Use 's16le' for raw PCM export if 'raw' causes issues, discord.py expects signed 16-bit Little Endian
        audio_segment.export(pcm_data_io, format="s16le") # Explicitly PCM s16le
        pcm_data_io.seek(0)

        if pcm_data_io.getbuffer().nbytes > 0:
            audio_source = discord.PCMAudio(pcm_data_io)
            bot_logger.debug(f"AUDIO: Successfully processed '{os.path.basename(sound_path)}'")
        else:
            bot_logger.error(f"AUDIO: Exported raw audio data for '{os.path.basename(sound_path)}' is empty!")

    except CouldntDecodeError:
         bot_logger.error(f"AUDIO: Pydub CouldntDecodeError for '{os.path.basename(sound_path)}'. Is FFmpeg installed and in PATH? Is the file corrupted or an unsupported format?", exc_info=True)
    except FileNotFoundError: # Should be caught earlier, but safety net
        bot_logger.error(f"AUDIO: File not found during processing: '{sound_path}'")
    except Exception as e:
        bot_logger.error(f"AUDIO: Unexpected error processing '{os.path.basename(sound_path)}' for {member_display_name}: {e}", exc_info=True)

    return audio_source


# --- Core Join Sound Queue Logic (Queue Processor) ---
async def play_next_in_queue(guild: discord.Guild):
    """Processes the join sound queue for a given guild."""
    guild_id = guild.id
    task_id = asyncio.current_task().get_name() if asyncio.current_task() else 'Unknown Task'
    bot_logger.debug(f"QUEUE CHECK [{task_id}]: Checking queue for guild {guild_id}")

    if guild_id not in guild_sound_queues or not guild_sound_queues[guild_id]:
        bot_logger.debug(f"QUEUE [{task_id}]: Empty or non-existent for guild {guild_id}. Attempting disconnect.")
        await safe_disconnect(discord.utils.get(bot.voice_clients, guild=guild))
        # Clean up task tracker only if WE initiated the disconnect check and found queue empty
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

    # This check is crucial to prevent multiple sounds playing over each other
    if voice_client.is_playing():
        bot_logger.debug(f"QUEUE [{task_id}]: Bot is already playing in guild {guild_id}, play_next_in_queue will yield.")
        # The 'after' callback of the currently playing sound will trigger this function again.
        # Do NOT proceed to pop from queue or play now.
        return

    # Get next item from the queue *only if not currently playing*
    try:
        member, sound_path = guild_sound_queues[guild_id].popleft()
        bot_logger.info(f"QUEUE [{task_id}]: Processing join sound for {member.display_name} in {guild.name}. Path: {os.path.basename(sound_path)}. Remaining: {len(guild_sound_queues[guild_id])}")
    except IndexError:
        # Should be caught by the initial check, but safeguard
        bot_logger.debug(f"QUEUE [{task_id}]: Became empty unexpectedly for guild {guild_id} after play check.")
        await safe_disconnect(voice_client)
        if guild_id in guild_play_tasks and guild_play_tasks[guild_id] is asyncio.current_task(): del guild_play_tasks[guild_id]
        return

    # --- Prepare Audio Source ---
    audio_source = process_audio(sound_path, member.display_name)

    # --- Play Audio ---
    if audio_source:
        try:
            bot_logger.info(f"QUEUE PLAYBACK [{task_id}]: Playing join sound for {member.display_name}...")
            # Ensure the voice_client is passed to the after handler
            voice_client.play(audio_source, after=lambda e: after_play_handler(e, voice_client)) # Pass voice_client
            bot_logger.debug(f"QUEUE PLAYBACK [{task_id}]: vc.play() called for join sound of {member.display_name}.")
        except discord.errors.ClientException as e:
            bot_logger.error(f"QUEUE PLAYBACK ERROR [{task_id}] (ClientException): Bot potentially already playing or disconnected unexpectedly. {e}", exc_info=True)
            # If play failed immediately, put item back? Or just try next? Let's try next.
            bot.loop.create_task(play_next_in_queue(guild), name=f"QueueRetry_{guild_id}")
        except Exception as e:
            bot_logger.error(f"QUEUE PLAYBACK ERROR [{task_id}] (Unexpected): {e}", exc_info=True)
            bot.loop.create_task(play_next_in_queue(guild), name=f"QueueRetry_{guild_id}")
    else:
        bot_logger.warning(f"QUEUE PLAYBACK [{task_id}]: No valid audio source for {member.display_name}'s join sound ({os.path.basename(sound_path)}). Skipping.")
        # Trigger next item check immediately since nothing played
        bot.loop.create_task(play_next_in_queue(guild), name=f"QueueSkip_{guild_id}")


# --- Voice State Update Handler (Adds to Join Sound Queue) ---
@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot: return # Ignore bots joining/leaving

    # Trigger only when joining a channel the bot can see, or switching TO a new channel
    if after.channel is not None and before.channel != after.channel:
        channel_to_join = after.channel
        guild = member.guild
        bot_logger.info(f"EVENT: {member.display_name} ({member.id}) entered voice channel {channel_to_join.name} ({channel_to_join.id}) in guild {guild.name} ({guild.id})")

        # --- Check Permissions in the target channel ---
        bot_perms = channel_to_join.permissions_for(guild.me)
        if not bot_perms.connect or not bot_perms.speak:
            bot_logger.warning(f"Missing Connect ({bot_perms.connect}) or Speak ({bot_perms.speak}) permission in '{channel_to_join.name}'. Cannot play join sound for {member.display_name}.")
            return

        # --- Determine Join Sound Path ---
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
                del user_sound_config[user_id_str] # Remove broken config entry
                save_config()
                is_tts = True
        else:
            is_tts = True
            bot_logger.info(f"SOUND: No custom join sound config found for {member.display_name} ({user_id_str}). Using TTS.")

        if is_tts:
            tts_filename = f"tts_{member.id}.mp3" # Use user ID for TTS filename consistency
            tts_path = os.path.join(SOUNDS_DIR, tts_filename)

            # Generate TTS only if it doesn't exist
            if not os.path.exists(tts_path):
                bot_logger.info(f"TTS: Generating for {member.display_name} ('{tts_path}')...")
                tts_text = f"{member.display_name} joined" # Simple join message
                try:
                    # Run gTTS in an executor to avoid blocking the event loop
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, lambda: gTTS(text=tts_text, lang='en').save(tts_path))
                    bot_logger.info(f"TTS: Saved '{tts_path}'")
                    sound_path = tts_path
                except Exception as e:
                    bot_logger.error(f"TTS: Failed generation for {member.display_name}: {e}", exc_info=True)
                    sound_path = None # Don't queue if TTS failed
            else:
                 bot_logger.info(f"TTS: Using existing file: '{tts_path}'")
                 sound_path = tts_path # Use existing TTS file

        # If after all checks, we don't have a sound path, log and return
        if not sound_path:
            bot_logger.error(f"Could not determine or generate a join sound/TTS path for {member.display_name}. Skipping queue add.")
            return

        # --- Add to Guild Queue ---
        guild_id = guild.id
        if guild_id not in guild_sound_queues:
            guild_sound_queues[guild_id] = deque()

        queue_item = (member, sound_path) # Store member object and sound path
        guild_sound_queues[guild_id].append(queue_item)
        bot_logger.info(f"QUEUE: Added join sound for {member.display_name} to queue for guild {guild.name}. Queue size: {len(guild_sound_queues[guild_id])}")

        # --- Ensure Connection and Trigger Player Task ---
        voice_client = discord.utils.get(bot.voice_clients, guild=guild)

        # Only connect/move if the bot isn't *currently* playing a sound.
        # Let the after_play_handler manage connection/playing if busy.
        if voice_client and voice_client.is_playing():
            bot_logger.info(f"VOICE: Bot is currently playing in {guild.name}. Join sound for {member.display_name} queued. Connection/play deferred.")
            # Ensure a play task exists to process the queue later
            if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
                 task_name = f"QueueTrigger_{guild_id}"
                 guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(guild), name=task_name)
                 bot_logger.debug(f"VOICE: Created deferred play task '{task_name}' due to active playback.")
            return # Stop here, don't try to connect/move now

        # If not playing, proceed with connection/move logic
        should_start_play_task = False
        try:
            if not voice_client or not voice_client.is_connected():
                bot_logger.info(f"VOICE: Connecting to '{channel_to_join.name}' to start join sound queue processing.")
                voice_client = await channel_to_join.connect(timeout=30.0, reconnect=True) # Added timeout/reconnect
                bot_logger.info(f"VOICE: Successfully connected to '{channel_to_join.name}'.")
                should_start_play_task = True # Start task after successful connect
            elif voice_client.channel != channel_to_join:
                 bot_logger.info(f"VOICE: Moving from '{voice_client.channel.name}' to '{channel_to_join.name}' to process join sound queue.")
                 await voice_client.move_to(channel_to_join)
                 bot_logger.info(f"VOICE: Successfully moved to '{channel_to_join.name}'.")
                 should_start_play_task = True # Start task after successful move
            else: # Already connected in the right channel and not playing
                 bot_logger.debug(f"VOICE: Bot already connected in '{channel_to_join.name}' and not playing.")
                 should_start_play_task = True # Ensure task runs if queue isn't empty

        except asyncio.TimeoutError:
            bot_logger.error(f"VOICE: Connection to '{channel_to_join.name}' timed out.")
            if guild_id in guild_sound_queues: guild_sound_queues[guild_id].clear() # Clear queue on connect fail
        except discord.errors.ClientException as e:
            bot_logger.error(f"VOICE: ClientException during connect/move to '{channel_to_join.name}': {e}", exc_info=True)
            # Potentially already connected elsewhere or permissions issue
            if guild_id in guild_sound_queues: guild_sound_queues[guild_id].clear()
        except Exception as e:
             bot_logger.error(f"VOICE: Unexpected error during connect/move to '{channel_to_join.name}': {e}", exc_info=True)
             if guild_id in guild_sound_queues: guild_sound_queues[guild_id].clear()

        # Start the play task if needed and no other task is active/pending
        if should_start_play_task and voice_client and voice_client.is_connected():
            if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
                task_name = f"QueueStart_{guild_id}"
                guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(guild), name=task_name)
                bot_logger.info(f"VOICE: Started play task '{task_name}' for guild {guild_id}.")
            else:
                 bot_logger.debug(f"VOICE: Play task for guild {guild_id} already exists and is not done.")


# --- After Play Handler (Triggers Next in Queue or Disconnect Check) ---
def after_play_handler(error: Optional[Exception], voice_client: discord.VoiceClient):
    """Callback registered in voice_client.play(). Runs after ANY sound finishes."""
    guild_id = voice_client.guild.id if voice_client and voice_client.guild else None
    if error:
        bot_logger.error(f'PLAYBACK ERROR (In after_play_handler for guild {guild_id}): {error}', exc_info=error)

    if not guild_id:
        bot_logger.warning("after_play_handler called with invalid/disconnected voice_client or no guild.")
        return

    bot_logger.debug(f"Playback finished for guild {guild_id}. Triggering queue check.")

    # Crucially, schedule the play_next_in_queue task to run again.
    # This function will check the queue:
    # 1. If queue has items -> play the next one.
    # 2. If queue is empty -> disconnect (via safe_disconnect).
    # Ensure only one check task runs or is scheduled.
    if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
         task_name = f"QueueCheckAfterPlay_{guild_id}"
         guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(voice_client.guild), name=task_name)
         bot_logger.debug(f"AFTER_PLAY: Scheduled task '{task_name}' for guild {guild_id}.")
    else:
         # An existing task is already running/scheduled, let it handle the next step.
         bot_logger.debug(f"AFTER_PLAY: Task for guild {guild_id} already exists, not creating duplicate check task.")


# --- Disconnect Logic (Called by play_next_in_queue) ---
async def safe_disconnect(voice_client: Optional[discord.VoiceClient]):
    """Safely disconnects if connected, not playing, AND join queue is empty."""
    if not voice_client or not voice_client.is_connected():
        # bot_logger.debug("Safe disconnect called but client already disconnected or invalid.")
        return # Nothing to do

    guild = voice_client.guild
    guild_id = guild.id

    # Check queue is empty AND bot isn't somehow still marked as playing
    is_queue_empty = guild_id not in guild_sound_queues or not guild_sound_queues[guild_id]
    is_playing = voice_client.is_playing() # Re-check playing status

    if is_queue_empty and not is_playing:
        bot_logger.info(f"DISCONNECT: Conditions met for guild {guild_id} (Queue empty, not playing). Disconnecting...")
        try:
            # Stop potentially lingering playback just in case before disconnecting
            if voice_client.is_playing(): voice_client.stop()
            await voice_client.disconnect(force=False) # force=False preferred
            bot_logger.info(f"DISCONNECT: Bot disconnected from '{guild.name}'.")
            # Clean up task tracker associated with this guild IF it exists
            if guild_id in guild_play_tasks:
                 del guild_play_tasks[guild_id]
                 bot_logger.debug(f"DISCONNECT: Removed play task tracker for guild {guild_id}.")
        except Exception as e:
            bot_logger.error(f"DISCONNECT ERROR: Failed to disconnect from {guild.name}: {e}", exc_info=True)
    else:
         bot_logger.debug(f"Disconnect skipped for guild {guild.name}: Queue empty={is_queue_empty}, Playing={is_playing}.")


# --- Single Sound Playback Logic ---
async def play_single_sound(interaction: discord.Interaction, sound_path: str):
    """Connects (if needed), plays a single sound, and uses after_play_handler."""
    user = interaction.user
    guild = interaction.guild

    # Use followup for responses as the initial response was likely deferred
    if not guild:
        await interaction.followup.send("This command only works in a server.", ephemeral=True)
        return

    if not user.voice or not user.voice.channel:
        await interaction.followup.send("You need to be in a voice channel to use this command.", ephemeral=True)
        return

    target_channel = user.voice.channel
    guild_id = guild.id

    # Check bot permissions in the target channel
    bot_perms = target_channel.permissions_for(guild.me)
    if not bot_perms.connect or not bot_perms.speak:
        await interaction.followup.send(f"❌ I don't have permission to Connect or Speak in {target_channel.mention}.", ephemeral=True)
        return

    if not os.path.exists(sound_path):
         await interaction.followup.send("❌ Error: The sound file seems to be missing.", ephemeral=True)
         bot_logger.error(f"SINGLE PLAY: File not found: {sound_path}")
         return

    voice_client = discord.utils.get(bot.voice_clients, guild=guild)

    # --- Connection / Movement / Busy Check Logic ---
    try:
        if voice_client and voice_client.is_connected():
            if voice_client.is_playing():
                # If already playing ANY sound (join or single), tell user to wait.
                await interaction.followup.send("⏳ Bot is currently playing another sound. Please wait a moment.", ephemeral=True)
                bot_logger.info(f"SINGLE PLAY: Bot busy in {guild.name}, user {user.name} tried to play '{os.path.basename(sound_path)}'. Request ignored.")
                return # Don't proceed
            elif voice_client.channel != target_channel:
                # Connected but wrong channel, move.
                bot_logger.info(f"SINGLE PLAY: Moving from '{voice_client.channel.name}' to '{target_channel.name}' for {user.name}.")
                await voice_client.move_to(target_channel)
                bot_logger.info(f"SINGLE PLAY: Moved successfully.")
            # else: Already connected to the right channel and not playing. Good to go.
        else:
            # Not connected, connect fresh.
            bot_logger.info(f"SINGLE PLAY: Connecting to '{target_channel.name}' for {user.name}.")
            voice_client = await target_channel.connect(timeout=30.0, reconnect=True)
            bot_logger.info(f"SINGLE PLAY: Connected successfully.")

        # Ensure we have a valid voice_client after connect/move attempt
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

    # --- Process and Play Audio ---
    bot_logger.info(f"SINGLE PLAY: Processing '{os.path.basename(sound_path)}' for {user.name}...")
    audio_source = process_audio(sound_path, user.display_name)

    if audio_source:
        # Safety check: Ensure not playing *again* right before calling play
        # (Might be redundant due to earlier checks, but belts and suspenders)
        if voice_client.is_playing():
             bot_logger.warning(f"SINGLE PLAY: Voice client became busy between check and play call for {user.name}. Aborting playback.")
             await interaction.followup.send("⏳ Bot became busy just now. Please try again.", ephemeral=True)
             return

        try:
            bot_logger.info(f"SINGLE PLAYBACK: Playing '{os.path.basename(sound_path)}' requested by {user.display_name}...")
            # Use the SAME after_play_handler - it correctly triggers the queue check afterwards
            voice_client.play(audio_source, after=lambda e: after_play_handler(e, voice_client))
            # Send confirmation *after* starting playback attempt
            await interaction.followup.send(f"▶️ Playing `{os.path.basename(sound_path)}`...", ephemeral=True) # Ephemeral confirmation
        except discord.errors.ClientException as e:
            await interaction.followup.send("❌ Error: Already playing audio or another client issue occurred.", ephemeral=True)
            bot_logger.error(f"SINGLE PLAYBACK ERROR (ClientException): {e}", exc_info=True)
            # If play fails immediately, trigger the handler manually to check queue/disconnect state
            after_play_handler(e, voice_client)
        except Exception as e:
            await interaction.followup.send("❌ An unexpected error occurred during playback.", ephemeral=True)
            bot_logger.error(f"SINGLE PLAYBACK ERROR (Unexpected): {e}", exc_info=True)
            after_play_handler(e, voice_client) # Also trigger handler
    else:
        await interaction.followup.send("❌ Error: Could not process the audio file. Check bot logs.", ephemeral=True)
        bot_logger.error(f"SINGLE PLAYBACK: Failed to get audio source for '{sound_path}'")
        # Since nothing played, manually trigger the handler to potentially disconnect if needed
        # Check if VC is still valid before calling handler
        if voice_client and voice_client.is_connected():
            after_play_handler(None, voice_client)


# --- Helper: Sanitize Filename ---
def sanitize_filename(name: str) -> str:
    """Removes disallowed characters for filenames and limits length."""
    # Remove characters disallowed in Windows/Linux filenames, replace spaces
    name = re.sub(r'[<>:"/\\|?*\.\s]+', '_', name)
    name = re.sub(r'_+', '_', name) # Collapse multiple underscores
    name = name.strip('_') # Remove leading/trailing underscores
    return name[:50] # Limit length to avoid excessively long filenames


# --- Helper: Get User Sound Files ---
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
def find_user_sound_path(user_id: int, sound_name: str) -> Optional[str]:
    """Finds the full path for a user's command sound by name, checking allowed extensions."""
    user_dir = os.path.join(USER_SOUNDS_DIR, str(user_id))
    if not os.path.isdir(user_dir):
        return None
    # Check common extensions first for slight optimization
    preferred_order = ['.mp3', '.wav'] + [ext for ext in ALLOWED_EXTENSIONS if ext not in ['.mp3', '.wav']]
    for ext in preferred_order:
        # Check for exact match first (case-sensitive filename might matter on Linux)
        potential_path_exact = os.path.join(user_dir, f"{sound_name}{ext}")
        if os.path.exists(potential_path_exact):
            return potential_path_exact
        # Add case-insensitive check if needed, though sanitize_filename helps standardize
    bot_logger.debug(f"Sound '{sound_name}' not found for user {user_id} in {user_dir} with extensions {ALLOWED_EXTENSIONS}")
    return None # Not found


# --- Autocomplete Functions ---
async def user_sound_autocomplete(ctx: discord.AutocompleteContext) -> List[str]:
    """Provides autocomplete suggestions for the user's uploaded command sounds."""
    user_id = ctx.interaction.user.id
    try:
        user_sounds = get_user_sound_files(user_id)
        current_value = ctx.value.lower() if ctx.value else ""
        # Filter suggestions based on current input
        suggestions = [
            name for name in user_sounds if current_value in name.lower()
        ]
        # Sort suggestions alphabetically
        suggestions.sort()
        # Return up to 25 suggestions (Discord limit)
        return suggestions[:25]
    except Exception as e:
         bot_logger.error(f"Error during autocomplete for user {user_id}: {e}", exc_info=True)
         return [] # Return empty list on error


# --- Slash Commands ---

# === Join Sound Commands ===

@bot.slash_command(
    name="setjoinsound",
    description="Upload your custom join sound (MP3, WAV etc). Replaces any existing one."
)
@commands.cooldown(1, 15, commands.BucketType.user)
async def setjoinsound(
    ctx: discord.ApplicationContext,
    # NOTE: Using discord.Option directly in type hints is the standard Pycord v2+ way.
    # Your IDE linter (Pyright/Pylance) might show a "Call not allowed in type expression" warning.
    # This warning can usually be ignored as Pycord processes this annotation correctly.
    sound_file: discord.Option(
        discord.Attachment,
        description=f"Sound file ({', '.join(ALLOWED_EXTENSIONS)}). Max {MAX_USER_SOUND_SIZE_MB}MB.",
        required=True
    ) # type: ignore <-- Optional: Add this comment to potentially suppress IDE warnings
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

    # Basic content type check (less reliable than extension but good fallback)
    if not sound_file.content_type or not sound_file.content_type.startswith('audio/'):
         bot_logger.warning(f"Content-Type '{sound_file.content_type}' for '{sound_file.filename}' is not 'audio/*'. Proceeding based on extension '{file_extension}'.")

    if sound_file.size > MAX_USER_SOUND_SIZE_MB * 1024 * 1024:
        await ctx.followup.send(f"❌ File is too large (`{sound_file.size / (1024*1024):.2f}` MB). Maximum size is {MAX_USER_SOUND_SIZE_MB}MB.", ephemeral=True)
        return

    # --- Save Temporarily and Validate ---
    # Use a consistent naming scheme for join sounds (userid + original extension)
    temp_save_filename = f"temp_joinvalidate_{user_id_str}{file_extension}"
    temp_save_path = os.path.join(SOUNDS_DIR, temp_save_filename)
    final_save_filename = f"{user_id_str}{file_extension}" # Final name for join sound file
    final_save_path = os.path.join(SOUNDS_DIR, final_save_filename)

    # Ensure temp file is cleaned up
    async def cleanup_temp():
        if os.path.exists(temp_save_path):
            try:
                os.remove(temp_save_path)
                bot_logger.debug(f"Cleaned up temporary file: {temp_save_path}")
            except Exception as del_e:
                bot_logger.warning(f"Failed to cleanup temporary file {temp_save_path}: {del_e}")

    try:
        # Download the file from Discord
        await sound_file.save(temp_save_path)
        bot_logger.info(f"Saved temporary join sound for validation: '{temp_save_path}'")

        # --- Pydub Validation (Decoding Check) ---
        try:
            bot_logger.debug(f"Attempting Pydub decode validation: '{temp_save_path}'")
            # Try loading the file to see if pydub can decode it
            _ = AudioSegment.from_file(temp_save_path, format=file_extension.strip('.'))
            bot_logger.info(f"Pydub validation successful for join sound: '{temp_save_path}'")

            # --- Overwrite/Rename Logic ---
            # Check if user already has a join sound config AND the old file exists
            if user_id_str in user_sound_config:
                old_config_filename = user_sound_config[user_id_str]
                # Only remove if the old filename is different (e.g., changing mp3 to wav)
                if old_config_filename != final_save_filename:
                    old_path = os.path.join(SOUNDS_DIR, old_config_filename)
                    if os.path.exists(old_path):
                        try:
                            os.remove(old_path)
                            bot_logger.info(f"Removed previous join sound file due to overwrite: '{old_path}'")
                        except Exception as e:
                            bot_logger.warning(f"Could not remove previous join sound file '{old_path}' during overwrite: {e}")

            # Rename temp file to final filename (atomic replace if possible)
            try:
                os.replace(temp_save_path, final_save_path) # Overwrites if final_save_path exists
                bot_logger.info(f"Final join sound saved: '{final_save_path}'")
            except OSError as rep_e:
                bot_logger.error(f"Failed to replace/rename '{temp_save_path}' to '{final_save_path}': {rep_e}.", exc_info=True)
                await cleanup_temp() # Ensure temp file is removed on failure
                await ctx.followup.send("❌ Error saving the sound file. Please try again.", ephemeral=True)
                return # Stop processing

            # Update config JSON and save it
            user_sound_config[user_id_str] = final_save_filename
            save_config() # Persist the change
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

        # Remove from config first
        del user_sound_config[user_id_str]
        save_config() # Persist the change
        bot_logger.info(f"Removed join sound config entry for {author.name} ({user_id_str})")

        # Attempt to remove the actual file
        if os.path.exists(file_path_to_remove):
            try:
                os.remove(file_path_to_remove)
                bot_logger.info(f"Deleted join sound file: '{file_path_to_remove}'")
            except OSError as e:
                # Log warning but don't stop user notification
                bot_logger.warning(f"Could not delete join sound file '{file_path_to_remove}': {e}")
        else:
            # File mentioned in config was already gone
            bot_logger.warning(f"Join sound file '{filename_to_remove}' for user {user_id_str} was configured but not found at '{file_path_to_remove}' during removal.")

        await ctx.followup.send("🗑️ Your custom join sound has been removed. The default TTS will be used next time you join.", ephemeral=True)
    else:
        # User didn't have a custom sound set
        await ctx.followup.send("🤷 You don't currently have a custom join sound configured.", ephemeral=True)


# === User Command Sound Commands ===

@bot.slash_command(
    name="uploadsound",
    description=f"Upload a named sound for use with /playsound (Max {MAX_USER_SOUNDS_PER_USER} sounds)."
)
@commands.cooldown(2, 20, commands.BucketType.user) # Allow 2 uploads per 20 sec per user
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

    # --- Input Validation ---
    clean_name = sanitize_filename(name)
    if not clean_name:
        await ctx.followup.send("❌ Please provide a valid name using only letters, numbers, or underscores.", ephemeral=True)
        return
    if clean_name != name:
         bot_logger.warning(f"Sanitized sound name for user {user_id}: '{name}' -> '{clean_name}'")
         # Optionally notify user: await ctx.followup.send(f"ℹ️ Your sound name was sanitized to `{clean_name}` for compatibility.", ephemeral=True)

    # --- File Validation (Similar to joinsound) ---
    file_extension = os.path.splitext(sound_file.filename)[1].lower()
    if file_extension not in ALLOWED_EXTENSIONS:
        await ctx.followup.send(f"❌ Invalid file type (`{file_extension}`). Allowed: {', '.join(ALLOWED_EXTENSIONS)}", ephemeral=True)
        return
    if not sound_file.content_type or not sound_file.content_type.startswith('audio/'):
         bot_logger.warning(f"Content-Type '{sound_file.content_type}' for '{sound_file.filename}' not 'audio/*'. Proceeding based on extension.")
    if sound_file.size > MAX_USER_SOUND_SIZE_MB * 1024 * 1024:
        await ctx.followup.send(f"❌ File too large (`{sound_file.size / (1024*1024):.2f}` MB). Max: {MAX_USER_SOUND_SIZE_MB}MB.", ephemeral=True)
        return

    # --- User Sound Limit Check ---
    user_dir = os.path.join(USER_SOUNDS_DIR, str(user_id))
    ensure_dir(user_dir) # Create user's directory if it doesn't exist yet
    current_sounds = get_user_sound_files(user_id) # Gets names without extension

    # Check if replacing existing sound OR adding new one would exceed limit
    # Use find_user_sound_path to see if a file with this *name* already exists (regardless of ext)
    existing_sound_path = find_user_sound_path(user_id, clean_name)
    is_replacing = existing_sound_path is not None

    if not is_replacing and len(current_sounds) >= MAX_USER_SOUNDS_PER_USER:
         await ctx.followup.send(f"❌ You have reached the maximum limit of {MAX_USER_SOUNDS_PER_USER} sounds. Use `/deletesound` to remove some before adding new ones.", ephemeral=True)
         return

    # --- Save Temporarily and Validate ---
    # Temp file in main user sounds dir to avoid cluttering specific user dirs with temps
    temp_save_filename = f"temp_cmdvalidate_{user_id}_{clean_name}{file_extension}"
    temp_save_path = os.path.join(USER_SOUNDS_DIR, temp_save_filename)
    # Final path inside the user's specific folder
    final_save_filename = f"{clean_name}{file_extension}"
    final_save_path = os.path.join(user_dir, final_save_filename)

    # Cleanup function for user temp file
    async def cleanup_temp_user():
        if os.path.exists(temp_save_path):
            try: os.remove(temp_save_path); bot_logger.debug(f"Cleaned up {temp_save_path}")
            except Exception as del_e: bot_logger.warning(f"Failed cleanup {temp_save_path}: {del_e}")

    try:
        await sound_file.save(temp_save_path)
        bot_logger.info(f"Saved temporary command sound for validation: '{temp_save_path}'")

        # --- Pydub Validation ---
        try:
            bot_logger.debug(f"Attempting Pydub decode validation: '{temp_save_path}'")
            _ = AudioSegment.from_file(temp_save_path, format=file_extension.strip('.'))
            bot_logger.info(f"Pydub validation successful for command sound: '{temp_save_path}'")

            # --- Overwrite/Rename Logic ---
            # If replacing, remove the OLD file first (which might have a different extension)
            if is_replacing and existing_sound_path and existing_sound_path != final_save_path:
                try:
                    os.remove(existing_sound_path)
                    bot_logger.info(f"Removed existing sound '{os.path.basename(existing_sound_path)}' for user {user_id} due to overwrite with new extension.")
                except Exception as e:
                    bot_logger.warning(f"Could not remove conflicting existing sound file '{existing_sound_path}': {e}")

            # Move the validated file to the user's directory
            try:
                os.replace(temp_save_path, final_save_path) # Move/overwrite into user's dir
                bot_logger.info(f"Final command sound saved for user {user_id}: '{final_save_path}'")
            except OSError as rep_e:
                bot_logger.error(f"Failed to replace/rename '{temp_save_path}' to '{final_save_path}': {rep_e}.", exc_info=True)
                await cleanup_temp_user()
                await ctx.followup.send("❌ Error saving the sound file to your collection.", ephemeral=True)
                return

            action_word = "updated" if is_replacing else "uploaded"
            await ctx.followup.send(f"✅ Success! Sound `{clean_name}` {action_word}. Use `/playsound name:{clean_name}`.", ephemeral=True)

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
    description="Lists your uploaded command sounds."
)
@commands.cooldown(1, 10, commands.BucketType.user)
async def mysounds(ctx: discord.ApplicationContext):
    """Displays a list of the user's uploaded command sounds."""
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id = author.id
    bot_logger.info(f"COMMAND: /mysounds invoked by {author.name} ({user_id})")
    user_sounds = get_user_sound_files(user_id) # Gets list of names (no ext)

    if not user_sounds:
        await ctx.followup.send("You haven't uploaded any sounds yet. Use `/uploadsound` to add some!", ephemeral=True)
        return

    # Create a formatted list for the embed
    # Sort alphabetically for consistency
    sorted_sounds = sorted(user_sounds)
    sound_list_str = "\n".join([f"- `{name}`" for name in sorted_sounds])

    # Handle potential description length limits in Embed
    if len(sound_list_str) > 4000: # Embed field value limit is 1024, description is 4096
         sound_list_str = sound_list_str[:4000] + "\n... (list truncated)"

    embed = discord.Embed(
        title=f"{author.display_name}'s Sounds ({len(sorted_sounds)}/{MAX_USER_SOUNDS_PER_USER})",
        description=f"Use `/playsound name:<sound_name>` to play one.\n\n{sound_list_str}",
        color=discord.Color.blurple() # Or user.color
    )
    embed.set_footer(text="Use /deletesound to remove sounds.")

    await ctx.followup.send(embed=embed, ephemeral=True)


@bot.slash_command(
    name="deletesound",
    description="Deletes one of your uploaded command sounds by name."
)
@commands.cooldown(1, 5, commands.BucketType.user)
async def deletesound(
    ctx: discord.ApplicationContext,
    name: discord.Option(
        str,
        description="The name of the sound to delete (use /mysounds to see names).",
        required=True,
        autocomplete=user_sound_autocomplete # Use the autocomplete helper
    ) # type: ignore
):
    """Handles deleting one of the user's command sounds."""
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id = author.id
    bot_logger.info(f"COMMAND: /deletesound invoked by {author.name} ({user_id}), trying to delete name: '{name}'")

    # Find the actual file path based on the name (checks extensions)
    sound_path = find_user_sound_path(user_id, name)

    if not sound_path:
        # Check if the sanitized version exists if the raw input doesn't
        clean_name = sanitize_filename(name)
        if clean_name != name:
            sound_path = find_user_sound_path(user_id, clean_name)

    if not sound_path:
        await ctx.followup.send(f"❌ Sound named `{name}` not found in your collection. Use `/mysounds` to check available names.", ephemeral=True)
        return

    try:
        deleted_filename = os.path.basename(sound_path)
        os.remove(sound_path)
        bot_logger.info(f"Deleted command sound '{deleted_filename}' ({sound_path}) for user {user_id}.")
        await ctx.followup.send(f"🗑️ Sound `{name}` (file: `{deleted_filename}`) deleted successfully.", ephemeral=True)
    except OSError as e:
        bot_logger.error(f"Failed to delete command sound file '{sound_path}' for user {user_id}: {e}", exc_info=True)
        await ctx.followup.send(f"❌ Failed to delete sound `{name}` due to a file system error. Please try again later or contact support if it persists.", ephemeral=True)
    except Exception as e:
         bot_logger.error(f"Unexpected error during sound deletion for user {user_id}, path '{sound_path}': {e}", exc_info=True)
         await ctx.followup.send(f"❌ An unexpected error occurred while trying to delete `{name}`.", ephemeral=True)


@bot.slash_command(
    name="playsound",
    description="Plays one of your uploaded sounds in your current voice channel."
)
@commands.cooldown(1, 4, commands.BucketType.user) # Allow playing every 4 seconds per user
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
    # Defer publicly initially, the actual feedback will come from play_single_sound (often ephemeral)
    await ctx.defer() # Let Discord know we received the command
    author = ctx.author
    user_id = author.id
    bot_logger.info(f"COMMAND: /playsound invoked by {author.name} ({user_id}), requesting sound name: '{name}'")

    sound_path = find_user_sound_path(user_id, name)

    # Try sanitized name if original not found
    if not sound_path:
        clean_name = sanitize_filename(name)
        if clean_name != name:
             sound_path = find_user_sound_path(user_id, clean_name)

    if not sound_path:
        await ctx.followup.send(f"❌ Sound named `{name}` not found. Use `/mysounds` to see your uploads.", ephemeral=True)
        return

    # Call the generic single play function, passing the interaction context
    await play_single_sound(ctx.interaction, sound_path)


# === Soundboard ===

class SoundboardView(discord.ui.View):
    """A View containing buttons to play sounds from the soundboard directory."""
    def __init__(self, *, timeout: Optional[float] = 300.0): # Timeout after 5 minutes (300s)
        super().__init__(timeout=timeout)
        self.message: Optional[discord.Message] = None # To store the message this view is attached to
        self.populate_buttons()

    def populate_buttons(self):
        """Scans the SOUNDBOARD_DIR and adds buttons for valid audio files."""
        bot_logger.debug(f"Populating soundboard buttons from: {os.path.abspath(SOUNDBOARD_DIR)}")
        if not os.path.isdir(SOUNDBOARD_DIR):
            bot_logger.error(f"Soundboard directory '{SOUNDBOARD_DIR}' not found or is not a directory!")
            # Add a single disabled button indicating the error
            button = discord.ui.Button(label="Error: Soundboard Directory Missing", style=discord.ButtonStyle.danger, disabled=True, row=0)
            self.add_item(button)
            return

        sounds_found = 0
        button_row = 0
        max_buttons_per_row = 5
        max_rows = 5 # Discord View limit
        max_buttons_total = max_buttons_per_row * max_rows # 25 button limit

        try:
            # Sort files for somewhat consistent order (OS-dependent)
            files_in_dir = sorted(os.listdir(SOUNDBOARD_DIR))
        except OSError as e:
            bot_logger.error(f"Error listing soundboard directory '{SOUNDBOARD_DIR}': {e}")
            button = discord.ui.Button(label="Error: Cannot Read Soundboard Dir", style=discord.ButtonStyle.danger, disabled=True, row=0)
            self.add_item(button)
            return

        for filename in files_in_dir:
            if sounds_found >= max_buttons_total:
                bot_logger.warning(f"Reached maximum soundboard button limit ({max_buttons_total}). Skipping remaining files in '{SOUNDBOARD_DIR}'.")
                break

            filepath = os.path.join(SOUNDBOARD_DIR, filename)
            if os.path.isfile(filepath):
                base_name, ext = os.path.splitext(filename)
                if ext.lower() in ALLOWED_EXTENSIONS:
                    # Use base name for label, full filename for ID
                    button_label = base_name.replace("_", " ")[:80] # Replace underscores, limit length
                    button_custom_id = f"sb_play:{filename}" # Store filename securely in ID

                    # Check if custom_id exceeds 100 chars (Discord limit)
                    if len(button_custom_id) > 100:
                        bot_logger.warning(f"Skipping soundboard file '{filename}' because its custom_id ('{button_custom_id}') would exceed 100 characters.")
                        continue

                    button = discord.ui.Button(
                        label=button_label,
                        style=discord.ButtonStyle.secondary, # Or primary, success, danger
                        custom_id=button_custom_id,
                        row=button_row
                    )
                    # Assign the single callback method to this button
                    button.callback = self.soundboard_button_callback
                    self.add_item(button)
                    sounds_found += 1

                    # Move to next row if current row is full
                    if sounds_found % max_buttons_per_row == 0:
                        button_row += 1
                        if button_row >= max_rows:
                             bot_logger.warning(f"Reached maximum soundboard row limit ({max_rows}). Skipping remaining files.")
                             break # Stop adding buttons
                else:
                    bot_logger.debug(f"Skipping non-audio file in soundboard dir: '{filename}'")

        if sounds_found == 0:
             bot_logger.warning(f"No valid sound files ({', '.join(ALLOWED_EXTENSIONS)}) found in soundboard directory: '{SOUNDBOARD_DIR}'")
             # Add a placeholder if no sounds were found
             button = discord.ui.Button(label="No sounds available", style=discord.ButtonStyle.secondary, disabled=True, row=0)
             self.add_item(button)


    async def soundboard_button_callback(self, interaction: discord.Interaction):
        """Callback executed when ANY soundboard button in this view is pressed."""
        # interaction.data contains information about the component interaction
        custom_id = interaction.data["custom_id"]
        user = interaction.user
        bot_logger.info(f"SOUNDBOARD: Button '{custom_id}' pressed by {user.name} ({user.id})")

        # Defer the interaction response quickly and ephemerally.
        # This acknowledges the button press to Discord privately for the user.
        # REMOVED thinking=True from the line below
        await interaction.response.defer(ephemeral=True)

        # --- Extract filename and play ---
        if not custom_id.startswith("sb_play:"):
            bot_logger.error(f"Invalid custom_id format received from soundboard button: '{custom_id}'")
            # Use followup because we deferred
            await interaction.followup.send("❌ Internal error: Invalid button data.", ephemeral=True)
            return

        sound_filename = custom_id.split(":", 1)[1]
        sound_path = os.path.join(SOUNDBOARD_DIR, sound_filename)

        # Call the generic single play function, passing the interaction
        # play_single_sound will handle connection, playback, errors, and followup messages.
        await play_single_sound(interaction, sound_path)

    async def on_timeout(self):
        """Called when the view times out (no interaction for the specified duration)."""
        bot_logger.debug(f"Soundboard view timed out for message {self.message.id if self.message else 'Unknown'}")
        # Disable all buttons visually
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True

        # Edit the original message if we have a reference to it
        if self.message:
            try:
                # Edit content slightly to indicate expiration
                await self.message.edit(content="🔊 **Sound Panel (Expired)**", view=self)
            except discord.NotFound:
                bot_logger.debug(f"Soundboard message {self.message.id} not found on timeout (likely deleted).")
            except discord.Forbidden:
                 bot_logger.warning(f"Missing permissions to edit soundboard message {self.message.id} on timeout.")
            except Exception as e:
                # Catch other potential errors during edit
                bot_logger.warning(f"Failed to edit soundboard message {self.message.id} on timeout: {e}", exc_info=True)
        # Children are automatically stopped listening after timeout by discord.py


@bot.slash_command(
    name="soundpanel",
    description="Displays buttons to play sounds from the shared soundboard."
)
@commands.cooldown(1, 30, commands.BucketType.channel) # Limit panel creation per channel
@commands.has_permissions(use_application_commands=True) # Basic check
async def soundpanel(ctx: discord.ApplicationContext):
    """Sends the soundboard panel message with interactive buttons."""
    # Defer publicly as the panel itself should be visible
    await ctx.defer()
    bot_logger.info(f"COMMAND: /soundpanel invoked by {ctx.author.name} ({ctx.author.id}) in channel {ctx.channel_id}")

    # Create a new view instance EACH time the command is run.
    # This ensures it reflects the current state of the soundboard/ directory.
    view = SoundboardView(timeout=600.0) # 10 minute timeout for the panel

    # Check if the view populated any buttons successfully (handles empty/missing dir)
    if not view.children or all(getattr(item, 'disabled', False) for item in view.children):
         # If no buttons were added, or only disabled error buttons were added
         await ctx.followup.send("⚠️ The soundboard is currently unavailable or has no sounds loaded. Check the `soundboard` directory and bot logs.", ephemeral=True)
         return

    # Send the panel message with the view attached
    message = await ctx.followup.send("🔊 **Sound Panel** - Click a button to play!", view=view)
    # Store the message reference in the view so it can be edited on timeout
    view.message = message


# --- Error Handler for Application Commands ---
@bot.event
async def on_application_command_error(ctx: discord.ApplicationContext, error: discord.DiscordException):
    """Handles errors raised during slash command execution."""
    command_name = ctx.command.qualified_name if ctx.command else "Unknown Command"

    if isinstance(error, commands.CommandOnCooldown):
        retry_after = error.retry_after
        # Send ephemeral cooldown message
        message = f"⏳ This command (`/{command_name}`) is on cooldown. Please try again in {retry_after:.1f} seconds."
        if not ctx.interaction.response.is_done():
            await ctx.respond(message, ephemeral=True)
        else:
            await ctx.followup.send(message, ephemeral=True) # Use followup if already deferred/responded

    elif isinstance(error, commands.MissingPermissions):
        # Let user know they lack permissions for the command
        perms_list = "\n".join([f"- `{perm}`" for perm in error.missing_permissions])
        message = f"🚫 You do not have the required permissions to use `/{command_name}`.\nMissing:\n{perms_list}"
        if not ctx.interaction.response.is_done():
            await ctx.respond(message, ephemeral=True)
        else:
            await ctx.followup.send(message, ephemeral=True)

    elif isinstance(error, commands.BotMissingPermissions):
         # Let user know the BOT lacks permissions
         perms_list = "\n".join([f"- `{perm}`" for perm in error.missing_permissions])
         message = f"🚫 I don't have the required permissions to execute `/{command_name}`.\nPlease ensure I have:\n{perms_list}"
         # Try sending in channel, fallback to ephemeral if needed/failed
         try:
             if not ctx.interaction.response.is_done(): await ctx.respond(message, ephemeral=True) # Safer default
             else: await ctx.followup.send(message, ephemeral=True)
         except discord.Forbidden: # Cannot even send ephemeral? Log it.
              bot_logger.error(f"Cannot inform user about missing bot permissions for '/{command_name}' in channel {ctx.channel_id}.")

    # Add more specific error checks here if needed (e.g., CheckFailure)

    else:
        # Handle unexpected errors - Log detailed traceback, inform user generically.
        bot_logger.error(f"Unhandled error in application command '/{command_name}':", exc_info=error)

        # Send a generic error message to the user
        error_message = f"❌ An unexpected error occurred while running `/{command_name}`. The issue has been logged."
        try:
            if not ctx.interaction.response.is_done():
                await ctx.respond(error_message, ephemeral=True)
            else:
                # Use followup if initial response (e.g., defer) already happened
                await ctx.followup.send(error_message, ephemeral=True)
        except Exception as e_resp:
            # Log failure to notify user if sending the error message itself fails
            bot_logger.error(f"Failed to send error response message to user for command '/{command_name}': {e_resp}", exc_info=e_resp)


# --- Run the Bot ---
if __name__ == "__main__":
    # Pre-run checks
    if not PYDUB_AVAILABLE:
        bot_logger.critical("Pydub library is not available. Install it ('pip install pydub') and ensure FFmpeg is in your PATH. Bot cannot start.")
        exit(1)
    if not BOT_TOKEN:
        bot_logger.critical("BOT_TOKEN environment variable not set. Create a .env file or set the environment variable. Bot cannot start.")
        exit(1)

    # Check for Opus library (essential for voice)
    if not discord.opus.is_loaded():
         bot_logger.warning("="*30 + "\nOpus library not loaded. Voice functionality WILL FAIL.\n"
                         "Ensure libopus is installed on your system:\n"
                         "  Debian/Ubuntu: sudo apt update && sudo apt install libopus0\n"
                         "  Fedora: sudo dnf install opus\n"
                         "  Arch: sudo pacman -S opus\n"
                         "  macOS (Homebrew): brew install opus\n"
                         "  Windows: Usually bundled with ffmpeg builds, ensure ffmpeg is in PATH.\n" + "="*30)
         # Depending on strictness, you might exit() here, but we'll let it try to run for now.
         # exit(1)

    try:
        bot_logger.info("Attempting to start the bot...")
        bot.run(BOT_TOKEN)
    except discord.errors.LoginFailure:
        bot_logger.critical("="*30 + "\nLOGIN FAILURE: Invalid BOT_TOKEN provided. Check your token.\n" + "="*30)
    except discord.errors.PrivilegedIntentsRequired as e:
        bot_logger.critical("="*30 + f"\nINTENT ERROR: Required intents are missing: {e.shard_id}\n"
                         "Ensure 'Server Members Intent' and 'Voice State Intent' are enabled in the Discord Developer Portal.\n" + "="*30)
    # OpusNotLoaded is checked above, but catch it here just in case
    except discord.errors.OpusNotLoaded:
         bot_logger.critical("="*30 + "\nOPUS ERROR: Opus library failed to load during runtime startup.\n" + "="*30)
    except Exception as e:
        # Catch any other unexpected errors during startup
        bot_logger.critical(f"FATAL RUNTIME ERROR during bot startup: {e}", exc_info=True)