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
    logging.critical("CRITICAL: Pydub library not found. Please install it: pip install pydub")
    PYDUB_AVAILABLE = False

# --- Configuration ---
BOT_TOKEN = os.getenv('BOT_TOKEN')
SOUNDS_DIR = "sounds"
CONFIG_FILE = "user_sounds.json"
TARGET_LOUDNESS_DBFS = -14.0

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')
logger = logging.getLogger('discord')
bot_logger = logging.getLogger('JoinSoundBot')
bot_logger.setLevel(logging.INFO)

# --- Validate Critical Config ---
if not BOT_TOKEN: bot_logger.critical("CRITICAL ERROR: Bot token not found."); exit()
if not PYDUB_AVAILABLE: bot_logger.critical("CRITICAL ERROR: Pydub library failed to import."); exit()

# --- Intents ---
intents = discord.Intents.default()
intents.voice_states = True
intents.guilds = True

# --- Bot Definition ---
bot = discord.Bot(intents=intents)

# --- Data Storage & Helpers ---
user_sound_config = {}
# NEW: Dictionary to hold sound queues per guild {guild_id: deque([(member, sound_path), ...])}
guild_sound_queues = {}
# NEW: Dictionary to track if a play_next task is running per guild {guild_id: asyncio.Task}
guild_play_tasks = {}

def load_config():
    global user_sound_config
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f: user_sound_config = json.load(f)
            bot_logger.info(f"Loaded {len(user_sound_config)} configs")
        except Exception as e:
             bot_logger.error(f"Error loading config: {e}", exc_info=True); user_sound_config = {}
    else: user_sound_config = {}; bot_logger.info(f"{CONFIG_FILE} not found.")

def save_config():
     try:
        with open(CONFIG_FILE, 'w') as f: json.dump(user_sound_config, f, indent=4)
        bot_logger.debug(f"Saved config")
     except Exception as e: bot_logger.error(f"Error saving config: {e}", exc_info=True)

if not os.path.exists(SOUNDS_DIR):
    try: os.makedirs(SOUNDS_DIR); bot_logger.info(f"Created sounds dir")
    except Exception as e: bot_logger.critical(f"CRITICAL: Could not create sounds dir: {e}", exc_info=True); exit()

# --- Bot Events ---
@bot.event
async def on_ready():
    bot_logger.info(f'Logged in as {bot.user.name} ({bot.user.id})')
    load_config()
    bot_logger.info('------')
    bot_logger.info(f"Audio Normalization Target: {TARGET_LOUDNESS_DBFS} dBFS")
    bot_logger.info("Join Sound Bot is operational.")

# --- Core Audio Playing Logic (Queue Processor) ---
async def play_next_in_queue(guild: discord.Guild):
    """Processes the sound queue for a given guild."""
    if guild.id not in guild_sound_queues or not guild_sound_queues[guild.id]:
        bot_logger.debug(f"Queue empty or non-existent for guild {guild.id}. Ending play task.")
        await safe_disconnect(discord.utils.get(bot.voice_clients, guild=guild))
        # Clean up task tracker
        if guild.id in guild_play_tasks:
            del guild_play_tasks[guild.id]
        return

    voice_client = discord.utils.get(bot.voice_clients, guild=guild)
    if not voice_client or not voice_client.is_connected():
        bot_logger.warning(f"Play task running for {guild.id}, but bot is not connected to voice.")
        # Clear queue maybe? Or just let it wait for next connection? Let's clear it to avoid stale sounds.
        guild_sound_queues[guild.id].clear()
        if guild.id in guild_play_tasks: del guild_play_tasks[guild.id]
        return

    if voice_client.is_playing():
        bot_logger.debug(f"Bot is already playing in guild {guild.id}, play_next_in_queue will wait.")
        # The 'after' callback will trigger this function again when done.
        return

    # Get next item from the queue
    try:
        member, sound_path = guild_sound_queues[guild.id].popleft()
        bot_logger.info(f"QUEUE: Processing sound for {member.display_name} in {guild.name}. Remaining: {len(guild_sound_queues[guild.id])}")
    except IndexError:
        # Should be caught by the initial check, but safeguard
        bot_logger.debug(f"Queue became empty unexpectedly for guild {guild.id}.")
        await safe_disconnect(voice_client)
        if guild.id in guild_play_tasks: del guild_play_tasks[guild.id]
        return

    # --- Prepare Audio Source ---
    audio_source = None
    if sound_path:
        try:
            # (Normalization and Pydub processing - same as before)
            bot_logger.debug(f"AUDIO: Loading '{sound_path}'...")
            file_extension = os.path.splitext(sound_path)[1].lower().strip('.') or 'mp3'
            audio_segment = AudioSegment.from_file(sound_path, format=file_extension)
            peak_dbfs = audio_segment.max_dBFS
            if not math.isinf(peak_dbfs):
                gain = TARGET_LOUDNESS_DBFS - peak_dbfs
                bot_logger.info(f"AUDIO: Normalizing {member.display_name}'s sound. Peak:{peak_dbfs:.2f} Target:{TARGET_LOUDNESS_DBFS:.2f} Gain:{gain:.2f} dB.")
                if gain <= 0: audio_segment = audio_segment.apply_gain(gain)
                else: bot_logger.info("AUDIO: Skipping positive gain.")
            else: bot_logger.warning("AUDIO: Cannot normalize silent sound.")
            audio_segment = audio_segment.set_frame_rate(48000).set_channels(2)
            pcm_data_io = io.BytesIO()
            audio_segment.export(pcm_data_io, format="raw")
            pcm_data_io.seek(0)
            if len(pcm_data_io.getvalue()) > 0: audio_source = discord.PCMAudio(pcm_data_io)
            else: bot_logger.error("AUDIO: Exported raw audio data empty!")

        except Exception as e: # Catch broad exceptions during processing
            bot_logger.error(f"AUDIO: Error processing '{sound_path}' for {member.display_name}: {e}", exc_info=True)
            # Don't play this sound, try next in queue
            bot.loop.create_task(play_next_in_queue(guild)) # Trigger next check
            return # Stop processing this failed item

    # --- Play Audio ---
    if audio_source:
        try:
            bot_logger.info(f"PLAYBACK: Playing sound for {member.display_name}...")
            # Pass the voice_client to the after handler
            voice_client.play(audio_source, after=lambda e: after_play_handler(e, voice_client))
            bot_logger.debug(f"PLAYBACK: vc.play() called for {member.display_name}.")
        except discord.errors.ClientException as e:
            bot_logger.error(f"PLAYBACK ERROR (ClientException): {e}", exc_info=True)
            # Try to play the next item if play failed immediately
            bot.loop.create_task(play_next_in_queue(guild))
        except Exception as e:
            bot_logger.error(f"PLAYBACK ERROR (Unexpected): {e}", exc_info=True)
            bot.loop.create_task(play_next_in_queue(guild))
    else:
        bot_logger.warning(f"PLAYBACK: No valid audio source for {member.display_name}. Skipping.")
        # Trigger next item check immediately since nothing played
        bot.loop.create_task(play_next_in_queue(guild))


# --- Voice State Update Handler (Adds to Queue) ---
@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot: return

    # Trigger on joining or switching channels
    if after.channel is not None and before.channel != after.channel:
        channel_to_join = after.channel
        guild = member.guild
        bot_logger.info(f"EVENT: {member.display_name} entered {channel_to_join.name} (Prev: {before.channel.name if before.channel else 'None'})")

        # --- Check Permissions (Still important) ---
        bot_perms = channel_to_join.permissions_for(guild.me)
        if not bot_perms.connect or not bot_perms.speak:
            bot_logger.warning(f"Missing Connect/Speak perms in {channel_to_join.name}, cannot process.")
            return

        # --- Determine Sound Path ---
        sound_path = None
        is_tts = False
        user_id_str = str(member.id)
        # (Same logic as before to determine sound_path)
        if user_id_str in user_sound_config:
            sound_filename = user_sound_config[user_id_str]
            potential_path = os.path.join(SOUNDS_DIR, sound_filename)
            if os.path.exists(potential_path): sound_path = potential_path; bot_logger.info(f"SOUND: Using configured: {sound_filename}")
            else: bot_logger.warning(f"SOUND: Config file '{sound_filename}' not found."); is_tts = True
        else: is_tts = True; bot_logger.info(f"SOUND: No config found. Using TTS.")

        if is_tts:
            tts_filename = f"tts_{member.id}.mp3"
            tts_path = os.path.join(SOUNDS_DIR, tts_filename)
            sound_path = tts_path
            if not os.path.exists(tts_path):
                bot_logger.info(f"TTS: Generating for {member.display_name}...")
                tts_text = f"{member.display_name} joined" # Shorter TTS maybe?
                try: gTTS(text=tts_text, lang='en').save(tts_path); bot_logger.info(f"TTS: Saved {tts_path}")
                except Exception as e: bot_logger.error(f"TTS: Failed generation: {e}"); sound_path = None
            else: bot_logger.info(f"TTS: Using existing: {tts_path}")

        if not sound_path:
            bot_logger.error(f"Could not determine or generate sound path for {member.display_name}. Skipping queue add.")
            return

        # --- Add to Guild Queue ---
        if guild.id not in guild_sound_queues:
            guild_sound_queues[guild.id] = deque() # Use deque for efficient append/popleft

        # Store member object to get display name later
        queue_item = (member, sound_path)
        guild_sound_queues[guild.id].append(queue_item)
        bot_logger.info(f"QUEUE: Added sound for {member.display_name} to queue for {guild.name}. Queue size: {len(guild_sound_queues[guild.id])}")

        # --- Ensure Connection and Trigger Player ---
        voice_client = discord.utils.get(bot.voice_clients, guild=guild)
        if not voice_client or not voice_client.is_connected():
            # Not connected, try to connect
            try:
                bot_logger.info(f"Connecting to {channel_to_join.name} to start queue processing.")
                await channel_to_join.connect()
                # Connection successful, trigger the player task
                if guild.id not in guild_play_tasks or guild_play_tasks[guild.id].done():
                     guild_play_tasks[guild.id] = bot.loop.create_task(play_next_in_queue(guild))
            except discord.errors.ClientException as e:
                bot_logger.error(f"Failed to connect to {channel_to_join.name}: {e}", exc_info=True)
                # Clear queue if connection fails?
                if guild.id in guild_sound_queues: guild_sound_queues[guild.id].clear()
            except Exception as e:
                 bot_logger.error(f"Unexpected error during connection: {e}", exc_info=True)
                 if guild.id in guild_sound_queues: guild_sound_queues[guild.id].clear()

        elif voice_client.channel != channel_to_join:
             # Connected but in wrong channel, move
             try:
                 bot_logger.info(f"Moving to {channel_to_join.name} to process queue.")
                 await voice_client.move_to(channel_to_join)
                 # Trigger player task if not already running
                 if guild.id not in guild_play_tasks or guild_play_tasks[guild.id].done():
                     guild_play_tasks[guild.id] = bot.loop.create_task(play_next_in_queue(guild))
             except Exception as e:
                 bot_logger.error(f"Failed to move to {channel_to_join.name}: {e}", exc_info=True)

        else:
            # Already connected in the right channel, just ensure player task is running
            if guild.id not in guild_play_tasks or guild_play_tasks[guild.id].done():
                bot_logger.debug(f"Bot already connected in {channel_to_join.name}, ensuring player task runs.")
                guild_play_tasks[guild.id] = bot.loop.create_task(play_next_in_queue(guild))


# --- After Play Handler (Triggers Next in Queue) ---
def after_play_handler(error, voice_client: discord.VoiceClient):
    """Handles triggering the next item after playback."""
    if error:
        bot_logger.error(f'PLAYBACK ERROR (in after callback): {error}', exc_info=error)

    # Regardless of error, try to play the next item in the queue for this guild
    if voice_client and voice_client.guild:
        guild = voice_client.guild
        bot_logger.debug(f"Playback finished for guild {guild.id}, checking queue...")
        # Ensure player task runs again to check queue/disconnect
        if guild.id not in guild_play_tasks or guild_play_tasks[guild.id].done():
             guild_play_tasks[guild.id] = bot.loop.create_task(play_next_in_queue(guild))
        else:
             # Task might exist but hasn't yielded yet, create_task is safe.
             # Or maybe the task itself should handle looping? For now, creating task here is safer.
             bot_logger.debug(f"Player task for {guild.id} already exists, creating new check task.")
             bot.loop.create_task(play_next_in_queue(guild)) # Ensure check happens
    else:
        bot_logger.warning("after_play_handler called with invalid voice_client or guild.")


# --- Disconnect Logic (Called by play_next_in_queue) ---
async def safe_disconnect(voice_client: discord.VoiceClient):
    """Safely disconnects if connected and not playing (and queue is empty)."""
    if voice_client and voice_client.is_connected():
        guild_id = voice_client.guild.id
        # Double check queue is empty and bot isn't somehow playing again
        if (guild_id not in guild_sound_queues or not guild_sound_queues[guild_id]) and not voice_client.is_playing():
            try:
                channel_name = voice_client.channel.name
                await voice_client.disconnect(force=False)
                bot_logger.info(f"DISCONNECT: Bot disconnected from {channel_name} (queue empty).")
                # Clean up task tracker on successful disconnect
                if guild_id in guild_play_tasks: del guild_play_tasks[guild_id]
            except Exception as e:
                bot_logger.error(f"DISCONNECT ERROR: {e}", exc_info=True)
        # else: bot_logger.debug(f"Disconnect skipped for {guild_id}: Queue not empty or bot playing.")


# --- Slash Commands ---
# (addsound and removesound remain the same as the previous version with validation)
@bot.slash_command(name="addsound", description="Upload your custom join sound (MP3, WAV recommended).")
@commands.cooldown(1, 15, commands.BucketType.user)
async def addsound( ctx: discord.ApplicationContext, sound_file: discord.Option(discord.Attachment, "Sound file (MP3, WAV, OGG, etc.). Max 5MB. Will be validated.")):
    # ... (Keep the addsound code from the previous full file) ...
    await ctx.defer(ephemeral=True)
    bot_logger.info(f"COMMAND: /addsound invoked by {ctx.author.name} ({ctx.author.id}) file: {sound_file.filename}")
    user_id_str = str(ctx.author.id)
    if not sound_file.content_type or not sound_file.content_type.startswith('audio/'):
        await ctx.followup.send("⚠️ Invalid file type.", ephemeral=True); return
    max_size_mb = 5
    if sound_file.size > max_size_mb * 1024 * 1024:
        await ctx.followup.send(f"⚠️ File too large (> {max_size_mb}MB).", ephemeral=True); return
    file_extension = os.path.splitext(sound_file.filename)[1].lower()
    temp_save_filename = f"temp_validate_{user_id_str}{file_extension}"
    temp_save_path = os.path.join(SOUNDS_DIR, temp_save_filename)
    final_save_filename = f"{user_id_str}{file_extension}"
    final_save_path = os.path.join(SOUNDS_DIR, final_save_filename)
    async def cleanup_temp():
        if os.path.exists(temp_save_path):
            try: os.remove(temp_save_path)
            except Exception as del_e: bot_logger.warning(f"Failed cleanup {temp_save_path}: {del_e}")
    try:
        await sound_file.save(temp_save_path)
        bot_logger.info(f"Saved temporary file for validation: {temp_save_path}")
        try:
            bot_logger.debug(f"Attempting decode: {temp_save_path}")
            _ = AudioSegment.from_file(temp_save_path, format=file_extension.strip('.'))
            bot_logger.info(f"Validation successful: {temp_save_path}")
            if user_id_str in user_sound_config:
                old_filename = user_sound_config[user_id_str]
                if old_filename != final_save_filename:
                    old_path = os.path.join(SOUNDS_DIR, old_filename)
                    if os.path.exists(old_path):
                        try: os.remove(old_path); bot_logger.info(f"Removed old: {old_path}")
                        except Exception as e: bot_logger.warning(f"Could not remove old {old_path}: {e}")
            try: os.rename(temp_save_path, final_save_path); bot_logger.info(f"Renamed to final: {final_save_path}")
            except OSError as ren_e: bot_logger.error(f"Failed rename {temp_save_path} to {final_save_path}: {ren_e}."); await cleanup_temp(); raise
            user_sound_config[user_id_str] = final_save_filename; save_config()
            bot_logger.info(f"Updated config for {ctx.author.name} to use {final_save_filename}")
            await ctx.followup.send(f"✅ Success! `{sound_file.filename}` validated and set.", ephemeral=True)
        except CouldntDecodeError as decode_error:
            bot_logger.error(f"AUDIO VALIDATION FAILED (user: {ctx.author.id}): {decode_error}")
            await cleanup_temp()
            await ctx.followup.send(f"❌ **Validation Failed!** Could not process `{sound_file.filename}`. Try MP3/WAV.", ephemeral=True)
        except Exception as validate_e:
            bot_logger.error(f"AUDIO VALIDATION FAILED (user: {ctx.author.id}): {validate_e}", exc_info=True)
            await cleanup_temp()
            await ctx.followup.send(f"❌ **Validation Failed!** Unexpected error processing.", ephemeral=True)
    except discord.HTTPException as e:
        bot_logger.error(f"Error downloading temp file for {ctx.author.id}: {e}", exc_info=True); await cleanup_temp()
        await ctx.followup.send("❌ Error downloading file.", ephemeral=True)
    except Exception as e:
        bot_logger.error(f"Error in addsound for {ctx.author.id}: {e}", exc_info=True); await cleanup_temp()
        await ctx.followup.send("❌ Unexpected error.", ephemeral=True)


@bot.slash_command(name="removesound", description="Remove your custom join sound and revert to TTS.")
@commands.cooldown(1, 5, commands.BucketType.user)
async def removesound(ctx: discord.ApplicationContext):
    # ... (Keep the removesound code from the previous full file) ...
    await ctx.defer(ephemeral=True)
    bot_logger.info(f"COMMAND: /removesound invoked by {ctx.author.name} ({ctx.author.id})")
    user_id_str = str(ctx.author.id)
    if user_id_str in user_sound_config:
        filename_to_remove = user_sound_config[user_id_str]
        file_path_to_remove = os.path.join(SOUNDS_DIR, filename_to_remove)
        del user_sound_config[user_id_str]; save_config()
        bot_logger.info(f"Removed config for {ctx.author.name}")
        if os.path.exists(file_path_to_remove):
            try: os.remove(file_path_to_remove); bot_logger.info(f"Deleted file: {file_path_to_remove}")
            except Exception as e: bot_logger.warning(f"Could not delete file {file_path_to_remove}: {e}")
        await ctx.followup.send("🗑️ Custom sound removed. TTS will be used.", ephemeral=True)
    else:
        await ctx.followup.send("🤷 No custom sound configured.", ephemeral=True)


# --- Error Handler for Commands ---
# (Same as previous version)
@bot.event
async def on_application_command_error(ctx: discord.ApplicationContext, error: discord.DiscordException):
    if isinstance(error, commands.CommandOnCooldown):
        if not ctx.interaction.is_done(): await ctx.respond(f"⏳ Cooldown. Try again in {error.retry_after:.1f}s.", ephemeral=True)
    elif isinstance(error, commands.errors.MissingPermissions):
         if not ctx.interaction.is_done(): await ctx.respond("🚫 No permission.", ephemeral=True)
    else:
        bot_logger.error(f"Error in slash command '{ctx.command.qualified_name if ctx.command else 'Unknown'}': {error}", exc_info=error)
        try:
            if not ctx.interaction.is_done(): await ctx.respond("❌ Command error.", ephemeral=True)
            else: await ctx.followup.send("❌ Command error.", ephemeral=True)
        except Exception as e: bot_logger.error(f"Failed to send error response: {e}")

# --- Run the Bot ---
# (Same as previous version)
if __name__ == "__main__":
    try:
        bot_logger.info("Attempting to start bot...")
        bot.run(BOT_TOKEN)
    except discord.errors.LoginFailure: bot_logger.critical("="*30 + "\nLOGIN FAILURE: Invalid BOT_TOKEN.\n" + "="*30)
    except discord.errors.PrivilegedIntentsRequired: bot_logger.critical("="*30 + "\nINTENT ERROR: Enable Voice States Intent.\n" + "="*30)
    except discord.errors.OpusNotLoaded: bot_logger.critical("="*30 + "\nOPUS ERROR: Opus library failed to load.\n" + "="*30)
    except Exception as e: bot_logger.critical(f"FATAL RUNTIME ERROR: {e}", exc_info=True)