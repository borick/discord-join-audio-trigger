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
if not BOT_TOKEN:
    bot_logger.critical("CRITICAL ERROR: Bot token not found in environment variables.")
    exit()
if not PYDUB_AVAILABLE:
    bot_logger.critical("CRITICAL ERROR: Pydub library failed to import. Bot cannot process audio.")
    exit()

# --- Intents ---
intents = discord.Intents.default()
intents.voice_states = True
intents.guilds = True

# --- Bot Definition ---
bot = discord.Bot(intents=intents)

# --- Data Storage & Helpers ---
user_sound_config = {}

def load_config():
    global user_sound_config
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f: user_sound_config = json.load(f)
            bot_logger.info(f"Loaded {len(user_sound_config)} user configurations from {CONFIG_FILE}")
        except Exception as e:
             bot_logger.error(f"Error loading config from {CONFIG_FILE}: {e}", exc_info=True); user_sound_config = {}
    else:
        user_sound_config = {}; bot_logger.info(f"{CONFIG_FILE} not found. Starting with empty config.")

def save_config():
     try:
        with open(CONFIG_FILE, 'w') as f: json.dump(user_sound_config, f, indent=4)
        bot_logger.debug(f"Saved config to {CONFIG_FILE}")
     except Exception as e:
        bot_logger.error(f"Error saving config to {CONFIG_FILE}: {e}", exc_info=True)

if not os.path.exists(SOUNDS_DIR):
    try:
        os.makedirs(SOUNDS_DIR); bot_logger.info(f"Created sounds directory: {SOUNDS_DIR}")
    except Exception as e:
        bot_logger.critical(f"CRITICAL ERROR: Could not create sounds directory '{SOUNDS_DIR}': {e}", exc_info=True); exit()

# --- Bot Events ---
@bot.event
async def on_ready():
    bot_logger.info(f'Logged in as {bot.user.name} ({bot.user.id})')
    load_config()
    bot_logger.info('------')
    bot_logger.info(f"Audio Normalization Target: {TARGET_LOUDNESS_DBFS} dBFS")
    bot_logger.info("Join Sound Bot is operational.")

# CORRECTED on_voice_state_update Structure
@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    # Ignore bots
    if member.bot:
        return

    # --- Check if user entered a new voice channel (join or switch) ---
    if after.channel is not None and before.channel != after.channel:
        # --- This is the main block that runs on join/switch ---
        channel_to_join = after.channel
        guild = member.guild

        bot_logger.info(f"EVENT: {member.display_name} ({member.id}) entered voice channel: {channel_to_join.name} ({channel_to_join.id}) in Guild {member.guild.name} ({member.guild.id}) (Previous: {before.channel.name if before.channel else 'None'})")

        # --- Check Permissions ---
        bot_perms = channel_to_join.permissions_for(guild.me)
        if not bot_perms.connect:
            bot_logger.warning(f"PERMISSION DENIED: Cannot connect to {channel_to_join.name}. Missing 'Connect'.")
            return # Stop if cannot connect
        if not bot_perms.speak:
            bot_logger.warning(f"PERMISSION DENIED: Cannot speak in {channel_to_join.name}. Missing 'Speak'.")
            # return # Optionally stop if cannot speak

        # --- Check if Bot is Busy ---
        voice_client: discord.VoiceClient = discord.utils.get(bot.voice_clients, guild=guild)
        if voice_client and voice_client.is_playing():
            bot_logger.info(f"BUSY: Bot already playing in {guild.name}. Skipping for {member.display_name}.")
            return # Stop if already playing

        # --- Determine Sound Path (Custom or TTS) ---
        sound_path = None
        is_tts = False
        user_id_str = str(member.id)

        if user_id_str in user_sound_config:
            sound_filename = user_sound_config[user_id_str]
            potential_path = os.path.join(SOUNDS_DIR, sound_filename)
            if os.path.exists(potential_path):
                sound_path = potential_path
                bot_logger.info(f"SOUND: Using configured sound: {sound_filename}")
            else:
                bot_logger.warning(f"SOUND: Configured sound '{sound_filename}' not found. Falling back to TTS.")
                is_tts = True
        else:
            is_tts = True
            bot_logger.info(f"SOUND: No configured sound found. Using TTS.")

        if is_tts:
            tts_filename = f"tts_{member.id}.mp3"
            tts_path = os.path.join(SOUNDS_DIR, tts_filename)
            sound_path = tts_path

            if not os.path.exists(tts_path):
                bot_logger.info(f"TTS: Generating persistent TTS for {member.display_name}...")
                tts_text = f"{member.display_name} has joined the channel" # Consistent message
                try:
                    tts = gTTS(text=tts_text, lang='en')
                    tts.save(tts_path)
                    bot_logger.info(f"TTS: Saved persistent TTS file: {tts_path}")
                except Exception as e:
                    bot_logger.error(f"TTS: Failed to generate/save TTS: {e}", exc_info=True)
                    sound_path = None
            else:
                 bot_logger.info(f"TTS: Using existing persistent TTS file: {tts_path}")

        # --- Prepare Audio Source with Normalization ---
        audio_source = None
        if sound_path:
            try:
                bot_logger.debug(f"AUDIO: Loading '{sound_path}' with pydub...")
                file_extension = os.path.splitext(sound_path)[1].lower().strip('.')
                if not file_extension: file_extension = 'mp3'

                audio_segment = AudioSegment.from_file(sound_path, format=file_extension)

                peak_dbfs = audio_segment.max_dBFS
                if not math.isinf(peak_dbfs):
                    gain_difference = TARGET_LOUDNESS_DBFS - peak_dbfs
                    bot_logger.info(f"AUDIO: Normalizing. Peak:{peak_dbfs:.2f} Target:{TARGET_LOUDNESS_DBFS:.2f} Gain:{gain_difference:.2f} dB.")
                    if gain_difference <= 0: # Only apply reduction or zero gain
                         audio_segment = audio_segment.apply_gain(gain_difference)
                    else: bot_logger.info("AUDIO: Skipping positive gain (already quieter than target).")
                else: bot_logger.warning("AUDIO: Cannot normalize silent audio.")

                audio_segment = audio_segment.set_frame_rate(48000).set_channels(2)
                bot_logger.debug(f"AUDIO: Converted format. Rate:{audio_segment.frame_rate}, Channels:{audio_segment.channels}")

                pcm_data_io = io.BytesIO()
                audio_segment.export(pcm_data_io, format="raw")
                pcm_data_io.seek(0)
                raw_data_len = len(pcm_data_io.getvalue())
                bot_logger.debug(f"AUDIO: Exported raw data (length: {raw_data_len}).")

                if raw_data_len > 0: audio_source = discord.PCMAudio(pcm_data_io)
                else: bot_logger.error("AUDIO: Exported raw audio data is empty!")

            except CouldntDecodeError as e: bot_logger.error(f"AUDIO: Pydub CouldntDecodeError for '{sound_path}'. Error: {e}", exc_info=True)
            except FileNotFoundError: bot_logger.error(f"AUDIO: File not found: '{sound_path}'")
            except Exception as e: bot_logger.error(f"AUDIO: Unexpected error processing '{sound_path}': {e}", exc_info=True)
        else:
            bot_logger.warning("AUDIO: No sound_path. Cannot prepare audio.")

        # --- Connect or Move and Play ---
        vc = None # Define vc here for access in finally block if needed
        try:
            bot_logger.debug("CONNECTION: Checking voice client status...")
            if voice_client and voice_client.is_connected():
                if voice_client.channel != channel_to_join:
                    bot_logger.info(f"CONNECTION: Moving bot to {channel_to_join.name}")
                    await voice_client.move_to(channel_to_join)
                    vc = voice_client
                else:
                    bot_logger.info(f"CONNECTION: Bot already in {channel_to_join.name}")
                    vc = voice_client # Already connected in the correct channel
            else: # Not connected in this guild, or client is None
                bot_logger.info(f"CONNECTION: Bot connecting to {channel_to_join.name}")
                vc = await channel_to_join.connect() # Connect fresh
                bot_logger.info(f"CONNECTION: Bot connected successfully.")

            if not vc:
               bot_logger.error("CONNECTION: Failed to establish voice connection (vc is None).")
               return # Cannot proceed without a voice client

            await asyncio.sleep(0.5) # Stability delay

            if vc.is_playing():
                 bot_logger.info(f"BUSY: Bot started playing something else during connect/move. Skipping.")
                 # If we just connected, should we disconnect? Maybe leave it for now.
                 return

            # Play Audio Source
            if audio_source:
                bot_logger.info(f"PLAYBACK: Attempting to play normalized sound for {member.display_name}...")
                vc.play(audio_source, after=lambda e: after_play_handler(e, vc))
                bot_logger.info(f"PLAYBACK: vc.play() called.")
            else:
                 bot_logger.warning("PLAYBACK: No valid audio source prepared. Skipping.")
                 await schedule_disconnect(vc, delay=1.0) # Disconnect if nothing played

        except discord.errors.ClientException as e:
             bot_logger.error(f"CONNECTION/PLAYBACK ERROR (ClientException): {e}", exc_info=True)
             if vc and vc.is_connected(): await safe_disconnect(vc)
        except discord.errors.OpusNotLoaded:
             bot_logger.critical("OPUS ERROR: Opus library not loaded. Voice cannot work.")
             if vc and vc.is_connected(): await safe_disconnect(vc)
        except Exception as e:
            bot_logger.error(f"UNEXPECTED ERROR in voice state update processing: {e}", exc_info=True)
            if vc and vc.is_connected(): await safe_disconnect(vc)
    # --- End of the main 'if after.channel is not None and before.channel != after.channel:' block ---

# --- After Play & Disconnect Logic ---
# (These functions remain the same)
def after_play_handler(error, voice_client):
    if error: bot_logger.error(f'PLAYBACK ERROR (in after callback): {error}', exc_info=error)
    bot_logger.debug(f"PLAYBACK FINISHED: Scheduling disconnect...")
    bot.loop.create_task(schedule_disconnect(voice_client, delay=1.0))

async def schedule_disconnect(voice_client: discord.VoiceClient, delay: float = 1.0):
    await asyncio.sleep(delay)
    await safe_disconnect(voice_client)

async def safe_disconnect(voice_client: discord.VoiceClient):
    if voice_client and voice_client.is_connected() and not voice_client.is_playing():
        try:
            channel_name = voice_client.channel.name
            await voice_client.disconnect(force=False)
            bot_logger.info(f"DISCONNECT: Bot disconnected from {channel_name}")
        except Exception as e: bot_logger.error(f"DISCONNECT ERROR: {e}", exc_info=True)

# --- Slash Commands ---
# (addsound and removesound remain the same as the previous version with validation)
@bot.slash_command(name="addsound", description="Upload your custom join sound (MP3, WAV recommended).")
@commands.cooldown(1, 15, commands.BucketType.user)
async def addsound( ctx: discord.ApplicationContext, sound_file: discord.Option(discord.Attachment, "Sound file (MP3, WAV, OGG, etc.). Max 5MB. Will be validated.")):
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

            if user_id_str in user_sound_config: # Remove old if different name/ext
                old_filename = user_sound_config[user_id_str]
                if old_filename != final_save_filename:
                    old_path = os.path.join(SOUNDS_DIR, old_filename)
                    if os.path.exists(old_path):
                        try: os.remove(old_path); bot_logger.info(f"Removed old: {old_path}")
                        except Exception as e: bot_logger.warning(f"Could not remove old {old_path}: {e}")

            try: # Rename temp to final
                os.rename(temp_save_path, final_save_path)
                bot_logger.info(f"Renamed to final: {final_save_path}")
            except OSError as ren_e:
                 bot_logger.error(f"Failed rename {temp_save_path} to {final_save_path}: {ren_e}."); await cleanup_temp(); raise

            user_sound_config[user_id_str] = final_save_filename; save_config()
            bot_logger.info(f"Updated config for {ctx.author.name} to use {final_save_filename}")
            await ctx.followup.send(f"✅ Success! `{sound_file.filename}` validated and set.", ephemeral=True)

        except CouldntDecodeError as decode_error:
            bot_logger.error(f"AUDIO VALIDATION FAILED (user: {ctx.author.id}): {decode_error}")
            await cleanup_temp()
            await ctx.followup.send(f"❌ **Validation Failed!** Could not process `{sound_file.filename}`. Bad format or missing backend? Try MP3/WAV.", ephemeral=True)
        except Exception as validate_e:
            bot_logger.error(f"AUDIO VALIDATION FAILED (user: {ctx.author.id}): {validate_e}", exc_info=True)
            await cleanup_temp()
            await ctx.followup.send(f"❌ **Validation Failed!** Unexpected error processing `{sound_file.filename}`.", ephemeral=True)

    except discord.HTTPException as e:
        bot_logger.error(f"Error downloading temp file for {ctx.author.id}: {e}", exc_info=True); await cleanup_temp()
        await ctx.followup.send("❌ Error downloading file.", ephemeral=True)
    except Exception as e:
        bot_logger.error(f"Error in addsound for {ctx.author.id}: {e}", exc_info=True); await cleanup_temp()
        await ctx.followup.send("❌ Unexpected error.", ephemeral=True)


@bot.slash_command(name="removesound", description="Remove your custom join sound and revert to TTS.")
@commands.cooldown(1, 5, commands.BucketType.user)
async def removesound(ctx: discord.ApplicationContext):
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
@bot.event
async def on_application_command_error(ctx: discord.ApplicationContext, error: discord.DiscordException):
    # (Same as before)
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
if __name__ == "__main__":
    try:
        bot_logger.info("Attempting to start bot...")
        bot.run(BOT_TOKEN)
    except discord.errors.LoginFailure: bot_logger.critical("="*30 + "\nLOGIN FAILURE: Invalid BOT_TOKEN.\n" + "="*30)
    except discord.errors.PrivilegedIntentsRequired: bot_logger.critical("="*30 + "\nINTENT ERROR: Enable Voice States Intent.\n" + "="*30)
    except discord.errors.OpusNotLoaded: bot_logger.critical("="*30 + "\nOPUS ERROR: Opus library failed to load.\n" + "="*30)
    except Exception as e: bot_logger.critical(f"FATAL RUNTIME ERROR: {e}", exc_info=True)