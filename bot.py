# bot.py

import discord
from discord.ext import commands
import os
import json
import asyncio
import logging
import io # Required for BytesIO
import math # For checking infinite values in dBFS
from collections import deque # Efficient queue structure
import re # For cleaning filenames
from typing import List, Optional, Tuple, Dict, Any, Coroutine # For type hinting
import shutil # For copying/moving files

# Import edge-tts
try:
    import edge_tts
    EDGE_TTS_AVAILABLE = True
except ImportError:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')
    logging.critical("CRITICAL: edge-tts library not found. Please install it: pip install edge-tts")
    EDGE_TTS_AVAILABLE = False

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
GUILD_SETTINGS_FILE = "guild_settings.json"
TARGET_LOUDNESS_DBFS = -14.0
MAX_USER_SOUND_SIZE_MB = 5
MAX_USER_SOUNDS_PER_USER = 25
ALLOWED_EXTENSIONS = ['.mp3', '.wav', '.ogg', '.m4a', '.aac']
MAX_TTS_LENGTH = 350 # Max characters for TTS command
DEFAULT_TTS_VOICE = "en-US-JennyNeural" # Bot's default Edge-TTS voice
MAX_PLAYBACK_DURATION_MS = 10 * 1000 # Max duration in milliseconds (10 seconds)
# --- NEW: Auto Leave Configuration ---
AUTO_LEAVE_TIMEOUT_SECONDS = 4 * 60 * 60 # 4 hours in seconds
# --- End Configuration ---


# --- Define FULL List of Voices (For Autocomplete) ---
# (Voice list code remains unchanged)
# List of all available voice IDs extracted from `edge-tts --list-voices`
ALL_VOICE_IDS = [
    "af-ZA-AdriNeural", "af-ZA-WillemNeural", "am-ET-AmehaNeural", "am-ET-MekdesNeural",
    "ar-AE-FatimaNeural", "ar-AE-HamdanNeural", "ar-BH-AliNeural", "ar-BH-LailaNeural",
    "ar-DZ-AminaNeural", "ar-DZ-IsmaelNeural", "ar-EG-SalmaNeural", "ar-EG-ShakirNeural",
    "ar-IQ-BasselNeural", "ar-IQ-RanaNeural", "ar-JO-SanaNeural", "ar-JO-TaimNeural",
    "ar-KW-FahedNeural", "ar-KW-NouraNeural", "ar-LB-LaylaNeural", "ar-LB-RamiNeural",
    "ar-LY-ImanNeural", "ar-LY-OmarNeural", "ar-MA-JamalNeural", "ar-MA-MounaNeural",
    "ar-OM-AbdullahNeural", "ar-OM-AyshaNeural", "ar-QA-AmalNeural", "ar-QA-MoazNeural",
    "ar-SA-HamedNeural", "ar-SA-ZariyahNeural", "ar-SY-AmanyNeural", "ar-SY-LaithNeural",
    "ar-TN-HediNeural", "ar-TN-ReemNeural", "ar-YE-MaryamNeural", "ar-YE-SalehNeural",
    "az-AZ-BabekNeural", "az-AZ-BanuNeural", "bg-BG-BorislavNeural", "bg-BG-KalinaNeural",
    "bn-BD-NabanitaNeural", "bn-BD-PradeepNeural", "bn-IN-BashkarNeural", "bn-IN-TanishaaNeural",
    "bs-BA-GoranNeural", "bs-BA-VesnaNeural", "ca-ES-EnricNeural", "ca-ES-JoanaNeural",
    "cs-CZ-AntoninNeural", "cs-CZ-VlastaNeural", "cy-GB-AledNeural", "cy-GB-NiaNeural",
    "da-DK-ChristelNeural", "da-DK-JeppeNeural", "de-AT-IngridNeural", "de-AT-JonasNeural",
    "de-CH-JanNeural", "de-CH-LeniNeural", "de-DE-AmalaNeural", "de-DE-ConradNeural",
    "de-DE-FlorianMultilingualNeural", "de-DE-KatjaNeural", "de-DE-KillianNeural",
    "de-DE-SeraphinaMultilingualNeural", "el-GR-AthinaNeural", "el-GR-NestorasNeural",
    "en-AU-NatashaNeural", "en-AU-WilliamNeural", "en-CA-ClaraNeural", "en-CA-LiamNeural",
    "en-GB-LibbyNeural", "en-GB-MaisieNeural", "en-GB-RyanNeural", "en-GB-SoniaNeural",
    "en-GB-ThomasNeural", "en-HK-SamNeural", "en-HK-YanNeural", "en-IE-ConnorNeural",
    "en-IE-EmilyNeural", "en-IN-NeerjaExpressiveNeural", "en-IN-NeerjaNeural", "en-IN-PrabhatNeural",
    "en-KE-AsiliaNeural", "en-KE-ChilembaNeural", "en-NG-AbeoNeural", "en-NG-EzinneNeural",
    "en-NZ-MitchellNeural", "en-NZ-MollyNeural", "en-PH-JamesNeural", "en-PH-RosaNeural",
    "en-SG-LunaNeural", "en-SG-WayneNeural", "en-TZ-ElimuNeural", "en-TZ-ImaniNeural",
    "en-US-AnaNeural", "en-US-AndrewMultilingualNeural", "en-US-AndrewNeural", "en-US-AriaNeural",
    "en-US-AvaMultilingualNeural", "en-US-AvaNeural", "en-US-BrianMultilingualNeural", "en-US-BrianNeural",
    "en-US-ChristopherNeural", "en-US-EmmaMultilingualNeural", "en-US-EmmaNeural", "en-US-EricNeural",
    "en-US-GuyNeural", "en-US-JennyNeural", "en-US-MichelleNeural", "en-US-RogerNeural",
    "en-US-SteffanNeural", "en-ZA-LeahNeural", "en-ZA-LukeNeural", "es-AR-ElenaNeural",
    "es-AR-TomasNeural", "es-BO-MarceloNeural", "es-BO-SofiaNeural", "es-CL-CatalinaNeural",
    "es-CL-LorenzoNeural", "es-CO-GonzaloNeural", "es-CO-SalomeNeural", "es-CR-JuanNeural",
    "es-CR-MariaNeural", "es-CU-BelkysNeural", "es-CU-ManuelNeural", "es-DO-EmilioNeural",
    "es-DO-RamonaNeural", "es-EC-AndreaNeural", "es-EC-LuisNeural", "es-ES-AlvaroNeural",
    "es-ES-ElviraNeural", "es-ES-XimenaNeural", "es-GQ-JavierNeural", "es-GQ-TeresaNeural",
    "es-GT-AndresNeural", "es-GT-MartaNeural", "es-HN-CarlosNeural", "es-HN-KarlaNeural",
    "es-MX-DaliaNeural", "es-MX-JorgeNeural", "es-NI-FedericoNeural", "es-NI-YolandaNeural",
    "es-PA-MargaritaNeural", "es-PA-RobertoNeural", "es-PE-AlexNeural", "es-PE-CamilaNeural",
    "es-PR-KarinaNeural", "es-PR-VictorNeural", "es-PY-MarioNeural", "es-PY-TaniaNeural",
    "es-SV-LorenaNeural", "es-SV-RodrigoNeural", "es-US-AlonsoNeural", "es-US-PalomaNeural",
    "es-UY-MateoNeural", "es-UY-ValentinaNeural", "es-VE-PaolaNeural", "es-VE-SebastianNeural",
    "et-EE-AnuNeural", "et-EE-KertNeural", "fa-IR-DilaraNeural", "fa-IR-FaridNeural",
    "fi-FI-HarriNeural", "fi-FI-NooraNeural", "fil-PH-AngeloNeural", "fil-PH-BlessicaNeural",
    "fr-BE-CharlineNeural", "fr-BE-GerardNeural", "fr-CA-AntoineNeural", "fr-CA-JeanNeural",
    "fr-CA-SylvieNeural", "fr-CA-ThierryNeural", "fr-CH-ArianeNeural", "fr-CH-FabriceNeural",
    "fr-FR-DeniseNeural", "fr-FR-EloiseNeural", "fr-FR-HenriNeural", "fr-FR-RemyMultilingualNeural",
    "fr-FR-VivienneMultilingualNeural", "ga-IE-ColmNeural", "ga-IE-OrlaNeural", "gl-ES-RoiNeural",
    "gl-ES-SabelaNeural", "gu-IN-DhwaniNeural", "gu-IN-NiranjanNeural", "he-IL-AvriNeural",
    "he-IL-HilaNeural", "hi-IN-MadhurNeural", "hi-IN-SwaraNeural", "hr-HR-GabrijelaNeural",
    "hr-HR-SreckoNeural", "hu-HU-NoemiNeural", "hu-HU-TamasNeural", "id-ID-ArdiNeural",
    "id-ID-GadisNeural", "is-IS-GudrunNeural", "is-IS-GunnarNeural", "it-IT-DiegoNeural",
    "it-IT-ElsaNeural", "it-IT-GiuseppeMultilingualNeural", "it-IT-IsabellaNeural",
    "iu-Cans-CA-SiqiniqNeural", "iu-Cans-CA-TaqqiqNeural", "iu-Latn-CA-SiqiniqNeural",
    "iu-Latn-CA-TaqqiqNeural", "ja-JP-KeitaNeural", "ja-JP-NanamiNeural", "jv-ID-DimasNeural",
    "jv-ID-SitiNeural", "ka-GE-EkaNeural", "ka-GE-GiorgiNeural", "kk-KZ-AigulNeural",
    "kk-KZ-DauletNeural", "km-KH-PisethNeural", "km-KH-SreymomNeural", "kn-IN-GaganNeural",
    "kn-IN-SapnaNeural", "ko-KR-HyunsuMultilingualNeural", "ko-KR-InJoonNeural", "ko-KR-SunHiNeural",
    "lo-LA-ChanthavongNeural", "lo-LA-KeomanyNeural", "lt-LT-LeonasNeural", "lt-LT-OnaNeural",
    "lv-LV-EveritaNeural", "lv-LV-NilsNeural", "mk-MK-AleksandarNeural", "mk-MK-MarijaNeural",
    "ml-IN-MidhunNeural", "ml-IN-SobhanaNeural", "mn-MN-BataaNeural", "mn-MN-YesuiNeural",
    "mr-IN-AarohiNeural", "mr-IN-ManoharNeural", "ms-MY-OsmanNeural", "ms-MY-YasminNeural",
    "mt-MT-GraceNeural", "mt-MT-JosephNeural", "my-MM-NilarNeural", "my-MM-ThihaNeural",
    "nb-NO-FinnNeural", "nb-NO-PernilleNeural", "ne-NP-HemkalaNeural", "ne-NP-SagarNeural",
    "nl-BE-ArnaudNeural", "nl-BE-DenaNeural", "nl-NL-ColetteNeural", "nl-NL-FennaNeural",
    "nl-NL-MaartenNeural", "pl-PL-MarekNeural", "pl-PL-ZofiaNeural", "ps-AF-GulNawazNeural",
    "ps-AF-LatifaNeural", "pt-BR-AntonioNeural", "pt-BR-FranciscaNeural", "pt-BR-ThalitaMultilingualNeural",
    "pt-PT-DuarteNeural", "pt-PT-RaquelNeural", "ro-RO-AlinaNeural", "ro-RO-EmilNeural",
    "ru-RU-DmitryNeural", "ru-RU-SvetlanaNeural", "si-LK-SameeraNeural", "si-LK-ThiliniNeural",
    "sk-SK-LukasNeural", "sk-SK-ViktoriaNeural", "sl-SI-PetraNeural", "sl-SI-RokNeural",
    "so-SO-MuuseNeural", "so-SO-UbaxNeural", "sq-AL-AnilaNeural", "sq-AL-IlirNeural",
    "sr-RS-NicholasNeural", "sr-RS-SophieNeural", "su-ID-JajangNeural", "su-ID-TutiNeural",
    "sv-SE-MattiasNeural", "sv-SE-SofieNeural", "sw-KE-RafikiNeural", "sw-KE-ZuriNeural",
    "sw-TZ-DaudiNeural", "sw-TZ-RehemaNeural", "ta-IN-PallaviNeural", "ta-IN-ValluvarNeural",
    "ta-LK-KumarNeural", "ta-LK-SaranyaNeural", "ta-MY-KaniNeural", "ta-MY-SuryaNeural",
    "ta-SG-AnbuNeural", "ta-SG-VenbaNeural", "te-IN-MohanNeural", "te-IN-ShrutiNeural",
    "th-TH-NiwatNeural", "th-TH-PremwadeeNeural", "tr-TR-AhmetNeural", "tr-TR-EmelNeural",
    "uk-UA-OstapNeural", "uk-UA-PolinaNeural", "ur-IN-GulNeural", "ur-IN-SalmanNeural",
    "ur-PK-AsadNeural", "ur-PK-UzmaNeural", "uz-UZ-MadinaNeural", "uz-UZ-SardorNeural",
    "vi-VN-HoaiMyNeural", "vi-VN-NamMinhNeural", "zh-CN-XiaoxiaoNeural", "zh-CN-XiaoyiNeural",
    "zh-CN-YunjianNeural", "zh-CN-YunxiNeural", "zh-CN-YunxiaNeural", "zh-CN-YunyangNeural",
    "zh-CN-liaoning-XiaobeiNeural", "zh-CN-shaanxi-XiaoniNeural", "zh-HK-HiuGaaiNeural",
    "zh-HK-HiuMaanNeural", "zh-HK-WanLungNeural", "zh-TW-HsiaoChenNeural", "zh-TW-HsiaoYuNeural",
    "zh-TW-YunJheNeural", "zu-ZA-ThandoNeural", "zu-ZA-ThembaNeural"
]
# Function to generate a more readable name from the ID
def create_display_name(voice_id: str) -> str:
    parts = voice_id.split('-')
    if len(parts) >= 3:
        lang_code = parts[0]
        region_code = parts[1]
        name_part = parts[2]
        # Handle names like XiaobeiNeural, AndrewMultilingualNeural, NeerjaExpressiveNeural
        name = name_part.replace("Neural", "").replace("Multilingual", " Multi").replace("Expressive", " Expr")
        # Special handling for dialects/regions within a language code
        if region_code == "liaoning" and lang_code == "zh":
             return f"Chinese (Liaoning) {name}"
        if region_code == "shaanxi" and lang_code == "zh":
            return f"Chinese (Shaanxi) {name}"
        # Special handling for script variants (example: Inuktitut)
        if "Cans" in region_code: region_code = region_code.replace("Cans", "CA-Cans")
        elif "Latn" in region_code: region_code = region_code.replace("Latn", "CA-Latn")
        # General format: LANG-REGION Name
        return f"{lang_code.upper()}-{region_code.upper()} {name}"
    return voice_id # Fallback to original ID if parsing fails

# Create the FULL OptionChoice list for Autocomplete
# This list contains ALL voices for searching via autocomplete
FULL_EDGE_TTS_VOICE_CHOICES = []
for voice_id in ALL_VOICE_IDS:
    display_name = create_display_name(voice_id)
    # Truncate display name if it exceeds Discord's limit (100 chars)
    if len(display_name) > 100:
        display_name = display_name[:97] + "..."
    FULL_EDGE_TTS_VOICE_CHOICES.append(discord.OptionChoice(name=display_name, value=voice_id))

# Sort the full list alphabetically by display name
FULL_EDGE_TTS_VOICE_CHOICES.sort(key=lambda x: x.name)

# --- Define CURATED List of Voices (For Dropdown Choices) ---
# A smaller, more manageable list for the initial dropdown (max 25 choices)
# Prioritizing common languages, variety, and interesting personalities.
CURATED_VOICE_IDS = [
    "en-US-JennyNeural",      # Default US Female
    "en-US-AriaNeural",       # US Female (News)
    "en-US-GuyNeural",        # US Male (News, Passionate)
    "en-US-AnaNeural",        # US Female (Cartoon, Cute) - Interesting Personality
    "en-GB-LibbyNeural",      # UK Female
    "en-GB-RyanNeural",       # UK Male
    "en-AU-NatashaNeural",    # AU Female
    "en-CA-ClaraNeural",      # CA Female
    "en-IN-NeerjaExpressiveNeural", # IN Female (Expressive)
    "es-ES-ElviraNeural",     # ES Female
    "es-MX-JorgeNeural",      # MX Male
    "fr-FR-DeniseNeural",     # FR Female
    "fr-CA-JeanNeural",       # CA Male
    "de-DE-KatjaNeural",      # DE Female
    "de-DE-ConradNeural",     # DE Male
    "it-IT-IsabellaNeural",   # IT Female
    "ja-JP-NanamiNeural",     # JP Female
    "ja-JP-KeitaNeural",      # JP Male
    "ko-KR-SunHiNeural",      # KR Female
    "pt-BR-FranciscaNeural",  # BR Female (Portuguese)
    "ru-RU-SvetlanaNeural",   # RU Female
    "zh-CN-XiaoxiaoNeural",   # CN Female
    "ar-EG-SalmaNeural",      # EG Female (Arabic)
    "hi-IN-SwaraNeural",      # IN Female (Hindi)
    "nl-NL-MaartenNeural",    # NL Male
]

# Create the CURATED OptionChoice list from the curated IDs
CURATED_EDGE_TTS_VOICE_CHOICES = []
for voice_id in CURATED_VOICE_IDS:
     # Find the corresponding entry in the full list to reuse the generated name
     found = False
     for full_choice in FULL_EDGE_TTS_VOICE_CHOICES:
         if full_choice.value == voice_id:
             CURATED_EDGE_TTS_VOICE_CHOICES.append(full_choice)
             found = True
             break
     if not found:
         # Fallback if somehow the curated ID wasn't in the full list (shouldn't happen)
         display_name = create_display_name(voice_id)
         if len(display_name) > 100: display_name = display_name[:97] + "..."
         CURATED_EDGE_TTS_VOICE_CHOICES.append(discord.OptionChoice(name=display_name, value=voice_id))
         # Add logging here, assuming bot_logger is defined before this point
         # bot_logger.warning(f"Curated voice ID '{voice_id}' not found in generated FULL_EDGE_TTS_VOICE_CHOICES list during setup.")

# Sort the curated list as well
CURATED_EDGE_TTS_VOICE_CHOICES.sort(key=lambda x: x.name)
# --- End Voice List Setup ---

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')
discord_logger = logging.getLogger('discord')
discord_logger.setLevel(logging.WARNING)
bot_logger = logging.getLogger('SoundBot')
bot_logger.setLevel(logging.INFO) # Keep INFO for general operation, DEBUG for detailed queue/timer logs

# --- Validate Critical Config ---
if not BOT_TOKEN or not PYDUB_AVAILABLE or not EDGE_TTS_AVAILABLE:
    bot_logger.critical("CRITICAL ERROR: Bot token missing, Pydub failed, or edge-tts failed.")
    exit()

# --- Intents ---
intents = discord.Intents.default()
intents.voice_states = True
intents.guilds = True
intents.message_content = False # Not needed for slash commands
intents.members = True # NEEDED to accurately check channel members

# --- Bot Definition ---
bot = discord.Bot(intents=intents)

# --- Data Storage & Helpers ---
# User config: { "user_id_str": { "join_sound": "filename.mp3", "tts_defaults": {"voice": "en-US-JennyNeural"} } }
user_sound_config: Dict[str, Dict[str, Any]] = {}
# Guild settings: { "guild_id_str": { "stay_in_channel": bool } }
guild_settings: Dict[str, Dict[str, Any]] = {}
guild_sound_queues: Dict[int, deque[Tuple[discord.Member, str]]] = {}
guild_play_tasks: Dict[int, asyncio.Task[Any]] = {}
# --- NEW: Auto Leave Timer Storage ---
guild_leave_timers: Dict[int, asyncio.Task[Any]] = {}

# --- Config/Dir Functions ---
# (load_config, save_config, load_guild_settings, save_guild_settings, ensure_dir remain unchanged)
def load_config():
    global user_sound_config
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f: user_sound_config = json.load(f)
            upgraded_count = 0
            for user_id, data in list(user_sound_config.items()):
                # Check for and potentially update old TTS format if needed (e.g., language/slow to voice)
                if isinstance(data, dict) and "tts_defaults" in data:
                    defaults = data["tts_defaults"]
                    if "language" in defaults or "slow" in defaults:
                         # Simple migration: Use default voice if old keys found
                        if "voice" not in defaults:
                             defaults["voice"] = DEFAULT_TTS_VOICE
                        if "language" in defaults: del defaults["language"]
                        if "slow" in defaults: del defaults["slow"]
                        bot_logger.info(f"Upgraded TTS defaults format for user {user_id}")
                        upgraded_count += 1
                elif isinstance(data, str): # Original upgrade logic for join sound only
                    user_sound_config[user_id] = {"join_sound": data}
                    bot_logger.info(f"Upgraded join sound format for user {user_id}")
                    upgraded_count += 1

            if upgraded_count > 0:
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

def load_guild_settings():
    """Loads guild-specific settings from GUILD_SETTINGS_FILE."""
    global guild_settings
    if os.path.exists(GUILD_SETTINGS_FILE):
        try:
            with open(GUILD_SETTINGS_FILE, 'r') as f:
                loaded_data = json.load(f)
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
    load_guild_settings()
    bot_logger.info(f"Py-cord: {discord.__version__}, Norm Target: {TARGET_LOUDNESS_DBFS}dBFS")
    bot_logger.info(f"TTS Engine: edge-tts, Default Voice: {DEFAULT_TTS_VOICE}")
    bot_logger.info(f"Loaded {len(FULL_EDGE_TTS_VOICE_CHOICES)} total TTS voices for autocomplete.")
    bot_logger.info(f"Using {len(CURATED_EDGE_TTS_VOICE_CHOICES)} curated voices for command choices.")
    bot_logger.info(f"Allowed: {', '.join(ALLOWED_EXTENSIONS)}, Max TTS: {MAX_TTS_LENGTH}")
    bot_logger.info(f"Playback limited to first {MAX_PLAYBACK_DURATION_MS / 1000} seconds.")
    # --- NEW: Log Auto Leave Timeout ---
    bot_logger.info(f"Auto-leave timeout set to {AUTO_LEAVE_TIMEOUT_SECONDS} seconds ({AUTO_LEAVE_TIMEOUT_SECONDS / 3600:.1f} hours).")
    # --- End New Log ---
    bot_logger.info(f"Dirs: {os.path.abspath(SOUNDS_DIR)}, {os.path.abspath(USER_SOUNDS_DIR)}, {os.path.abspath(PUBLIC_SOUNDS_DIR)}")
    bot_logger.info("Sound Bot is operational.")

# --- Audio Processing Helper ---
# (process_audio function remains unchanged)
def process_audio(sound_path: str, member_display_name: str = "User") -> Optional[discord.PCMAudio]:
    """Loads, TRIMS, normalizes, and prepares audio returning a PCMAudio source or None."""
    if not PYDUB_AVAILABLE or not os.path.exists(sound_path):
        bot_logger.error(f"AUDIO: Pydub missing or File not found: '{sound_path}'")
        return None

    audio_source = None
    basename = os.path.basename(sound_path)
    try:
        bot_logger.debug(f"AUDIO: Loading '{basename}'...")
        ext = os.path.splitext(sound_path)[1].lower().strip('. ') or 'mp3'
        audio_segment = AudioSegment.from_file(sound_path, format=ext)

        if len(audio_segment) > MAX_PLAYBACK_DURATION_MS:
            bot_logger.info(f"AUDIO: Trimming '{basename}' from {len(audio_segment)}ms to first {MAX_PLAYBACK_DURATION_MS}ms.")
            audio_segment = audio_segment[:MAX_PLAYBACK_DURATION_MS]
        else:
            bot_logger.debug(f"AUDIO: '{basename}' is {len(audio_segment)}ms (<= {MAX_PLAYBACK_DURATION_MS}ms), no trimming needed.")

        peak_dbfs = audio_segment.max_dBFS
        if not math.isinf(peak_dbfs) and peak_dbfs > -90.0:
            change_in_dbfs = TARGET_LOUDNESS_DBFS - peak_dbfs
            bot_logger.info(f"AUDIO: Normalizing '{basename}'. Peak:{peak_dbfs:.2f} Target:{TARGET_LOUDNESS_DBFS:.2f} Gain:{change_in_dbfs:.2f} dB.")
            if change_in_dbfs < 0:
                audio_segment = audio_segment.apply_gain(change_in_dbfs)
            else:
                bot_logger.info(f"AUDIO: Skipping positive gain for '{basename}'.")
        elif math.isinf(peak_dbfs):
            bot_logger.warning(f"AUDIO: Cannot normalize silent audio '{basename}'. Peak is -inf.")
        else:
             bot_logger.warning(f"AUDIO: Skipping normalization for very quiet audio '{basename}'. Peak: {peak_dbfs:.2f}")

        audio_segment = audio_segment.set_frame_rate(48000).set_channels(2)

        pcm_data_io = io.BytesIO()
        audio_segment.export(pcm_data_io, format="s16le")
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


# --- Auto Leave Helper Functions ---

def is_bot_alone(vc: Optional[discord.VoiceClient]) -> bool:
    """Checks if the bot is the only non-bot user in its voice channel."""
    if not vc or not vc.channel:
        return False # Can't be alone if not in a channel
    if not bot.user:
        bot_logger.warning("is_bot_alone called before bot.user is available.")
        return False # Should not happen in normal operation

    human_members = [m for m in vc.channel.members if not m.bot]
    bot_logger.debug(f"ALONE CHECK (Guild: {vc.guild.id}, Channel: {vc.channel.name}): Found {len(human_members)} human(s). Members: {[m.name for m in vc.channel.members]}")
    return len(human_members) == 0

def cancel_leave_timer(guild_id: int, reason: str = "unknown"):
    """Cancels the automatic leave timer for a guild if it exists."""
    if guild_id in guild_leave_timers:
        timer_task = guild_leave_timers.pop(guild_id, None)
        if timer_task and not timer_task.done():
            try:
                timer_task.cancel()
                bot_logger.info(f"LEAVE TIMER: Cancelled for Guild {guild_id}. Reason: {reason}")
            except Exception as e:
                bot_logger.warning(f"LEAVE TIMER: Error cancelling timer for Guild {guild_id}: {e}")
        elif timer_task:
             bot_logger.debug(f"LEAVE TIMER: Attempted to cancel completed timer for Guild {guild_id}.")

async def start_leave_timer(vc: discord.VoiceClient):
    """Starts the automatic leave timer if conditions are met."""
    if not vc or not vc.is_connected() or not vc.guild:
        return

    guild_id = vc.guild.id
    log_prefix = f"LEAVE TIMER (Guild {guild_id}):"

    # 1. Cancel any existing timer for this guild first
    cancel_leave_timer(guild_id, reason="starting new timer check")

    # 2. Check conditions: Bot must be alone AND stay is disabled
    if not is_bot_alone(vc):
        bot_logger.debug(f"{log_prefix} Not starting timer - bot is not alone.")
        return
    if should_bot_stay(guild_id):
        bot_logger.debug(f"{log_prefix} Not starting timer - 'stay' setting is enabled.")
        return
    if vc.is_playing():
         bot_logger.debug(f"{log_prefix} Not starting timer - bot is currently playing.")
         return

    bot_logger.info(f"{log_prefix} Conditions met (alone, stay disabled, idle). Starting {AUTO_LEAVE_TIMEOUT_SECONDS}s timer.")

    async def _leave_after_delay(voice_client: discord.VoiceClient, g_id: int):
        try:
            await asyncio.sleep(AUTO_LEAVE_TIMEOUT_SECONDS)

            # --- Re-check conditions AFTER sleep ---
            # Use a fresh reference to the VC if possible
            current_vc = discord.utils.get(bot.voice_clients, guild__id=g_id)
            if not current_vc or not current_vc.is_connected() or current_vc.channel != voice_client.channel:
                 bot_logger.info(f"{log_prefix} Timer expired, but bot is no longer connected or moved. Aborting leave.")
                 return
            if not is_bot_alone(current_vc):
                 bot_logger.info(f"{log_prefix} Timer expired, but bot is no longer alone. Aborting leave.")
                 return
            if should_bot_stay(g_id):
                 bot_logger.info(f"{log_prefix} Timer expired, but 'stay' was enabled during wait. Aborting leave.")
                 return
            if current_vc.is_playing():
                bot_logger.info(f"{log_prefix} Timer expired, but bot started playing again. Aborting leave.")
                return

            # --- Conditions still met - Disconnect ---
            bot_logger.info(f"{log_prefix} Timer expired. Conditions still met. Triggering automatic disconnect.")
            await safe_disconnect(current_vc, manual_leave=False) # Use safe_disconnect

        except asyncio.CancelledError:
             bot_logger.info(f"{log_prefix} Timer explicitly cancelled.")
             # No need to raise, cancellation is expected behavior
        except Exception as e:
             bot_logger.error(f"{log_prefix} Error during leave timer delay/check: {e}", exc_info=True)
        finally:
             # Clean up the task entry if it still exists (it might have been removed by cancel_leave_timer)
             if g_id in guild_leave_timers and guild_leave_timers[g_id] is asyncio.current_task():
                 del guild_leave_timers[g_id]
                 bot_logger.debug(f"{log_prefix} Cleaned up timer task reference.")

    # Create and store the task
    timer_task = bot.loop.create_task(_leave_after_delay(vc, guild_id), name=f"AutoLeave_{guild_id}")
    guild_leave_timers[guild_id] = timer_task

# --- End Auto Leave Helper Functions ---

# --- Core Join Sound Queue Logic ---
# (play_next_in_queue remains largely unchanged, but needs timer cancellation on play start)
async def play_next_in_queue(guild: discord.Guild):
    guild_id = guild.id
    task_id = asyncio.current_task().get_name() if asyncio.current_task() else 'Unknown'
    bot_logger.debug(f"QUEUE CHECK [{task_id}]: Guild {guild_id}")

    if guild_id in guild_play_tasks and guild_play_tasks[guild_id] is asyncio.current_task() and asyncio.current_task().cancelled():
        bot_logger.debug(f"QUEUE CHECK [{task_id}]: Task cancelled externally for guild {guild_id}, removing tracker.")
        if guild_id in guild_play_tasks: del guild_play_tasks[guild_id] # Ensure removal
        return

    # Check if bot should leave (queue empty, etc.) handled by after_play_handler and voice state updates now
    if guild_id not in guild_sound_queues or not guild_sound_queues[guild_id]:
        bot_logger.debug(f"QUEUE [{task_id}]: Empty/Non-existent for {guild_id}. Playback task ending. Disconnect/timer check handled elsewhere.")
        if guild_id in guild_play_tasks and guild_play_tasks[guild_id] is asyncio.current_task(): del guild_play_tasks[guild_id]
        # Trigger timer check if idle and connected
        vc_check = discord.utils.get(bot.voice_clients, guild=guild)
        if vc_check and vc_check.is_connected() and not vc_check.is_playing():
             bot.loop.create_task(start_leave_timer(vc_check)) # Check if timer should start
        return

    vc = discord.utils.get(bot.voice_clients, guild=guild)
    if not vc or not vc.is_connected():
        bot_logger.warning(f"QUEUE [{task_id}]: Task running for {guild_id}, but bot not connected. Clearing.")
        if guild_id in guild_sound_queues: guild_sound_queues[guild_id].clear()
        if guild_id in guild_play_tasks and guild_play_tasks[guild_id] is asyncio.current_task(): del guild_play_tasks[guild_id]
        cancel_leave_timer(guild_id, reason="bot not connected") # Ensure timer is cancelled
        return

    if vc.is_playing():
        bot_logger.debug(f"QUEUE [{task_id}]: Bot already playing in {guild_id}, yielding.")
        return

    try:
        member, sound_path = guild_sound_queues[guild_id].popleft()
        bot_logger.info(f"QUEUE [{task_id}]: Processing {member.display_name} in {guild.name}. Path: {os.path.basename(sound_path)}. Left: {len(guild_sound_queues[guild_id])}")
    except IndexError:
        bot_logger.debug(f"QUEUE [{task_id}]: Became empty unexpectedly for {guild_id}. Playback task ending. Disconnect/timer check handled elsewhere.")
        if guild_id in guild_play_tasks and guild_play_tasks[guild_id] is asyncio.current_task(): del guild_play_tasks[guild_id]
        # Trigger timer check if idle and connected
        if vc and vc.is_connected() and not vc.is_playing():
             bot.loop.create_task(start_leave_timer(vc))
        return

    # --- Delete temporary TTS file after use ---
    is_temp_tts = os.path.basename(sound_path).startswith("tts_join_")

    def after_play_cleanup(error: Optional[Exception], vc_ref: discord.VoiceClient, path_to_delete: Optional[str] = None):
        # Call the original handler first
        after_play_handler(error, vc_ref)
        # Then attempt cleanup
        if path_to_delete and os.path.exists(path_to_delete):
            try:
                os.remove(path_to_delete)
                bot_logger.debug(f"CLEANUP: Deleted temporary TTS file: {path_to_delete}")
            except OSError as e_del:
                bot_logger.warning(f"CLEANUP: Failed to delete temporary TTS file '{path_to_delete}': {e_del}")

    audio_source = process_audio(sound_path, member.display_name)

    if audio_source:
        try:
            # --- Cancel leave timer before playing ---
            cancel_leave_timer(guild_id, reason="starting playback")
            # ---
            bot_logger.info(f"QUEUE PLAYBACK [{task_id}]: Playing for {member.display_name}...")
            # Pass the sound_path to the cleanup function if it's a temporary TTS file
            cleanup_path = sound_path if is_temp_tts else None
            vc.play(audio_source, after=lambda e: after_play_cleanup(e, vc, cleanup_path))
            bot_logger.debug(f"QUEUE PLAYBACK [{task_id}]: vc.play() called for {member.display_name}.")
        except (discord.errors.ClientException, Exception) as e:
            bot_logger.error(f"QUEUE PLAYBACK ERROR [{task_id}]: {type(e).__name__}: {e}", exc_info=True)
            # Still call after_play_handler on error to process next item or disconnect
            # Manually attempt cleanup if playback failed immediately
            cleanup_path = sound_path if is_temp_tts else None
            after_play_cleanup(e, vc, cleanup_path) # This will call after_play_handler which triggers timer check
    else:
        bot_logger.warning(f"QUEUE PLAYBACK [{task_id}]: No valid source for {member.display_name} ({os.path.basename(sound_path)}). Skipping.")
        # Manually attempt cleanup if processing failed
        if is_temp_tts and os.path.exists(sound_path):
            try:
                os.remove(sound_path)
                bot_logger.debug(f"CLEANUP: Deleted failed TTS file: {sound_path}")
            except OSError as e_del:
                bot_logger.warning(f"CLEANUP: Failed to delete failed TTS file '{sound_path}': {e_del}")
        # Schedule next check if processing failed
        bot.loop.create_task(play_next_in_queue(guild), name=f"QueueSkip_{guild_id}")


# --- on_voice_state_update ---
@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    guild = member.guild
    guild_id = guild.id
    vc = discord.utils.get(bot.voice_clients, guild=guild)

    # --- Handle User JOINING a channel ---
    if not member.bot and after.channel and before.channel != after.channel:
        channel_to_join = after.channel
        bot_logger.info(f"EVENT: {member.display_name} ({member.id}) entered {channel_to_join.name} in {guild.name}")

        # If user joins the channel the bot is *already* in, cancel any leave timer
        if vc and vc.is_connected() and vc.channel == channel_to_join:
            bot_logger.debug(f"User {member.display_name} joined bot's channel ({vc.channel.name}).")
            cancel_leave_timer(guild_id, reason=f"user {member.display_name} joined")

        # --- Join Sound Logic (largely unchanged) ---
        bot_perms = channel_to_join.permissions_for(guild.me)
        if not bot_perms.connect or not bot_perms.speak:
            bot_logger.warning(f"Missing Connect/Speak permission in '{channel_to_join.name}'. Cannot play sound.")
            return # Don't proceed if perms missing

        sound_path: Optional[str] = None
        is_tts = False
        user_id_str = str(member.id)
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
                if not user_config:
                    if user_id_str in user_sound_config: del user_sound_config[user_id_str]
                save_config()
                is_tts = True
        else:
            is_tts = True
            bot_logger.info(f"SOUND: No custom join sound for {member.display_name}. Using TTS.")

        if is_tts:
            tts_path = os.path.join(SOUNDS_DIR, f"tts_join_{member.id}_{os.urandom(4).hex()}.mp3")
            bot_logger.info(f"TTS: Generating join TTS for {member.display_name} ('{os.path.basename(tts_path)}')...")
            try:
                tts_defaults = user_config.get("tts_defaults", {}) if user_config else {}
                tts_voice = tts_defaults.get("voice", DEFAULT_TTS_VOICE)
                if not any(v.value == tts_voice for v in FULL_EDGE_TTS_VOICE_CHOICES):
                    bot_logger.warning(f"TTS Join: Invalid default voice '{tts_voice}' for user {member.id}. Falling back to bot default '{DEFAULT_TTS_VOICE}'.")
                    tts_voice = DEFAULT_TTS_VOICE

                bot_logger.debug(f"TTS Join using voice: {tts_voice}")
                text_to_speak = f"{member.display_name} joined"
                communicate = edge_tts.Communicate(text_to_speak, tts_voice)
                await communicate.save(tts_path)
                if not os.path.exists(tts_path) or os.path.getsize(tts_path) == 0:
                    raise RuntimeError(f"edge-tts failed to create a non-empty file: {tts_path}")
                bot_logger.info(f"TTS: Saved join TTS file '{os.path.basename(tts_path)}'")
                sound_path = tts_path
            except Exception as e:
                bot_logger.error(f"TTS: Failed join TTS generation for {member.display_name} (voice={tts_voice}): {e}", exc_info=True)
                sound_path = None
                if os.path.exists(tts_path):
                    try: os.remove(tts_path)
                    except OSError: pass

        if not sound_path:
            bot_logger.error(f"Could not determine or generate a sound/TTS path for {member.display_name}. Skipping playback.")
            return

        # Queueing and Playback Initiation Logic
        if guild_id not in guild_sound_queues:
            guild_sound_queues[guild_id] = deque()

        guild_sound_queues[guild_id].append((member, sound_path))
        bot_logger.info(f"QUEUE: Added join sound for {member.display_name}. Queue size: {len(guild_sound_queues[guild_id])}")

        # --- Start/Ensure playback task is running ---
        current_vc = discord.utils.get(bot.voice_clients, guild=guild) # Re-get VC reference
        should_start_play_task = False

        try:
            if not current_vc or not current_vc.is_connected():
                bot_logger.info(f"VOICE: Connecting to '{channel_to_join.name}' to start queue.")
                # Cancel timer *before* connecting
                cancel_leave_timer(guild_id, reason="connecting for join sound")
                current_vc = await channel_to_join.connect(timeout=30.0, reconnect=True)
                bot_logger.info(f"VOICE: Connected to '{channel_to_join.name}'.")
                should_start_play_task = True
            elif current_vc.channel != channel_to_join:
                 bot_logger.info(f"VOICE: Moving from '{current_vc.channel.name}' to '{channel_to_join.name}' to start queue.")
                 # Cancel timer *before* moving
                 cancel_leave_timer(guild_id, reason="moving for join sound")
                 await current_vc.move_to(channel_to_join)
                 bot_logger.info(f"VOICE: Moved to '{channel_to_join.name}'.")
                 should_start_play_task = True
            elif not current_vc.is_playing(): # Already in correct channel, just need to start playing if idle
                 bot_logger.debug(f"VOICE: Bot already in '{channel_to_join.name}' and idle. Will start queue.")
                 should_start_play_task = True
            else: # Already in correct channel and playing
                bot_logger.info(f"VOICE: Bot playing in {guild.name}. Join sound queued. Playback deferred.")
                 # Ensure a play task exists if playing but maybe the task ended? Unlikely but safe.
                if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
                     task_name = f"QueueTriggerDeferred_{guild_id}"
                     if guild_sound_queues.get(guild_id):
                         # Cancel timer before starting new task
                         cancel_leave_timer(guild_id, reason="starting deferred play task")
                         guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(guild), name=task_name)
                         bot_logger.debug(f"VOICE: Created deferred play task '{task_name}'.")
                     else:
                         bot_logger.debug(f"VOICE: Deferred task '{task_name}' skipped, queue emptied concurrently.")

        except asyncio.TimeoutError:
            bot_logger.error(f"VOICE: Timeout connecting/moving to '{channel_to_join.name}'. Clearing queue.")
            if guild_id in guild_sound_queues: guild_sound_queues[guild_id].clear()
            current_vc = None
        except discord.errors.ClientException as e:
            bot_logger.warning(f"VOICE: ClientException during connect/move to '{channel_to_join.name}': {e}")
            current_vc = discord.utils.get(bot.voice_clients, guild=guild) # Re-get VC state
        except Exception as e:
            bot_logger.error(f"VOICE: Unexpected error connecting/moving to '{channel_to_join.name}': {e}", exc_info=True)
            if guild_id in guild_sound_queues: guild_sound_queues[guild_id].clear()
            current_vc = None

        if should_start_play_task and current_vc and current_vc.is_connected():
             # Cancel timer just before starting the task if not already cancelled
            cancel_leave_timer(guild_id, reason="starting play task")
            if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
                task_name = f"QueueStart_{guild_id}"
                if guild_sound_queues.get(guild_id):
                    guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(guild), name=task_name)
                    bot_logger.info(f"VOICE: Started play task '{task_name}' for guild {guild_id}.")
                else:
                    bot_logger.debug(f"VOICE: Start task '{task_name}' skipped, queue emptied concurrently.")
            else:
                 bot_logger.debug(f"VOICE: Play task for {guild_id} already running/scheduled.")
        elif not current_vc or not current_vc.is_connected():
             bot_logger.warning(f"VOICE: Bot could not connect/move to {channel_to_join.name}, cannot start playback task.")
             cancel_leave_timer(guild_id, reason="connection failed") # Ensure timer cleanup


    # --- Handle User LEAVING a channel ---
    elif not member.bot and before.channel and before.channel != after.channel:
        # Check if the user left the channel the bot is currently in
        if vc and vc.is_connected() and vc.channel == before.channel:
            bot_logger.info(f"EVENT: {member.display_name} left bot's channel ({before.channel.name}). Checking if bot is alone.")
            # Check if the bot is now alone *after* this user left
            # Schedule the check slightly delayed to allow Discord state to fully update
            bot.loop.call_later(1.0, lambda: bot.loop.create_task(start_leave_timer(vc)))


    # --- Handle Bot's own state changes ---
    elif member.id == bot.user.id:
        if before.channel and not after.channel:
            # Bot was disconnected (manually or otherwise)
            bot_logger.info(f"EVENT: Bot disconnected from {before.channel.name} in {guild.name}. Cleaning up timers and tasks.")
            cancel_leave_timer(guild_id, reason="bot disconnected")
            # Clean up play task just in case
            if guild_id in guild_play_tasks:
                 play_task = guild_play_tasks.pop(guild_id, None)
                 if play_task and not play_task.done():
                     try: play_task.cancel()
                     except Exception: pass
            # Clear queue
            if guild_id in guild_sound_queues:
                 guild_sound_queues[guild_id].clear()


# --- after_play_handler ---
def after_play_handler(error: Optional[Exception], vc: discord.VoiceClient):
    guild_id = vc.guild.id if vc and vc.guild else None
    if error:
        bot_logger.error(f'PLAYBACK ERROR (In after_play_handler for guild {guild_id}): {error}', exc_info=error)

    if not guild_id or not vc.is_connected():
        bot_logger.warning(f"after_play_handler called with invalid/disconnected vc (Guild ID: {guild_id}). Cleaning up tasks.")
        if guild_id:
            cancel_leave_timer(guild_id, reason="after_play on disconnected VC")
            play_task = guild_play_tasks.pop(guild_id, None)
            if play_task and not play_task.done():
                 try: play_task.cancel()
                 except Exception: pass
                 bot_logger.debug(f"Cancelled lingering play task for disconnected guild {guild_id}.")
        return

    bot_logger.debug(f"Playback finished/errored for {guild_id}. Checking queue and idle state.")

    is_join_queue_empty = guild_id not in guild_sound_queues or not guild_sound_queues[guild_id]

    if not is_join_queue_empty:
        bot_logger.debug(f"AFTER_PLAY: Join queue for {guild_id} not empty. Ensuring task runs.")
        # Cancel any potential leave timer that might have wrongly started
        cancel_leave_timer(guild_id, reason="playback finished, queue not empty")
        if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
             task_name = f"QueueCheckAfterPlay_{guild_id}"
             if guild_sound_queues.get(guild_id): # Double check queue state
                 guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(vc.guild), name=task_name)
                 bot_logger.debug(f"AFTER_PLAY: Scheduled task '{task_name}' for {guild_id}.")
             else:
                 bot_logger.debug(f"AFTER_PLAY: Task '{task_name}' creation skipped, queue emptied concurrently. Triggering idle check.")
                 # Queue became empty, check if timer should start
                 bot.loop.create_task(start_leave_timer(vc))
        else:
             bot_logger.debug(f"AFTER_PLAY: Existing play task found for {guild_id}, letting it continue.")
    else:
         bot_logger.debug(f"AFTER_PLAY: Join queue for {guild_id} is empty. Bot is now idle.")
         # Bot is idle, check if the leave timer should start
         bot.loop.create_task(start_leave_timer(vc))

# --- Helper Function: Check if bot should stay ---
# (should_bot_stay function remains unchanged)
def should_bot_stay(guild_id: int) -> bool:
    settings = guild_settings.get(str(guild_id), {})
    stay = settings.get("stay_in_channel", False)
    bot_logger.debug(f"Checked stay setting for guild {guild_id}: {stay}")
    return stay is True

# --- safe_disconnect ---
# (safe_disconnect updated to always cancel timers)
async def safe_disconnect(vc: Optional[discord.VoiceClient], *, manual_leave: bool = False):
    if not vc or not vc.is_connected():
        return

    guild = vc.guild
    guild_id = guild.id

    # --- ALWAYS cancel leave timer before disconnecting ---
    cancel_leave_timer(guild_id, reason="safe_disconnect called")
    # ---

    # Check if disconnect should be skipped due to 'stay' setting (only if not manual)
    if not manual_leave and should_bot_stay(guild_id):
        bot_logger.debug(f"Disconnect skipped for {guild.name}: 'Stay in channel' is enabled.")
        # Clean up play task if bot is idle but staying
        is_playing_check = vc.is_playing()
        is_join_queue_empty_check = guild_id not in guild_sound_queues or not guild_sound_queues[guild_id]
        if is_join_queue_empty_check and not is_playing_check:
            if guild_id in guild_play_tasks:
                play_task = guild_play_tasks.pop(guild_id, None)
                if play_task:
                    if not play_task.done():
                        try: play_task.cancel()
                        except Exception: pass
                        bot_logger.debug(f"STAY MODE: Cancelled lingering play task for idle bot in {guild_id}.")
                    else:
                        bot_logger.debug(f"STAY MODE: Cleaned up completed play task tracker for idle bot in {guild_id}.")
        return # Don't disconnect

    # Determine if disconnect should happen (manual bypasses checks)
    is_join_queue_empty = guild_id not in guild_sound_queues or not guild_sound_queues[guild_id]
    is_playing = vc.is_playing()
    # Disconnect if manual, OR if queue is empty and not playing (and stay isn't enabled - checked above)
    should_disconnect = manual_leave or (is_join_queue_empty and not is_playing)

    if should_disconnect:
        disconnect_reason = "Manual /leave or auto-timer" if manual_leave else "Idle, queue empty, and stay disabled"
        bot_logger.info(f"DISCONNECT: Conditions met for {guild.name} ({disconnect_reason}). Disconnecting...")
        try:
            if vc.is_playing():
                log_level = logging.WARNING if not manual_leave else logging.DEBUG
                bot_logger.log(log_level, f"DISCONNECT: Called stop() during disconnect for {guild.name} (Manual: {manual_leave}).")
                vc.stop() # This should trigger after_play_handler which handles queue/timer logic again, but safe_disconnect is the final step.

            await vc.disconnect(force=False) # Let discord handle cleanup
            bot_logger.info(f"DISCONNECT: Bot disconnected from '{guild.name}'. (VC state change will trigger further cleanup if needed)")

            # Explicit cleanup of tasks/queues associated with the guild after disconnect command
            if guild_id in guild_play_tasks:
                play_task = guild_play_tasks.pop(guild_id, None)
                if play_task and not play_task.done():
                    try: play_task.cancel()
                    except Exception: pass
                    bot_logger.debug(f"DISCONNECT: Cancelled play task for {guild_id} after disconnect.")
            if guild_id in guild_sound_queues:
                guild_sound_queues[guild_id].clear()
                bot_logger.debug(f"DISCONNECT: Cleared sound queue for {guild_id} after disconnect.")
            # Timer was already cancelled at the start of the function.

        except Exception as e:
            bot_logger.error(f"DISCONNECT ERROR: Failed disconnect from {guild.name}: {e}", exc_info=True)
    else:
         # This case should ideally not be reached often if called correctly,
         # as non-manual calls should be guarded by the stay check earlier.
         bot_logger.debug(f"Disconnect skipped for {guild.name}: Manual={manual_leave}, QueueEmpty={is_join_queue_empty}, Playing={is_playing}, StayEnabled={should_bot_stay(guild_id)}.")


# --- Voice Client Connection/Busy Check Helper ---
# (ensure_voice_client_ready needs to cancel timer on connect/move)
async def _ensure_voice_client_ready(interaction: discord.Interaction, target_channel: discord.VoiceChannel, action_type: str = "Playback") -> Optional[discord.VoiceClient]:
    """Helper to connect/move/check busy status and permissions. Returns VC or None."""
    # Ensure the response function is available based on interaction state
    responder = interaction.followup if interaction.response.is_done() else interaction.edit_original_response

    guild = interaction.guild
    user = interaction.user
    guild_id = guild.id
    log_prefix = f"{action_type.upper()}:"

    if not guild:
        await responder(content="This command must be used in a server.", ephemeral=True)
        return None

    bot_perms = target_channel.permissions_for(guild.me)
    if not bot_perms.connect or not bot_perms.speak:
        await responder(content=f"❌ I don't have permission to Connect or Speak in {target_channel.mention}.", ephemeral=True)
        bot_logger.warning(f"{log_prefix} Missing Connect/Speak perms in {target_channel.name} ({guild.name}).")
        return None

    vc = discord.utils.get(bot.voice_clients, guild=guild)
    try:
        if vc and vc.is_connected():
            # Check if playing FIRST - this is the most common busy state
            if vc.is_playing():
                join_queue_active = guild_id in guild_sound_queues and guild_sound_queues[guild_id]
                msg = "⏳ Bot is currently playing join sounds. Please wait." if join_queue_active else "⏳ Bot is currently playing another sound/TTS. Please wait."
                log_msg = f"{log_prefix} Bot busy ({'join queue' if join_queue_active else 'non-join'}) in {guild.name}, user {user.name}'s request ignored."
                await responder(content=msg, ephemeral=True)
                bot_logger.info(log_msg)
                return None # Indicate busy

            elif vc.channel != target_channel:
                # Allow moving if user is in the target channel OR if stay is disabled
                should_move = (user.voice and user.voice.channel == target_channel) or not should_bot_stay(guild_id)

                if should_move:
                     bot_logger.info(f"{log_prefix} Moving from '{vc.channel.name}' to '{target_channel.name}' for {user.name}.")
                     # Cancel timer before moving
                     cancel_leave_timer(guild_id, reason=f"moving for {action_type}")
                     await vc.move_to(target_channel)
                     bot_logger.info(f"{log_prefix} Moved successfully.")
                else: # Stay enabled, user not in target channel (bot is elsewhere)
                    bot_logger.debug(f"{log_prefix} Not moving from '{vc.channel.name}' to '{target_channel.name}' because stay is enabled and user isn't there.")
                    await responder(content=f"ℹ️ I'm currently staying in {vc.channel.mention}. Please join that channel or disable the stay setting.", ephemeral=True)
                    return None # Indicate wrong channel due to stay mode
            # else: Bot is connected to the right channel and idle - proceed

        else: # Bot not connected
            bot_logger.info(f"{log_prefix} Connecting to '{target_channel.name}' for {user.name}.")
            # Cancel timer before connecting
            cancel_leave_timer(guild_id, reason=f"connecting for {action_type}")
            vc = await target_channel.connect(timeout=30.0, reconnect=True)
            bot_logger.info(f"{log_prefix} Connected successfully.")

        if not vc or not vc.is_connected():
             bot_logger.error(f"{log_prefix} Failed to establish voice client for {target_channel.name} after connect/move attempt.")
             await responder(content="❌ Failed to connect or move to the voice channel.", ephemeral=True)
             return None

        # --- Bot is now connected and idle in the correct channel ---
        # Cancel timer again just to be absolutely sure before returning VC
        cancel_leave_timer(guild_id, reason=f"ensured ready for {action_type}")
        return vc # Success

    except asyncio.TimeoutError:
         await responder(content="❌ Connection to the voice channel timed out.", ephemeral=True)
         bot_logger.error(f"{log_prefix} Connection/Move Timeout in {guild.name} to {target_channel.name}")
         return None
    except discord.errors.ClientException as e:
        msg = "⏳ Bot is busy connecting/disconnecting. Please wait a moment." if "already connect" in str(e).lower() else "❌ Error connecting/moving. Check permissions or try again."
        await responder(content=msg, ephemeral=True)
        bot_logger.warning(f"{log_prefix} Connection/Move ClientException in {guild.name}: {e}")
        return None
    except Exception as e:
        await responder(content="❌ An unexpected error occurred while joining the voice channel.", ephemeral=True)
        bot_logger.error(f"{log_prefix} Connection/Move unexpected error in {guild.name}: {e}", exc_info=True)
        return None


# --- Single Sound Playback Logic (For Files) ---
# (play_single_sound needs timer cancellation on play)
async def play_single_sound(interaction: discord.Interaction, sound_path: str):
    """Connects (if needed), plays a single sound FILE (processed/trimmed), and uses after_play_handler."""
    # Use edit_original_response as commands using this helper defer publicly
    responder = interaction.edit_original_response

    user = interaction.user
    guild = interaction.guild

    if not guild or not isinstance(user, discord.Member) or not user.voice or not user.voice.channel:
        await responder(content="You need to be in a voice channel in this server to use this.")
        return

    target_channel = user.voice.channel
    guild_id = guild.id # Get guild_id for timer cancellation

    if not os.path.exists(sound_path):
         await responder(content="❌ Error: The requested sound file seems to be missing on the server.")
         bot_logger.error(f"SINGLE PLAY: File not found: {sound_path}")
         return

    voice_client = await _ensure_voice_client_ready(interaction, target_channel, action_type="SINGLE PLAY (File)")
    if not voice_client:
        # _ensure_voice_client_ready already sent feedback via responder
        return

    sound_basename = os.path.basename(sound_path)
    bot_logger.info(f"SINGLE PLAY (File): Processing '{sound_basename}' for {user.name}...")
    audio_source = process_audio(sound_path, user.display_name)

    if audio_source:
        # Double-check playing status and cancel timer right before playing
        if voice_client.is_playing():
             bot_logger.warning(f"SINGLE PLAY (File): VC became busy between check and play for {user.name}. Aborting.")
             await responder(content="⏳ Bot became busy just now. Please try again.")
             # Don't start timer here, let after_play_handler from the *other* sound handle it
             return

        try:
            # --- Cancel leave timer before playing ---
            cancel_leave_timer(guild_id, reason="starting single sound playback")
            # ---
            sound_display_name = os.path.splitext(sound_basename)[0]
            bot_logger.info(f"SINGLE PLAYBACK (File): Playing '{sound_display_name}' requested by {user.display_name}...")
            voice_client.play(audio_source, after=lambda e: after_play_handler(e, voice_client))
            # Edit the original deferred response to show the playing message
            await responder(content=f"▶️ Playing `{sound_display_name}` (max {MAX_PLAYBACK_DURATION_MS / 1000}s)...")
        except discord.errors.ClientException as e:
            msg = "❌ Error: Bot is already playing or encountered a client issue."
            await responder(content=msg) # Edit deferred response with error
            bot_logger.error(f"SINGLE PLAYBACK ERROR (File - ClientException): {e}", exc_info=True)
            after_play_handler(e, voice_client) # Trigger handler to check state/timer
        except Exception as e:
            await responder(content="❌ An unexpected error occurred during playback.") # Edit deferred response with error
            bot_logger.error(f"SINGLE PLAYBACK ERROR (File - Unexpected): {e}", exc_info=True)
            after_play_handler(e, voice_client) # Trigger handler to check state/timer
    else:
        await responder(content="❌ Error: Could not process the audio file. It might be corrupted or unsupported.") # Edit deferred response with error
        bot_logger.error(f"SINGLE PLAYBACK (File): Failed to get audio source for '{sound_path}' requested by {user.name}")
        # If processing failed, the bot is idle, trigger timer check
        if voice_client and voice_client.is_connected():
            bot.loop.create_task(start_leave_timer(voice_client))


# --- Helper Functions ---
# (sanitize_filename, _find_sound_path_in_dir, _get_sound_files_from_dir, get_user_sound_files, find_user_sound_path, get_public_sound_files, find_public_sound_path remain unchanged)
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
    # Check both original and sanitized versions for robustness
    for name_variant in [sound_name, sanitize_filename(sound_name)]:
        try:
            # Prioritize preferred extensions
            found_path = None
            for ext in preferred_order:
                for filename in os.listdir(directory):
                    base, file_ext = os.path.splitext(filename)
                    if file_ext.lower() == ext and base.lower() == name_variant.lower():
                         found_path = os.path.join(directory, filename)
                         break # Found preferred match for this variant
                if found_path: break # Stop checking extensions if found
            if found_path: return found_path # Return if found

        except OSError as e:
             bot_logger.error(f"Error listing files in {directory} during find: {e}")
             return None # Error occurred during listing
    return None # Not found after checking both variants and all extensions

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

# --- Autocomplete Helper ---
# (_generic_sound_autocomplete, user_sound_autocomplete, public_sound_autocomplete, tts_voice_autocomplete remain unchanged)
async def _generic_sound_autocomplete(ctx: discord.AutocompleteContext, source_func, *args) -> List[discord.OptionChoice]:
    """Generic autocomplete handler returning OptionChoices from a list function."""
    try:
        sounds = source_func(*args)
        current_value = ctx.value.lower() if ctx.value else ""
        # Filter and sort suggestions
        suggestions = sorted(
            [discord.OptionChoice(name=name, value=name)
             for name in sounds if current_value in name.lower()],
            key=lambda choice: choice.name
        )
        return suggestions[:25] # Discord limit for autocomplete suggestions
    except Exception as e:
         bot_logger.error(f"Error during autocomplete ({source_func.__name__} for user {ctx.interaction.user.id}): {e}", exc_info=True)
         return []

async def user_sound_autocomplete(ctx: discord.AutocompleteContext) -> List[discord.OptionChoice]:
    """Autocomplete for user's personal sounds."""
    return await _generic_sound_autocomplete(ctx, get_user_sound_files, ctx.interaction.user.id)

async def public_sound_autocomplete(ctx: discord.AutocompleteContext) -> List[discord.OptionChoice]:
    """Autocomplete for public sounds."""
    return await _generic_sound_autocomplete(ctx, get_public_sound_files)

async def tts_voice_autocomplete(ctx: discord.AutocompleteContext) -> List[discord.OptionChoice]:
    """Autocomplete for Edge-TTS voices using the FULL list."""
    try:
        current_value = ctx.value.lower() if ctx.value else ""
        # Filter the pre-generated FULL list based on name or value (ID)
        suggestions = [
            choice for choice in FULL_EDGE_TTS_VOICE_CHOICES # Use the full list here
            if current_value in choice.name.lower() or current_value in choice.value.lower()
        ]
        # Return top 25 matches
        return suggestions[:25]
    except Exception as e:
        bot_logger.error(f"Error during TTS voice autocomplete for user {ctx.interaction.user.id}: {e}", exc_info=True)
        return []

# --- File Upload Validation Helper ---
# (_validate_and_save_upload remains unchanged)
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
        bot_logger.warning(f"{log_prefix}: Content-Type '{sound_file.content_type}' for '{sound_file.filename}' (user: {user_id}) not 'audio/*'. Proceeding with caution.")

    # Use a more robust temporary filename in user's sound dir
    temp_save_filename = f"temp_{command_name}_{user_id}_{os.urandom(4).hex()}{file_extension}"
    user_temp_dir = os.path.join(USER_SOUNDS_DIR, str(user_id)) # Temp file inside user's specific dir
    ensure_dir(user_temp_dir) # Ensure user's temp dir exists
    temp_save_path = os.path.join(user_temp_dir, temp_save_filename)

    async def cleanup_temp():
        if os.path.exists(temp_save_path):
            try: os.remove(temp_save_path); bot_logger.debug(f"Cleaned up temp: {temp_save_path}")
            except Exception as del_e: bot_logger.warning(f"Failed temp cleanup '{temp_save_path}': {del_e}")

    try:
        await sound_file.save(temp_save_path)
        bot_logger.info(f"{log_prefix}: Saved temp file for {user_id}: '{temp_save_path}'")

        # Pydub Validation
        try:
            bot_logger.debug(f"{log_prefix}: Pydub decode check: '{temp_save_path}'")
            audio = AudioSegment.from_file(temp_save_path, format=file_extension.strip('.'))
            # Optional: Add duration check here if needed
            # if len(audio) > MAX_UPLOAD_DURATION_MS: ... return False ...
            bot_logger.info(f"{log_prefix}: Pydub validation OK for '{temp_save_path}' (Duration: {len(audio)}ms)")

            # Move validated file to final destination
            try:
                target_dir = os.path.dirname(target_save_path)
                ensure_dir(target_dir) # Ensure final directory exists
                # Use os.replace for atomic move where possible
                os.replace(temp_save_path, target_save_path)
                bot_logger.info(f"{log_prefix}: Final file saved (atomic replace): '{target_save_path}'")
                return True, None # Success

            except OSError as rep_e:
                # Fallback to shutil.move if os.replace fails (e.g., across different filesystems)
                bot_logger.warning(f"{log_prefix}: os.replace failed ('{rep_e}'), trying shutil.move for '{temp_save_path}' -> '{target_save_path}'.")
                try:
                    shutil.move(temp_save_path, target_save_path)
                    bot_logger.info(f"{log_prefix}: Final file saved (fallback move): '{target_save_path}'")
                    return True, None # Success
                except Exception as move_e:
                    bot_logger.error(f"{log_prefix}: FAILED final save (replace: {rep_e}, move: {move_e})", exc_info=True)
                    await cleanup_temp()
                    return False, "❌ Error saving the sound file after validation."

        except CouldntDecodeError as decode_error:
            bot_logger.error(f"{log_prefix}: FAILED (Pydub Decode - user: {user_id}, file: '{sound_file.filename}'): {decode_error}", exc_info=True)
            await cleanup_temp()
            return False, f"❌ **Audio Validation Failed!** Could not process `{sound_file.filename}`. It might be corrupted or in an unsupported format. Ensure FFmpeg is correctly installed and accessible by the bot if needed for this file type ({file_extension})."
        except Exception as validate_e:
            bot_logger.error(f"{log_prefix}: FAILED (Unexpected Pydub check error - user: {user_id}, file: '{sound_file.filename}'): {validate_e}", exc_info=True)
            await cleanup_temp()
            return False, "❌ **Audio Validation Failed!** An unexpected error occurred during audio processing."

    except discord.HTTPException as e:
        bot_logger.error(f"{log_prefix}: Error downloading temp file for {user_id} ('{sound_file.filename}'): {e}", exc_info=True)
        await cleanup_temp()
        return False, "❌ Error downloading the sound file from Discord."
    except Exception as e:
        bot_logger.error(f"{log_prefix}: Unexpected error during initial temp save for {user_id} ('{sound_file.filename}'): {e}", exc_info=True)
        await cleanup_temp()
        return False, "❌ An unexpected server error occurred during file handling."


# --- Slash Commands ---

# === Join Sound Commands ===
# (setjoinsound, removejoinsound remain unchanged)
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
    # Use a consistent naming scheme for join sounds
    final_save_filename = f"joinsound_{user_id_str}{file_extension}"
    final_save_path = os.path.join(SOUNDS_DIR, final_save_filename)

    user_config = user_sound_config.get(user_id_str, {})
    old_config_filename = user_config.get("join_sound")

    success, error_msg = await _validate_and_save_upload(ctx, sound_file, final_save_path, command_name="setjoinsound")

    if success:
        bot_logger.info(f"Join sound validation successful for {author.name}, saved to '{final_save_path}'")
        # Remove old join sound file if it existed and had a different name (e.g., different extension)
        if old_config_filename and old_config_filename != final_save_filename:
            old_path = os.path.join(SOUNDS_DIR, old_config_filename)
            if os.path.exists(old_path):
                try:
                    os.remove(old_path)
                    bot_logger.info(f"Removed previous join sound file: '{old_path}'")
                except Exception as e:
                    bot_logger.warning(f"Could not remove previous join sound file '{old_path}': {e}")

        # Update config
        user_config["join_sound"] = final_save_filename
        user_sound_config[user_id_str] = user_config
        save_config()
        bot_logger.info(f"Updated join sound config for {author.name} to '{final_save_filename}'")
        await ctx.followup.send(f"✅ Success! Your join sound is set to `{sound_file.filename}`.", ephemeral=True)
    else:
        # Send the error message from validation
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

        if not user_config: # Remove user entry if it's now empty
            if user_id_str in user_sound_config: del user_sound_config[user_id_str]
            bot_logger.info(f"Removed empty user config for {author.name} after join sound removal.")
        save_config()

        file_path_to_remove = os.path.join(SOUNDS_DIR, filename_to_remove)
        removed_custom = False

        if os.path.exists(file_path_to_remove):
            try:
                os.remove(file_path_to_remove)
                bot_logger.info(f"Deleted file: '{file_path_to_remove}' (custom join sound)")
                removed_custom = True
            except OSError as e: bot_logger.warning(f"Could not delete file '{file_path_to_remove}': {e}")
        else:
             bot_logger.warning(f"Configured join sound '{filename_to_remove}' not found at '{file_path_to_remove}' during removal.")

        # Clean up potentially orphaned temporary join TTS files (best effort)
        prefix_to_clean = f"tts_join_{user_id_str}"
        cleaned_temp_count = 0
        try:
            for f in os.listdir(SOUNDS_DIR):
                if f.startswith(prefix_to_clean) and f.endswith(".mp3"):
                     path_to_clean = os.path.join(SOUNDS_DIR, f)
                     try:
                         os.remove(path_to_clean)
                         cleaned_temp_count += 1
                         bot_logger.debug(f"Cleaned up old temp join TTS: {f}")
                     except OSError as e_clean:
                         bot_logger.warning(f"Could not clean temp TTS file '{path_to_clean}': {e_clean}")
        except OSError as e_list:
             bot_logger.warning(f"Could not list SOUNDS_DIR for TTS cleanup: {e_list}")

        msg = "🗑️ Custom join sound removed."
        if cleaned_temp_count > 0: msg += f" Cleaned up {cleaned_temp_count} cached join TTS file(s)."
        msg += " Default TTS will now be used for your join message."
        await ctx.followup.send(msg, ephemeral=True)
    else:
        await ctx.followup.send("🤷 You don't have a custom join sound configured.", ephemeral=True)

# === User Command Sound / Soundboard Commands ===
# (uploadsound, mysounds, deletesound, playsound remain unchanged)
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
        await ctx.followup.send("❌ Please provide a valid name containing letters, numbers, or underscores.", ephemeral=True); return
    followup_prefix = f"ℹ️ Name sanitized to `{clean_name}`.\n" if clean_name != name else ""

    file_extension = os.path.splitext(sound_file.filename)[1].lower()
    final_filename = f"{clean_name}{file_extension}"

    if make_public:
        target_dir = PUBLIC_SOUNDS_DIR
        # Check if public sound with this sanitized name already exists
        if find_public_sound_path(clean_name):
            await ctx.followup.send(f"{followup_prefix}❌ A public sound named `{clean_name}` already exists.", ephemeral=True); return
        replacing_personal = False # Cannot replace personal when uploading public
        scope = "public"
    else:
        # Personal sound
        target_dir = os.path.join(USER_SOUNDS_DIR, str(user_id))
        ensure_dir(target_dir) # Ensure user's personal directory exists
        existing_personal_path = find_user_sound_path(user_id, clean_name)
        replacing_personal = existing_personal_path is not None
        # Check personal sound limit only if adding a new one
        if not replacing_personal and len(get_user_sound_files(user_id)) >= MAX_USER_SOUNDS_PER_USER:
             await ctx.followup.send(f"{followup_prefix}❌ You have reached the maximum limit of {MAX_USER_SOUNDS_PER_USER} personal sounds.", ephemeral=True); return
        scope = "personal"

    final_path = os.path.join(target_dir, final_filename)
    success, error_msg = await _validate_and_save_upload(ctx, sound_file, final_path, command_name="uploadsound")

    if success:
        bot_logger.info(f"Sound validation successful for {author.name}, saved to '{final_path}' ({scope})")
        # Handle removing old personal sound if replaced with different extension
        if replacing_personal and not make_public and existing_personal_path:
            # existing_personal_path was captured before validation/save
            if existing_personal_path != final_path and os.path.exists(existing_personal_path):
                 try:
                     os.remove(existing_personal_path)
                     bot_logger.info(f"Removed old personal sound file '{os.path.basename(existing_personal_path)}' for {user_id} due to replacement with different extension.")
                 except Exception as e:
                     bot_logger.warning(f"Could not remove old personal sound file '{existing_personal_path}' during replacement: {e}")

        action = "updated" if replacing_personal and not make_public else "uploaded"
        play_cmd = "playpublic" if make_public else "playsound"
        list_cmd = "publicsounds" if make_public else "mysounds"
        msg = f"{followup_prefix}✅ Success! Sound `{clean_name}` {action} as {scope}.\n"
        msg += f"Use `/{play_cmd} name:{clean_name}`"
        if not make_public: msg += f", `/{list_cmd}`, `/soundpanel`, or `/publishsound name:{clean_name}`."
        else: msg += f" or `/{list_cmd}`."
        await ctx.followup.send(msg, ephemeral=True)
    else:
        # Send validation error message
        await ctx.followup.send(f"{followup_prefix}{error_msg or '❌ An unknown error occurred during validation.'}", ephemeral=True)


@bot.slash_command(name="mysounds", description="Lists your personal uploaded sounds.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def mysounds(ctx: discord.ApplicationContext):
    await ctx.defer(ephemeral=True)
    author = ctx.author
    bot_logger.info(f"COMMAND: /mysounds by {author.name} ({author.id})")
    user_sounds = get_user_sound_files(author.id)

    if not user_sounds:
        await ctx.followup.send("You haven't uploaded any personal sounds yet. Use `/uploadsound`!", ephemeral=True); return

    sorted_sounds = sorted(user_sounds, key=str.lower)
    # Paginate if the list is very long (more robust than simple string joining)
    items_per_page = 20 # Adjust as needed
    pages = []
    current_page_lines = []
    for i, name in enumerate(sorted_sounds):
        current_page_lines.append(f"- `{name}`")
        if (i + 1) % items_per_page == 0 or i == len(sorted_sounds) - 1:
            pages.append("\n".join(current_page_lines))
            current_page_lines = []

    # Create embeds for each page
    embeds = []
    total_sounds = len(sorted_sounds)
    for page_num, page_content in enumerate(pages):
        embed = discord.Embed(
            title=f"{author.display_name}'s Sounds ({total_sounds}/{MAX_USER_SOUNDS_PER_USER})",
            description=f"Use `/playsound`, `/soundpanel`, or `/publishsound`.\n\n{page_content}",
            color=discord.Color.blurple()
        )
        if len(pages) > 1:
            embed.set_footer(text=f"Page {page_num + 1}/{len(pages)} | Use /deletesound to remove.")
        else:
            embed.set_footer(text="Use /deletesound to remove.")
        embeds.append(embed)

    # Send the first page (add pagination buttons later if desired)
    await ctx.followup.send(embed=embeds[0], ephemeral=True)
    # TODO: If len(embeds) > 1, add pagination View


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
    sound_base_name = name # Keep original name for feedback unless found via sanitized
    if not sound_path:
        # Try finding sanitized version if original name fails
        sanitized = sanitize_filename(name)
        if sanitized != name:
            sound_path = find_user_sound_path(user_id, sanitized)
            if sound_path: sound_base_name = sanitized # Use sanitized name if found via that

    if not sound_path:
        await ctx.followup.send(f"❌ Personal sound `{name}` not found. Use `/mysounds` to see your sounds.", ephemeral=True); return

    # Security check: Ensure the path is within the user's directory
    user_dir_abs = os.path.abspath(os.path.join(USER_SOUNDS_DIR, str(user_id)))
    resolved_path_abs = os.path.abspath(sound_path)
    if not resolved_path_abs.startswith(user_dir_abs + os.sep):
         bot_logger.critical(f"CRITICAL SECURITY ALERT: Path traversal attempt in /deletesound. User: {user_id}, Input: '{name}', Resolved Path: '{resolved_path_abs}'")
         await ctx.followup.send("❌ Internal security error preventing deletion.", ephemeral=True); return

    try:
        deleted_filename = os.path.basename(sound_path)
        os.remove(sound_path)
        bot_logger.info(f"Deleted PERSONAL sound '{deleted_filename}' for user {user_id}.")
        await ctx.followup.send(f"🗑️ Personal sound `{sound_base_name}` deleted.", ephemeral=True)
    except OSError as e:
        bot_logger.error(f"Failed to delete personal sound '{sound_path}' for {user_id}: {e}", exc_info=True)
        await ctx.followup.send(f"❌ Failed to delete `{sound_base_name}`: Could not remove file ({type(e).__name__}).", ephemeral=True)
    except Exception as e:
        bot_logger.error(f"Unexpected error deleting personal sound '{sound_path}' for {user_id}: {e}", exc_info=True)
        await ctx.followup.send(f"❌ An unexpected error occurred while deleting `{sound_base_name}`.", ephemeral=True)


@bot.slash_command(name="playsound", description="Plays one of your PERSONAL sounds in your current VC.")
@commands.cooldown(1, 4, commands.BucketType.user)
async def playsound(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Name of the personal sound to play.", required=True, autocomplete=user_sound_autocomplete) # type: ignore
):
    # Defer publicly so the "Playing..." message can be shown later
    await ctx.defer()
    author = ctx.author
    bot_logger.info(f"COMMAND: /playsound by {author.name} ({author.id}), request: '{name}'")

    sound_path = find_user_sound_path(author.id, name)
    display_name = name
    if not sound_path:
        # Try sanitized version if original fails
        sanitized = sanitize_filename(name)
        if sanitized != name:
             sound_path = find_user_sound_path(author.id, sanitized)
             if sound_path: display_name = sanitized # Use sanitized name for display if found that way

    if not sound_path:
        # Edit the deferred response to show the error
        await ctx.edit_original_response(content=f"❌ Personal sound `{name}` not found. Use `/mysounds` or `/soundpanel`."); return

    # Pass the interaction to edit the response later
    await play_single_sound(ctx.interaction, sound_path)


# --- Sound Panel View ---
# (UserSoundboardView and soundpanel command remain unchanged)
class UserSoundboardView(discord.ui.View):
    def __init__(self, user_id: int, *, timeout: Optional[float] = 600.0): # 10 min timeout
        super().__init__(timeout=timeout)
        self.user_id = user_id
        self.message: Optional[discord.InteractionMessage] = None # Store the message object
        self.populate_buttons()

    def populate_buttons(self):
        user_dir = os.path.join(USER_SOUNDS_DIR, str(self.user_id))
        bot_logger.debug(f"Populating panel for user {self.user_id} from: {user_dir}")
        if not os.path.isdir(user_dir):
            # No directory means no sounds, add placeholder
            self.add_item(discord.ui.Button(label="No sounds uploaded yet!", style=discord.ButtonStyle.secondary, disabled=True, custom_id=f"usersb_no_sounds_{self.user_id}"))
            return

        sounds_found = 0
        button_row = 0
        max_buttons_per_row = 5
        max_rows = 5 # Discord limit
        max_buttons_total = max_buttons_per_row * max_rows
        try:
            # List and sort files ignoring case
            files_in_dir = sorted(os.listdir(user_dir), key=str.lower)
        except OSError as e:
            bot_logger.error(f"Error listing user dir '{user_dir}' for panel: {e}")
            self.add_item(discord.ui.Button(label="Error Reading Sounds", style=discord.ButtonStyle.danger, disabled=True, custom_id=f"usersb_error_{self.user_id}"))
            return

        for filename in files_in_dir:
            # Stop if we've hit the absolute max number of components (leave room for other potential items)
            if len(self.children) >= 25:
                bot_logger.warning(f"Max component limit (25) reached for user {self.user_id} panel. Skipping '{filename}'.")
                break

            if sounds_found >= max_buttons_total:
                bot_logger.warning(f"Button limit ({max_buttons_total}) reached for user {self.user_id}. File '{filename}' skipped.")
                # Consider adding a "More..." button if you hit the limit and want pagination
                # self.add_item(discord.ui.Button(label="More...", ...))
                break

            filepath = os.path.join(user_dir, filename)
            if os.path.isfile(filepath):
                base_name, ext = os.path.splitext(filename)
                if ext.lower() in ALLOWED_EXTENSIONS:
                    # Truncate label intelligently to fit button (max 80 chars)
                    label = base_name.replace("_", " ")
                    if len(label) > 78: label = label[:77] + "…"

                    # Use filename with extension in custom_id for reliable path reconstruction
                    custom_id = f"usersb_play:{filename}"
                    if len(custom_id) > 100: # Discord custom_id limit (100 chars)
                        bot_logger.warning(f"Skipping sound '{filename}' for {self.user_id} panel: custom_id too long after prefix.")
                        continue

                    button = discord.ui.Button(label=label, style=discord.ButtonStyle.secondary, custom_id=custom_id, row=button_row)
                    button.callback = self.user_soundboard_button_callback
                    self.add_item(button)
                    sounds_found += 1
                    # Move to next row if current row is full
                    if sounds_found > 0 and sounds_found % max_buttons_per_row == 0:
                        button_row += 1
                        # Stop if max rows reached
                        if button_row >= max_rows:
                            bot_logger.warning(f"Row limit ({max_rows}) reached for user {self.user_id} panel. Skipping remaining files starting with '{filename}'.")
                            break

        if sounds_found == 0:
             bot_logger.info(f"No valid sounds found for panel user {self.user_id} in '{user_dir}'.")
             # Add placeholder if no buttons were added and no error occurred
             if not any(item.custom_id == f"usersb_error_{self.user_id}" for item in self.children):
                 if not self.children: # Only add if view is completely empty
                    self.add_item(discord.ui.Button(label="No sounds uploaded yet!", style=discord.ButtonStyle.secondary, disabled=True, custom_id=f"usersb_no_sounds_{self.user_id}"))

    async def user_soundboard_button_callback(self, interaction: discord.Interaction):
        # Check if the interaction user is the one who requested the panel
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("✋ This is not your sound panel!", ephemeral=True)
            return

        custom_id = interaction.data["custom_id"]
        user = interaction.user # Should be self.user_id
        bot_logger.info(f"USER PANEL: Button '{custom_id}' clicked by {user.name} on panel for {self.user_id}")

        # Defer publicly, play_single_sound will edit the response later
        await interaction.response.defer()

        if not custom_id.startswith("usersb_play:"):
            bot_logger.error(f"Invalid custom_id from user panel: '{custom_id}'")
            await interaction.edit_original_response(content="❌ Internal error: Invalid button."); return

        sound_filename = custom_id.split(":", 1)[1]
        sound_path = os.path.join(USER_SOUNDS_DIR, str(self.user_id), sound_filename)

        # Pass interaction to play_single_sound so it can edit the response
        await play_single_sound(interaction, sound_path)

    async def on_timeout(self):
        if self.message:
            bot_logger.debug(f"User sound panel timed out for {self.user_id} (message: {self.message.id})")
            owner_name = f"User {self.user_id}"
            # Attempt to get display name, handle potential errors gracefully
            try:
                 panel_owner = None
                 if self.message.guild: panel_owner = self.message.guild.get_member(self.user_id)
                 # Fallback to fetching user if not found in guild cache or DM
                 if not panel_owner: panel_owner = await bot.fetch_user(self.user_id)
                 if panel_owner: owner_name = panel_owner.display_name
            except discord.NotFound: bot_logger.warning(f"Could not fetch panel owner {self.user_id} (NotFound) for timeout.")
            except discord.HTTPException as e: bot_logger.warning(f"Could not fetch panel owner {self.user_id} (HTTP {e.status}) for timeout.")
            except Exception as e: bot_logger.warning(f"Could not fetch panel owner {self.user_id} for timeout: {e}")

            # Disable all buttons in the view
            for item in self.children:
                if isinstance(item, discord.ui.Button): item.disabled = True
            try:
                # Edit the original message to show expired state and disabled buttons
                await self.message.edit(content=f"🔊 **{owner_name}'s Personal Panel (Expired)**", view=self)
            except discord.HTTPException as e:
                # Log error if editing fails (e.g., message deleted)
                bot_logger.warning(f"Failed to edit expired panel {self.message.id} for {self.user_id}: {e}")
        else:
            bot_logger.debug(f"User panel timed out for {self.user_id} but no message reference was stored.")


@bot.slash_command(name="soundpanel", description="Displays buttons to play YOUR personal sounds in your VC.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def soundpanel(ctx: discord.ApplicationContext):
    # Defer publicly so the panel is visible
    await ctx.defer()
    author = ctx.author
    bot_logger.info(f"COMMAND: /soundpanel invoked by {author.name} ({author.id})")
    view = UserSoundboardView(user_id=author.id, timeout=600.0) # 10 min timeout

    # Check if any playable buttons were actually added
    has_playable_buttons = any(
        isinstance(item, discord.ui.Button) and not item.disabled and item.custom_id and item.custom_id.startswith("usersb_play:")
        for item in view.children
    )

    if not has_playable_buttons:
         # Check if the "no sounds" or "error" placeholder exists
         is_placeholder = any(
            isinstance(item, discord.ui.Button) and item.disabled and item.custom_id and (item.custom_id.startswith("usersb_no_sounds_") or item.custom_id.startswith("usersb_error_"))
            for item in view.children
         )
         if is_placeholder:
             # Check the specific placeholder message
             no_sounds_msg = "No personal sounds uploaded yet. Use `/uploadsound`!"
             error_msg = "Error loading sounds. Please try again later or contact an admin if the issue persists."
             content = no_sounds_msg if any(item.custom_id.startswith("usersb_no_sounds_") for item in view.children) else error_msg
             await ctx.edit_original_response(content=content, view=None) # Clear view
         else: # Should not happen if populate_buttons works correctly
              await ctx.edit_original_response(content="Could not generate the sound panel. No sounds found or an error occurred.", view=None)
         return

    msg_content = f"🔊 **{author.display_name}'s Personal Sound Panel** - Click to play!"
    try:
        # Send the panel using edit_original_response as we deferred
        message = await ctx.edit_original_response(content=msg_content, view=view)
        # We get InteractionMessage from edit_original_response
        if isinstance(message, discord.InteractionMessage):
             view.message = message # Store the message reference for timeout editing
        else: # Fallback if needed, though edit_original should return InteractionMessage
             bot_logger.warning("edit_original_response did not return InteractionMessage for soundpanel")
             # Fetch manually if type mismatch and message object is available
             if hasattr(message, 'id'):
                 try:
                     fetched_message = await ctx.fetch_message(message.id)
                     if isinstance(fetched_message, discord.InteractionMessage):
                         view.message = fetched_message
                     else:
                         bot_logger.warning(f"Fetched message {message.id} for soundpanel was not InteractionMessage type.")
                         view.message = None # Cannot store non-InteractionMessage for editing later
                 except Exception as fetch_err:
                     bot_logger.error(f"Failed to fetch soundpanel message {message.id}: {fetch_err}")
                     view.message = None
             else: view.message = None


    except Exception as e:
        bot_logger.error(f"Failed to send soundpanel for user {author.id}: {e}", exc_info=True)
        # Try sending an ephemeral error if the public message failed
        try: await ctx.edit_original_response(content="❌ Failed to create the sound panel.", view=None) # Clear view on error
        except Exception: pass # Ignore errors sending the error message

# === Public Sound Commands ===
# (publishsound, removepublic, removepublic_error, publicsounds, playpublic remain unchanged)
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
    base_name = name # Keep original requested name for messages
    target_base = sanitize_filename(name) # Use sanitized name for checks/operations

    if not user_path:
        # If original name not found, try sanitized
        if target_base != name:
            user_path = find_user_sound_path(user_id, target_base)

    if not user_path:
        await ctx.followup.send(f"❌ Personal sound `{name}` not found.", ephemeral=True); return

    source_filename = os.path.basename(user_path)
    # Public filename uses the sanitized base name + original extension
    public_filename = target_base + os.path.splitext(source_filename)[1]
    public_path = os.path.join(PUBLIC_SOUNDS_DIR, public_filename)

    # Check if a public sound with the *sanitized* name already exists
    if find_public_sound_path(target_base):
        await ctx.followup.send(f"❌ A public sound named `{target_base}` already exists.", ephemeral=True); return

    try:
        ensure_dir(PUBLIC_SOUNDS_DIR)
        shutil.copy2(user_path, public_path) # Copy with metadata
        bot_logger.info(f"SOUND PUBLISHED: Copied '{user_path}' to '{public_path}' by {author.name}.")
        await ctx.followup.send(f"✅ Sound `{base_name}` (published as `{target_base}`) is now public!\nUse `/playpublic name:{target_base}`.", ephemeral=True)
    except Exception as e:
        bot_logger.error(f"Failed to copy user sound '{user_path}' to public '{public_path}': {e}", exc_info=True)
        await ctx.followup.send(f"❌ Failed to publish `{base_name}`: An error occurred during copying ({type(e).__name__}).", ephemeral=True)

@bot.slash_command(name="removepublic", description="[Admin Only] Remove a sound from the public collection.")
@commands.has_permissions(manage_guild=True) # Keep guild-level perm check
@commands.cooldown(1, 5, commands.BucketType.guild)
async def removepublic(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Name of the public sound to remove.", required=True, autocomplete=public_sound_autocomplete) # type: ignore
):
    await ctx.defer(ephemeral=True)
    admin = ctx.author
    guild_id_log = ctx.guild.id if ctx.guild else "DM"
    bot_logger.info(f"COMMAND: /removepublic by admin {admin.name} (context guild: {guild_id_log}), target: '{name}'")

    public_path = find_public_sound_path(name)
    base_name = name # Keep original name for messages
    target_base = sanitize_filename(name) # Use sanitized for operations

    if not public_path:
        # Try sanitized if original fails
        if target_base != name:
            public_path = find_public_sound_path(target_base)
            if public_path: base_name = target_base # Update message name if found via sanitized

    if not public_path:
        await ctx.followup.send(f"❌ Public sound `{name}` not found. Use `/publicsounds`.", ephemeral=True); return

    # Security check: Ensure path is within PUBLIC_SOUNDS_DIR
    public_dir_abs = os.path.abspath(PUBLIC_SOUNDS_DIR)
    resolved_path_abs = os.path.abspath(public_path)
    if not resolved_path_abs.startswith(public_dir_abs + os.sep):
         bot_logger.critical(f"CRITICAL SECURITY ALERT: Path traversal attempt in /removepublic. Admin: {admin.id}, Input: '{name}', Resolved Path: '{resolved_path_abs}'")
         await ctx.followup.send("❌ Internal security error preventing deletion.", ephemeral=True); return

    try:
        deleted_filename = os.path.basename(public_path)
        os.remove(public_path)
        bot_logger.info(f"ADMIN ACTION: Deleted public sound '{deleted_filename}' by {admin.name}.")
        await ctx.followup.send(f"🗑️ Public sound `{base_name}` deleted.", ephemeral=True)
    except OSError as e:
        bot_logger.error(f"Admin {admin.name} failed to delete public sound '{public_path}': {e}", exc_info=True)
        await ctx.followup.send(f"❌ Failed to delete public sound `{base_name}`: Could not remove file ({type(e).__name__}).", ephemeral=True)
    except Exception as e:
        bot_logger.error(f"Admin {admin.name} failed unexpectedly deleting public sound '{public_path}': {e}", exc_info=True)
        await ctx.followup.send(f"❌ An unexpected error occurred while deleting public sound `{base_name}`.", ephemeral=True)


@removepublic.error
async def removepublic_error(ctx: discord.ApplicationContext, error: discord.DiscordException):
    if isinstance(error, commands.MissingPermissions):
        bot_logger.warning(f"User {ctx.author.name} tried /removepublic without Manage Guild permission.")
        # Respond ephemerally since the command itself is ephemeral
        await ctx.respond("🚫 You need `Manage Server` permission in this server context to use this command.", ephemeral=True)
    else:
        # Let the global handler deal with other errors like cooldowns
        await on_application_command_error(ctx, error)


@bot.slash_command(name="publicsounds", description="Lists all available public sounds.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def publicsounds(ctx: discord.ApplicationContext):
    await ctx.defer(ephemeral=True)
    bot_logger.info(f"COMMAND: /publicsounds by {ctx.author.name}")
    public_sounds = get_public_sound_files()

    if not public_sounds:
        await ctx.followup.send("No public sounds have been added yet. Admins can use `/publishsound`.", ephemeral=True); return

    sorted_sounds = sorted(public_sounds, key=str.lower)
    # Paginate if needed
    items_per_page = 20
    pages = []
    current_page_lines = []
    for i, name in enumerate(sorted_sounds):
        current_page_lines.append(f"- `{name}`")
        if (i + 1) % items_per_page == 0 or i == len(sorted_sounds) - 1:
            pages.append("\n".join(current_page_lines))
            current_page_lines = []

    embeds = []
    total_sounds = len(sorted_sounds)
    for page_num, page_content in enumerate(pages):
         embed = discord.Embed(
             title=f"📢 Public Sounds ({total_sounds})",
             description=f"Use `/playpublic name:<sound_name>`.\n\n{page_content}",
             color=discord.Color.green()
         )
         footer_text = "Admins use /removepublic."
         if len(pages) > 1: footer_text += f" | Page {page_num + 1}/{len(pages)}"
         embed.set_footer(text=footer_text)
         embeds.append(embed)

    # Send first page
    await ctx.followup.send(embed=embeds[0], ephemeral=True)
    # TODO: Add pagination View if len(embeds) > 1


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
    display_name = name
    if not public_path:
        # Try sanitized if original fails
        sanitized = sanitize_filename(name)
        if sanitized != name:
            public_path = find_public_sound_path(sanitized)
            if public_path: display_name = sanitized # Update display name if found via sanitized

    if not public_path:
        await ctx.edit_original_response(content=f"❌ Public sound `{name}` not found. Use `/publicsounds`."); return

    await play_single_sound(ctx.interaction, public_path)


# === TTS Defaults Commands (Edge-TTS) ===
# (setttsdefaults, removettsdefaults remain unchanged)
@bot.slash_command(name="setttsdefaults", description="Set your preferred default Edge-TTS voice.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def setttsdefaults(
    ctx: discord.ApplicationContext,
    voice: discord.Option(str, description="Your preferred default voice (uses autocomplete).", required=True, autocomplete=tts_voice_autocomplete, choices=CURATED_EDGE_TTS_VOICE_CHOICES), # type: ignore # Choices uses curated list
):
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id_str = str(author.id)
    bot_logger.info(f"COMMAND: /setttsdefaults by {author.name}, voice: {voice}")

    # Validate if the provided voice ID is actually in our FULL list
    valid_choice = False
    voice_display_name = voice # Fallback
    for choice in FULL_EDGE_TTS_VOICE_CHOICES: # Check against FULL list
        if choice.value == voice:
            valid_choice = True
            voice_display_name = choice.name
            break

    if not valid_choice:
        await ctx.followup.send(f"❌ Invalid voice ID provided: `{voice}`. Please choose from the list or use autocomplete.", ephemeral=True)
        return

    user_config = user_sound_config.setdefault(user_id_str, {})
    # Store only voice in tts_defaults
    user_config['tts_defaults'] = {'voice': voice}
    save_config()

    await ctx.followup.send(
        f"✅ TTS default voice updated!\n"
        f"• Voice: **{voice_display_name}** (`{voice}`)\n\n"
        f"This voice will be used for `/tts` when you don't specify one, and for your join message if you haven't set a custom sound.",
        ephemeral=True
    )

@bot.slash_command(name="removettsdefaults", description="Remove your custom TTS voice default.")
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
        if not user_config: # If the user config dict becomes empty
            if user_id_str in user_sound_config: del user_sound_config[user_id_str]
            bot_logger.info(f"Removed empty user config for {author.name}.")
        save_config()

        # Get the display name for the bot's default voice
        default_voice_display = DEFAULT_TTS_VOICE
        for choice in FULL_EDGE_TTS_VOICE_CHOICES: # Check full list for display name
            if choice.value == DEFAULT_TTS_VOICE:
                default_voice_display = choice.name
                break

        await ctx.followup.send(
            f"🗑️ Custom TTS default voice removed.\nBot default voice (**{default_voice_display}** / `{DEFAULT_TTS_VOICE}`) will now be used.",
            ephemeral=True
        )
    else:
        await ctx.followup.send("🤷 No custom TTS defaults configured.", ephemeral=True)

# === TTS Command (Edge-TTS) ===
# (tts command needs timer cancellation on play)
@bot.slash_command(name="tts", description="Make the bot say something using Edge Text-to-Speech.")
@commands.cooldown(1, 6, commands.BucketType.user)
async def tts(
    ctx: discord.ApplicationContext,
    message: discord.Option(str, description=f"Text to speak (max {MAX_TTS_LENGTH} chars).", required=True), # type: ignore
    voice: discord.Option(str, description="Override TTS voice (start typing to search).", required=False, autocomplete=tts_voice_autocomplete, choices=CURATED_EDGE_TTS_VOICE_CHOICES), # type: ignore # Uses curated list for choices
):
    # Defer ephemerally initially, might edit later for confirmation
    await ctx.defer(ephemeral=True)
    user = ctx.author
    guild = ctx.guild
    user_id_str = str(user.id)
    guild_id = guild.id if guild else None # Get guild_id for timer cancellation

    bot_logger.info(f"COMMAND: /tts by {user.name}, voice: {voice}, msg: '{message[:50]}...'")

    if not guild or not isinstance(user, discord.Member) or not user.voice or not user.voice.channel:
        await ctx.followup.send("You must be in a voice channel to use TTS.", ephemeral=True); return
    if len(message) > MAX_TTS_LENGTH:
         await ctx.followup.send(f"❌ Message too long! Max {MAX_TTS_LENGTH} characters.", ephemeral=True); return
    if not message.strip():
         await ctx.followup.send("❌ Please provide some text for the bot to say.", ephemeral=True); return

    target_channel = user.voice.channel
    user_config = user_sound_config.get(user_id_str, {})
    saved_defaults = user_config.get("tts_defaults", {})

    # Determine the final voice to use
    final_voice = voice if voice is not None else saved_defaults.get('voice', DEFAULT_TTS_VOICE)
    voice_source = "explicit" if voice is not None else ("saved" if 'voice' in saved_defaults else "default")

    # Validate the final voice choice against the FULL list
    is_valid_voice = any(choice.value == final_voice for choice in FULL_EDGE_TTS_VOICE_CHOICES)
    if not is_valid_voice:
         bot_logger.warning(f"TTS: Invalid final voice '{final_voice}' selected for {user.name}. Falling back to default '{DEFAULT_TTS_VOICE}'.")
         # Check if the invalid voice came from user input or saved config
         if voice_source == "explicit" or voice_source == "saved":
             await ctx.followup.send(f"❌ Invalid voice ID (`{final_voice}`). Please select a valid voice from the list or use autocomplete.", ephemeral=True)
             return
         else: # Invalid default setting somehow? Use bot default.
             final_voice = DEFAULT_TTS_VOICE


    bot_logger.info(f"TTS Final: voice={final_voice}({voice_source}) for {user.name}")

    audio_source: Optional[discord.PCMAudio] = None
    pcm_fp = io.BytesIO() # For final PCM data after processing

    try:
        # Lets convert some of the message for certain characte types
        # Creating mappings for all styled letters
        styled_to_normal = {
            # Lowercase letters
            "𝓪": "a", "𝓫": "b", "𝓬": "c", "𝓭": "d", "𝓮": "e", "𝓯": "f", "𝓰": "g",
            "𝓱": "h", "𝓲": "i", "𝓳": "j", "𝓴": "k", "𝓵": "l", "𝓶": "m", "𝓷": "n",
            "𝓸": "o", "𝓹": "p", "𝓺": "q", "𝓻": "r", "𝓼": "s", "𝓽": "t", "𝓾": "u",
            "𝓿": "v", "𝔀": "w", "𝔁": "x", "𝔂": "y", "𝔃": "z",

            # Uppercase letters
            "𝓐": "A", "𝓑": "B", "𝓒": "C", "𝓓": "D", "𝓔": "E", "𝓕": "F", "𝓖": "G",
            "𝓗": "H", "𝓘": "I", "𝓙": "J", "𝓚": "K", "𝓛": "L", "𝓜": "M", "𝓝": "N",
            "𝓞": "O", "𝓟": "P", "𝓠": "Q", "𝓡": "R", "𝓢": "S", "𝓣": "T", "𝓤": "U",
            "𝓥": "V", "𝓦": "W", "𝓧": "X", "𝓨": "Y", "𝓩": "Z",
        }

        # Function to convert styled text to normal text
        def convert_to_normal(styled_text):
            return ''.join(styled_to_normal.get(char, char) for char in styled_text)

        print("Before text:", message)
        message = convert_to_normal(message)
        print("Normal text:", message)
        bot_logger.info(f"TTS: Generating audio with Edge-TTS for '{user.name}' (voice={final_voice})")

        # Use edge_tts Communicate to get audio data as bytes
        mp3_bytes_list = []
        communicate = edge_tts.Communicate(message, final_voice)
        async for chunk in communicate.stream():
            if chunk["type"] == "audio":
                mp3_bytes_list.append(chunk["data"])
            elif chunk["type"] == "WordBoundary":
                pass # We don't need word boundaries here

        if not mp3_bytes_list:
            raise ValueError("Edge-TTS generation yielded no audio data.")

        # Combine audio chunks into a single bytes object
        mp3_data = b"".join(mp3_bytes_list)
        if len(mp3_data) == 0:
            raise ValueError("Edge-TTS generation resulted in empty audio data after joining chunks.")

        mp3_fp = io.BytesIO(mp3_data)
        mp3_fp.seek(0)
        bot_logger.debug(f"TTS: MP3 generated in memory ({len(mp3_data)} bytes)")

        # Process with Pydub (Load, Trim, Resample, Export to PCM)
        seg = AudioSegment.from_file(mp3_fp, format="mp3")
        bot_logger.debug(f"TTS: Loaded MP3 into Pydub (duration: {len(seg)}ms)")

        if len(seg) > MAX_PLAYBACK_DURATION_MS:
            bot_logger.info(f"TTS: Trimming audio from {len(seg)}ms to {MAX_PLAYBACK_DURATION_MS}ms.")
            seg = seg[:MAX_PLAYBACK_DURATION_MS]

        # Convert format for Discord
        seg = seg.set_frame_rate(48000).set_channels(2)
        seg.export(pcm_fp, format="s16le") # Export as signed 16-bit little-endian PCM
        pcm_fp.seek(0)

        if pcm_fp.getbuffer().nbytes == 0:
            raise ValueError("Pydub export resulted in empty PCM data.")
        bot_logger.debug(f"TTS: PCM processed in memory ({pcm_fp.getbuffer().nbytes} bytes)")

        audio_source = discord.PCMAudio(pcm_fp) # Create the PCMAudio source
        bot_logger.info(f"TTS: PCMAudio source created successfully for {user.name}.")

    except Exception as e: # Catch errors during TTS generation or Pydub processing
        err_type = type(e).__name__
        msg = f"❌ Error generating/processing TTS ({err_type}). Please check the logs or try a different voice/message."
        # Provide more specific feedback for common issues
        if isinstance(e, FileNotFoundError) and 'ffmpeg' in str(e).lower():
             msg = "❌ Error: FFmpeg is needed for audio processing but wasn't found by the bot."
        elif "trustchain" in str(e).lower() or "ssl" in str(e).lower():
            msg = "❌ TTS Error: Could not establish a secure connection for TTS. Network or certificate issue?"
        elif "voice not found" in str(e).lower(): # Check for edge-tts voice errors
             msg = f"❌ Error: The TTS service reported voice '{final_voice}' not found."
        elif isinstance(e, ValueError) or isinstance(e, RuntimeError):
             msg = f"❌ Error processing TTS audio: {e}"

        await ctx.followup.send(msg, ephemeral=True)
        bot_logger.error(f"TTS: Failed generation/processing for {user.name} (Voice: {final_voice}): {e}", exc_info=True)
        pcm_fp.close() # Ensure buffer is closed on error
        return

    # --- Playback ---
    if not audio_source:
        await ctx.followup.send("❌ Failed to prepare TTS audio source after processing.", ephemeral=True)
        bot_logger.error("TTS: Audio source was None after processing block completed without error.")
        pcm_fp.close()
        return

    # Ensure bot is ready in the voice channel
    voice_client = await _ensure_voice_client_ready(ctx.interaction, target_channel, action_type="TTS")
    if not voice_client:
        pcm_fp.close() # Close buffer if connection failed
        return # Helper already sent feedback

    # Double-check if busy right before playing
    if voice_client.is_playing():
         bot_logger.warning(f"TTS: VC became busy between check and play for {user.name}.")
         await ctx.followup.send("⏳ Bot became busy just now. Please try again.", ephemeral=True)
         # Don't trigger timer here, let the other sound's after_play handle it
         pcm_fp.close() # Close buffer
         return

    try:
        # --- Cancel leave timer before playing ---
        if guild_id: cancel_leave_timer(guild_id, reason="starting TTS playback")
        # ---

        bot_logger.info(f"TTS PLAYBACK: Playing TTS requested by {user.display_name}...")

        # Define the 'after' callback to include closing the BytesIO buffer
        def tts_after_play(error: Optional[Exception]):
            bot_logger.debug("TTS after_play callback initiated.")
            try:
                pcm_fp.close() # Close the PCM buffer
                bot_logger.debug("TTS PCM buffer closed successfully.")
            except Exception as close_err:
                bot_logger.error(f"Error closing TTS PCM buffer: {close_err}")
            # Make sure vc is still valid before calling standard handler
            current_vc = discord.utils.get(bot.voice_clients, guild=voice_client.guild)
            if current_vc and current_vc.is_connected():
                 after_play_handler(error, current_vc) # Call the standard handler
            elif voice_client:
                 bot_logger.warning(f"TTS after_play: VC disconnected before handler could run for guild {voice_client.guild.id}")

        voice_client.play(audio_source, after=tts_after_play)

        # Provide confirmation to the user (ephemeral)
        voice_display_name = final_voice # Fallback
        for choice in FULL_EDGE_TTS_VOICE_CHOICES: # Find display name from full list
            if choice.value == final_voice: voice_display_name = choice.name; break
        display_msg = message[:150] + ('...' if len(message) > 150 else '')
        await ctx.followup.send(f"🗣️ Now saying with **{voice_display_name}** (max {MAX_PLAYBACK_DURATION_MS/1000}s): \"{display_msg}\"", ephemeral=True)

    except discord.errors.ClientException as e:
        msg = "❌ Error: Bot is already playing or encountered a client issue during playback."
        await ctx.followup.send(msg, ephemeral=True)
        bot_logger.error(f"TTS PLAYBACK ERROR (ClientException): {e}", exc_info=True)
        tts_after_play(e) # Call cleanup manually on immediate error
    except Exception as e:
        await ctx.followup.send("❌ An unexpected error occurred during TTS playback.", ephemeral=True)
        bot_logger.error(f"TTS PLAYBACK ERROR (Unexpected): {e}", exc_info=True)
        tts_after_play(e) # Call cleanup manually on immediate error


# === Stay/Leave Commands ===

# (togglestay updated to interact with timer)
@bot.slash_command(name="togglestay", description="[Admin Only] Toggle whether the bot stays in VC when idle.")
@commands.has_permissions(manage_guild=True)
@commands.cooldown(1, 5, commands.BucketType.guild)
async def togglestay(ctx: discord.ApplicationContext):
    """Toggles the 'stay_in_channel' setting for the current guild."""
    await ctx.defer(ephemeral=True)
    if not ctx.guild_id: # Should not happen with guild perm check, but safeguard
         await ctx.followup.send("This command can only be used in a server.", ephemeral=True)
         return

    guild_id_str = str(ctx.guild_id)
    guild_id = ctx.guild_id
    admin = ctx.author
    bot_logger.info(f"COMMAND: /togglestay by admin {admin.name} ({admin.id}) in guild {guild_id_str}")

    current_setting = guild_settings.get(guild_id_str, {}).get("stay_in_channel", False)
    new_setting = not current_setting

    guild_settings.setdefault(guild_id_str, {})['stay_in_channel'] = new_setting
    save_guild_settings()

    status_message = "ENABLED ✅ (Bot will stay in VC when idle)" if new_setting else "DISABLED ❌ (Bot will leave VC after being idle and alone)"
    await ctx.followup.send(f"Bot 'Stay in Channel' feature is now **{status_message}** for this server.", ephemeral=True)
    bot_logger.info(f"Guild {guild_id_str} 'stay_in_channel' set to {new_setting} by {admin.name}")

    # --- Timer Interaction ---
    vc = discord.utils.get(bot.voice_clients, guild__id=guild_id)
    if vc and vc.is_connected():
        if new_setting:
            # Stay enabled: Cancel any active leave timer
            cancel_leave_timer(guild_id, reason="togglestay enabled")
        else:
            # Stay disabled: If bot is idle and alone, start the leave timer now
            if not vc.is_playing() and is_bot_alone(vc):
                 bot_logger.info(f"TOGGLESTAY: Stay disabled, bot is idle and alone. Triggering leave timer check.")
                 await start_leave_timer(vc) # Use await here as it might modify state
            elif vc.is_playing():
                 bot_logger.debug("TOGGLESTAY: Stay disabled, but bot playing. Timer check will happen after play.")
            else: # Not playing, not alone
                bot_logger.debug("TOGGLESTAY: Stay disabled, but bot not alone. Timer check will happen if last user leaves.")


@togglestay.error
async def togglestay_error(ctx: discord.ApplicationContext, error: discord.DiscordException):
    """Error handler specifically for /togglestay permissions."""
    if isinstance(error, commands.MissingPermissions):
        bot_logger.warning(f"User {ctx.author.name} tried /togglestay without Manage Guild permission.")
        # Respond ephemerally as command is ephemeral
        await ctx.respond("🚫 You need the `Manage Server` permission to use this command.", ephemeral=True)
    elif isinstance(error, commands.CommandOnCooldown):
         await ctx.respond(f"⏳ This command is on cooldown. Try again in {error.retry_after:.1f}s.", ephemeral=True)
    else:
        await on_application_command_error(ctx, error)


# (leave command remains unchanged, relies on safe_disconnect to cancel timers)
@bot.slash_command(name="leave", description="Make the bot leave its current voice channel.")
@commands.cooldown(1, 5, commands.BucketType.user)
async def leave(ctx: discord.ApplicationContext):
    """Forces the bot to leave the voice channel in the current guild."""
    await ctx.defer(ephemeral=True)
    guild = ctx.guild
    user = ctx.author

    if not guild:
        await ctx.followup.send("This command must be used in a server.", ephemeral=True)
        return

    bot_logger.info(f"COMMAND: /leave invoked by {user.name} ({user.id}) in guild {guild.id}")

    vc = discord.utils.get(bot.voice_clients, guild=guild)

    if vc and vc.is_connected():
        bot_logger.info(f"LEAVE: Manually disconnecting from {vc.channel.name} in {guild.name}...")
        await safe_disconnect(vc, manual_leave=True) # manual_leave=True bypasses stay check and cancels timer
        await ctx.followup.send("👋 Leaving the voice channel.", ephemeral=True)
    else:
        bot_logger.info(f"LEAVE: Request by {user.name}, but bot not connected in {guild.name}.")
        await ctx.followup.send("🤷 I'm not currently in a voice channel in this server.", ephemeral=True)


# --- Error Handler for Application Commands ---
# (on_application_command_error remains unchanged)
@bot.event
async def on_application_command_error(ctx: discord.ApplicationContext, error: discord.DiscordException):
    """Global handler for slash command errors."""
    # Extract command name safely
    cmd_name = "Unknown Command"
    invoked_with = ctx.invoked_with if hasattr(ctx, 'invoked_with') else None
    if ctx.command:
        cmd_name = ctx.command.qualified_name
    elif hasattr(ctx.interaction, 'custom_id') and ctx.interaction.custom_id: # Handle component interactions
        cmd_name = f"Component ({ctx.interaction.custom_id[:30]}...)"
        invoked_with = ctx.interaction.custom_id # Use custom_id for invoked_with for components

    user_name = f"{ctx.author.name}({ctx.author.id})" if ctx.author else "Unknown User"
    guild_name = f"{ctx.guild.name}({ctx.guild.id})" if ctx.guild else "DM"
    log_prefix = f"CMD ERROR (/{invoked_with or cmd_name}, user: {user_name}, guild: {guild_name}):"

    async def send_error_response(message: str, log_level=logging.WARNING):
        # Avoid logging CommandNotFound specifically, as it's common
        if not isinstance(error, commands.CommandNotFound):
             bot_logger.log(log_level, f"{log_prefix} {message} (ErrType: {type(error).__name__}, Details: {error})")

        try:

            # Use followup if already deferred/responded, otherwise respond
            if ctx.interaction.response.is_done():
                await ctx.followup.send(message, ephemeral=True)
            else:
                 # We should respond here, not defer again in the error handler
                 await ctx.respond(message, ephemeral=True)

        except discord.NotFound:
            bot_logger.warning(f"{log_prefix} Interaction not found (potentially deleted/expired) while sending error response.")
        except discord.Forbidden:
            bot_logger.error(f"{log_prefix} Missing permissions to send error response in channel {ctx.channel_id}.")
        except discord.InteractionResponded:
             bot_logger.warning(f"{log_prefix} Interaction already responded to when trying to send error.")
        except Exception as e_resp:
            bot_logger.error(f"{log_prefix} Unexpected error sending error response: {e_resp}", exc_info=e_resp)

    if isinstance(error, commands.CommandOnCooldown):
        await send_error_response(f"⏳ Command on cooldown. Please wait {error.retry_after:.1f} seconds.")
    elif isinstance(error, commands.MissingPermissions):
        perms = ', '.join(f"`{p}`" for p in error.missing_permissions)
        await send_error_response(f"🚫 You lack the required permissions: {perms}", log_level=logging.WARNING)
    elif isinstance(error, commands.BotMissingPermissions):
        perms = ', '.join(f"`{p}`" for p in error.missing_permissions)
        await send_error_response(f"🚫 I lack the required permissions: {perms}. Please check my role settings.", log_level=logging.ERROR)
    elif isinstance(error, commands.CheckFailure): # General check failure (like permissions)
        # More specific CheckFailures might be handled above, this is a fallback
        await send_error_response("🚫 You do not have permission to use this command or perform this action.")
    # Handle ApplicationCommandInvokeError separately for better tracing
    elif isinstance(error, discord.errors.ApplicationCommandInvokeError):
        original = error.original
        bot_logger.error(f"{log_prefix} An error occurred within the command code itself.", exc_info=original) # Log the *original* exception trace
        user_msg = "❌ An internal error occurred while running the command. The developers have been notified (check logs)."
        # Add specific messages for known critical internal errors
        if isinstance(original, FileNotFoundError) and ('ffmpeg' in str(original).lower() or 'ffprobe' in str(original).lower()):
             user_msg = "❌ Internal Error: FFmpeg/FFprobe (required for audio) was not found by the bot."
        elif isinstance(original, CouldntDecodeError):
             user_msg = "❌ Internal Error: Failed to decode an audio file. It might be corrupted or unsupported."
        elif isinstance(original, discord.errors.Forbidden):
             user_msg = f"❌ Internal Error: I encountered a permission issue ({original.text}). Please check my role permissions."
        elif "edge_tts" in str(type(original)): # Basic check for edge-tts related exceptions
             user_msg = f"❌ Internal TTS Error: Failed during text-to-speech generation ({type(original).__name__}). Check bot logs for details."
        # Add other specific checks as needed...

        await send_error_response(user_msg, log_level=logging.ERROR)
    elif isinstance(error, discord.errors.InteractionResponded):
         bot_logger.warning(f"{log_prefix} Interaction already responded to. Error: {error}")
         # Don't try to respond again
    elif isinstance(error, discord.errors.NotFound):
         bot_logger.warning(f"{log_prefix} Interaction or message not found (possibly deleted/expired). Error: {error}")
         # Can't respond if the interaction is gone
    elif isinstance(error, commands.CommandNotFound):
         # Usually safe to ignore logging, but can respond ephemerally if desired
         # await send_error_response(f"❓ Unknown command invoked.", log_level=logging.DEBUG)
         pass # Ignore CommandNotFound errors
    else:
        # Catch-all for other discord.DiscordException types
        bot_logger.error(f"{log_prefix} An unexpected Discord API or library error occurred: {error}", exc_info=error)
        await send_error_response(f"❌ An unexpected error occurred ({type(error).__name__}). If this persists, please contact support.", log_level=logging.ERROR)


# --- Run the Bot ---
if __name__ == "__main__":
    if not PYDUB_AVAILABLE:
        bot_logger.critical("Pydub library missing or failed to import. Install: pip install pydub ffmpeg")
        exit(1)
    if not EDGE_TTS_AVAILABLE:
        bot_logger.critical("edge-tts library missing or failed to import. Install: pip install edge-tts")
        exit(1)
    if not BOT_TOKEN:
        bot_logger.critical("BOT_TOKEN missing in environment variables or .env file.")
        exit(1)

    # Opus Loading Check (Recommended)
    opus_loaded = discord.opus.is_loaded()
    if not opus_loaded:
        bot_logger.warning("Default Opus load failed. Ensure libopus is installed and accessible by the bot process (check PATH or library paths).")
        # Explicitly try loading opus if needed (replace 'opus' with the actual library name/path if necessary)
        # Example paths (adjust for your system):
        # opus_paths = ['opus', '/usr/lib/libopus.so.0', 'libopus-0.x64.dll', 'libopus-0.x86.dll']
        # for opus_path in opus_paths:
        #     try:
        #         discord.opus.load_opus(opus_path)
        #         opus_loaded = discord.opus.is_loaded()
        #         if opus_loaded:
        #             bot_logger.info(f"Opus loaded successfully from: {opus_path}")
        #             break
        #     except discord.OpusNotLoaded:
        #         pass # Try next path
        #     except Exception as e:
        #         bot_logger.error(f"Error trying to load opus from {opus_path}: {e}")

        if not discord.opus.is_loaded(): # Check again after attempts
             bot_logger.critical("CRITICAL: Opus library failed to load even after explicit attempts. Voice functionality WILL NOT WORK.")
             # Consider exiting if Opus is mandatory: exit(1)
    else:
         bot_logger.info("Opus library loaded successfully.")

    try:
        bot_logger.info("Attempting bot startup...")
        bot.run(BOT_TOKEN)
    except discord.errors.LoginFailure:
        bot_logger.critical("CRITICAL STARTUP ERROR: Login Failure - Invalid BOT_TOKEN provided.")
    except discord.errors.PrivilegedIntentsRequired as e:
        bot_logger.critical(f"CRITICAL STARTUP ERROR: Missing Privileged Intents: {e}. Please enable required intents (like Members and Voice State) in the Discord Developer Portal.")
    except Exception as e:
        log_level = logging.CRITICAL if not opus_loaded and "opus" in str(e).lower() else logging.ERROR
        bot_logger.log(log_level, f"FATAL RUNTIME ERROR during bot execution: {e}", exc_info=True)
        exit(1)