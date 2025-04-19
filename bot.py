# -*- coding: utf-8 -*-
from typing import List, Optional, Tuple, Dict, Any, Coroutine, Callable
import discord
from discord.ext import commands
import os
import json
import asyncio
import logging
import io # Required for BytesIO
import math # For checking infinite values in dBFS
from collections import deque # Efficient queue structure
import re # For cleaning filenames and potentially other text processing
from typing import List, Optional, Tuple, Dict, Any, Coroutine # For type hinting
import shutil # For copying/moving files
import unicodedata # For advanced character normalization
import sys, struct

# Import edge-tts
try:
    import edge_tts
    EDGE_TTS_AVAILABLE = True
except ImportError:
    # Basic logging config for critical failures if full setup hasn't happened
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
SOUNDS_DIR = "sounds" # For join sounds AND temporary TTS storage
USER_SOUNDS_DIR = "usersounds"
PUBLIC_SOUNDS_DIR = "publicsounds"
CONFIG_FILE = "user_sounds.json"
GUILD_SETTINGS_FILE = "guild_settings.json"
TARGET_LOUDNESS_DBFS = -14.0
MAX_USER_SOUND_SIZE_MB = 5
MAX_USER_SOUNDS_PER_USER = 25
ALLOWED_EXTENSIONS = ['.mp3', '.wav', '.ogg', '.m4a', '.aac']
MAX_TTS_LENGTH = 350 # Max characters for TTS command (before spacing)
DEFAULT_TTS_VOICE = "en-US-JennyNeural" # Bot's default Edge-TTS voice
MAX_PLAYBACK_DURATION_MS = 10 * 1000 # Max duration in milliseconds (10 seconds)
# --- NEW: Auto Leave Configuration ---
AUTO_LEAVE_TIMEOUT_SECONDS = 4 * 60 * 60 # 4 hours in seconds
# --- End Configuration ---


# --- Define FULL List of Voices (For Autocomplete) ---
# List of all available voice IDs extracted from `edge-tts --list-voices`
# (Ensure this list is kept up-to-date if Edge TTS adds/removes voices)
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
         # Add logging if a logger is available here
         # bot_logger.warning(f"Curated voice ID '{voice_id}' not found in generated FULL_EDGE_TTS_VOICE_CHOICES list during setup.")

# Sort the curated list as well
CURATED_EDGE_TTS_VOICE_CHOICES.sort(key=lambda x: x.name)
# --- End Voice List Setup ---


# --- Helper Function for Stylized Text NORMALIZATION ---
# Mapping from common stylized characters to normal ASCII/Latin characters
# Add more mappings if you encounter other styles
STYLED_TO_NORMAL_MAP = {
    # Script Capitals (Example: 𝓐𝓑𝓒)
    '𝓐': 'A', '𝓑': 'B', '𝓒': 'C', '𝓓': 'D', '𝓔': 'E', '𝓕': 'F', '𝓖': 'G',
    '𝓗': 'H', '𝓘': 'I', '𝓙': 'J', '𝓚': 'K', '𝓛': 'L', '𝓜': 'M', '𝓝': 'N',
    '𝓞': 'O', '𝓟': 'P', '𝓠': 'Q', '𝓡': 'R', '𝓢': 'S', '𝓣': 'T', '𝓤': 'U',
    '𝓥': 'V', '𝓦': 'W', '𝓧': 'X', '𝓨': 'Y', '𝓩': 'Z',
    # Script Lowercase (Example: 𝓪𝓫𝓬)
    '𝓪': 'a', '𝓫': 'b', '𝓬': 'c', '𝓭': 'd', '𝓮': 'e', '𝓯': 'f', '𝓰': 'g',
    '𝓱': 'h', '𝓲': 'i', '𝓳': 'j', '𝓴': 'k', '𝓵': 'l', '𝓶': 'm', '𝓷': 'n',
    '𝓸': 'o', '𝓹': 'p', '𝓺': 'q', '𝓻': 'r', '𝓼': 's', '𝓽': 't', '𝓾': 'u',
    '𝓿': 'v', '𝔀': 'w', '𝔁': 'x', '𝔂': 'y', '𝔃': 'z',
    # Bold Capitals (Example: 𝐀𝐁𝐂) - Often used
    '𝐀': 'A', '𝐁': 'B', '𝐂': 'C', '𝐃': 'D', '𝐄': 'E', '𝐅': 'F', '𝐆': 'G',
    '𝐇': 'H', '𝐈': 'I', '𝐉': 'J', '𝐊': 'K', '𝐋': 'L', '𝐌': 'M', '𝐍': 'N',
    '𝐎': 'O', '𝐏': 'P', '𝐐': 'Q', '𝐑': 'R', '𝐒': 'S', '𝐓': 'T', '𝐔': 'U',
    '𝐕': 'V', '𝐖': 'W', '𝐗': 'X', '𝐘': 'Y', '𝐙': 'Z',
    # Bold Lowercase (Example: 𝐚𝐛𝐜) - Often used
    '𝐚': 'a', '𝐛': 'b', '𝐜': 'c', '𝐝': 'd', '𝐞': 'e', '𝐟': 'f', '𝐠': 'g',
    '𝐡': 'h', '𝐢': 'i', '𝐣': 'j', '𝐤': 'k', '𝐥': 'l', '𝐦': 'm', '𝐧': 'n',
    '𝐨': 'o', '𝐩': 'p', '𝐪': 'q', '𝐫': 'r', '𝐬': 's', '𝐭': 't', '𝐮': 'u',
    '𝐯': 'v', '𝐰': 'w', '𝐱': 'x', '𝐲': 'y', '𝐳': 'z',
    # Italic Capitals (Example: 𝘈𝘉𝘊)
    '𝘈': 'A', '𝘉': 'B', '𝘊': 'C', '𝘋': 'D', '𝘌': 'E', '𝘍': 'F', '𝘎': 'G',
    '𝘏': 'H', '𝘐': 'I', '𝘑': 'J', '𝘒': 'K', '𝘓': 'L', '𝘔': 'M', '𝘕': 'N',
    '𝘖': 'O', '𝘗': 'P', '𝘘': 'Q', '𝘙': 'R', '𝘚': 'S', '𝘛': 'T', '𝘜': 'U',
    '𝘝': 'V', '𝘞': 'W', '𝘟': 'X', '𝘠': 'Y', '𝘡': 'Z',
    # Italic Lowercase (Example: 𝘢𝘣𝘤)
    '𝘢': 'a', '𝘣': 'b', '𝘤': 'c', '𝘥': 'd', '𝘦': 'e', '𝘧': 'f', '𝘨': 'g',
    '𝘩': 'h', '𝘪': 'i', '𝘫': 'j', '𝘬': 'k', '𝘭': 'l', '𝘮': 'm', '𝘯': 'n',
    '𝘰': 'o', '𝘱': 'p', '𝘲': 'q', '𝘳': 'r', '𝘴': 's', '𝘵': 't', '𝘶': 'u',
    '𝘷': 'v', '𝘸': 'w', '𝘹': 'x', '𝘺': 'y', '𝘻': 'z',
    # Sans-serif Bold Capitals (Example: 𝗔𝗕𝗖)
    '𝗔': 'A', '𝗕': 'B', '𝗖': 'C', '𝗗': 'D', '𝗘': 'E', '𝗙': 'F', '𝗚': 'G',
    '𝗛': 'H', '𝗜': 'I', '𝗝': 'J', '𝗞': 'K', '𝗟': 'L', '𝗠': 'M', '𝗡': 'N',
    '𝗢': 'O', '𝗣': 'P', '𝗤': 'Q', '𝗥': 'R', '𝗦': 'S', '𝗧': 'T', '𝗨': 'U',
    '𝗩': 'V', '𝗪': 'W', '𝗫': 'X', '𝗬': 'Y', '𝗭': 'Z',
    # Sans-serif Bold Lowercase (Example: 𝗮𝗯𝗰)
    '𝗮': 'a', '𝗯': 'b', '𝗰': 'c', '𝗱': 'd', '𝗲': 'e', '𝗳': 'f', '𝗴': 'g',
    '𝗵': 'h', '𝗶': 'i', '𝗷': 'j', '𝗸': 'k', '𝗹': 'l', '𝗺': 'm', '𝗻': 'n',
    '𝗼': 'o', '𝗽': 'p', '𝗾': 'q', '𝗿': 'r', '𝘀': 's', '𝘁': 't', '𝘂': 'u',
    '𝘃': 'v', '𝘄': 'w', '𝘅': 'x', '𝘆': 'y', '𝘇': 'z',
    # Circled letters (Example: ⓒ)
    'ⓐ': 'a', 'ⓑ': 'b', 'ⓒ': 'c', 'ⓓ': 'd', 'ⓔ': 'e', 'ⓕ': 'f', 'ⓖ': 'g',
    'ⓗ': 'h', 'ⓘ': 'i', 'ⓙ': 'j', 'ⓚ': 'k', 'ⓛ': 'l', 'ⓜ': 'm', 'ⓝ': 'n',
    'ⓞ': 'o', 'ⓟ': 'p', 'ⓠ': 'q', 'ⓡ': 'r', 'ⓢ': 's', 'ⓣ': 't', 'ⓤ': 'u',
    'ⓥ': 'v', 'ⓦ': 'w', 'ⓧ': 'x', 'ⓨ': 'y', 'ⓩ': 'z',
    'Ⓐ': 'A', 'Ⓑ': 'B', 'Ⓒ': 'C', 'Ⓓ': 'D', 'Ⓔ': 'E', 'Ⓕ': 'F', 'Ⓖ': 'G',
    'Ⓗ': 'H', 'Ⓘ': 'I', 'Ⓙ': 'J', 'Ⓚ': 'K', 'Ⓛ': 'L', 'Ⓜ': 'M', 'Ⓝ': 'N',
    'Ⓞ': 'O', 'Ⓟ': 'P', 'Ⓠ': 'Q', 'Ⓡ': 'R', 'Ⓢ': 'S', 'Ⓣ': 'T', 'Ⓤ': 'U',
    'Ⓥ': 'V', 'Ⓦ': 'W', 'Ⓧ': 'X', 'Ⓨ': 'Y', 'Ⓩ': 'Z',
    # Fraktur Capitals (Example: 𝕬𝕭𝕮)
    '𝕬': 'A', '𝕭': 'B', '𝕮': 'C', '𝕯': 'D', '𝕰': 'E', '𝕱': 'F', '𝕲': 'G',
    '𝕳': 'H', '𝕴': 'I', '𝕵': 'J', '𝕶': 'K', '𝕷': 'L', '𝕸': 'M', '𝕹': 'N',
    '𝕺': 'O', '𝕻': 'P', '𝕼': 'Q', '𝕽': 'R', '𝕾': 'S', '𝕿': 'T', '𝖀': 'U',
    '𝖁': 'V', '𝖂': 'W', '𝖃': 'X', '𝖄': 'Y', '𝖅': 'Z',
    # Fraktur Lowercase (Example: 𝖆𝖇𝖈)
    '𝖆': 'a', '𝖇': 'b', '𝖈': 'c', '𝖉': 'd', '𝖊': 'e', '𝖋': 'f', '𝖌': 'g',
    '𝖍': 'h', '𝖎': 'i', '𝖏': 'j', '𝖐': 'k', '𝖑': 'l', '𝖒': 'm', '𝖓': 'n',
    '𝖔': 'o', '𝖕': 'p', '𝖖': 'q', '𝖗': 'r', '𝖘': 's', '𝖙': 't', '𝖚': 'u',
    '𝖛': 'v', '𝖜': 'w', '𝖝': 'x', '𝖞': 'y', '𝖟': 'z',
    # Subscript / Superscript Numbers (Map to normal numbers)
    '₀': '0', '₁': '1', '₂': '2', '₃': '3', '₄': '4', '₅': '5', '₆': '6', '₇': '7', '₈': '8', '₉': '9',
    '⁰': '0', '¹': '1', '²': '2', '³': '3', '⁴': '4', '⁵': '5', '⁶': '6', '⁷': '7', '⁸': '8', '⁹': '9',
    # Specific Characters from Example Name
    '₵': 'C', # CENT SIGN mapped to C
    'г': 'r', # CYRILLIC SMALL LETTER GHE mapped to r
    'ђ': 'h', # CYRILLIC SMALL LETTER DJE mapped to h
    '†': '',  # DAGGER mapped to empty string (skip)
    '⚰': '',  # COFFIN mapped to empty string (skip)
    '𖤐': '',  # PENTAGRAM mapped to empty string (skip)
    # Handling Combining Characters (like in ђ̥ͦ) - Simple removal (mapping to empty string)
    # Common diacritics / combining marks from Unicode block U+0300 to U+036F
    '\u0300': '', '\u0301': '', '\u0302': '', '\u0303': '', '\u0304': '', '\u0305': '', '\u0306': '', '\u0307': '', '\u0308': '', '\u0309': '', '\u030A': '', '\u030B': '', '\u030C': '', '\u030D': '', '\u030E': '', '\u030F': '',
    '\u0310': '', '\u0311': '', '\u0312': '', '\u0313': '', '\u0314': '', '\u0315': '', '\u0316': '', '\u0317': '', '\u0318': '', '\u0319': '', '\u031A': '', '\u031B': '', '\u031C': '', '\u031D': '', '\u031E': '', '\u031F': '',
    '\u0320': '', '\u0321': '', '\u0322': '', '\u0323': '', '\u0324': '', '\u0325': '', '\u0326': '', '\u0327': '', '\u0328': '', '\u0329': '', '\u032A': '', '\u032B': '', '\u032C': '', '\u032D': '', '\u032E': '', '\u032F': '',
    '\u0330': '', '\u0331': '', '\u0332': '', '\u0333': '', '\u0334': '', '\u0335': '', '\u0336': '', '\u0337': '', '\u0338': '', '\u0339': '', '\u033A': '', '\u033B': '', '\u033C': '', '\u033D': '', '\u033E': '', '\u033F': '',
    '\u0340': '', '\u0341': '', '\u0342': '', '\u0343': '', '\u0344': '', '\u0345': '', '\u0346': '', '\u0347': '', '\u0348': '', '\u0349': '', '\u034A': '', '\u034B': '', '\u034C': '', '\u034D': '', '\u034E': '', '\u034F': '',
    '\u0350': '', '\u0351': '', '\u0352': '', '\u0353': '', '\u0354': '', '\u0355': '', '\u0356': '', '\u0357': '', '\u0358': '', '\u0359': '', '\u035A': '', '\u035B': '', '\u035C': '', '\u035D': '', '\u035E': '', '\u035F': '',
    '\u0360': '', '\u0361': '', '\u0362': '', '\u0363': '', '\u0364': '', '\u0365': '', '\u0366': '', '\u0367': '', '\u0368': '', '\u0369': '', '\u036A': '', '\u036B': '', '\u036C': '', '\u036D': '', '\u036E': '', '\u036F': '',
    # Add other specific mappings if needed for other styles
}

def normalize_for_tts(text: str) -> str:
    """
    Converts common stylized Unicode characters to their normal equivalents,
    removes combining marks, and handles specific symbols.
    """
    if not isinstance(text, str): # Basic type check
        return ""

    # --- More Advanced Normalization Attempt using unicodedata ---
    # 1. NFKD Decomposition: Separates base characters from combining marks
    #    Example: 'é' becomes 'e' + '\u0301' (COMBINING ACUTE ACCENT)
    #    Example: '𝕮' might become 'C' (depending on Unicode data)
    try:
        decomposed_text = unicodedata.normalize('NFKD', text)
    except TypeError:
        # Handle potential errors if text is not a valid string for normalization
        return text # Return original text if normalization fails

    # 2. Character-by-Character Processing:
    normalized_chars = []
    for char in decomposed_text:
        # a) Check our specific mapping dictionary first (handles Fraktur, Bold, etc.)
        mapped_char = STYLED_TO_NORMAL_MAP.get(char)
        if mapped_char is not None:
            normalized_chars.append(mapped_char) # Use the mapped value (can be empty string to remove)
        # b) If not in map, check if it's a non-spacing mark (combining character)
        elif unicodedata.category(char) == 'Mn':
            continue # Skip (remove) combining marks
        # c) Keep the character if it's not mapped and not a combining mark
        else:
            # Optional: Add checks here to filter out other unwanted categories
            # like 'So' (Symbols, Other) if needed, but be careful not to remove
            # essential punctuation or symbols supported by the TTS.
            # For now, keep most other characters.
            normalized_chars.append(char)

    # 3. Join the processed characters back together
    normalized_text = "".join(normalized_chars)

    # 4. Optional NFC Recomposition: Can sometimes help clean up representation,
    #    but might re-introduce things removed in step 2b if not careful.
    #    Let's skip it for now unless specific issues arise.
    #    final_text = unicodedata.normalize('NFC', normalized_text)

    # 5. Remove extra whitespace that might result from removed characters
    normalized_text = ' '.join(normalized_text.split())

    return normalized_text
# --- End Helper ---


# --- Logging Setup ---
# Setup logging AFTER potential critical failures above
logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')
discord_logger = logging.getLogger('discord')
discord_logger.setLevel(logging.WARNING) # Reduce discord lib noise
bot_logger = logging.getLogger('SoundBot')
bot_logger.setLevel(logging.INFO) # Keep INFO for operation, DEBUG for detailed queue/timer logs

# --- Validate Critical Config ---
# Exit if core dependencies or token are missing after library imports
if not BOT_TOKEN or not PYDUB_AVAILABLE or not EDGE_TTS_AVAILABLE:
    bot_logger.critical("CRITICAL ERROR: Bot token missing or core libraries (Pydub/edge-tts) failed to import. Exiting.")
    exit(1)

# --- Intents ---
intents = discord.Intents.default()
intents.voice_states = True # Needed for join/leave events and VC state
intents.guilds = True       # Needed for guild information and commands
intents.message_content = False # Not needed for slash commands
intents.members = True      # NEEDED to accurately get display names and check channel members

# --- Bot Definition ---
# Consider adding allowed_mentions=discord.AllowedMentions.none() if you don't want the bot to ping anyone
bot = discord.Bot(intents=intents)

# --- Data Storage & Helpers ---
# User config: { "user_id_str": { "join_sound": "filename.mp3", "tts_defaults": {"voice": "en-US-JennyNeural"} } }
user_sound_config: Dict[str, Dict[str, Any]] = {}
# Guild settings: { "guild_id_str": { "stay_in_channel": bool } }
guild_settings: Dict[str, Dict[str, Any]] = {}
# Queues: { guild_id: deque[(member, sound_path)] }
guild_sound_queues: Dict[int, deque[Tuple[discord.Member, str]]] = {}
# Active playback tasks: { guild_id: asyncio.Task }
guild_play_tasks: Dict[int, asyncio.Task[Any]] = {}
# Auto-leave timers: { guild_id: asyncio.Task }
guild_leave_timers: Dict[int, asyncio.Task[Any]] = {}

# --- Config/Dir Functions ---
def load_config():
    """Loads user sound configurations from JSON file."""
    global user_sound_config
    if os.path.exists(CONFIG_FILE):
        try:
            # Ensure UTF-8 is used for reading potentially diverse filenames/paths
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                user_sound_config = json.load(f)
            upgraded_count = 0
            # --- Data Format Upgrade Logic ---
            for user_id, data in list(user_sound_config.items()): # Iterate over copy for modification
                # Check for and potentially update old TTS format if needed
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
                elif isinstance(data, str): # Original upgrade logic for join sound only (string -> dict)
                    user_sound_config[user_id] = {"join_sound": data}
                    bot_logger.info(f"Upgraded join sound format for user {user_id}")
                    upgraded_count += 1
            # Save immediately if any upgrades occurred
            if upgraded_count > 0:
                save_config()
            bot_logger.info(f"Loaded {len(user_sound_config)} user configs from {CONFIG_FILE}")
        except (json.JSONDecodeError, UnicodeDecodeError, Exception) as e:
             bot_logger.error(f"Error loading {CONFIG_FILE}: {e}", exc_info=True)
             user_sound_config = {} # Start fresh on error
    else:
        user_sound_config = {}
        bot_logger.info(f"{CONFIG_FILE} not found. Starting fresh.")

def save_config():
     """Saves user sound configurations to JSON file."""
     try:
        # Ensure UTF-8 is used for writing, allow non-ASCII characters
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(user_sound_config, f, indent=4, ensure_ascii=False)
        bot_logger.debug(f"Saved {len(user_sound_config)} user configs to {CONFIG_FILE}")
     except Exception as e:
         bot_logger.error(f"Error saving {CONFIG_FILE}: {e}", exc_info=True)

def load_guild_settings():
    """Loads guild-specific settings from JSON file."""
    global guild_settings
    if os.path.exists(GUILD_SETTINGS_FILE):
        try:
            with open(GUILD_SETTINGS_FILE, 'r', encoding='utf-8') as f:
                loaded_data = json.load(f)
                # Ensure keys are strings (JSON standard)
                guild_settings = {str(k): v for k, v in loaded_data.items()}
            bot_logger.info(f"Loaded {len(guild_settings)} guild settings from {GUILD_SETTINGS_FILE}")
        except (json.JSONDecodeError, UnicodeDecodeError, Exception) as e:
             bot_logger.error(f"Error loading {GUILD_SETTINGS_FILE}: {e}", exc_info=True)
             guild_settings = {} # Start fresh on error
    else:
        guild_settings = {}
        bot_logger.info(f"{GUILD_SETTINGS_FILE} not found. Starting with no persistent guild settings.")

def save_guild_settings():
    """Saves guild-specific settings to JSON file."""
    try:
        with open(GUILD_SETTINGS_FILE, 'w', encoding='utf-8') as f:
            json.dump(guild_settings, f, indent=4, ensure_ascii=False)
        bot_logger.debug(f"Saved {len(guild_settings)} guild settings to {GUILD_SETTINGS_FILE}")
    except Exception as e:
         bot_logger.error(f"Error saving {GUILD_SETTINGS_FILE}: {e}", exc_info=True)

def ensure_dir(dir_path: str):
    """Creates a directory if it doesn't exist."""
    if not os.path.exists(dir_path):
        try:
            os.makedirs(dir_path)
            bot_logger.info(f"Created directory: {dir_path}")
        except Exception as e:
            bot_logger.critical(f"CRITICAL: Could not create directory '{dir_path}': {e}", exc_info=True)
            # Exit if essential directories cannot be created
            if dir_path in [SOUNDS_DIR, USER_SOUNDS_DIR, PUBLIC_SOUNDS_DIR]:
                exit(f"Failed to create essential directory: {dir_path}")

# Ensure essential directories exist on startup
ensure_dir(SOUNDS_DIR); ensure_dir(USER_SOUNDS_DIR); ensure_dir(PUBLIC_SOUNDS_DIR)

# --- Bot Events ---
@bot.event
async def on_ready():
    """Called when the bot is ready and connected to Discord."""
    bot_logger.info(f'Logged in as {bot.user.name} ({bot.user.id})')
    load_config()
    load_guild_settings()
    bot_logger.info(f"Py-cord: {discord.__version__}, Norm Target: {TARGET_LOUDNESS_DBFS}dBFS")
    bot_logger.info(f"TTS Engine: edge-tts, Default Voice: {DEFAULT_TTS_VOICE}")
    bot_logger.info(f"Loaded {len(FULL_EDGE_TTS_VOICE_CHOICES)} total TTS voices for autocomplete.")
    bot_logger.info(f"Using {len(CURATED_EDGE_TTS_VOICE_CHOICES)} curated voices for command choices.")
    bot_logger.info(f"Allowed Uploads: {', '.join(ALLOWED_EXTENSIONS)}, Max Size: {MAX_USER_SOUND_SIZE_MB}MB")
    bot_logger.info(f"Max TTS Input Length: {MAX_TTS_LENGTH} (before potential spacing)")
    bot_logger.info(f"Playback limited to first {MAX_PLAYBACK_DURATION_MS / 1000} seconds.")
    bot_logger.info(f"Auto-leave timeout set to {AUTO_LEAVE_TIMEOUT_SECONDS} seconds ({AUTO_LEAVE_TIMEOUT_SECONDS / 3600:.1f} hours).")
    bot_logger.info(f"Sound Dirs: {os.path.abspath(SOUNDS_DIR)}, {os.path.abspath(USER_SOUNDS_DIR)}, {os.path.abspath(PUBLIC_SOUNDS_DIR)}")
    # Optional: Add presence update
    # await bot.change_presence(activity=discord.Game(name="/help | Sounding good!"))
    bot_logger.info("Sound Bot is operational.")

# --- Audio Processing Helper ---
def process_audio(sound_path: str, member_display_name: str = "User") -> Tuple[Optional[discord.PCMAudio], Optional[io.BytesIO]]:
    """
    Loads, TRIMS, normalizes, and prepares audio for Discord playback.
    Returns a tuple: (PCMAudio source or None, BytesIO buffer or None).
    The BytesIO buffer MUST be closed by the caller after playback is finished or fails.
    """
    if not PYDUB_AVAILABLE or not os.path.exists(sound_path):
        bot_logger.error(f"AUDIO: Pydub missing or File not found: '{sound_path}'")
        return None, None

    audio_source: Optional[discord.PCMAudio] = None
    pcm_data_io: Optional[io.BytesIO] = None # Initialize buffer variable
    basename = os.path.basename(sound_path)

    try:
        bot_logger.debug(f"AUDIO: Loading '{basename}'...")
        # Guess extension if missing, default to mp3 as a common fallback
        # Use sound_path directly for splitext
        ext = os.path.splitext(sound_path)[1].lower().strip('. ') or 'mp3'
        if not ext: # Handle case of filename with no extension at all
             bot_logger.warning(f"AUDIO: File '{basename}' has no extension. Assuming mp3.")
             ext = 'mp3'

        # Load audio using Pydub
        try:
            audio_segment = AudioSegment.from_file(sound_path, format=ext)
        except CouldntDecodeError as decode_err:
            # Re-raise specifically for the outer handler
            raise decode_err
        except Exception as load_e:
            bot_logger.warning(f"AUDIO: Initial load failed for '{basename}', trying explicit format if possible. Error: {load_e}")
            # Add specific format guesses if Pydub struggles with detection
            if ext == 'm4a': audio_segment = AudioSegment.from_file(sound_path, format="m4a")
            elif ext == 'aac': audio_segment = AudioSegment.from_file(sound_path, format="aac")
            # Add more explicit formats if needed for .ogg (e.g., "ogg", "opus")
            elif ext == 'ogg': audio_segment = AudioSegment.from_file(sound_path, format="ogg")
            else: raise load_e # Re-raise if we can't guess better

        # Trim audio if it exceeds the maximum allowed duration
        if len(audio_segment) > MAX_PLAYBACK_DURATION_MS:
            bot_logger.info(f"AUDIO: Trimming '{basename}' from {len(audio_segment)}ms to first {MAX_PLAYBACK_DURATION_MS}ms.")
            audio_segment = audio_segment[:MAX_PLAYBACK_DURATION_MS]
        else:
            bot_logger.debug(f"AUDIO: '{basename}' is {len(audio_segment)}ms (<= {MAX_PLAYBACK_DURATION_MS}ms), no trimming needed.")

        # Normalize loudness to target dBFS
        peak_dbfs = audio_segment.max_dBFS
        if not math.isinf(peak_dbfs) and peak_dbfs > -90.0: # Avoid normalizing silence/near silence
            change_in_dbfs = TARGET_LOUDNESS_DBFS - peak_dbfs
            bot_logger.info(f"AUDIO: Normalizing '{basename}'. Peak:{peak_dbfs:.2f} Target:{TARGET_LOUDNESS_DBFS:.2f} Gain:{change_in_dbfs:.2f} dB.")
            # Apply gain, but limit excessive positive gain to prevent potential clipping
            if change_in_dbfs < 6.0: # Allow up to +6dB gain (adjust if needed)
                audio_segment = audio_segment.apply_gain(change_in_dbfs)
            else:
                bot_logger.info(f"AUDIO: Limiting gain to +6dB for '{basename}' to prevent potential clipping (calculated: {change_in_dbfs:.2f}dB).")
                audio_segment = audio_segment.apply_gain(6.0)
        elif math.isinf(peak_dbfs):
            bot_logger.warning(f"AUDIO: Cannot normalize silent audio '{basename}'. Peak is -inf.")
        else: # peak_dbfs <= -90.0
             bot_logger.warning(f"AUDIO: Skipping normalization for very quiet audio '{basename}'. Peak: {peak_dbfs:.2f}")

        # Resample for Discord (48kHz) and ensure stereo (2 channels)
        audio_segment = audio_segment.set_frame_rate(48000).set_channels(2)

        # Export to PCM S16LE format in memory using BytesIO
        pcm_data_io = io.BytesIO()
        audio_segment.export(pcm_data_io, format="s16le") # Signed 16-bit Little-Endian PCM
        pcm_data_io.seek(0) # Rewind the buffer to the beginning for reading

        # Create the discord.PCMAudio source if data was successfully exported
        if pcm_data_io.getbuffer().nbytes > 0:
            audio_source = discord.PCMAudio(pcm_data_io) # Pass the buffer here
            bot_logger.debug(f"AUDIO: Successfully processed '{basename}'")
            # Return both the source and the buffer (caller must close buffer)
            return audio_source, pcm_data_io
        else:
            bot_logger.error(f"AUDIO: Exported raw audio for '{basename}' is empty!")
            if pcm_data_io: pcm_data_io.close() # Close empty buffer immediately
            return None, None

    except CouldntDecodeError as decode_err:
        bot_logger.error(f"AUDIO: Pydub CouldntDecodeError for '{basename}'. Is FFmpeg installed and in PATH? Is the file corrupt? Error: {decode_err}", exc_info=True)
        if pcm_data_io: pcm_data_io.close()
        return None, None
    except FileNotFoundError:
         bot_logger.error(f"AUDIO: File not found during processing: '{sound_path}'")
         if pcm_data_io: pcm_data_io.close()
         return None, None
    except Exception as e:
        bot_logger.error(f"AUDIO: Unexpected error processing '{basename}': {e}", exc_info=True)
        # Ensure buffer is closed on any unexpected error if it was created
        if pcm_data_io and not pcm_data_io.closed:
            try: pcm_data_io.close()
            except Exception: pass
        return None, None


# --- Auto Leave Helper Functions ---
def is_bot_alone(vc: Optional[discord.VoiceClient]) -> bool:
    """Checks if the bot is the only non-bot user in its voice channel."""
    if not vc or not vc.channel or not bot.user:
        return False # Can't be alone if not in a channel or bot user unknown
    # Filter out the bot itself AND any other bots
    human_members = [m for m in vc.channel.members if not m.bot]
    bot_logger.debug(f"ALONE CHECK (Guild: {vc.guild.id}, Chan: {vc.channel.name}): {len(human_members)} human(s). Members: {[m.name for m in vc.channel.members]}")
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
    """Starts the automatic leave timer if conditions are met (bot alone, stay disabled, idle)."""
    # Check vc validity immediately
    if not vc or not vc.is_connected() or not vc.guild:
        if vc: bot_logger.warning(f"start_leave_timer called with invalid/disconnected VC for guild {vc.guild.id if vc.guild else 'Unknown'}")
        return

    guild_id = vc.guild.id
    log_prefix = f"LEAVE TIMER (Guild {guild_id}):"

    # 1. Cancel any existing timer first
    cancel_leave_timer(guild_id, reason="starting new timer check")

    # 2. Check conditions: Bot must be alone AND stay is disabled AND not playing
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

    async def _leave_after_delay(voice_client_ref: discord.VoiceClient, g_id: int):
        """Coroutine that waits and then checks conditions again before leaving."""
        original_channel = voice_client_ref.channel # Store original channel for comparison
        try:
            await asyncio.sleep(AUTO_LEAVE_TIMEOUT_SECONDS)

            # --- Re-check conditions AFTER sleep ---
            # Use a fresh reference to the VC if possible
            current_vc = discord.utils.get(bot.voice_clients, guild__id=g_id)
            # Check if bot is still connected AND in the SAME channel it started the timer in
            if not current_vc or not current_vc.is_connected() or current_vc.channel != original_channel:
                 bot_logger.info(f"{log_prefix} Timer expired, but bot disconnected/moved from {original_channel.name if original_channel else 'orig chan'}. Aborting leave.")
                 return
            if not is_bot_alone(current_vc):
                 bot_logger.info(f"{log_prefix} Timer expired, but bot no longer alone in {current_vc.channel.name}. Aborting leave.")
                 return
            if should_bot_stay(g_id):
                 bot_logger.info(f"{log_prefix} Timer expired, but 'stay' enabled during wait. Aborting leave.")
                 return
            if current_vc.is_playing():
                bot_logger.info(f"{log_prefix} Timer expired, but bot started playing again. Aborting leave.")
                return

            # --- Conditions still met - Trigger Disconnect ---
            bot_logger.info(f"{log_prefix} Timer expired. Conditions still met in {current_vc.channel.name}. Triggering automatic disconnect.")
            await safe_disconnect(current_vc, manual_leave=False) # Use safe_disconnect

        except asyncio.CancelledError:
             bot_logger.info(f"{log_prefix} Timer explicitly cancelled.")
             # No need to raise, cancellation is expected behavior
        except Exception as e:
             bot_logger.error(f"{log_prefix} Error during leave timer delay/check: {e}", exc_info=True)
        finally:
             # Clean up the task entry if it still exists and matches the current task
             # (it might have been removed already by cancel_leave_timer)
             task_obj = asyncio.current_task()
             if task_obj and g_id in guild_leave_timers and guild_leave_timers[g_id] is task_obj:
                 del guild_leave_timers[g_id]
                 bot_logger.debug(f"{log_prefix} Cleaned up timer task reference.")

    # Create and store the timer task
    timer_task = bot.loop.create_task(_leave_after_delay(vc, guild_id), name=f"AutoLeave_{guild_id}")
    guild_leave_timers[guild_id] = timer_task
# --- End Auto Leave Helper Functions ---


# --- Core Join Sound Queue Logic ---
async def play_next_in_queue(guild: discord.Guild):
    """Plays the next sound in the guild's join queue."""
    guild_id = guild.id
    # Get current task to check for cancellation and manage task dictionary
    task_id_obj = asyncio.current_task()
    task_id = task_id_obj.get_name() if task_id_obj else 'Unknown'
    bot_logger.debug(f"QUEUE CHECK [{task_id}]: Guild {guild_id}")

    # Check if this task instance was cancelled externally
    if task_id_obj and task_id_obj.cancelled():
        bot_logger.debug(f"QUEUE CHECK [{task_id}]: Task cancelled externally for guild {guild_id}, removing tracker.")
        # Ensure the tracker is removed if it points to this cancelled task
        if guild_id in guild_play_tasks and guild_play_tasks.get(guild_id) is task_id_obj:
            del guild_play_tasks[guild_id]
        return

    # Check if queue is empty or doesn't exist
    if guild_id not in guild_sound_queues or not guild_sound_queues[guild_id]:
        bot_logger.debug(f"QUEUE [{task_id}]: Empty/Non-existent for {guild_id}. Playback task ending.")
        # Ensure the tracker is removed if it points to this completed task
        if guild_id in guild_play_tasks and guild_play_tasks.get(guild_id) is task_id_obj:
            del guild_play_tasks[guild_id]
        # Trigger timer check if the bot is idle and connected
        vc_check = discord.utils.get(bot.voice_clients, guild=guild)
        if vc_check and vc_check.is_connected() and not vc_check.is_playing():
             bot.loop.create_task(start_leave_timer(vc_check))
        return

    # Get current voice client for the guild
    vc = discord.utils.get(bot.voice_clients, guild=guild)
    if not vc or not vc.is_connected():
        bot_logger.warning(f"QUEUE [{task_id}]: Task running for {guild_id}, but bot not connected. Clearing queue and task.")
        if guild_id in guild_sound_queues: guild_sound_queues[guild_id].clear()
        if guild_id in guild_play_tasks and guild_play_tasks.get(guild_id) is task_id_obj: del guild_play_tasks[guild_id]
        cancel_leave_timer(guild_id, reason="bot not connected during queue check")
        return

    # Don't proceed if already playing (another sound or TTS)
    if vc.is_playing():
        bot_logger.debug(f"QUEUE [{task_id}]: Bot already playing in {guild_id}, yielding.")
        return

    # Get the next item from the queue
    try:
        member, sound_path = guild_sound_queues[guild_id].popleft()
        bot_logger.info(f"QUEUE [{task_id}]: Processing {member.display_name} in {guild.name}. Path: {os.path.basename(sound_path)}. Queue Left: {len(guild_sound_queues[guild_id])}")
    except IndexError: # Queue became empty between check and pop
        bot_logger.debug(f"QUEUE [{task_id}]: Became empty unexpectedly for {guild_id} during pop. Ending task.")
        if guild_id in guild_play_tasks and guild_play_tasks.get(guild_id) is task_id_obj: del guild_play_tasks[guild_id]
        if vc and vc.is_connected() and not vc.is_playing(): bot.loop.create_task(start_leave_timer(vc))
        return

    # Check if the sound path points to a temporary TTS file needing deletion
    sound_basename = os.path.basename(sound_path)
    is_temp_tts = sound_basename.startswith("tts_join_") and sound_basename.endswith(".mp3") # More specific check

    # Define the cleanup function to be called after playback finishes or errors
    def after_play_cleanup(error: Optional[Exception], vc_ref: discord.VoiceClient, path_to_delete: Optional[str] = None, audio_buffer: Optional[io.BytesIO] = None, is_temp: bool = False):
        """
        Cleanup function called after vc.play finishes.
        Handles standard after_play logic, buffer closing, and temp file deletion.
        """
        guild_id_cleanup = vc_ref.guild.id if vc_ref.guild else 'Unknown'
        log_prefix_cleanup = f"AFTER_PLAY_CLEANUP (Guild {guild_id_cleanup}):"

        # --- Call standard after_play_handler FIRST ---
        # This ensures queue and timer logic runs regardless of cleanup success/failure
        if vc_ref and vc_ref.is_connected():
             after_play_handler(error, vc_ref) # Pass the error and current VC reference
        else:
             bot_logger.warning(f"{log_prefix_cleanup} VC disconnected before standard after_play_handler could run.")

        # --- Attempt to close audio buffer ---
        if audio_buffer:
            try:
                if not audio_buffer.closed: # Check if not already closed
                    audio_buffer.close()
                    bot_logger.debug(f"{log_prefix_cleanup} Closed audio buffer for '{os.path.basename(path_to_delete or 'sound')}'.")
            except Exception as buf_e:
                bot_logger.warning(f"{log_prefix_cleanup} Error closing audio buffer: {buf_e}")

        # --- Attempt file cleanup for temporary TTS files ---
        # Check is_temp flag AND if path_to_delete is provided
        if is_temp and path_to_delete:
            bot_logger.debug(f"{log_prefix_cleanup} Attempting cleanup for temp file: {path_to_delete}")
            if os.path.exists(path_to_delete):
                try:
                    os.remove(path_to_delete)
                    bot_logger.info(f"{log_prefix_cleanup} Deleted temporary file: {path_to_delete}")
                except OSError as e_del:
                    bot_logger.warning(f"{log_prefix_cleanup} Failed to delete temporary file '{path_to_delete}': {e_del}")
            else:
                bot_logger.debug(f"{log_prefix_cleanup} Temp file '{path_to_delete}' not found for deletion (possibly already cleaned).")


    # Process the audio file (normalize, trim, convert)
    audio_source, audio_buffer_to_close = process_audio(sound_path, member.display_name)

    if audio_source:
        try:
            # Cancel any potential leave timer before starting playback
            cancel_leave_timer(guild_id, reason="starting playback")
            bot_logger.info(f"QUEUE PLAYBACK [{task_id}]: Playing for {member.display_name}...")

            # Call vc.play, passing the custom cleanup callback with the path and temp flag
            vc.play(
                audio_source,
                after=lambda e: after_play_cleanup(
                    e,
                    vc, # Pass the current vc reference
                    path_to_delete=sound_path, # Pass the path for potential deletion
                    audio_buffer=audio_buffer_to_close, # Pass the buffer to close
                    is_temp=is_temp_tts # Pass the flag indicating if it's temporary
                )
            )
            bot_logger.debug(f"QUEUE PLAYBACK [{task_id}]: vc.play() called for {member.display_name}.")
        except (discord.errors.ClientException, Exception) as e:
            bot_logger.error(f"QUEUE PLAYBACK ERROR [{task_id}] while calling vc.play(): {type(e).__name__}: {e}", exc_info=True)
            # Manually call cleanup function if play() fails immediately
            after_play_cleanup(
                e,
                vc,
                path_to_delete=sound_path,
                audio_buffer=audio_buffer_to_close,
                is_temp=is_temp_tts
            )
    else:
        # Audio processing failed, skip this item
        bot_logger.warning(f"QUEUE PLAYBACK [{task_id}]: No valid audio source for {member.display_name} ({sound_basename}). Skipping.")
        # Clean up buffer even if source is None but buffer exists
        if audio_buffer_to_close and not audio_buffer_to_close.closed:
             try: audio_buffer_to_close.close(); bot_logger.debug("Cleaned up buffer after failed processing.")
             except Exception: pass
        # Manually attempt file cleanup for FAILED temporary TTS
        if is_temp_tts and os.path.exists(sound_path):
            try:
                os.remove(sound_path)
                bot_logger.info(f"CLEANUP: Deleted FAILED temporary TTS file: {sound_path}")
            except OSError as e_del:
                bot_logger.warning(f"CLEANUP: Failed to delete failed TTS file '{sound_path}': {e_del}")
        # Schedule the next check immediately to continue the queue or trigger idle timer
        bot.loop.create_task(play_next_in_queue(guild), name=f"QueueSkip_{guild_id}")

@bot.event
# --- on_voice_state_update --- *** CORRECTED TTS JOIN LOGIC *** ---
@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    """Handles voice state changes for users joining/leaving channels and the bot itself."""
    # Ignore events not involving channel changes for non-bot users
    if before.channel == after.channel and not member.bot:
        if member.id == bot.user.id and before.channel != after.channel:
             pass # Bot moved or disconnected, handled below
        else:
            return # No relevant channel change

    guild = member.guild
    if not guild: return
    guild_id = guild.id
    vc = discord.utils.get(bot.voice_clients, guild=guild)

    # --- Handle User JOINING a channel (or moving into one) ---
    if not member.bot and after.channel and before.channel != after.channel:
        channel_to_join = after.channel
        user_display_name = member.display_name
        user_id_str = str(member.id)
        bot_logger.info(f"EVENT: {user_display_name} ({user_id_str}) entered {channel_to_join.name} in {guild.name}")

        # If user joins the channel the bot is *already* in, cancel any leave timer
        if vc and vc.is_connected() and vc.channel == channel_to_join:
            bot_logger.debug(f"User {user_display_name} joined bot's current channel ({vc.channel.name}).")
            cancel_leave_timer(guild_id, reason=f"user {user_display_name} joined")

        # --- Join Sound Logic ---
        bot_perms = channel_to_join.permissions_for(guild.me)
        if not bot_perms.connect or not bot_perms.speak:
            bot_logger.warning(f"Missing Connect/Speak permission in '{channel_to_join.name}'. Cannot play sound for {user_display_name}.")
            return

        sound_path: Optional[str] = None
        use_tts_join = False # Flag to determine if TTS should be used
        user_config = user_sound_config.get(user_id_str)

        # 1. Prioritize custom join sound
        if user_config and "join_sound" in user_config:
            filename = user_config["join_sound"]
            potential_path = os.path.join(SOUNDS_DIR, filename)
            if os.path.exists(potential_path):
                sound_path = potential_path
                bot_logger.info(f"SOUND: Using configured join sound: '{filename}' for {user_display_name}")
            else:
                bot_logger.warning(f"SOUND: Configured join sound '{filename}' not found for {user_display_name}. Removing broken entry, using TTS join.")
                del user_config["join_sound"]
                if not user_config: # If config becomes empty, remove user entry
                    if user_id_str in user_sound_config: del user_sound_config[user_id_str]
                save_config()
                use_tts_join = True # Fallback to TTS
        else:
            # No custom sound configured, use TTS
            use_tts_join = True
            bot_logger.info(f"SOUND: No custom join sound for {user_display_name}. Using TTS join.")

        # 2. Generate TTS if needed
        if use_tts_join and EDGE_TTS_AVAILABLE:
            # --- TTS Generation for Join ---
            tts_filename = f"tts_join_{member.id}_{os.urandom(4).hex()}.mp3"
            tts_path = os.path.join(SOUNDS_DIR, tts_filename) # Store temp TTS in SOUNDS_DIR
            bot_logger.info(f"TTS JOIN: Generating for {user_display_name} ('{tts_filename}')...")

            try:
                # Determine TTS voice (user default -> bot default)
                tts_defaults = user_config.get("tts_defaults", {}) if user_config else {}
                tts_voice = tts_defaults.get("voice", DEFAULT_TTS_VOICE)

                # Validate voice
                if not any(v.value == tts_voice for v in FULL_EDGE_TTS_VOICE_CHOICES):
                    bot_logger.warning(f"TTS JOIN: Invalid voice '{tts_voice}' for user {user_id_str}. Falling back to bot default '{DEFAULT_TTS_VOICE}'.")
                    tts_voice = DEFAULT_TTS_VOICE
                bot_logger.debug(f"TTS JOIN: Using voice: {tts_voice}")

                # --- Normalization ONLY (NO spell out for join) ---
                original_name = user_display_name
                normalized_name = normalize_for_tts(original_name) # Normalize the name

                # Construct the final text to speak USING THE NORMALIZED NAME
                # Use strip() to handle names that normalize to whitespace only
                text_to_speak = f"{normalized_name} joined" if normalized_name.strip() else "Someone joined" # Fallback

                if original_name != normalized_name:
                    bot_logger.info(f"TTS JOIN: Normalized Name: '{original_name}' -> '{normalized_name}'")
                bot_logger.info(f"TTS JOIN: Final Text to Speak: '{text_to_speak}'")
                # --- End Normalization ---

                # Generate TTS audio file using edge-tts library
                communicate = edge_tts.Communicate(text_to_speak, tts_voice)
                await communicate.save(tts_path) # Save to the temporary file path

                # Validate file creation and size
                if not os.path.exists(tts_path) or os.path.getsize(tts_path) == 0:
                    raise RuntimeError(f"Edge-TTS failed to create a non-empty file: {tts_path}")

                bot_logger.info(f"TTS JOIN: Successfully saved TTS file '{tts_filename}'")
                sound_path = tts_path # Set sound_path to the generated temporary TTS file

            except Exception as e:
                bot_logger.error(f"TTS JOIN: Failed generation for {user_display_name} (voice={tts_voice}): {e}", exc_info=True)
                sound_path = None # Ensure sound_path is None on failure
                # Attempt to clean up failed/empty TTS file immediately
                if os.path.exists(tts_path):
                    try:
                        os.remove(tts_path)
                        bot_logger.warning(f"TTS JOIN: Cleaned up failed temporary file: {tts_path}")
                    except OSError as del_err:
                        bot_logger.warning(f"TTS JOIN: Could not clean up failed temporary file '{tts_path}': {del_err}")
        elif use_tts_join and not EDGE_TTS_AVAILABLE:
             bot_logger.error("TTS JOIN: Cannot generate join sound for {user_display_name}, edge-tts library not available.")
             sound_path = None # Cannot proceed

        # --- Queueing and Playback Initiation ---
        if not sound_path:
            bot_logger.error(f"SOUND/TTS JOIN: Could not find or generate a sound path for {user_display_name}. Skipping playback.")
            return # Do not proceed

        # Ensure queue exists
        if guild_id not in guild_sound_queues:
            guild_sound_queues[guild_id] = deque()

        # Add the sound (custom file or temporary TTS path) to the queue
        guild_sound_queues[guild_id].append((member, sound_path))
        bot_logger.info(f"QUEUE: Added join sound/TTS for {user_display_name}. Queue size: {len(guild_sound_queues[guild_id])}")

        # --- Connect/Move/Start Playback Task (Logic remains the same) ---
        current_vc = discord.utils.get(bot.voice_clients, guild=guild)
        should_start_play_task = False

        try:
            if not current_vc or not current_vc.is_connected():
                bot_logger.info(f"VOICE: Connecting to '{channel_to_join.name}' for {user_display_name}'s join sound/TTS.")
                cancel_leave_timer(guild_id, reason="connecting for join sound")
                current_vc = await channel_to_join.connect(timeout=30.0, reconnect=True)
                bot_logger.info(f"VOICE: Connected to '{channel_to_join.name}'.")
                should_start_play_task = True
            elif current_vc.channel != channel_to_join:
                 bot_logger.info(f"VOICE: Moving from '{current_vc.channel.name}' to '{channel_to_join.name}' for join sound/TTS.")
                 cancel_leave_timer(guild_id, reason="moving for join sound")
                 await current_vc.move_to(channel_to_join)
                 bot_logger.info(f"VOICE: Moved to '{channel_to_join.name}'.")
                 should_start_play_task = True
            elif not current_vc.is_playing():
                 bot_logger.debug(f"VOICE: Bot already in '{channel_to_join.name}' and idle. Will trigger queue.")
                 should_start_play_task = True
            else:
                bot_logger.info(f"VOICE: Bot already playing in {guild.name}. Join sound/TTS for {user_display_name} queued.")
                # Safeguard: Ensure a play task exists if playing
                if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
                     task_name = f"QueueTriggerDeferred_{guild_id}"
                     if guild_sound_queues.get(guild_id):
                         cancel_leave_timer(guild_id, reason="starting deferred play task")
                         guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(guild), name=task_name)
                         bot_logger.debug(f"VOICE: Created deferred play task '{task_name}' as safeguard.")
                     else:
                         bot_logger.debug(f"VOICE: Deferred task '{task_name}' skipped, queue emptied concurrently.")

        except asyncio.TimeoutError:
            bot_logger.error(f"VOICE: Timeout connecting/moving to '{channel_to_join.name}'. Clearing queue for {guild.name}.")
            if guild_id in guild_sound_queues: guild_sound_queues[guild_id].clear()
            current_vc = None
        except discord.errors.ClientException as e:
            bot_logger.warning(f"VOICE: ClientException during connect/move to '{channel_to_join.name}': {e}")
            current_vc = discord.utils.get(bot.voice_clients, guild=guild)
        except Exception as e:
            bot_logger.error(f"VOICE: Unexpected error connecting/moving to '{channel_to_join.name}': {e}", exc_info=True)
            if guild_id in guild_sound_queues: guild_sound_queues[guild_id].clear()
            current_vc = None

        # Start playback task if conditions met and VC valid
        if should_start_play_task and current_vc and current_vc.is_connected():
             cancel_leave_timer(guild_id, reason="starting play task")
             if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
                task_name = f"QueueStart_{guild_id}"
                if guild_sound_queues.get(guild_id):
                    guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(guild), name=task_name)
                    bot_logger.info(f"VOICE: Started play task '{task_name}' for guild {guild_id}.")
                else:
                    bot_logger.debug(f"VOICE: Start task '{task_name}' skipped, queue emptied concurrently.")
                    if not current_vc.is_playing(): bot.loop.create_task(start_leave_timer(current_vc))
             else:
                 bot_logger.debug(f"VOICE: Play task for {guild_id} already running/scheduled.")
        elif not current_vc or not current_vc.is_connected():
             bot_logger.warning(f"VOICE: Bot could not connect/move to {channel_to_join.name}, cannot start playback for {user_display_name}.")
             cancel_leave_timer(guild_id, reason="connection failed")


    # --- Handle User LEAVING a channel (or moving out) --- (No changes here)
    elif not member.bot and before.channel and before.channel != after.channel:
        if vc and vc.is_connected() and vc.channel == before.channel:
            bot_logger.info(f"EVENT: {member.display_name} left bot's channel ({before.channel.name}). Checking if bot is alone.")
            # Schedule delayed check
            bot.loop.call_later(1.0, lambda current_vc=vc: bot.loop.create_task(start_leave_timer(current_vc)))


    # --- Handle Bot's own state changes --- (No changes here)
    elif member.id == bot.user.id:
        if before.channel and not after.channel:
            bot_logger.info(f"EVENT: Bot disconnected from {before.channel.name} in {guild.name}. Cleaning up.")
            cancel_leave_timer(guild_id, reason="bot disconnected")
            # Clean up play task
            if guild_id in guild_play_tasks:
                 play_task = guild_play_tasks.pop(guild_id, None)
                 if play_task and not play_task.done():
                     try: play_task.cancel()
                     except Exception: pass
                     bot_logger.debug(f"Cleaned up play task for disconnected guild {guild_id}.")
            # Clear queue
            if guild_id in guild_sound_queues:
                 guild_sound_queues[guild_id].clear()
                 bot_logger.debug(f"Cleared sound queue for disconnected guild {guild_id}.")
        elif before.channel != after.channel and after.channel:
             bot_logger.info(f"EVENT: Bot moved from {before.channel.name} to {after.channel.name} in {guild.name}.")
             # Timer logic handled by commands/events causing move

# --- after_play_handler ---
# No changes needed here, logic moved to after_play_cleanup in play_next_in_queue
def after_play_handler(error: Optional[Exception], vc: discord.VoiceClient):
    """Callback function executed after a sound finishes playing or errors."""
    guild_id = vc.guild.id if vc and vc.guild else None
    if error:
        bot_logger.error(f'PLAYBACK ERROR (In after_play_handler for guild {guild_id}): {error}', exc_info=error)

    if not guild_id or not vc or not vc.is_connected():
        bot_logger.warning(f"after_play_handler called with invalid/disconnected vc (Guild ID: {guild_id}). Cleaning up related tasks.")
        if guild_id:
            cancel_leave_timer(guild_id, reason="after_play on disconnected VC")
            play_task = guild_play_tasks.pop(guild_id, None)
            if play_task and not play_task.done():
                 try: play_task.cancel()
                 except Exception: pass
                 bot_logger.debug(f"Cancelled lingering play task for disconnected guild {guild_id} in after_play.")
        return

    bot_logger.debug(f"Playback finished/errored for guild {guild_id}. Checking queue and idle state.")

    is_join_queue_empty = guild_id not in guild_sound_queues or not guild_sound_queues[guild_id]

    if not is_join_queue_empty:
        bot_logger.debug(f"AFTER_PLAY: Join queue for {guild_id} not empty. Ensuring task runs.")
        cancel_leave_timer(guild_id, reason="playback finished, queue not empty")
        if guild_id not in guild_play_tasks or guild_play_tasks[guild_id].done():
             task_name = f"QueueCheckAfterPlay_{guild_id}"
             if guild_sound_queues.get(guild_id):
                 guild_play_tasks[guild_id] = bot.loop.create_task(play_next_in_queue(vc.guild), name=task_name)
                 bot_logger.debug(f"AFTER_PLAY: Scheduled task '{task_name}' for {guild_id} as current task was done.")
             else:
                 bot_logger.debug(f"AFTER_PLAY: Task '{task_name}' creation skipped, queue emptied concurrently. Triggering idle check.")
                 bot.loop.create_task(start_leave_timer(vc))
        else:
             bot_logger.debug(f"AFTER_PLAY: Existing play task found for {guild_id}, letting it continue.")
    else:
         bot_logger.debug(f"AFTER_PLAY: Join queue for {guild_id} is empty. Bot is now idle.")
         play_task = guild_play_tasks.pop(guild_id, None)
         if play_task and play_task.done():
             bot_logger.debug(f"AFTER_PLAY: Cleaned up completed play task tracker for guild {guild_id}.")
         elif play_task:
             bot_logger.warning(f"AFTER_PLAY: Play task tracker existed for {guild_id} but task wasn't marked done during cleanup.")

         bot.loop.create_task(start_leave_timer(vc))


# --- Helper Function: Check if bot should stay ---
def should_bot_stay(guild_id: int) -> bool:
    """Checks the guild setting for whether the bot should stay in channel when idle."""
    settings = guild_settings.get(str(guild_id), {})
    stay = settings.get("stay_in_channel", False) # Default to False (leave when idle and alone)
    bot_logger.debug(f"Checked stay setting for guild {guild_id}: {stay}")
    return stay is True


# --- safe_disconnect ---
async def safe_disconnect(vc: Optional[discord.VoiceClient], *, manual_leave: bool = False):
    """Handles disconnecting the bot, considering stay settings and cleaning up tasks/timers."""
    if not vc or not vc.is_connected():
        bot_logger.debug("safe_disconnect called but VC is already disconnected or invalid.")
        return

    guild = vc.guild
    guild_id = guild.id

    # --- ALWAYS cancel leave timer before attempting disconnect ---
    cancel_leave_timer(guild_id, reason="safe_disconnect called")
    # ---

    # Check if disconnect should be skipped due to 'stay' setting (only if not manual)
    if not manual_leave and should_bot_stay(guild_id):
        bot_logger.debug(f"Disconnect skipped for {guild.name}: 'Stay in channel' is enabled.")
        # Clean up play task if bot is idle but staying (defensive check)
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

    # Determine if disconnect should happen
    is_join_queue_empty = guild_id not in guild_sound_queues or not guild_sound_queues[guild_id]
    is_playing = vc.is_playing()
    # Disconnect if manual, OR if queue is empty and not playing (and stay isn't enabled - checked above)
    should_disconnect = manual_leave or (is_join_queue_empty and not is_playing)

    if should_disconnect:
        disconnect_reason = "Manual /leave or auto-timer" if manual_leave else "Idle, queue empty, and stay disabled"
        bot_logger.info(f"DISCONNECT: Conditions met for {guild.name} ({disconnect_reason}). Disconnecting...")
        try:
            # Stop playback if disconnecting while playing (especially for manual leave)
            if vc.is_playing():
                log_level = logging.WARNING if not manual_leave else logging.DEBUG
                bot_logger.log(log_level, f"DISCONNECT: Called stop() during disconnect for {guild.name} (Manual: {manual_leave}).")
                vc.stop() # This should trigger after_play_handler which handles queue/timer logic again

            # Perform the disconnect
            await vc.disconnect(force=False) # Let discord handle internal cleanup
            bot_logger.info(f"DISCONNECT: Bot disconnected from '{guild.name}'. (VC state change event will trigger final cleanup if needed)")

            # Explicit cleanup of tasks/queues associated with the guild AFTER disconnect command/timer
            # Note: on_voice_state_update for the bot leaving also handles this, but doing it here
            # ensures cleanup even if the event handler fails for some reason.
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
         # This case implies the bot is playing OR the queue is not empty,
         # and it wasn't a manual leave, and stay is disabled.
         bot_logger.debug(f"Disconnect skipped for {guild.name}: Manual={manual_leave}, QueueEmpty={is_join_queue_empty}, Playing={is_playing}, StayEnabled={should_bot_stay(guild_id)}.")


# --- Voice Client Connection/Busy Check Helper ---
async def _ensure_voice_client_ready(interaction: discord.Interaction, target_channel: discord.VoiceChannel, action_type: str = "Playback") -> Optional[discord.VoiceClient]:
    """
    Helper to connect/move VC, check permissions, and check busy status.
    Returns the VoiceClient if ready, otherwise None. Sends feedback to user.
    """
    # Ensure the response function is available based on interaction state
    # Use followup if already deferred/responded, edit if first response
    responder = interaction.followup if interaction.response.is_done() else interaction.edit_original_response

    guild = interaction.guild
    user = interaction.user
    # Basic check for guild context
    if not guild:
        try: await responder(content="This command must be used in a server.", ephemeral=True)
        except discord.NotFound: pass
        except Exception as e: bot_logger.warning(f"Error responding in _ensure_voice_client_ready (no guild): {e}")
        return None

    guild_id = guild.id
    log_prefix = f"{action_type.upper()}:"

    # Check bot permissions in the target channel
    bot_perms = target_channel.permissions_for(guild.me)
    if not bot_perms.connect or not bot_perms.speak:
        try: await responder(content=f"❌ I don't have permission to Connect or Speak in {target_channel.mention}.", ephemeral=True)
        except discord.NotFound: pass
        except Exception as e: bot_logger.warning(f"Error responding in _ensure_voice_client_ready (perms): {e}")
        bot_logger.warning(f"{log_prefix} Missing Connect/Speak perms in {target_channel.name} ({guild.name}).")
        return None

    vc = discord.utils.get(bot.voice_clients, guild=guild)
    try:
        # Case 1: Bot is already connected
        if vc and vc.is_connected():
            # Check if playing FIRST - most common busy state
            if vc.is_playing():
                # Distinguish between join queue playing and other playback (TTS/single sound)
                join_queue_active = guild_id in guild_sound_queues and guild_sound_queues[guild_id]
                msg = "⏳ Bot is currently playing join sounds. Please wait." if join_queue_active else "⏳ Bot is currently playing another sound/TTS. Please wait."
                log_msg = f"{log_prefix} Bot busy ({'join queue' if join_queue_active else 'non-join'}) in {guild.name}, user {user.name}'s request ignored."
                try: await responder(content=msg, ephemeral=True)
                except discord.NotFound: pass
                except Exception as e: bot_logger.warning(f"Error responding in _ensure_voice_client_ready (busy): {e}")
                bot_logger.info(log_msg)
                return None # Indicate busy

            # Case 1b: Bot connected, but to a different channel
            elif vc.channel != target_channel:
                # Allow moving if user is in the target channel OR if stay mode is disabled
                should_move = (isinstance(user, discord.Member) and user.voice and user.voice.channel == target_channel) or not should_bot_stay(guild_id)

                if should_move:
                     bot_logger.info(f"{log_prefix} Moving from '{vc.channel.name}' to '{target_channel.name}' for {user.name}.")
                     cancel_leave_timer(guild_id, reason=f"moving for {action_type}") # Cancel timer before move
                     await vc.move_to(target_channel)
                     bot_logger.info(f"{log_prefix} Moved successfully.")
                     # VC reference remains valid after move
                else: # Stay enabled, user not in target channel (bot is elsewhere)
                    bot_logger.debug(f"{log_prefix} Not moving from '{vc.channel.name}' to '{target_channel.name}' because stay is enabled and user isn't there.")
                    try: await responder(content=f"ℹ️ I'm currently staying in {vc.channel.mention}. Please join that channel or disable the stay setting with `/togglestay` (admin).", ephemeral=True)
                    except discord.NotFound: pass
                    except Exception as e: bot_logger.warning(f"Error responding in _ensure_voice_client_ready (stay): {e}")
                    return None # Indicate wrong channel due to stay mode

            # Case 1c: Bot connected to the right channel and idle - proceed (VC is already correct)

        # Case 2: Bot not connected at all
        else:
            bot_logger.info(f"{log_prefix} Connecting to '{target_channel.name}' for {user.name}.")
            cancel_leave_timer(guild_id, reason=f"connecting for {action_type}") # Cancel timer before connect
            vc = await target_channel.connect(timeout=30.0, reconnect=True)
            bot_logger.info(f"{log_prefix} Connected successfully.")

        # Final check: Ensure VC is valid and connected after connect/move attempts
        if not vc or not vc.is_connected():
             bot_logger.error(f"{log_prefix} Failed to establish voice client for {target_channel.name} after connect/move attempt.")
             try: await responder(content="❌ Failed to connect or move to the voice channel.", ephemeral=True)
             except discord.NotFound: pass
             except Exception as e: bot_logger.warning(f"Error responding in _ensure_voice_client_ready (connect failed): {e}")
             return None

        # --- Bot is now connected and idle in the correct channel ---
        # Cancel timer again just to be absolutely sure before returning VC
        cancel_leave_timer(guild_id, reason=f"ensured ready for {action_type}")
        return vc # Success! Return the valid voice client

    # --- Error Handling for Connection/Move ---
    except asyncio.TimeoutError:
         try: await responder(content="❌ Connection to the voice channel timed out.", ephemeral=True)
         except discord.NotFound: pass
         except Exception as e: bot_logger.warning(f"Error responding in _ensure_voice_client_ready (timeout): {e}")
         bot_logger.error(f"{log_prefix} Connection/Move Timeout in {guild.name} to {target_channel.name}")
         return None
    except discord.errors.ClientException as e:
        # Handle common client exceptions like "already connecting/connected"
        msg = "⏳ Bot is busy connecting/disconnecting. Please wait a moment." if "already connect" in str(e).lower() else f"❌ Error connecting/moving: {e}. Check permissions or try again."
        try: await responder(content=msg, ephemeral=True)
        except discord.NotFound: pass
        except Exception as e_resp: bot_logger.warning(f"Error responding in _ensure_voice_client_ready (ClientException): {e_resp}")
        bot_logger.warning(f"{log_prefix} Connection/Move ClientException in {guild.name}: {e}")
        return None
    except Exception as e: # Catch any other unexpected errors
        try: await responder(content="❌ An unexpected error occurred while joining the voice channel.", ephemeral=True)
        except discord.NotFound: pass
        except Exception as e_resp: bot_logger.warning(f"Error responding in _ensure_voice_client_ready (unexpected): {e_resp}")
        bot_logger.error(f"{log_prefix} Connection/Move unexpected error in {guild.name}: {e}", exc_info=True)
        return None


# --- Single Sound Playback Logic (For Files) ---
async def play_single_sound(interaction: discord.Interaction, sound_path: str):
    """
    Connects (if needed), plays a single sound FILE (processed/trimmed),
    and handles cleanup via callbacks. Edits the original interaction response.
    """
    # Determine how to respond based on interaction state
    responder = interaction.followup if interaction.response.is_done() else interaction.edit_original_response

    user = interaction.user
    guild = interaction.guild

    # Basic checks - User must be in a voice channel in a server
    if not guild or not isinstance(user, discord.Member) or not user.voice or not user.voice.channel:
        try: await responder(content="You need to be in a voice channel in this server to use this.")
        # Fallback if interaction response fails (e.g., timed out during defer)
        except discord.NotFound:
            if interaction.channel: await interaction.channel.send("You need to be in a voice channel in this server to use this.", ephemeral=True, delete_after=10)
        except Exception as e: bot_logger.warning(f"Error responding in play_single_sound (no VC): {e}")
        return

    target_channel = user.voice.channel
    guild_id = guild.id # Get guild_id for timer cancellation

    # Check if the sound file actually exists on the server
    if not os.path.exists(sound_path):
        try: await responder(content="❌ Error: The requested sound file seems to be missing on the server.")
        except discord.NotFound: pass # Ignore if interaction gone
        except Exception as e: bot_logger.warning(f"Error responding in play_single_sound (file missing): {e}")
        bot_logger.error(f"SINGLE PLAY: File not found: {sound_path}")
        return

    # Ensure bot is connected, has perms, and is not busy
    voice_client = await _ensure_voice_client_ready(interaction, target_channel, action_type="SINGLE PLAY (File)")
    if not voice_client:
        # _ensure_voice_client_ready already sent feedback via responder/edit
        return

    # --- Process Audio ---
    sound_basename = os.path.basename(sound_path)
    bot_logger.info(f"SINGLE PLAY (File): Processing '{sound_basename}' for {user.name}...")
    audio_source, audio_buffer_to_close = process_audio(sound_path, user.display_name)

    if audio_source:
        # Double-check playing status right before calling play()
        if voice_client.is_playing():
             bot_logger.warning(f"SINGLE PLAY (File): VC became busy between check and play for {user.name}. Aborting.")
             try: await responder(content="⏳ Bot became busy just now. Please try again.")
             except discord.NotFound: pass # Ignore if interaction gone
             except Exception as e: bot_logger.warning(f"Error responding in play_single_sound (VC busy): {e}")
             # Close buffer if playback is aborted here
             if audio_buffer_to_close and not audio_buffer_to_close.closed:
                 try: audio_buffer_to_close.close()
                 except Exception: pass
             return

        # --- Play Audio ---
        try:
            # Cancel leave timer before playing
            cancel_leave_timer(guild_id, reason="starting single sound playback")
            sound_display_name = os.path.splitext(sound_basename)[0]
            bot_logger.info(f"SINGLE PLAYBACK (File): Playing '{sound_display_name}' requested by {user.display_name}...")

            # Define the 'after' callback locally to include buffer closing
            def single_sound_after_play(error: Optional[Exception]):
                 log_prefix_after = f"AFTER_PLAY_SINGLE (Guild {guild_id}, Sound {sound_display_name}):"
                 bot_logger.debug(f"{log_prefix_after} Callback initiated.")
                 # Use utils.get to ensure VC ref is current when calling standard handler
                 current_vc = discord.utils.get(bot.voice_clients, guild=voice_client.guild)
                 if current_vc and current_vc.is_connected():
                     after_play_handler(error, current_vc) # Call standard handler
                 elif voice_client: # Log if VC disconnected before handler could run
                     bot_logger.warning(f"{log_prefix_after} VC disconnected before standard after_play_handler could run.")

                 # Close the specific buffer for this sound
                 try:
                     if audio_buffer_to_close and not audio_buffer_to_close.closed: # Check buffer exists and not closed
                         audio_buffer_to_close.close()
                         bot_logger.debug(f"{log_prefix_after} Closed audio buffer.")
                 except Exception as close_err:
                     bot_logger.error(f"{log_prefix_after} Error closing audio buffer: {close_err}")

            # Play the sound, passing the audio source and the custom after-callback
            voice_client.play(audio_source, after=single_sound_after_play)

            # Edit the original deferred response to confirm playback started
            try: await responder(content=f"▶️ Playing `{sound_display_name}` (max {MAX_PLAYBACK_DURATION_MS / 1000}s)...")
            except discord.NotFound: pass # Ignore if interaction gone
            except Exception as e: bot_logger.warning(f"Error responding in play_single_sound (playing msg): {e}")

        # --- Error Handling for Playback Call ---
        except discord.errors.ClientException as e:
            # Handle errors like "already playing" or other client issues
            try: await responder(content="❌ Error: Bot is already playing or encountered a client issue.")
            except discord.NotFound: pass # Ignore if interaction gone
            except Exception as resp_e: bot_logger.warning(f"Error responding in play_single_sound (client exc): {resp_e}")
            bot_logger.error(f"SINGLE PLAYBACK ERROR (File - ClientException): {e}", exc_info=True)
            # Trigger cleanup manually if play() fails immediately
            single_sound_after_play(e)
        except Exception as e:
            # Handle any other unexpected errors during playback initiation
            try: await responder(content="❌ An unexpected error occurred during playback.")
            except discord.NotFound: pass # Ignore if interaction gone
            except Exception as resp_e: bot_logger.warning(f"Error responding in play_single_sound (unexpected exc): {resp_e}")
            bot_logger.error(f"SINGLE PLAYBACK ERROR (File - Unexpected): {e}", exc_info=True)
            # Trigger cleanup manually
            single_sound_after_play(e)
    else:
        # Audio processing failed
        try: await responder(content="❌ Error: Could not process the audio file. It might be corrupted or unsupported.")
        except discord.NotFound: pass # Ignore if interaction gone
        except Exception as e: bot_logger.warning(f"Error responding in play_single_sound (processing failed): {e}")
        bot_logger.error(f"SINGLE PLAYBACK (File): Failed to get audio source for '{sound_path}' requested by {user.name}")
        # Clean up buffer if it exists even if processing failed
        if audio_buffer_to_close and not audio_buffer_to_close.closed:
             try: audio_buffer_to_close.close()
             except Exception: pass
        # Start leave timer check if bot is now idle in channel after failure
        if voice_client and voice_client.is_connected():
            bot.loop.create_task(start_leave_timer(voice_client))


# --- Sound File Helper Functions ---
def sanitize_filename(name: str) -> str:
    """Removes/replaces invalid chars for filenames and limits length."""
    # Replace invalid Windows/Unix filename characters and whitespace with underscore
    # Chars: <>:"/\|?* and control characters (0-31) and whitespace
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f\s]+', '_', name)
    name = re.sub(r'_+', '_', name) # Collapse multiple underscores
    name = name.strip('_') # Remove leading/trailing underscores
    # Limit length, ensuring it's not empty
    name = name[:50] if len(name) > 50 else name
    return name if name else "sound" # Return "sound" if sanitation results in empty string

def _find_sound_path_in_dir(directory: str, sound_name: str) -> Optional[str]:
    """
    Generic helper to find a sound file by name (case-insensitive, checks extensions).
    Handles sanitized names if exact match fails.
    """
    if not os.path.isdir(directory): return None
    # Order to check extensions, prioritizing common ones
    preferred_order = ['.mp3', '.wav'] + [ext for ext in ALLOWED_EXTENSIONS if ext not in ['.mp3', '.wav']]

    # Check both original and sanitized versions for robustness
    # Prioritize exact match (case-insensitive) before sanitized match
    name_variants_to_check = [sound_name]
    sanitized = sanitize_filename(sound_name)
    # Add sanitized name only if it's different and valid
    if sanitized and sanitized != sound_name:
        name_variants_to_check.append(sanitized)

    for name_variant in name_variants_to_check:
        try:
            # Use scandir for potentially better performance on large directories
            with os.scandir(directory) as entries:
                found_paths: Dict[str, str] = {} # Store found paths by extension (lowercase)
                for entry in entries:
                    if entry.is_file():
                        base, file_ext = os.path.splitext(entry.name)
                        file_ext_lower = file_ext.lower()
                        # Case-insensitive comparison of base names
                        if base.lower() == name_variant.lower() and file_ext_lower in ALLOWED_EXTENSIONS:
                             found_paths[file_ext_lower] = entry.path # Store full path

                # Check found paths in preferred order
                for ext in preferred_order:
                    if ext in found_paths:
                        return found_paths[ext] # Return first match in preferred order

        except OSError as e:
            bot_logger.error(f"Error listing files in {directory} during find: {e}")
            return None # Error occurred during directory scan

    return None # Not found after checking all variants and extensions

def _get_sound_files_from_dir(directory: str) -> List[str]:
    """Generic helper to list sound base names (without extension) from a directory."""
    sounds = set() # Use set to avoid duplicates if filename == sanitized_filename
    if os.path.isdir(directory):
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if entry.is_file():
                        base_name, ext = os.path.splitext(entry.name)
                        if ext.lower() in ALLOWED_EXTENSIONS:
                            sounds.add(base_name) # Add base name only
        except OSError as e:
            bot_logger.error(f"Error listing files in {directory}: {e}")
    # Return a sorted list of unique base names
    return sorted(list(sounds), key=str.lower)

def get_user_sound_files(user_id: int) -> List[str]:
    """Lists base names of sound files for a specific user."""
    return _get_sound_files_from_dir(os.path.join(USER_SOUNDS_DIR, str(user_id)))

def find_user_sound_path(user_id: int, sound_name: str) -> Optional[str]:
    """Finds the full path for a user's sound by base name."""
    return _find_sound_path_in_dir(os.path.join(USER_SOUNDS_DIR, str(user_id)), sound_name)

def get_public_sound_files() -> List[str]:
    """Lists base names of public sound files."""
    return _get_sound_files_from_dir(PUBLIC_SOUNDS_DIR)

def find_public_sound_path(sound_name: str) -> Optional[str]:
    """Finds the full path for a public sound by base name."""
    return _find_sound_path_in_dir(PUBLIC_SOUNDS_DIR, sound_name)


# --- Autocomplete Helper ---
async def _generic_sound_autocomplete(ctx: discord.AutocompleteContext, source_func: Callable[..., List[str]], *args) -> List[discord.OptionChoice]:
    """Generic autocomplete handler returning OptionChoices from a sound list function."""
    try:
        # Get the list of sound base names using the provided function
        sounds = source_func(*args)
        current_value = ctx.value.lower() if ctx.value else ""

        # Filter and sort suggestions: prioritize matches starting with input
        starts_with = []
        contains = []
        for name in sounds:
             lower_name = name.lower()
             if lower_name.startswith(current_value):
                 # Truncate name if > 100 chars for OptionChoice limit
                 display_name = name if len(name) <= 100 else name[:97] + "..."
                 starts_with.append(discord.OptionChoice(name=display_name, value=name))
             elif current_value in lower_name:
                 display_name = name if len(name) <= 100 else name[:97] + "..."
                 contains.append(discord.OptionChoice(name=display_name, value=name))

        # Sort each list alphabetically (case-insensitive)
        starts_with.sort(key=lambda c: c.name.lower())
        contains.sort(key=lambda c: c.name.lower())

        # Combine lists (starts_with first) and limit to Discord's max (25)
        suggestions = (starts_with + contains)[:25]
        return suggestions

    except Exception as e:
         # Log error but return empty list to avoid breaking autocomplete
         bot_logger.error(f"Error during sound autocomplete ({source_func.__name__} for user {ctx.interaction.user.id}): {e}", exc_info=True)
         return []

async def user_sound_autocomplete(ctx: discord.AutocompleteContext) -> List[discord.OptionChoice]:
    """Autocomplete for user's personal sounds."""
    # Ensure user ID is passed correctly
    return await _generic_sound_autocomplete(ctx, get_user_sound_files, ctx.interaction.user.id)

async def public_sound_autocomplete(ctx: discord.AutocompleteContext) -> List[discord.OptionChoice]:
    """Autocomplete for public sounds."""
    return await _generic_sound_autocomplete(ctx, get_public_sound_files)

async def tts_voice_autocomplete(ctx: discord.AutocompleteContext) -> List[discord.OptionChoice]:
    """Autocomplete for Edge-TTS voices using the FULL pre-generated list."""
    try:
        current_value = ctx.value.lower() if ctx.value else ""
        # Filter the FULL list based on display name or voice ID containing input
        suggestions = [
            choice for choice in FULL_EDGE_TTS_VOICE_CHOICES
            if current_value in choice.name.lower() or current_value in choice.value.lower()
        ]
        # Return top 25 matches (list is already sorted by name)
        return suggestions[:25]
    except Exception as e:
        bot_logger.error(f"Error during TTS voice autocomplete for user {ctx.interaction.user.id}: {e}", exc_info=True)
        return []


# --- File Upload Validation Helper ---
async def _validate_and_save_upload(
    ctx: discord.ApplicationContext,
    sound_file: discord.Attachment,
    target_save_path: str, # The FINAL desired path for the file
    command_name: str = "upload"
) -> Tuple[bool, Optional[str]]:
    """
    Validates attachment (type, size), saves temporarily, checks with Pydub,
    moves/renames to final path if valid.
    Returns (success_bool, error_message_or_None). Sends NO user feedback itself.
    """
    user_id = ctx.author.id
    log_prefix = f"{command_name.upper()} VALIDATION"

    # 1. Basic File Checks
    file_extension = os.path.splitext(sound_file.filename)[1].lower()
    if file_extension not in ALLOWED_EXTENSIONS:
        bot_logger.warning(f"{log_prefix}: User {user_id} tried invalid extension '{file_extension}'.")
        return False, f"❌ Invalid file type (`{file_extension}`). Allowed: {', '.join(ALLOWED_EXTENSIONS)}"

    if sound_file.size > MAX_USER_SOUND_SIZE_MB * 1024 * 1024:
        bot_logger.warning(f"{log_prefix}: User {user_id} tried oversized file '{sound_file.filename}' ({sound_file.size / (1024*1024):.2f} MB).")
        return False, f"❌ File too large (`{sound_file.size / (1024*1024):.2f}` MB). Max: {MAX_USER_SOUND_SIZE_MB}MB."

    # Log potentially incorrect content type but don't reject based on it alone
    if not sound_file.content_type or not sound_file.content_type.startswith('audio/'):
        bot_logger.warning(f"{log_prefix}: Content-Type '{sound_file.content_type}' for '{sound_file.filename}' (user: {user_id}) not 'audio/*'. Proceeding with caution.")

    # 2. Temporary Save
    # Save temp file in the *target directory* to avoid cross-filesystem move issues later
    temp_save_dir = os.path.dirname(target_save_path)
    ensure_dir(temp_save_dir) # Ensure final directory exists for temp file
    # Create a unique temporary filename
    temp_save_filename = f"temp_{command_name}_{user_id}_{os.urandom(4).hex()}{file_extension}"
    temp_save_path = os.path.join(temp_save_dir, temp_save_filename)

    # Helper to ensure temporary file is cleaned up on errors
    async def cleanup_temp():
        if os.path.exists(temp_save_path):
            try:
                os.remove(temp_save_path)
                bot_logger.debug(f"Cleaned up temporary file: {temp_save_path}")
            except Exception as del_e:
                bot_logger.warning(f"Failed to clean up temporary file '{temp_save_path}': {del_e}")

    try:
        await sound_file.save(temp_save_path)
        bot_logger.info(f"{log_prefix}: Saved temporary file for {user_id}: '{temp_save_path}'")

        # 3. Pydub Validation (Crucial)
        try:
            bot_logger.debug(f"{log_prefix}: Pydub decode check starting for: '{temp_save_path}'")
            # Explicitly provide format hint to Pydub
            audio_format = file_extension.strip('.') if file_extension else None # Handle no extension case
            if not audio_format: # Default if no extension
                bot_logger.warning(f"{log_prefix}: No file extension for Pydub check, trying auto-detection.")
                audio = AudioSegment.from_file(temp_save_path)
            else:
                audio = AudioSegment.from_file(temp_save_path, format=audio_format)

            # Optional: Add duration check here if needed using MAX_PLAYBACK_DURATION_MS
            # if len(audio) > MAX_PLAYBACK_DURATION_MS * 1.1: # Allow slightly longer uploads
            #    await cleanup_temp()
            #    return False, f"❌ Audio duration too long (>{MAX_PLAYBACK_DURATION_MS/1000}s)."

            bot_logger.info(f"{log_prefix}: Pydub validation OK for '{temp_save_path}' (Duration: {len(audio)}ms)")

            # 4. Move/Rename validated file to final destination
            try:
                # Use os.replace for atomic move/rename where possible (preferred)
                # This overwrites target_save_path if it exists
                os.replace(temp_save_path, target_save_path)
                bot_logger.info(f"{log_prefix}: Final file saved (atomic replace/rename): '{target_save_path}'")
                return True, None # Success! Temp file is now the final file.

            except OSError as rep_e:
                # Fallback to shutil.move if os.replace fails (e.g., diff filesystem, perms)
                # shutil.move might fail if target_save_path exists, so remove it first
                bot_logger.warning(f"{log_prefix}: os.replace failed ('{rep_e}'), trying shutil.move for '{temp_save_path}' -> '{target_save_path}'.")
                try:
                    if os.path.exists(target_save_path):
                        os.remove(target_save_path) # Remove existing target before move
                    shutil.move(temp_save_path, target_save_path)
                    bot_logger.info(f"{log_prefix}: Final file saved (fallback move): '{target_save_path}'")
                    return True, None # Success!
                except Exception as move_e:
                    bot_logger.error(f"{log_prefix}: FAILED final save (replace error: {rep_e}, fallback move error: {move_e})", exc_info=True)
                    await cleanup_temp() # Clean up temp file on move failure
                    return False, "❌ Error saving the sound file after validation."

        except CouldntDecodeError as decode_error:
            # Log detailed error if Pydub fails
            bot_logger.error(f"{log_prefix}: FAILED (Pydub Decode - user: {user_id}, file: '{sound_file.filename}'): {decode_error}", exc_info=True)
            await cleanup_temp()
            return False, f"❌ **Audio Validation Failed!** Could not process `{sound_file.filename}`. It might be corrupted or in an unsupported format. Ensure FFmpeg is installed and accessible by the bot if needed for this file type ({file_extension})."
        except Exception as validate_e:
            # Catch other unexpected errors during Pydub processing
            bot_logger.error(f"{log_prefix}: FAILED (Unexpected Pydub check error - user: {user_id}, file: '{sound_file.filename}'): {validate_e}", exc_info=True)
            await cleanup_temp()
            return False, "❌ **Audio Validation Failed!** An unexpected error occurred during audio processing."

    except discord.HTTPException as e:
        # Error downloading the file from Discord
        bot_logger.error(f"{log_prefix}: Error downloading temp file for {user_id} ('{sound_file.filename}'): {e}", exc_info=True)
        await cleanup_temp() # Attempt cleanup even if download failed
        return False, "❌ Error downloading the sound file from Discord."
    except Exception as e:
        # Error during the initial save of the temporary file
        bot_logger.error(f"{log_prefix}: Unexpected error during initial temp save for {user_id} ('{sound_file.filename}'): {e}", exc_info=True)
        await cleanup_temp()
        return False, "❌ An unexpected server error occurred during file handling."


# --- Slash Commands ---

# === Join Sound Commands ===
@bot.slash_command(name="setjoinsound", description="Upload your custom join sound. Replaces existing.")
@commands.cooldown(1, 15, commands.BucketType.user) # Cooldown per user
async def setjoinsound(
    ctx: discord.ApplicationContext,
    sound_file: discord.Option(discord.Attachment, description=f"Sound file ({', '.join(ALLOWED_EXTENSIONS)}). Max {MAX_USER_SOUND_SIZE_MB}MB.", required=True) # type: ignore
):
    """Allows a user to upload and set their custom join sound."""
    await ctx.defer(ephemeral=True) # Defer response ephemerally
    author = ctx.author
    user_id_str = str(author.id)
    bot_logger.info(f"COMMAND: /setjoinsound by {author.name} ({user_id_str}), filename: '{sound_file.filename}'")

    # Determine final filename and path
    file_extension = os.path.splitext(sound_file.filename)[1].lower()
    # Use a consistent naming scheme for join sounds (easier cleanup)
    final_save_filename = f"joinsound_{user_id_str}{file_extension}"
    final_save_path = os.path.join(SOUNDS_DIR, final_save_filename)

    # Get current config to find old filename for cleanup
    user_config = user_sound_config.get(user_id_str, {})
    old_config_filename = user_config.get("join_sound")

    # Validate and save the uploaded file
    success, error_msg = await _validate_and_save_upload(ctx, sound_file, final_save_path, command_name="setjoinsound")

    if success:
        bot_logger.info(f"Join sound validation successful for {author.name}, saved to '{final_save_path}'")
        # Remove old join sound file if it existed and had a different name (e.g., different extension)
        if old_config_filename and old_config_filename != final_save_filename:
            old_path = os.path.join(SOUNDS_DIR, old_config_filename)
            if os.path.exists(old_path):
                try:
                    os.remove(old_path)
                    bot_logger.info(f"Removed previous join sound file: '{old_path}' for user {user_id_str}")
                except Exception as e:
                    bot_logger.warning(f"Could not remove previous join sound file '{old_path}' for user {user_id_str}: {e}")

        # Update user config with the new filename
        user_config["join_sound"] = final_save_filename
        user_sound_config[user_id_str] = user_config
        save_config() # Persist changes
        bot_logger.info(f"Updated join sound config for {author.name} to '{final_save_filename}'")
        await ctx.followup.send(f"✅ Success! Your join sound is set to `{sound_file.filename}`. The bot will now use this instead of TTS.", ephemeral=True)
    else:
        # Send the error message from validation helper
        await ctx.followup.send(error_msg or "❌ An unknown error occurred during validation.", ephemeral=True)


@bot.slash_command(name="removejoinsound", description="Remove your custom join sound, revert to TTS.")
@commands.cooldown(1, 5, commands.BucketType.user)
async def removejoinsound(ctx: discord.ApplicationContext):
    """Removes the user's custom join sound configuration and file."""
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id_str = str(author.id)
    bot_logger.info(f"COMMAND: /removejoinsound by {author.name} ({user_id_str})")

    user_config = user_sound_config.get(user_id_str)
    # Check if user actually has a custom sound configured
    if user_config and "join_sound" in user_config:
        filename_to_remove = user_config.pop("join_sound") # Remove key from dict
        bot_logger.info(f"Removing join sound config for {author.name} (was '{filename_to_remove}')")

        # If removing join_sound makes the user config empty, remove the user entry entirely
        if not user_config:
            if user_id_str in user_sound_config:
                del user_sound_config[user_id_str]
                bot_logger.info(f"Removed empty user config entry for {author.name} after join sound removal.")
        save_config() # Save the updated config

        # Remove the actual sound file from SOUNDS_DIR
        file_path_to_remove = os.path.join(SOUNDS_DIR, filename_to_remove)
        removed_custom = False
        if os.path.exists(file_path_to_remove):
            try:
                os.remove(file_path_to_remove)
                bot_logger.info(f"Deleted file: '{file_path_to_remove}' (custom join sound for {user_id_str})")
                removed_custom = True
            except OSError as e:
                bot_logger.warning(f"Could not delete file '{file_path_to_remove}' during join sound removal: {e}")
        else:
             # Log if the configured file was already missing
             bot_logger.warning(f"Configured join sound '{filename_to_remove}' not found at '{file_path_to_remove}' during removal for user {user_id_str}.")

        # Clean up potentially orphaned temporary join TTS files (best effort)
        prefix_to_clean = f"tts_join_{user_id_str}_"
        cleaned_temp_count = 0
        try:
            with os.scandir(SOUNDS_DIR) as entries:
                for entry in entries:
                    if entry.is_file() and entry.name.startswith(prefix_to_clean) and entry.name.endswith(".mp3"):
                         try:
                             os.remove(entry.path)
                             cleaned_temp_count += 1
                             bot_logger.debug(f"Cleaned up old temp join TTS: {entry.name}")
                         except OSError as e_clean:
                             bot_logger.warning(f"Could not clean temp TTS file '{entry.path}': {e_clean}")
        except OSError as e_list:
             bot_logger.warning(f"Could not list SOUNDS_DIR for TTS cleanup: {e_list}")

        # Send confirmation message
        msg = "🗑️ Custom join sound removed."
        if cleaned_temp_count > 0: msg += f" Cleaned up {cleaned_temp_count} cached join TTS file(s)."
        msg += " The bot will now use TTS to announce your name when you join." # Updated message
        await ctx.followup.send(msg, ephemeral=True)
    else:
        # User didn't have a custom sound set
        await ctx.followup.send("🤷 You don't have a custom join sound configured. The bot uses TTS for your join message by default.", ephemeral=True)

# === User Command Sound / Soundboard Commands ===
@bot.slash_command(name="uploadsound", description=f"Upload a sound (personal/public). Limit: {MAX_USER_SOUNDS_PER_USER} personal.")
@commands.cooldown(2, 20, commands.BucketType.user) # Allow 2 uploads per 20 sec
async def uploadsound(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Short name (letters, numbers, underscore). Will be sanitized.", required=True), # type: ignore
    sound_file: discord.Option(discord.Attachment, description=f"Sound ({', '.join(ALLOWED_EXTENSIONS)}). Max {MAX_USER_SOUND_SIZE_MB}MB.", required=True), # type: ignore
    make_public: discord.Option(bool, description="Make available for everyone? (Default: False)", default=False) # type: ignore
):
    """Allows users to upload personal or public sounds."""
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id = author.id
    bot_logger.info(f"COMMAND: /uploadsound by {author.name} ({user_id}), name: '{name}', public: {make_public}, file: '{sound_file.filename}'")

    # Sanitize the desired name
    clean_name = sanitize_filename(name)
    if not clean_name:
        await ctx.followup.send("❌ Invalid name provided. Please use letters, numbers, or underscores.", ephemeral=True); return
    # Add note if name was changed
    followup_prefix = f"ℹ️ Name sanitized to `{clean_name}`.\n" if clean_name != name else ""

    file_extension = os.path.splitext(sound_file.filename)[1].lower()
    # Final filename uses the sanitized base name + original extension
    final_filename = f"{clean_name}{file_extension}"

    if make_public:
        target_dir = PUBLIC_SOUNDS_DIR
        ensure_dir(target_dir) # Ensure public dir exists
        # Check if public sound with this *sanitized* name already exists (using helper)
        if find_public_sound_path(clean_name):
            await ctx.followup.send(f"{followup_prefix}❌ A public sound named `{clean_name}` already exists.", ephemeral=True); return
        replacing_personal = False # Cannot replace personal when uploading public
        scope = "public"
    else:
        # Personal sound
        target_dir = os.path.join(USER_SOUNDS_DIR, str(user_id))
        ensure_dir(target_dir) # Ensure user's personal directory exists
        # Check if user already has a sound with this sanitized name
        existing_personal_path = find_user_sound_path(user_id, clean_name)
        replacing_personal = existing_personal_path is not None
        # Check personal sound limit only if adding a *new* sound (not replacing)
        if not replacing_personal and len(get_user_sound_files(user_id)) >= MAX_USER_SOUNDS_PER_USER:
             await ctx.followup.send(f"{followup_prefix}❌ You have reached the maximum limit of {MAX_USER_SOUNDS_PER_USER} personal sounds. Use `/deletesound` to remove some.", ephemeral=True); return
        scope = "personal"

    # Define the final full path for the sound
    final_path = os.path.join(target_dir, final_filename)

    # Validate the upload (size, type, pydub check) and save if valid
    success, error_msg = await _validate_and_save_upload(ctx, sound_file, final_path, command_name="uploadsound")

    if success:
        bot_logger.info(f"Sound validation successful for {author.name}, saved to '{final_path}' ({scope})")
        # Handle removing old personal sound file if it was replaced (e.g., with different extension)
        if replacing_personal and not make_public and existing_personal_path:
            # Compare full paths to see if the file actually changed (not just metadata)
            if existing_personal_path != final_path and os.path.exists(existing_personal_path):
                 try:
                     os.remove(existing_personal_path)
                     bot_logger.info(f"Removed old personal sound file '{os.path.basename(existing_personal_path)}' for user {user_id} due to replacement with different extension.")
                 except Exception as e:
                     bot_logger.warning(f"Could not remove old personal sound file '{existing_personal_path}' during replacement: {e}")

        # Construct success message
        action = "updated" if replacing_personal and not make_public else "uploaded"
        play_cmd = "playpublic" if make_public else "playsound"
        list_cmd = "publicsounds" if make_public else "mysounds"
        msg = f"{followup_prefix}✅ Success! Sound `{clean_name}` {action} as {scope}.\n"
        msg += f"Use `/{play_cmd} name:{clean_name}`"
        # Add extra command info based on scope
        if not make_public: msg += f", `/{list_cmd}`, `/soundpanel`, or make it public with `/publishsound name:{clean_name}`."
        else: msg += f" or list with `/{list_cmd}`."
        await ctx.followup.send(msg, ephemeral=True)
    else:
        # Send the error message from validation helper
        await ctx.followup.send(f"{followup_prefix}{error_msg or '❌ An unknown error occurred during validation.'}", ephemeral=True)


@bot.slash_command(name="mysounds", description="Lists your personal uploaded sounds.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def mysounds(ctx: discord.ApplicationContext):
    """Lists the calling user's uploaded personal sounds."""
    await ctx.defer(ephemeral=True)
    author = ctx.author
    bot_logger.info(f"COMMAND: /mysounds by {author.name} ({author.id})")
    user_sounds = get_user_sound_files(author.id) # Returns sorted list of base names

    if not user_sounds:
        await ctx.followup.send("You haven't uploaded any personal sounds yet. Use `/uploadsound`!", ephemeral=True); return

    # Paginate if the list is long (more robust than simple string joining)
    items_per_page = 20 # Number of sounds per embed page
    pages_content = []
    current_page_lines = []
    for i, name in enumerate(user_sounds):
        # Format each sound name clearly
        current_page_lines.append(f"- `{name}`")
        # If page is full or it's the last item, add page content and reset lines
        if (i + 1) % items_per_page == 0 or i == len(user_sounds) - 1:
            pages_content.append("\n".join(current_page_lines))
            current_page_lines = []

    # Create embeds for each page
    embeds = []
    total_sounds = len(user_sounds)
    num_pages = len(pages_content)
    for page_num, page_text in enumerate(pages_content):
        embed = discord.Embed(
            title=f"{author.display_name}'s Sounds ({total_sounds}/{MAX_USER_SOUNDS_PER_USER})",
            description=f"Use `/playsound`, `/soundpanel`, or `/publishsound`.\n\n{page_text}",
            color=discord.Color.blurple()
        )
        footer_text = "Use /deletesound to remove."
        if num_pages > 1:
            footer_text += f" | Page {page_num + 1}/{num_pages}"
        embed.set_footer(text=footer_text)
        embeds.append(embed)

    # Send the first page (add pagination View later if needed)
    # TODO: Implement pagination using discord.ui.View for multiple pages
    if embeds:
        await ctx.followup.send(embed=embeds[0], ephemeral=True)
    else: # Should not happen if user_sounds is not empty, but safety check
        await ctx.followup.send("Could not generate sound list.", ephemeral=True)


@bot.slash_command(name="deletesound", description="Deletes one of your PERSONAL sounds.")
@commands.cooldown(1, 5, commands.BucketType.user)
async def deletesound(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Name of the personal sound to delete.", required=True, autocomplete=user_sound_autocomplete) # type: ignore
):
    """Deletes one of the user's personal sounds by name."""
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id = author.id
    bot_logger.info(f"COMMAND: /deletesound by {author.name} ({user_id}), target sound name: '{name}'")

    # Find the sound path (handles sanitized names internally)
    sound_path = find_user_sound_path(user_id, name)

    if not sound_path:
        await ctx.followup.send(f"❌ Personal sound `{name}` not found. Use `/mysounds` to see your available sounds.", ephemeral=True); return

    # Get the actual base name from the found file for user feedback
    sound_base_name = os.path.splitext(os.path.basename(sound_path))[0]

    # Security check: Ensure the path is within the user's designated directory
    user_dir_abs = os.path.abspath(os.path.join(USER_SOUNDS_DIR, str(user_id)))
    resolved_path_abs = os.path.abspath(sound_path)
    # Check if the resolved path starts with the user's directory path + separator
    if not resolved_path_abs.startswith(user_dir_abs + os.sep):
         bot_logger.critical(f"CRITICAL SECURITY ALERT: Path traversal attempt in /deletesound. User: {user_id}, Input: '{name}', Resolved Path: '{resolved_path_abs}'")
         await ctx.followup.send("❌ Internal security error preventing deletion.", ephemeral=True); return

    # Attempt to delete the file
    try:
        os.remove(sound_path)
        bot_logger.info(f"Deleted PERSONAL sound file '{os.path.basename(sound_path)}' for user {user_id}.")
        await ctx.followup.send(f"🗑️ Personal sound `{sound_base_name}` deleted successfully.", ephemeral=True)
    except OSError as e:
        # Handle file system errors (e.g., permissions)
        bot_logger.error(f"Failed to delete personal sound file '{sound_path}' for user {user_id}: {e}", exc_info=True)
        await ctx.followup.send(f"❌ Failed to delete `{sound_base_name}`: Could not remove file ({type(e).__name__}). Check permissions?", ephemeral=True)
    except Exception as e:
        # Handle other unexpected errors
        bot_logger.error(f"Unexpected error deleting personal sound '{sound_path}' for user {user_id}: {e}", exc_info=True)
        await ctx.followup.send(f"❌ An unexpected error occurred while deleting `{sound_base_name}`.", ephemeral=True)


@bot.slash_command(name="playsound", description="Plays one of your PERSONAL sounds in your current VC.")
@commands.cooldown(1, 4, commands.BucketType.user) # Allow playing every 4 seconds
async def playsound(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Name of the personal sound to play.", required=True, autocomplete=user_sound_autocomplete) # type: ignore
):
    """Plays one of the user's personal sounds by name."""
    # Defer publicly so the "Playing..." message can be shown later via edit
    await ctx.defer()
    author = ctx.author
    bot_logger.info(f"COMMAND: /playsound by {author.name} ({author.id}), requested sound: '{name}'")

    # Find the sound path (handles sanitized names)
    sound_path = find_user_sound_path(author.id, name)

    if not sound_path:
        # Edit the deferred response to show the error
        await ctx.edit_original_response(content=f"❌ Personal sound `{name}` not found. Use `/mysounds` or `/soundpanel`."); return

    # Pass the interaction to the playback function so it can edit the response
    await play_single_sound(ctx.interaction, sound_path)


# --- Sound Panel View ---
class UserSoundboardView(discord.ui.View):
    """A view containing buttons for a user's personal sounds."""
    def __init__(self, user_id: int, *, timeout: Optional[float] = 600.0): # 10 min timeout
        super().__init__(timeout=timeout)
        self.user_id = user_id
        # Store the message object to edit it on timeout
        self.message: Optional[discord.InteractionMessage | discord.WebhookMessage] = None
        self.populate_buttons()

    def populate_buttons(self):
        """Adds buttons for each valid sound file found for the user."""
        user_dir = os.path.join(USER_SOUNDS_DIR, str(self.user_id))
        bot_logger.debug(f"Populating sound panel for user {self.user_id} from: {user_dir}")

        # Handle case where user directory doesn't exist
        if not os.path.isdir(user_dir):
            self.add_item(discord.ui.Button(label="No sounds uploaded yet!", style=discord.ButtonStyle.secondary, disabled=True, custom_id=f"usersb_placeholder_nosounds_{self.user_id}"))
            return

        sounds_found_count = 0
        button_row = 0
        max_buttons_per_row = 5
        max_rows = 5 # Discord limit of 5 Action Rows
        max_buttons_total = max_buttons_per_row * max_rows

        try:
            # Get the sorted list of sound base names for the user
            sound_names = get_user_sound_files(self.user_id)
        except Exception as e:
            bot_logger.error(f"Error getting sound files for panel population (user {self.user_id}): {e}")
            self.add_item(discord.ui.Button(label="Error Reading Sounds", style=discord.ButtonStyle.danger, disabled=True, custom_id=f"usersb_placeholder_error_{self.user_id}"))
            return

        # Iterate through the sound names and create buttons
        for base_name in sound_names:
            # Stop if we've hit the absolute max number of components (25)
            if len(self.children) >= 25:
                bot_logger.warning(f"Max component limit (25) reached for user {self.user_id} panel. Skipping remaining sounds starting with '{base_name}'.")
                # Optionally add a "..." button here if desired
                break
            # Stop if we've hit the practical button limit based on rows/cols
            if sounds_found_count >= max_buttons_total:
                bot_logger.warning(f"Button limit ({max_buttons_total}) reached for user {self.user_id} panel. Sound '{base_name}' and subsequent sounds skipped.")
                # Optionally add a "More..." button for pagination
                break

            # Find the actual file path to get the extension for the custom_id
            sound_path = find_user_sound_path(self.user_id, base_name)
            if not sound_path:
                bot_logger.warning(f"Could not find path for listed sound '{base_name}' during panel population for user {self.user_id}. Skipping.")
                continue

            filename_with_ext = os.path.basename(sound_path)

            # Create button label (use base name, replace underscores, truncate)
            label = base_name.replace("_", " ")
            if len(label) > 78: label = label[:77] + "…" # Max label length is 80

            # Create custom_id: prefix + filename with extension
            # This allows reliable reconstruction of the path in the callback
            custom_id = f"usersb_play:{filename_with_ext}"
            if len(custom_id) > 100: # Discord custom_id limit (100 chars)
                bot_logger.warning(f"Skipping sound '{filename_with_ext}' for {self.user_id} panel: custom_id too long after prefix.")
                continue

            # Create the button and add it to the view
            button = discord.ui.Button(
                label=label,
                style=discord.ButtonStyle.secondary, # Use secondary for less visual noise
                custom_id=custom_id,
                row=button_row # Assign button to the current row
            )
            button.callback = self.user_soundboard_button_callback # Assign the callback function
            self.add_item(button)
            sounds_found_count += 1

            # Move to the next row if the current one is full
            if sounds_found_count > 0 and sounds_found_count % max_buttons_per_row == 0:
                button_row += 1
                # Stop if max rows reached (should be covered by max_buttons_total)
                if button_row >= max_rows:
                    bot_logger.warning(f"Row limit ({max_rows}) reached for user {self.user_id} panel. Skipping remaining files starting with '{base_name}'.")
                    break

        # If after iterating, no valid sounds were added, add a placeholder
        if sounds_found_count == 0:
             bot_logger.info(f"No valid sounds found to add to panel for user {self.user_id} in '{user_dir}'.")
             # Add placeholder only if no error placeholder exists and view is empty
             if not any(item.custom_id.startswith("usersb_placeholder_error_") for item in self.children) and not self.children:
                self.add_item(discord.ui.Button(label="No sounds uploaded yet!", style=discord.ButtonStyle.secondary, disabled=True, custom_id=f"usersb_placeholder_nosounds_{self.user_id}"))

    async def user_soundboard_button_callback(self, interaction: discord.Interaction):
        """Callback executed when a user clicks a sound button on their panel."""
        # Security Check: Ensure the interaction user is the owner of this panel
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("✋ This is not your personal sound panel!", ephemeral=True)
            return

        custom_id = interaction.data["custom_id"]
        user = interaction.user # Should be the same as self.user_id here
        bot_logger.info(f"USER PANEL: Button '{custom_id}' clicked by {user.name} on panel for {self.user_id}")

        # Respond to the interaction BEFORE the potentially long-running audio task
        # Use defer() which shows "Bot is thinking..." publicly
        await interaction.response.defer()

        # Ensure custom_id format is correct
        if not custom_id.startswith("usersb_play:"):
            bot_logger.error(f"Invalid custom_id format from user panel button: '{custom_id}'")
            await interaction.edit_original_response(content="❌ Internal error: Invalid button ID."); return

        # Extract filename (including extension) from custom_id
        sound_filename = custom_id.split(":", 1)[1]
        # Reconstruct the full path to the sound file
        sound_path = os.path.join(USER_SOUNDS_DIR, str(self.user_id), sound_filename)

        # Call the main playback function, passing the interaction context
        await play_single_sound(interaction, sound_path)

    async def on_timeout(self):
        """Called when the view times out (no interaction for the specified duration)."""
        if self.message:
            bot_logger.debug(f"User sound panel timed out for {self.user_id} (message ID: {self.message.id})")
            # Try to get the owner's display name gracefully
            owner_name = f"User {self.user_id}" # Fallback name
            try:
                 panel_owner = None
                 # Try getting member from guild context if message has it
                 if hasattr(self.message, 'guild') and self.message.guild:
                     panel_owner = self.message.guild.get_member(self.user_id)
                 # Fallback to fetching user if not found or no guild context (requires bot to be ready)
                 if not panel_owner and bot.is_ready():
                     panel_owner = await bot.fetch_user(self.user_id)
                 if panel_owner: owner_name = panel_owner.display_name
            except discord.NotFound: bot_logger.warning(f"Could not fetch panel owner {self.user_id} (NotFound) for timeout.")
            except discord.HTTPException as e: bot_logger.warning(f"Could not fetch panel owner {self.user_id} (HTTP {e.status}) for timeout.")
            except Exception as e: bot_logger.warning(f"Could not fetch panel owner {self.user_id} for timeout: {e}")

            # Disable all buttons in the view
            for item in self.children:
                if isinstance(item, discord.ui.Button):
                    item.disabled = True
            try:
                # Edit the original message to show expired state and disabled buttons
                # message.edit works for both InteractionMessage and WebhookMessage types
                await self.message.edit(content=f"🔊 **{owner_name}'s Personal Panel (Expired)**", view=self)
            except discord.HTTPException as e:
                # Log error if editing fails (e.g., message deleted, insufficient perms)
                bot_logger.warning(f"Failed to edit expired panel message {self.message.id} for {self.user_id}: {e}")
            except Exception as e:
                 bot_logger.error(f"Unexpected error editing expired panel {self.message.id} for {self.user_id}: {e}", exc_info=True)
        else:
            # This case should be rare if message reference is always stored
            bot_logger.debug(f"User panel timed out for {self.user_id} but no message reference was stored.")


@bot.slash_command(name="soundpanel", description="Displays buttons to play YOUR personal sounds in your VC.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def soundpanel(ctx: discord.ApplicationContext):
    """Displays an interactive panel with buttons for the user's personal sounds."""
    # Defer publicly so the panel is visible while buttons are generated
    await ctx.defer()
    author = ctx.author
    bot_logger.info(f"COMMAND: /soundpanel invoked by {author.name} ({author.id})")

    # Create the view, which populates buttons in its __init__
    view = UserSoundboardView(user_id=author.id, timeout=600.0) # 10 min timeout

    # Check if any playable buttons were actually added to the view
    has_playable_buttons = any(
        isinstance(item, discord.ui.Button) and not item.disabled and item.custom_id and item.custom_id.startswith("usersb_play:")
        for item in view.children
    )

    if not has_playable_buttons:
         # Check if a placeholder message (no sounds / error) was added instead
         is_placeholder = any(
            isinstance(item, discord.ui.Button) and item.disabled and item.custom_id and item.custom_id.startswith("usersb_placeholder_")
            for item in view.children
         )
         if is_placeholder:
             # Determine which placeholder message to show
             no_sounds_msg = "You haven't uploaded any personal sounds yet. Use `/uploadsound`!"
             error_msg = "Error loading your sounds. Please try again later or contact an admin if the issue persists."
             # Check the custom ID of the placeholder button
             placeholder_id = next((item.custom_id for item in view.children if item.custom_id.startswith("usersb_placeholder_")), None)
             content = no_sounds_msg if placeholder_id and "nosounds" in placeholder_id else error_msg
             await ctx.edit_original_response(content=content, view=None) # Show message, remove view
         else:
             # Should not happen if populate_buttons works correctly, but handle defensively
              await ctx.edit_original_response(content="Could not generate the sound panel. No sounds found or an error occurred.", view=None)
         return

    # If playable buttons exist, send the panel
    msg_content = f"🔊 **{author.display_name}'s Personal Sound Panel** - Click to play!"
    try:
        # Send the panel using edit_original_response as we deferred publicly
        # This returns the message object (can be InteractionMessage or WebhookMessage)
        message = await ctx.interaction.edit_original_response(content=msg_content, view=view)
        # Store the message reference in the view for timeout editing
        view.message = message

    except Exception as e:
        # Handle errors during sending/editing the panel message
        bot_logger.error(f"Failed to send soundpanel for user {author.id}: {e}", exc_info=True)
        # Try sending an ephemeral error if the public message failed
        try: await ctx.interaction.edit_original_response(content="❌ Failed to create the sound panel.", view=None)
        except Exception: pass # Ignore errors trying to send the error message


# === Public Sound Commands ===
@bot.slash_command(name="publishsound", description="Make one of your personal sounds public for everyone.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def publishsound(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Name of YOUR personal sound to make public.", required=True, autocomplete=user_sound_autocomplete) # type: ignore
):
    """Makes one of the user's personal sounds available publicly."""
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id = author.id
    bot_logger.info(f"COMMAND: /publishsound by {author.name} ({user_id}), target sound name: '{name}'")

    # Find the user's personal sound path
    user_path = find_user_sound_path(user_id, name)

    if not user_path:
        await ctx.followup.send(f"❌ Personal sound `{name}` not found. Use `/mysounds` to check.", ephemeral=True); return

    # Get the actual base name and extension from the found file
    source_filename = os.path.basename(user_path)
    source_base_name, source_ext = os.path.splitext(source_filename)

    # Sanitize the *original requested name* for the public filename's base
    # This ensures consistency if the user typed a name that needed sanitizing
    public_base_name = sanitize_filename(name)
    if not public_base_name: # Check if sanitization resulted in empty string
        await ctx.followup.send(f"❌ Invalid public name after sanitization (from '{name}').", ephemeral=True); return

    # Construct the target public filename and path
    public_filename = f"{public_base_name}{source_ext}" # Use sanitized base + original extension
    public_path = os.path.join(PUBLIC_SOUNDS_DIR, public_filename)

    # Check if a public sound with the target *public base name* already exists
    if find_public_sound_path(public_base_name):
        await ctx.followup.send(f"❌ A public sound named `{public_base_name}` already exists. Choose a different name.", ephemeral=True); return

    # Copy the file from user's dir to public dir
    try:
        ensure_dir(PUBLIC_SOUNDS_DIR) # Ensure public dir exists
        shutil.copy2(user_path, public_path) # copy2 preserves metadata like modification time
        bot_logger.info(f"SOUND PUBLISHED: Copied '{user_path}' to '{public_path}' by {author.name}.")
        # Notify user of success, showing original and published names
        await ctx.followup.send(
            f"✅ Sound `{source_base_name}` published as `{public_base_name}`!\n"
            f"Others can now play it using `/playpublic name:{public_base_name}`.",
            ephemeral=True
        )
    except Exception as e:
        # Handle errors during file copy
        bot_logger.error(f"Failed to copy user sound '{user_path}' to public '{public_path}': {e}", exc_info=True)
        await ctx.followup.send(f"❌ Failed to publish `{source_base_name}`: An error occurred during copying ({type(e).__name__}).", ephemeral=True)


@bot.slash_command(name="removepublic", description="[Admin Only] Remove a sound from the public collection.")
@commands.has_permissions(manage_guild=True) # Requires 'Manage Server' permission
@commands.cooldown(1, 5, commands.BucketType.guild) # Cooldown per guild
async def removepublic(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Name of the public sound to remove.", required=True, autocomplete=public_sound_autocomplete) # type: ignore
):
    """Allows server admins to remove a public sound."""
    await ctx.defer(ephemeral=True)
    admin = ctx.author
    # Log with guild context if available
    guild_id_log = ctx.guild.id if ctx.guild else "DM_Context"
    bot_logger.info(f"COMMAND: /removepublic by admin {admin.name} ({admin.id}) (context guild: {guild_id_log}), target sound name: '{name}'")

    # Find the public sound path
    public_path = find_public_sound_path(name)

    if not public_path:
        await ctx.followup.send(f"❌ Public sound `{name}` not found. Use `/publicsounds` to check.", ephemeral=True); return

    # Use the actual base name from the found file for user feedback
    public_base_name = os.path.splitext(os.path.basename(public_path))[0]

    # Security check: Ensure path is within the designated PUBLIC_SOUNDS_DIR
    public_dir_abs = os.path.abspath(PUBLIC_SOUNDS_DIR)
    resolved_path_abs = os.path.abspath(public_path)
    if not resolved_path_abs.startswith(public_dir_abs + os.sep):
         bot_logger.critical(f"CRITICAL SECURITY ALERT: Path traversal attempt in /removepublic. Admin: {admin.id}, Input: '{name}', Resolved Path: '{resolved_path_abs}'")
         await ctx.followup.send("❌ Internal security error preventing deletion.", ephemeral=True); return

    # Attempt to delete the public sound file
    try:
        deleted_filename = os.path.basename(public_path)
        os.remove(public_path)
        bot_logger.info(f"ADMIN ACTION: Deleted public sound file '{deleted_filename}' by {admin.name}.")
        await ctx.followup.send(f"🗑️ Public sound `{public_base_name}` deleted successfully.", ephemeral=True)
    except OSError as e:
        # Handle file system errors
        bot_logger.error(f"Admin {admin.name} failed to delete public sound '{public_path}': {e}", exc_info=True)
        await ctx.followup.send(f"❌ Failed to delete public sound `{public_base_name}`: Could not remove file ({type(e).__name__}).", ephemeral=True)
    except Exception as e:
        # Handle other unexpected errors
        bot_logger.error(f"Admin {admin.name} encountered unexpected error deleting public sound '{public_path}': {e}", exc_info=True)
        await ctx.followup.send(f"❌ An unexpected error occurred while deleting public sound `{public_base_name}`.", ephemeral=True)

# Error handler specifically for /removepublic permissions/cooldown
@removepublic.error
async def removepublic_error(ctx: discord.ApplicationContext, error: discord.DiscordException):
    if isinstance(error, commands.MissingPermissions):
        bot_logger.warning(f"User {ctx.author.name} tried /removepublic without Manage Guild permission.")
        # Respond ephemerally as command itself is ephemeral
        await ctx.respond("🚫 You need the `Manage Server` permission to use this command.", ephemeral=True)
    elif isinstance(error, commands.CommandOnCooldown):
         await ctx.respond(f"⏳ This command is on cooldown. Try again in {error.retry_after:.1f}s.", ephemeral=True)
    else:
        # Let the global handler deal with other errors for this command
        await on_application_command_error(ctx, error)


@bot.slash_command(name="publicsounds", description="Lists all available public sounds.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def publicsounds(ctx: discord.ApplicationContext):
    """Lists all available public sounds."""
    await ctx.defer(ephemeral=True)
    bot_logger.info(f"COMMAND: /publicsounds by {ctx.author.name}")
    public_sounds = get_public_sound_files() # Gets sorted list of base names

    if not public_sounds:
        await ctx.followup.send("No public sounds have been added yet. Admins can use `/publishsound` to add sounds from users.", ephemeral=True); return

    # Paginate if list is long
    items_per_page = 20
    pages_content = []
    current_page_lines = []
    for i, name in enumerate(public_sounds):
        current_page_lines.append(f"- `{name}`")
        if (i + 1) % items_per_page == 0 or i == len(public_sounds) - 1:
            pages_content.append("\n".join(current_page_lines))
            current_page_lines = []

    # Create embeds
    embeds = []
    total_sounds = len(public_sounds)
    num_pages = len(pages_content)
    for page_num, page_text in enumerate(pages_content):
         embed = discord.Embed(
             title=f"📢 Public Sounds ({total_sounds})",
             description=f"Use `/playpublic name:<sound_name>`.\n\n{page_text}",
             color=discord.Color.green()
         )
         footer_text = "Admins use /removepublic to remove sounds."
         if num_pages > 1: footer_text += f" | Page {page_num + 1}/{num_pages}"
         embed.set_footer(text=footer_text)
         embeds.append(embed)

    # Send first page (add pagination later if needed)
    # TODO: Implement pagination View if num_pages > 1
    if embeds:
        await ctx.followup.send(embed=embeds[0], ephemeral=True)
    else:
        await ctx.followup.send("Could not generate public sound list.", ephemeral=True)


@bot.slash_command(name="playpublic", description="Plays a public sound in your current voice channel.")
@commands.cooldown(1, 4, commands.BucketType.user) # Allow playing every 4 seconds
async def playpublic(
    ctx: discord.ApplicationContext,
    name: discord.Option(str, description="Name of the public sound to play.", required=True, autocomplete=public_sound_autocomplete) # type: ignore
):
    """Plays a specified public sound."""
    await ctx.defer() # Defer publicly
    author = ctx.author
    bot_logger.info(f"COMMAND: /playpublic by {author.name}, requested sound: '{name}'")

    # Find the public sound path
    public_path = find_public_sound_path(name)

    if not public_path:
        await ctx.edit_original_response(content=f"❌ Public sound `{name}` not found. Use `/publicsounds` to check available sounds."); return

    # Call the playback helper function
    await play_single_sound(ctx.interaction, public_path)


# === TTS Defaults Commands (Edge-TTS) ===
@bot.slash_command(name="setttsdefaults", description="Set your preferred default Edge-TTS voice.")
@commands.cooldown(1, 10, commands.BucketType.user)
async def setttsdefaults(
    ctx: discord.ApplicationContext,
    voice: discord.Option(str, description="Your preferred default voice (uses autocomplete).", required=True, autocomplete=tts_voice_autocomplete, choices=CURATED_EDGE_TTS_VOICE_CHOICES) # type: ignore # Choices uses curated list for quick access
):
    """Sets the user's default TTS voice preference."""
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id_str = str(author.id)
    bot_logger.info(f"COMMAND: /setttsdefaults by {author.name} ({user_id_str}), chosen voice: {voice}")

    # Validate if the provided voice ID is actually in our FULL list
    valid_choice = False
    voice_display_name = voice # Fallback to ID if display name not found
    for choice in FULL_EDGE_TTS_VOICE_CHOICES: # Check against the comprehensive list
        if choice.value == voice:
            valid_choice = True
            voice_display_name = choice.name # Get the formatted display name
            break

    if not valid_choice:
        # This should be rare if autocomplete/choices are used, but handles manual input errors
        await ctx.followup.send(f"❌ Invalid voice ID provided: `{voice}`. Please choose from the list or use autocomplete.", ephemeral=True)
        return

    # Update or create user config entry
    user_config = user_sound_config.setdefault(user_id_str, {})
    # Store only voice in tts_defaults sub-dictionary
    user_config['tts_defaults'] = {'voice': voice}
    save_config() # Persist the change

    await ctx.followup.send(
        f"✅ TTS default voice updated!\n"
        f"• Voice: **{voice_display_name}** (`{voice}`)\n\n"
        f"This voice will be used for `/tts` when you don't specify one, and for your join message if you haven't set a custom sound.",
        ephemeral=True
    )

@bot.slash_command(name="removettsdefaults", description="Remove your custom TTS voice default.")
@commands.cooldown(1, 5, commands.BucketType.user)
async def removettsdefaults(ctx: discord.ApplicationContext):
    """Removes the user's custom TTS voice preference."""
    await ctx.defer(ephemeral=True)
    author = ctx.author
    user_id_str = str(author.id)
    bot_logger.info(f"COMMAND: /removettsdefaults by {author.name} ({user_id_str})")

    user_config = user_sound_config.get(user_id_str)
    # Check if user has TTS defaults configured
    if user_config and 'tts_defaults' in user_config:
        del user_config['tts_defaults'] # Remove the defaults sub-dictionary
        bot_logger.info(f"Removed TTS defaults for {author.name}")

        # If the user config dict becomes empty after removal, remove the user entry
        if not user_config:
            if user_id_str in user_sound_config:
                del user_sound_config[user_id_str]
                bot_logger.info(f"Removed empty user config entry for {author.name} after TTS default removal.")
        save_config() # Save changes

        # Get the display name for the bot's overall default voice for the confirmation message
        default_voice_display = DEFAULT_TTS_VOICE # Fallback
        for choice in FULL_EDGE_TTS_VOICE_CHOICES:
            if choice.value == DEFAULT_TTS_VOICE:
                default_voice_display = choice.name
                break

        await ctx.followup.send(
            f"🗑️ Custom TTS default voice removed.\n"
            f"The bot's default voice (**{default_voice_display}** / `{DEFAULT_TTS_VOICE}`) will now be used for your join message and default for `/tts`.",
            ephemeral=True
        )
    else:
        # User didn't have any custom TTS defaults set
        await ctx.followup.send("🤷 You don't have any custom TTS defaults configured.", ephemeral=True)


# === TTS Command (Edge-TTS) ===
@bot.slash_command(name="tts", description="Make the bot say something using Edge Text-to-Speech.")
@commands.cooldown(1, 6, commands.BucketType.user) # Cooldown per user
async def tts(
    ctx: discord.ApplicationContext,
    message: discord.Option(str, description=f"Text to speak (max {MAX_TTS_LENGTH} chars).", required=True), # type: ignore
    voice: discord.Option(str, description="Override TTS voice (start typing to search).", required=False, autocomplete=tts_voice_autocomplete, choices=CURATED_EDGE_TTS_VOICE_CHOICES), # type: ignore # Uses curated list for quick choices
    spell_out: discord.Option(bool, description="Read out each character with spaces?", default=False) # type: ignore
):
    """Generates and plays TTS audio in the user's voice channel."""
    await ctx.defer(ephemeral=True)
    user = ctx.author
    guild = ctx.guild

    # --- Initial Checks ---
    if not guild or not isinstance(user, discord.Member) or not user.voice or not user.voice.channel:
        await ctx.followup.send("You must be in a voice channel in this server to use TTS.", ephemeral=True); return
    if len(message) > MAX_TTS_LENGTH:
         await ctx.followup.send(f"❌ Message too long! The maximum length is {MAX_TTS_LENGTH} characters.", ephemeral=True); return
    if not message.strip():
         await ctx.followup.send("❌ Please provide some text for the bot to say.", ephemeral=True); return

    user_id_str = str(user.id)
    guild_id = guild.id
    bot_logger.info(f"COMMAND: /tts by {user.name} ({user_id_str}), Guild: {guild_id}, Voice: {voice}, Spell: {spell_out}, Msg: '{message[:50]}...'")

    target_channel = user.voice.channel
    user_config = user_sound_config.get(user_id_str, {})
    saved_defaults = user_config.get("tts_defaults", {})

    # --- Determine Voice ---
    final_voice = voice if voice is not None else saved_defaults.get('voice', DEFAULT_TTS_VOICE)
    voice_source = "explicit" if voice is not None else ("saved default" if 'voice' in saved_defaults else "bot default")

    # Validate the final voice choice
    is_valid_voice = any(choice.value == final_voice for choice in FULL_EDGE_TTS_VOICE_CHOICES)
    if not is_valid_voice:
         bot_logger.warning(f"TTS: Invalid final voice '{final_voice}' ({voice_source}) selected for {user.name}. Falling back to default '{DEFAULT_TTS_VOICE}'.")
         if voice_source == "explicit" or voice_source == "saved default":
             await ctx.followup.send(f"❌ Invalid voice ID (`{final_voice}`). Please select a valid voice. Falling back to bot default.", ephemeral=True)
             final_voice = DEFAULT_TTS_VOICE
         else:
             final_voice = DEFAULT_TTS_VOICE

    bot_logger.info(f"TTS Final Voice Selection: {final_voice} (Source: {voice_source}) for {user.name}")

    # --- TTS Generation and Processing ---
    audio_source: Optional[discord.PCMAudio] = None
    pcm_fp: Optional[io.BytesIO] = None

    try:
        # --- Normalization & Spelling Logic ---
        original_message = message
        normalized_message = normalize_for_tts(original_message)

        if spell_out:
            text_to_speak = " ".join(filter(None, list(normalized_message)))
            log_msg_type = "Spaced"
            log_text_preview = text_to_speak[:150]
        else:
            text_to_speak = normalized_message
            log_msg_type = "Normalized" if original_message != normalized_message else "Original"
            log_text_preview = text_to_speak[:50]

        bot_logger.info(f"TTS Command {log_msg_type} Input: '{original_message[:50]}...' -> '{log_text_preview}...'")

        if not text_to_speak.strip():
             await ctx.followup.send("❌ Message became empty after removing unsupported characters.", ephemeral=True); return

        # --- Generate Audio ---
        bot_logger.info(f"TTS: Generating audio with Edge-TTS for '{user.name}' (voice={final_voice}) using text: '{text_to_speak[:100]}...'")

        mp3_bytes_list = []
        communicate = edge_tts.Communicate(text_to_speak, final_voice)
        async for chunk in communicate.stream():
            if chunk["type"] == "audio":
                mp3_bytes_list.append(chunk["data"])

        if not mp3_bytes_list: raise ValueError("Edge-TTS generation yielded no audio data chunks.")
        mp3_data = b"".join(mp3_bytes_list)
        if len(mp3_data) == 0: raise ValueError("Edge-TTS generation resulted in empty audio data.")

        # --- Process Audio with Pydub (In Memory) ---
        with io.BytesIO(mp3_data) as mp3_fp:
            seg = AudioSegment.from_file(mp3_fp, format="mp3")
            bot_logger.debug(f"TTS: Loaded MP3 into Pydub (duration: {len(seg)}ms)")
            if len(seg) > MAX_PLAYBACK_DURATION_MS:
                bot_logger.info(f"TTS: Trimming audio from {len(seg)}ms to {MAX_PLAYBACK_DURATION_MS}ms.")
                seg = seg[:MAX_PLAYBACK_DURATION_MS]
            # Normalize Gain (Optional - using process_audio's logic as reference)
            peak_dbfs = seg.max_dBFS
            if not math.isinf(peak_dbfs) and peak_dbfs > -90.0:
                change_in_dbfs = TARGET_LOUDNESS_DBFS - peak_dbfs
                bot_logger.info(f"TTS AUDIO: Normalizing. Peak:{peak_dbfs:.2f} Target:{TARGET_LOUDNESS_DBFS:.2f} Gain:{change_in_dbfs:.2f} dB.")
                gain_limit = 6.0 # Limit positive gain
                apply_gain = min(change_in_dbfs, gain_limit) if change_in_dbfs > 0 else change_in_dbfs
                if apply_gain != change_in_dbfs:
                    bot_logger.info(f"TTS AUDIO: Limiting gain to +{gain_limit}dB (calculated: {change_in_dbfs:.2f}dB).")
                seg = seg.apply_gain(apply_gain)
            elif math.isinf(peak_dbfs):
                bot_logger.warning("TTS AUDIO: Cannot normalize silent TTS audio.")
            else:
                bot_logger.warning(f"TTS AUDIO: Skipping normalization for very quiet TTS audio (Peak: {peak_dbfs:.2f})")

            # Convert format for Discord
            seg = seg.set_frame_rate(48000).set_channels(2)
            pcm_fp = io.BytesIO()
            seg.export(pcm_fp, format="s16le")
            pcm_fp.seek(0)

        if pcm_fp.getbuffer().nbytes == 0: raise ValueError("Pydub export resulted in empty PCM data.")
        bot_logger.debug(f"TTS: PCM processed in memory ({pcm_fp.getbuffer().nbytes} bytes)")
        audio_source = discord.PCMAudio(pcm_fp) # Pass the PCM buffer directly
        bot_logger.info(f"TTS: PCMAudio source created successfully for {user.name}.")

    except Exception as e:
        err_type = type(e).__name__
        msg = f"❌ Error generating/processing TTS ({err_type}). Check logs or try different voice/message."
        if isinstance(e, FileNotFoundError) and ('ffmpeg' in str(e).lower() or 'ffprobe' in str(e).lower()): msg = "❌ Error: FFmpeg needed for audio processing wasn't found."
        elif "trustchain" in str(e).lower() or "ssl" in str(e).lower(): msg = "❌ TTS Error: Secure connection issue."
        elif "voice not found" in str(e).lower(): msg = f"❌ Error: TTS service reported voice '{final_voice}' not found."
        elif isinstance(e, (ValueError, RuntimeError)): msg = f"❌ Error processing TTS audio: {e}"

        await ctx.followup.send(msg, ephemeral=True)
        bot_logger.error(f"TTS: Failed generation/processing for {user.name} (Voice: {final_voice}): {e}", exc_info=True)
        if pcm_fp and not pcm_fp.closed: pcm_fp.close()
        return

    # --- Playback ---
    if not audio_source:
        await ctx.followup.send("❌ Failed to prepare TTS audio source.", ephemeral=True)
        bot_logger.error("TTS: Audio source was None after processing block.")
        if pcm_fp and not pcm_fp.closed: pcm_fp.close()
        return

    tts_buffer_to_close = pcm_fp # Keep reference to the PCM buffer for closing

    # Ensure bot is ready
    voice_client = await _ensure_voice_client_ready(ctx.interaction, target_channel, action_type="TTS")
    if not voice_client:
        if tts_buffer_to_close and not tts_buffer_to_close.closed: tts_buffer_to_close.close()
        return

    # Double-check if busy
    if voice_client.is_playing():
         bot_logger.warning(f"TTS: VC became busy between check and play for {user.name}.")
         await ctx.followup.send("⏳ Bot became busy. Please try again.", ephemeral=True)
         if tts_buffer_to_close and not tts_buffer_to_close.closed: tts_buffer_to_close.close()
         return

    try:
        if guild_id: cancel_leave_timer(guild_id, reason="starting TTS playback")
        bot_logger.info(f"TTS PLAYBACK: Playing TTS requested by {user.display_name}...")

        # Define the 'after' callback for TTS
        def tts_after_play(error: Optional[Exception]):
            log_prefix_after = f"AFTER_PLAY_TTS (Guild {guild_id if guild_id else 'Unknown'}):"
            bot_logger.debug(f"{log_prefix_after} Callback initiated.")

            # Call standard handler first
            current_vc = discord.utils.get(bot.voice_clients, guild=voice_client.guild)
            if current_vc and current_vc.is_connected():
                 after_play_handler(error, current_vc)
            elif voice_client:
                 bot_logger.warning(f"{log_prefix_after} VC disconnected before standard handler.")

            # Close the TTS PCM buffer
            try:
                if tts_buffer_to_close and not tts_buffer_to_close.closed:
                    tts_buffer_to_close.close()
                    bot_logger.debug(f"{log_prefix_after} Closed TTS PCM buffer.")
            except Exception as close_err:
                bot_logger.error(f"{log_prefix_after} Error closing TTS PCM buffer: {close_err}")

        # Play audio
        voice_client.play(audio_source, after=tts_after_play)

        # Confirmation message
        voice_display_name = final_voice
        for choice in FULL_EDGE_TTS_VOICE_CHOICES:
            if choice.value == final_voice: voice_display_name = choice.name; break
        display_msg_truncated = original_message[:150] + ('...' if len(original_message) > 150 else '')
        spell_note = " (spelled out)" if spell_out else ""

        await ctx.followup.send(
            f"🗣️ Now saying{spell_note} with **{voice_display_name}** (max {MAX_PLAYBACK_DURATION_MS/1000}s):\n"
            f"\"_{display_msg_truncated}_\"",
            ephemeral=True
        )

    except discord.errors.ClientException as e:
        msg = "❌ Error: Bot is already playing or encountered a client issue."
        await ctx.followup.send(msg, ephemeral=True)
        bot_logger.error(f"TTS PLAYBACK ERROR (ClientException on play call): {e}", exc_info=True)
        tts_after_play(e) # Manual cleanup call
    except Exception as e:
        await ctx.followup.send("❌ An unexpected error occurred during TTS playback.", ephemeral=True)
        bot_logger.error(f"TTS PLAYBACK ERROR (Unexpected on play call): {e}", exc_info=True)
        tts_after_play(e) # Manual cleanup call


# === Stay/Leave Commands ===
@bot.slash_command(name="togglestay", description="[Admin Only] Toggle whether the bot stays in VC when idle.")
@commands.has_permissions(manage_guild=True) # Requires 'Manage Server' permission
@commands.cooldown(1, 5, commands.BucketType.guild) # Cooldown per guild
async def togglestay(ctx: discord.ApplicationContext):
    """Toggles the 'stay_in_channel' setting for the current guild."""
    await ctx.defer(ephemeral=True)
    if not ctx.guild_id or not ctx.guild:
         await ctx.followup.send("This command can only be used in a server.", ephemeral=True)
         return

    guild_id_str = str(ctx.guild_id)
    guild_id = ctx.guild_id
    admin = ctx.author
    bot_logger.info(f"COMMAND: /togglestay by admin {admin.name} ({admin.id}) in guild {ctx.guild.name} ({guild_id_str})")

    current_setting = guild_settings.get(guild_id_str, {}).get("stay_in_channel", False)
    new_setting = not current_setting

    guild_settings.setdefault(guild_id_str, {})['stay_in_channel'] = new_setting
    save_guild_settings()

    status_message = "ENABLED ✅ (Bot will now stay in VC when idle)" if new_setting else "DISABLED ❌ (Bot will now leave VC after being idle and alone)"
    await ctx.followup.send(f"Bot 'Stay in Channel' feature is now **{status_message}** for this server.", ephemeral=True)
    bot_logger.info(f"Guild {ctx.guild.name} ({guild_id_str}) 'stay_in_channel' set to {new_setting} by {admin.name}")

    vc = discord.utils.get(bot.voice_clients, guild__id=guild_id)
    if vc and vc.is_connected():
        if new_setting:
            cancel_leave_timer(guild_id, reason="togglestay enabled")
        else:
            if not vc.is_playing() and is_bot_alone(vc):
                 bot_logger.info(f"TOGGLESTAY: Stay disabled, bot is idle and alone. Triggering leave timer check.")
                 bot.loop.create_task(start_leave_timer(vc))
            elif vc.is_playing():
                 bot_logger.debug("TOGGLESTAY: Stay disabled, but bot currently playing.")
            else:
                bot_logger.debug("TOGGLESTAY: Stay disabled, but bot not alone.")

# Error handler specifically for /togglestay permissions/cooldown
@togglestay.error
async def togglestay_error(ctx: discord.ApplicationContext, error: discord.DiscordException):
    """Error handler specifically for /togglestay permissions and cooldown."""
    if isinstance(error, commands.MissingPermissions):
        bot_logger.warning(f"User {ctx.author.name} tried /togglestay without Manage Guild permission.")
        await ctx.respond("🚫 You need the `Manage Server` permission to use this command.", ephemeral=True)
    elif isinstance(error, commands.CommandOnCooldown):
         await ctx.respond(f"⏳ This command is on cooldown for this server. Try again in {error.retry_after:.1f}s.", ephemeral=True)
    else:
        await on_application_command_error(ctx, error)


@bot.slash_command(name="leave", description="Make the bot leave its current voice channel.")
@commands.cooldown(1, 5, commands.BucketType.user) # Per-user cooldown
async def leave(ctx: discord.ApplicationContext):
    """Forces the bot to leave the voice channel in the current guild."""
    await ctx.defer(ephemeral=True)
    guild = ctx.guild
    user = ctx.author

    if not guild:
        await ctx.followup.send("This command must be used in a server.", ephemeral=True)
        return

    bot_logger.info(f"COMMAND: /leave invoked by {user.name} ({user.id}) in guild {guild.name} ({guild.id})")

    vc = discord.utils.get(bot.voice_clients, guild=guild)

    if vc and vc.is_connected():
        channel_name = vc.channel.name if vc.channel else "Unknown Channel"
        bot_logger.info(f"LEAVE: Manually disconnecting from {channel_name} in {guild.name} due to /leave command...")
        await safe_disconnect(vc, manual_leave=True)
        await ctx.followup.send(f"👋 Leaving {channel_name}.", ephemeral=True)
    else:
        bot_logger.info(f"LEAVE: Request by {user.name}, but bot not connected in {guild.name}.")
        await ctx.followup.send("🤷 I'm not currently in a voice channel in this server.", ephemeral=True)


# --- Global Error Handler for Application Commands ---
@bot.event
async def on_application_command_error(ctx: discord.ApplicationContext, error: discord.DiscordException):
    """Global handler for slash command errors."""
    command_name = "Unknown Command"
    invoked_with = None
    if hasattr(ctx, 'invoked_with'): invoked_with = ctx.invoked_with
    if ctx.command: command_name = ctx.command.qualified_name
    elif hasattr(ctx.interaction, 'custom_id') and ctx.interaction.custom_id:
        command_name = f"Component ({ctx.interaction.custom_id[:30]}...)"; invoked_with = ctx.interaction.custom_id

    user_name = f"{ctx.author.name}({ctx.author.id})" if ctx.author else "Unknown User"
    guild_name = f"{ctx.guild.name}({ctx.guild.id})" if ctx.guild else "DM Context"
    log_prefix = f"CMD ERROR (/{invoked_with or command_name}, user: {user_name}, guild: {guild_name}):"

    async def send_error_response(message: str, log_level=logging.WARNING):
        log_level_actual = log_level if not isinstance(error, commands.CommandNotFound) else logging.DEBUG
        bot_logger.log(log_level_actual, f"{log_prefix} {message} (Error Type: {type(error).__name__}, Details: {error})")
        try:
            if ctx.interaction.response.is_done():
                await ctx.followup.send(message, ephemeral=True)
            else:
                 await ctx.respond(message, ephemeral=True)
        except discord.NotFound:
            bot_logger.warning(f"{log_prefix} Interaction not found while sending error response.")
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
        await send_error_response(f"🚫 I lack the required permissions: {perms}. Check my role settings.", log_level=logging.ERROR)
    elif isinstance(error, commands.CheckFailure):
        await send_error_response("🚫 You do not have permission to use this command or perform this action.")
    elif isinstance(error, discord.errors.ApplicationCommandInvokeError):
        original = error.original
        bot_logger.error(f"{log_prefix} An error occurred within the command code itself.", exc_info=original)

        user_msg = "❌ An internal error occurred. Check logs."
        if isinstance(original, FileNotFoundError) and ('ffmpeg' in str(original).lower() or 'ffprobe' in str(original).lower()):
             user_msg = "❌ Internal Error: FFmpeg/FFprobe not found by the bot. Install it and add to PATH."
        elif isinstance(original, CouldntDecodeError):
             user_msg = "❌ Internal Error: Failed to decode an audio file (corrupted/unsupported?)."
        elif isinstance(original, discord.errors.Forbidden):
             user_msg = f"❌ Internal Error: Permission issue ({original.text}). Check bot/channel permissions."
        elif "edge_tts" in str(type(original)):
             user_msg = f"❌ Internal TTS Error: ({type(original).__name__}). Check logs (network/input issue?)."

        await send_error_response(user_msg, log_level=logging.ERROR)
    elif isinstance(error, discord.errors.InteractionResponded):
         bot_logger.warning(f"{log_prefix} Interaction already responded to. Error: {error}")
    elif isinstance(error, discord.errors.NotFound):
         bot_logger.warning(f"{log_prefix} Interaction or component not found (deleted/expired?). Error: {error}")
    elif isinstance(error, commands.CommandNotFound):
         bot_logger.debug(f"{log_prefix} Unknown command/component invoked: {invoked_with or command_name}")
         pass
    else:
        bot_logger.error(f"{log_prefix} An unexpected Discord API/library error occurred: {error}", exc_info=error)
        await send_error_response(f"❌ An unexpected error occurred ({type(error).__name__}).", log_level=logging.ERROR)


# --- Run the Bot ---
if __name__ == "__main__":
    # Final Dependency Checks before running
    if not PYDUB_AVAILABLE: bot_logger.critical("Pydub missing/failed. Install: pip install pydub ffmpeg"); exit(1)
    if not EDGE_TTS_AVAILABLE: bot_logger.critical("edge-tts missing/failed. Install: pip install edge-tts"); exit(1)
    if not BOT_TOKEN: bot_logger.critical("BOT_TOKEN environment variable not set."); exit(1)

    # --- Modified Opus Loading Check ---
    opus_load_success = False
    if discord.opus.is_loaded():
        bot_logger.info("Opus library already loaded.")
        opus_load_success = True
    else:
        bot_logger.info("Opus library not initially loaded. Attempting default load...")
        try:
            # Simulate the internal _load_default() logic relevant for Windows
            if sys.platform == 'win32':
                basedir = os.path.dirname(os.path.abspath(discord.opus.__file__))
                _bitness = struct.calcsize('P') * 8
                _target = 'x64' if _bitness > 32 else 'x86'
                _filename = os.path.join(basedir, 'bin', f'libopus-0.{_target}.dll')
                if os.path.exists(_filename):
                    discord.opus.load_opus(_filename)
                    opus_load_success = discord.opus.is_loaded()
                    if opus_load_success:
                         bot_logger.info(f"Successfully loaded bundled Opus DLL: {_filename}")
                    else:
                         bot_logger.warning("Attempted to load bundled Opus DLL, but is_loaded() is still False.")
                else:
                    bot_logger.warning(f"Bundled Opus DLL not found at expected path: {_filename}")
            else:
                # Attempt find_library for other platforms
                found_path = ctypes.util.find_library('opus')
                if found_path:
                    discord.opus.load_opus(found_path)
                    opus_load_success = discord.opus.is_loaded()
                    if opus_load_success:
                         bot_logger.info(f"Successfully loaded Opus via find_library: {found_path}")
                    else:
                         bot_logger.warning("Found Opus via find_library, but is_loaded() is still False after load attempt.")
                else:
                     bot_logger.warning("Could not find Opus library using ctypes.util.find_library('opus').")

            # Final check if any method worked
            if not opus_load_success:
                 # Try loading just 'opus' as a last resort (might work if in PATH)
                 try:
                     discord.opus.load_opus('opus')
                     opus_load_success = discord.opus.is_loaded()
                     if opus_load_success:
                          bot_logger.info("Successfully loaded Opus using generic name 'opus'.")
                 except OSError:
                     bot_logger.warning("Failed to load Opus using generic name 'opus'.")

        except Exception as e:
            bot_logger.error(f"Error occurred during explicit Opus load attempt: {e}", exc_info=True)

    # Report final status
    if opus_load_success:
        try:
            version = discord.opus._OpusStruct.get_opus_version()
            bot_logger.info(f"Opus library loading confirmed. Version: {version}")
        except Exception as e:
            bot_logger.warning(f"Opus loaded, but failed to get version string: {e}")
    else:
        # Downgraded from CRITICAL since you don't want it to exit
        bot_logger.error("❌ FAILED to confirm Opus library loading during startup checks.")
        bot_logger.error("   While playback might seem to work sometimes, this indicates a non-standard setup.")
        bot_logger.error("   Voice stability issues or future failures are possible. Check library paths/permissions.")
        # NO exit(1) here anymore

    # PyNaCl Check (Still crucial for Voice)
    try: import nacl; bot_logger.info("PyNaCl library found.")
    except ImportError: bot_logger.critical("CRITICAL: PyNaCl library not found. Voice WILL NOT WORK. Install: pip install PyNaCl"); exit(1)

    # Add top-level error handling for bot.run()
    try:
        bot_logger.info("Attempting bot startup...")
        bot.run(BOT_TOKEN)
    except discord.errors.LoginFailure:
        bot_logger.critical("CRITICAL STARTUP ERROR: Login Failure - Invalid BOT_TOKEN.")
        exit(1)
    except discord.errors.PrivilegedIntentsRequired as e:
        bot_logger.critical(f"CRITICAL STARTUP ERROR: Missing Privileged Intents: {e}. Enable in Dev Portal.")
        exit(1)
    except Exception as e:
        bot_logger.critical(f"FATAL RUNTIME ERROR: {e}", exc_info=True)
        exit(1)
    finally:
        bot_logger.info("Bot process has ended.")