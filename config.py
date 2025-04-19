# -*- coding: utf-8 -*-
import os
import discord
from dotenv import load_dotenv

load_dotenv() # Load environment variables from .env file

# --- Core Settings ---
BOT_TOKEN = os.getenv('BOT_TOKEN')
SOUNDS_DIR = "sounds" # General sounds, used for join sounds and temporary TTS
USER_SOUNDS_DIR = "usersounds" # Root directory for user-specific sounds
PUBLIC_SOUNDS_DIR = "publicsounds" # Directory for sounds available to everyone
CONFIG_FILE = "user_sounds.json" # Stores user join sound and TTS prefs
GUILD_SETTINGS_FILE = "guild_settings.json" # Stores guild-specific settings (like stay_in_channel)

# --- Audio Processing ---
TARGET_LOUDNESS_DBFS = -14.0 # Target loudness for normalization
MAX_PLAYBACK_DURATION_MS = 10 * 1000 # Max duration for any played sound (10 seconds)

# --- User Sound Limits ---
MAX_USER_SOUND_SIZE_MB = 5 # Max upload size in Megabytes
MAX_USER_SOUNDS_PER_USER = 25 # Max personal sounds per user
ALLOWED_EXTENSIONS = ['.mp3', '.wav', '.ogg', '.m4a', '.aac'] # Allowed upload extensions

# --- TTS Settings ---
MAX_TTS_LENGTH = 350 # Max characters for TTS input
DEFAULT_TTS_VOICE = "en-US-JennyNeural" # Bot's default voice if user has none set

# --- Voice Channel Behavior ---
AUTO_LEAVE_TIMEOUT_SECONDS = 4 * 60 * 60 # Time in seconds bot waits alone before leaving (4 hours)

# --- TTS Voices (Generated from original bot.py) ---
# (Keep this section minimized in your editor if it's too long)
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
    # "de-DE-*", # Assuming these * were placeholders or errors
    "de-DE-KatjaNeural", "de-DE-KillianNeural",
    # "de-DE-*",
    "el-GR-AthinaNeural", "el-GR-NestorasNeural",
    "en-AU-NatashaNeural", "en-AU-WilliamNeural", "en-CA-ClaraNeural", "en-CA-LiamNeural",
    "en-GB-LibbyNeural", "en-GB-MaisieNeural", "en-GB-RyanNeural", "en-GB-SoniaNeural",
    "en-GB-ThomasNeural", "en-HK-SamNeural", "en-HK-YanNeural", "en-IE-ConnorNeural",
    "en-IE-EmilyNeural", #"en-IN-*",
    "en-IN-NeerjaNeural", "en-IN-PrabhatNeural",
    "en-KE-AsiliaNeural", "en-KE-ChilembaNeural", "en-NG-AbeoNeural", "en-NG-EzinneNeural",
    "en-NZ-MitchellNeural", "en-NZ-MollyNeural", "en-PH-JamesNeural", "en-PH-RosaNeural",
    "en-SG-LunaNeural", "en-SG-WayneNeural", "en-TZ-ElimuNeural", "en-TZ-ImaniNeural",
    "en-US-AnaNeural", #"en-US-*",
    "en-US-AndrewNeural", "en-US-AriaNeural",
    #"en-US-*",
     "en-US-AvaNeural", #"en-US-*",
     "en-US-BrianNeural",
    "en-US-ChristopherNeural", #"en-US-*",
    "en-US-EmmaNeural", "en-US-EricNeural",
    "en-US-GuyNeural", "en-US-JennyNeural", "en-US-MichelleNeural", "en-US-RogerNeural",
    "en-US-SteffanNeural", "en-ZA-LeahNeural", "en-ZA-LukeNeural", "es-AR-ElenaNeural",
    "es-AR-TomasNeural", "es-BO-MarceloNeural", "es-BO-SofiaNeural", "es-CL-CatalinaNeural",
    "es-CL-LorenzoNeural", "es-CO-GonzaloNeural", "es-CO-SalomeNeural", "es-CR-JuanNeural",
    "es-CR-MariaNeural", "es-CU-BelkysNeural", "es-CU-ManuelNeural", "es-DO-EmilioNeural",
    "es-DO-RamonaNeural", "es-EC-AndreaNeural", "es-EC-LuisNeural", "es-ES-AlvaroNeural",
    "es-ES-ElviraNeural", #"es-ES-XimenaNeural", # Removed apparent duplicate in original list
    "es-GQ-JavierNeural", "es-GQ-TeresaNeural",
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
    "fr-FR-DeniseNeural", "fr-FR-EloiseNeural", "fr-FR-HenriNeural", #"fr-FR-*",
    #"fr-FR-*",
     "ga-IE-ColmNeural", "ga-IE-OrlaNeural", "gl-ES-RoiNeural",
    "gl-ES-SabelaNeural", "gu-IN-DhwaniNeural", "gu-IN-NiranjanNeural", "he-IL-AvriNeural",
    "he-IL-HilaNeural", "hi-IN-MadhurNeural", "hi-IN-SwaraNeural", "hr-HR-GabrijelaNeural",
    "hr-HR-SreckoNeural", "hu-HU-NoemiNeural", "hu-HU-TamasNeural", "id-ID-ArdiNeural",
    "id-ID-GadisNeural", "is-IS-GudrunNeural", "is-IS-GunnarNeural", "it-IT-DiegoNeural",
    "it-IT-ElsaNeural", #"it-IT-*",
     "it-IT-IsabellaNeural",
    "iu-Cans-CA-SiqiniqNeural", "iu-Cans-CA-TaqqiqNeural", "iu-Latn-CA-SiqiniqNeural",
    "iu-Latn-CA-TaqqiqNeural", "ja-JP-KeitaNeural", "ja-JP-NanamiNeural", "jv-ID-DimasNeural",
    "jv-ID-SitiNeural", "ka-GE-EkaNeural", "ka-GE-GiorgiNeural", "kk-KZ-AigulNeural",
    "kk-KZ-DauletNeural", "km-KH-PisethNeural", "km-KH-SreymomNeural", "kn-IN-GaganNeural",
    "kn-IN-SapnaNeural", #"ko-KR-*",
     "ko-KR-InJoonNeural", "ko-KR-SunHiNeural",
    "lo-LA-ChanthavongNeural", "lo-LA-KeomanyNeural", "lt-LT-LeonasNeural", "lt-LT-OnaNeural",
    "lv-LV-EveritaNeural", "lv-LV-NilsNeural", "mk-MK-AleksandarNeural", "mk-MK-MarijaNeural",
    "ml-IN-MidhunNeural", "ml-IN-SobhanaNeural", "mn-MN-BataaNeural", "mn-MN-YesuiNeural",
    "mr-IN-AarohiNeural", "mr-IN-ManoharNeural", "ms-MY-OsmanNeural", "ms-MY-YasminNeural",
    "mt-MT-GraceNeural", "mt-MT-JosephNeural", "my-MM-NilarNeural", "my-MM-ThihaNeural",
    "nb-NO-FinnNeural", "nb-NO-PernilleNeural", "ne-NP-HemkalaNeural", "ne-NP-SagarNeural",
    "nl-BE-ArnaudNeural", "nl-BE-DenaNeural", "nl-NL-ColetteNeural", "nl-NL-FennaNeural",
    "nl-NL-MaartenNeural", "pl-PL-MarekNeural", "pl-PL-ZofiaNeural", "ps-AF-GulNawazNeural",
    "ps-AF-LatifaNeural", "pt-BR-AntonioNeural", "pt-BR-FranciscaNeural", #"pt-BR-*",
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

def create_display_name(voice_id: str) -> str:
    parts = voice_id.split('-')
    if len(parts) >= 3:
        lang_code = parts[0]
        region_code = parts[1]
        name_part = parts[2]
        # Handle special region codes first
        if region_code == "liaoning" and lang_code == "zh":
            name = name_part.replace("Neural", "")
            return f"Chinese (Liaoning) {name}"
        if region_code == "shaanxi" and lang_code == "zh":
            name = name_part.replace("Neural", "")
            return f"Chinese (Shaanxi) {name}"
        if "Cans" in region_code: region_code = region_code.replace("Cans", "CA-Cans") # Inuktitut Canadian Aboriginal Syllabics
        elif "Latn" in region_code: region_code = region_code.replace("Latn", "CA-Latn") # Inuktitut Latin

        # General case
        name = name_part.replace("Neural", "").replace("Multilingual", " Multi").replace("Expressive", " Expr")
        return f"{lang_code.upper()}-{region_code.upper()} {name}"
    return voice_id # Fallback

FULL_EDGE_TTS_VOICE_CHOICES = []
for voice_id in ALL_VOICE_IDS:
    display_name = create_display_name(voice_id)
    if len(display_name) > 100:
        display_name = display_name[:97] + "..." # Max length for OptionChoice name
    FULL_EDGE_TTS_VOICE_CHOICES.append(discord.OptionChoice(name=display_name, value=voice_id))

FULL_EDGE_TTS_VOICE_CHOICES.sort(key=lambda x: x.name) # Sort alphabetically by display name

# Curated list from original bot.py - less overwhelming for users
CURATED_VOICE_IDS = [
    "en-US-JennyNeural", "en-US-AriaNeural", "en-US-GuyNeural", "en-US-AnaNeural",
    "en-GB-LibbyNeural", "en-GB-RyanNeural", "en-AU-NatashaNeural", "en-CA-ClaraNeural",
    "en-IN-NeerjaNeural", # Replaced the '*' with a specific example
    "es-ES-ElviraNeural", "es-MX-JorgeNeural",
    "fr-FR-DeniseNeural", "fr-CA-JeanNeural", "de-DE-KatjaNeural", "de-DE-ConradNeural",
    "it-IT-IsabellaNeural", "ja-JP-NanamiNeural", "ja-JP-KeitaNeural", "ko-KR-SunHiNeural",
    "pt-BR-FranciscaNeural", "ru-RU-SvetlanaNeural", "zh-CN-XiaoxiaoNeural",
    "ar-EG-SalmaNeural", "hi-IN-SwaraNeural", "nl-NL-MaartenNeural",
]

CURATED_EDGE_TTS_VOICE_CHOICES = []
for voice_id in CURATED_VOICE_IDS:
    found = False
    for full_choice in FULL_EDGE_TTS_VOICE_CHOICES:
        if full_choice.value == voice_id:
            CURATED_EDGE_TTS_VOICE_CHOICES.append(full_choice)
            found = True
            break
    # If a curated ID isn't in the main list for some reason, create it manually
    if not found:
        display_name = create_display_name(voice_id)
        if len(display_name) > 100: display_name = display_name[:97] + "..."
        CURATED_EDGE_TTS_VOICE_CHOICES.append(discord.OptionChoice(name=display_name, value=voice_id))

CURATED_EDGE_TTS_VOICE_CHOICES.sort(key=lambda x: x.name)

# Check for essential libraries during startup (can be done in main_bot.py as well)
PYDUB_AVAILABLE = False
EDGE_TTS_AVAILABLE = False
NACL_AVAILABLE = False
try:
    from pydub import AudioSegment, exceptions as pydub_exceptions
    PYDUB_AVAILABLE = True
except ImportError: pass
try:
    import edge_tts
    EDGE_TTS_AVAILABLE = True
except ImportError: pass
try:
    import nacl
    NACL_AVAILABLE = True
except ImportError: pass


MUSIC_CACHE_TTL_DAYS = 30
MUSIC_CACHE_DIR = "music_cache"
MUSIC_DOWNLOAD_INTERVAL = 5 # seconds
MUSIC_CLEANUP_INTERVAL = 3600 # Once per hour