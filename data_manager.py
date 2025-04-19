# -*- coding: utf-8 -*-
import json
import os
import logging
from typing import Dict, Any, Tuple

import config # Import the config module

log = logging.getLogger('SoundBot.DataManager')

def load_config() -> Dict[str, Dict[str, Any]]:
    """Loads user sound configurations from JSON file specified in config."""
    user_sound_config: Dict[str, Dict[str, Any]] = {}
    if os.path.exists(config.CONFIG_FILE):
        try:
            with open(config.CONFIG_FILE, 'r', encoding='utf-8') as f:
                user_sound_config = json.load(f)
            log.info(f"Loaded {len(user_sound_config)} user configs from {config.CONFIG_FILE}")

            # --- Data Migration/Upgrade Logic (from original bot.py) ---
            upgraded_count = 0
            for user_id, data in list(user_sound_config.items()): # Iterate over a copy
                # Upgrade old TTS format (language/slow) to new (voice)
                if isinstance(data, dict) and "tts_defaults" in data:
                    defaults = data["tts_defaults"]
                    if "language" in defaults or "slow" in defaults:
                        if "voice" not in defaults:
                            defaults["voice"] = config.DEFAULT_TTS_VOICE
                            log.info(f"Upgraded TTS defaults format for user {user_id} - Added default voice.")
                        if "language" in defaults:
                            del defaults["language"]
                            log.info(f"Upgraded TTS defaults format for user {user_id} - Removed 'language'.")
                        if "slow" in defaults:
                            del defaults["slow"]
                            log.info(f"Upgraded TTS defaults format for user {user_id} - Removed 'slow'.")
                        upgraded_count += 1
                # Upgrade old simple join sound string to dictionary format
                elif isinstance(data, str):
                    user_sound_config[user_id] = {"join_sound": data}
                    log.info(f"Upgraded join sound format for user {user_id}")
                    upgraded_count += 1

            if upgraded_count > 0:
                log.info(f"Performed {upgraded_count} upgrades on user config data. Saving changes.")
                save_config(user_sound_config) # Save the potentially modified config
            # --- End Migration ---

        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            log.error(f"Error loading {config.CONFIG_FILE}: {e}. Starting with empty config.", exc_info=True)
            user_sound_config = {} # Reset on error
        except Exception as e:
             log.error(f"Unexpected error loading {config.CONFIG_FILE}: {e}. Starting with empty config.", exc_info=True)
             user_sound_config = {} # Reset on error
    else:
        log.info(f"{config.CONFIG_FILE} not found. Starting fresh.")
        user_sound_config = {}
    return user_sound_config

def save_config(user_sound_config: Dict[str, Dict[str, Any]]):
    """Saves user sound configurations to JSON file specified in config."""
    try:
        with open(config.CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(user_sound_config, f, indent=4, ensure_ascii=False)
        log.debug(f"Saved {len(user_sound_config)} user configs to {config.CONFIG_FILE}")
    except Exception as e:
        log.error(f"Error saving {config.CONFIG_FILE}: {e}", exc_info=True)

def load_guild_settings() -> Dict[str, Dict[str, Any]]:
    """Loads guild-specific settings from JSON file specified in config."""
    guild_settings: Dict[str, Dict[str, Any]] = {}
    if os.path.exists(config.GUILD_SETTINGS_FILE):
        try:
            with open(config.GUILD_SETTINGS_FILE, 'r', encoding='utf-8') as f:
                loaded_data = json.load(f)
                # Ensure keys are strings (JSON loads them as strings anyway, but good practice)
                guild_settings = {str(k): v for k, v in loaded_data.items()}
            log.info(f"Loaded {len(guild_settings)} guild settings from {config.GUILD_SETTINGS_FILE}")
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            log.error(f"Error loading {config.GUILD_SETTINGS_FILE}: {e}. Starting with empty settings.", exc_info=True)
            guild_settings = {} # Reset on error
        except Exception as e:
             log.error(f"Unexpected error loading {config.GUILD_SETTINGS_FILE}: {e}. Starting with empty settings.", exc_info=True)
             guild_settings = {} # Reset on error
    else:
        log.info(f"{config.GUILD_SETTINGS_FILE} not found. Starting with no persistent guild settings.")
        guild_settings = {}
    return guild_settings

def save_guild_settings(guild_settings: Dict[str, Dict[str, Any]]):
    """Saves guild-specific settings to JSON file specified in config."""
    try:
        with open(config.GUILD_SETTINGS_FILE, 'w', encoding='utf-8') as f:
            json.dump(guild_settings, f, indent=4, ensure_ascii=False)
        log.debug(f"Saved {len(guild_settings)} guild settings to {config.GUILD_SETTINGS_FILE}")
    except Exception as e:
        log.error(f"Error saving {config.GUILD_SETTINGS_FILE}: {e}", exc_info=True)

def load_all_data() -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """Loads both user config and guild settings."""
    user_cfg = load_config()
    guild_cfg = load_guild_settings()
    return user_cfg, guild_cfg

def save_all_data(user_sound_config: Dict[str, Dict[str, Any]], guild_settings: Dict[str, Dict[str, Any]]):
    """Saves both user config and guild settings."""
    save_config(user_sound_config)
    save_guild_settings(guild_settings)