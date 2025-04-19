# -*- coding: utf-8 -*-
import os
import re
import logging
import shutil
from typing import List, Optional, Tuple, Dict
import discord  # <--- ADD THIS LINE

import config # Import config for ALLOWED_EXTENSIONS

log = logging.getLogger('SoundBot.Utils.FileHelpers')

def ensure_dir(dir_path: str):
    """Creates a directory if it doesn't exist."""
    if not os.path.exists(dir_path):
        try:
            os.makedirs(dir_path)
            log.info(f"Created directory: {dir_path}")
        except Exception as e:
            log.critical(f"CRITICAL: Could not create directory '{dir_path}': {e}", exc_info=True)
            # Exit if essential directories cannot be created (consider if this is desired)
            if dir_path in [config.SOUNDS_DIR, config.USER_SOUNDS_DIR, config.PUBLIC_SOUNDS_DIR]:
                 raise RuntimeError(f"Failed to create essential directory: {dir_path}") from e

def sanitize_filename(name: str) -> str:
    """Removes/replaces invalid chars for filenames and limits length."""
    if not isinstance(name, str): return "sound" # Handle non-string input
    # Remove or replace potentially problematic characters for filenames
    # Keep underscores, letters, numbers. Replace whitespace and most symbols with underscore.
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f\s]+', '_', name)
    # Remove characters that might be misinterpreted by shells or web servers, though less common in basic filenames
    name = re.sub(r'[;&$()`\'"]', '', name)
    # Collapse multiple underscores
    name = re.sub(r'_+', '_', name)
    # Remove leading/trailing underscores
    name = name.strip('_')
    # Limit length to prevent excessively long filenames
    max_len = 50
    name = name[:max_len] if len(name) > max_len else name
    # Ensure the filename is not empty after sanitization
    return name if name else "sound"

def _find_sound_path_in_dir(directory: str, sound_name: str) -> Optional[str]:
    """
    Generic helper to find a sound file by name (case-insensitive, checks extensions).
    Handles sanitized names if exact match fails. Returns the full path.
    """
    if not os.path.isdir(directory):
        log.debug(f"Directory not found for searching: {directory}")
        return None

    # Prioritize common/efficient formats if multiple files with the same base name exist
    preferred_order = ['.mp3', '.wav', '.ogg', '.m4a', '.aac'] # Match config.ALLOWED_EXTENSIONS logic

    # Check both the raw name and the sanitized version
    name_variants_to_check = [sound_name]
    sanitized = sanitize_filename(sound_name) # Use the shared sanitizer
    if sanitized and sanitized != sound_name:
        name_variants_to_check.append(sanitized)
        log.debug(f"Searching for name variants: {name_variants_to_check}")

    for name_variant in name_variants_to_check:
        try:
            # Use scandir for potentially better performance on large directories
            with os.scandir(directory) as entries:
                found_paths: Dict[str, str] = {} # Store found paths by extension
                for entry in entries:
                    if entry.is_file():
                        base, file_ext = os.path.splitext(entry.name)
                        file_ext_lower = file_ext.lower()
                        # Case-insensitive base name comparison
                        if base.lower() == name_variant.lower() and file_ext_lower in config.ALLOWED_EXTENSIONS:
                            found_paths[file_ext_lower] = entry.path

                # Check found paths against preferred order
                for ext in preferred_order:
                    if ext in found_paths:
                        log.debug(f"Found sound '{name_variant}' with preferred ext '{ext}' at: {found_paths[ext]}")
                        return found_paths[ext] # Return the first match in preferred order

                # If not found in preferred order, return any valid match (less common)
                if found_paths:
                    first_found_path = next(iter(found_paths.values()))
                    log.debug(f"Found sound '{name_variant}' with non-preferred ext at: {first_found_path}")
                    return first_found_path

        except OSError as e:
            log.error(f"Error listing files in {directory} during find: {e}")
            return None # Abort search for this variant if directory listing fails

    log.debug(f"Sound '{sound_name}' (or sanitized variants) not found in {directory}")
    return None # Not found after checking all variants

def _get_sound_files_from_dir(directory: str) -> List[str]:
    """Generic helper to list sound base names (without extension) from a directory."""
    sounds = set() # Use a set to avoid duplicates if multiple extensions exist
    if os.path.isdir(directory):
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if entry.is_file():
                        base_name, ext = os.path.splitext(entry.name)
                        if ext.lower() in config.ALLOWED_EXTENSIONS:
                            sounds.add(base_name) # Add only the base name
        except OSError as e:
            log.error(f"Error listing files in {directory}: {e}")
    # Return sorted list for consistent display
    return sorted(list(sounds), key=str.lower)

# --- Specific Sound Directory Helpers ---

def get_user_sound_files(user_id: int) -> List[str]:
    """Lists base names of sound files for a specific user."""
    user_dir = os.path.join(config.USER_SOUNDS_DIR, str(user_id))
    return _get_sound_files_from_dir(user_dir)

def find_user_sound_path(user_id: int, sound_name: str) -> Optional[str]:
    """Finds the full path for a user's sound by base name."""
    user_dir = os.path.join(config.USER_SOUNDS_DIR, str(user_id))
    return _find_sound_path_in_dir(user_dir, sound_name)

def get_public_sound_files() -> List[str]:
    """Lists base names of public sound files."""
    return _get_sound_files_from_dir(config.PUBLIC_SOUNDS_DIR)

def find_public_sound_path(sound_name: str) -> Optional[str]:
    """Finds the full path for a public sound by base name."""
    return _find_sound_path_in_dir(config.PUBLIC_SOUNDS_DIR, sound_name)

async def validate_and_save_upload(
    ctx: discord.ApplicationContext, # Use ApplicationContext for slash commands
    sound_file: discord.Attachment,
    target_save_path: str,
    command_name: str = "upload"
) -> Tuple[bool, Optional[str]]:
    """
    Validates attachment (type, size), saves temporarily, checks with Pydub,
    moves/renames to final path if valid.
    Returns (success_bool, error_message_or_None). Sends NO user feedback itself.
    """
    # Ensure config is available for checks
    if not config.PYDUB_AVAILABLE:
        log.critical("Pydub is not available, cannot validate uploads.")
        return False, "❌ Server Error: Audio processing library (Pydub) is missing."

    from pydub import AudioSegment # Import locally to ensure PYDUB_AVAILABLE check passed
    from pydub.exceptions import CouldntDecodeError

    user_id = ctx.author.id
    log_prefix = f"{command_name.upper()} VALIDATION (User: {user_id})"

    file_extension = os.path.splitext(sound_file.filename)[1].lower()
    if not file_extension or file_extension not in config.ALLOWED_EXTENSIONS:
        log.warning(f"{log_prefix}: Invalid extension '{file_extension}' from '{sound_file.filename}'.")
        allowed_str = ', '.join(config.ALLOWED_EXTENSIONS)
        return False, f"❌ Invalid file type (`{file_extension}`). Allowed: {allowed_str}"

    if sound_file.size > config.MAX_USER_SOUND_SIZE_MB * 1024 * 1024:
        size_mb = sound_file.size / (1024 * 1024)
        log.warning(f"{log_prefix}: File too large '{sound_file.filename}' ({size_mb:.2f} MB). Max: {config.MAX_USER_SOUND_SIZE_MB}MB.")
        return False, f"❌ File too large (`{size_mb:.2f}` MB). Max: {config.MAX_USER_SOUND_SIZE_MB}MB."

    # Optional: Check content type header, but don't rely on it solely
    if not sound_file.content_type or not sound_file.content_type.startswith('audio/'):
        log.warning(f"{log_prefix}: Content-Type '{sound_file.content_type}' for '{sound_file.filename}' not 'audio/*'. Proceeding with Pydub check.")

    # --- Temporary Saving ---
    temp_save_dir = os.path.dirname(target_save_path)
    ensure_dir(temp_save_dir) # Make sure the target directory exists
    # Create a unique temporary filename
    temp_save_filename = f"temp_{command_name}_{user_id}_{os.urandom(4).hex()}{file_extension}"
    temp_save_path = os.path.join(temp_save_dir, temp_save_filename)

    async def cleanup_temp():
        if os.path.exists(temp_save_path):
            try:
                os.remove(temp_save_path)
                log.debug(f"{log_prefix}: Cleaned up temporary file: {temp_save_path}")
            except Exception as del_e:
                log.warning(f"{log_prefix}: Failed to clean up temporary file '{temp_save_path}': {del_e}")

    try:
        log.debug(f"{log_prefix}: Saving temporary file to '{temp_save_path}'...")
        await sound_file.save(temp_save_path)
        log.info(f"{log_prefix}: Saved temporary file: '{temp_save_path}' ({sound_file.size} bytes)")

        # --- Pydub Validation ---
        try:
            log.debug(f"{log_prefix}: Pydub decode check starting for: '{temp_save_path}'")
            # Explicitly provide format if possible, helps Pydub/FFmpeg
            audio_format = file_extension.strip('.') if file_extension else None
            if not audio_format:
                 log.warning(f"{log_prefix}: No file extension found for Pydub format hint, trying auto-detection.")
            # This is the core validation step
            audio = AudioSegment.from_file(temp_save_path, format=audio_format)
            log.info(f"{log_prefix}: Pydub validation OK for '{temp_save_path}' (Duration: {len(audio)}ms)")

            # --- Final Move/Rename ---
            try:
                # Use os.replace for atomic rename where possible (safer)
                os.replace(temp_save_path, target_save_path)
                log.info(f"{log_prefix}: Final file saved (atomic replace/rename): '{target_save_path}'")
                return True, None # SUCCESS
            except OSError as rep_e:
                # Fallback to shutil.move if os.replace fails (e.g., cross-device move)
                log.warning(f"{log_prefix}: os.replace failed ('{rep_e}'), trying shutil.move for '{temp_save_path}' -> '{target_save_path}'.")
                try:
                    # Ensure target doesn't exist if shutil.move might error on overwrite
                    if os.path.exists(target_save_path):
                        log.warning(f"{log_prefix}: Target path '{target_save_path}' exists, removing before fallback move.")
                        os.remove(target_save_path)
                    shutil.move(temp_save_path, target_save_path)
                    log.info(f"{log_prefix}: Final file saved (fallback move): '{target_save_path}'")
                    return True, None # SUCCESS
                except Exception as move_e:
                    log.error(f"{log_prefix}: FAILED final save (replace error: {rep_e}, fallback move error: {move_e})", exc_info=True)
                    await cleanup_temp() # Clean up temp file on final move failure
                    return False, "❌ Error saving the sound file after validation."

        except CouldntDecodeError as decode_error:
            log.error(f"{log_prefix}: FAILED (Pydub Decode - File: '{sound_file.filename}'): {decode_error}", exc_info=True)
            await cleanup_temp()
            # Provide helpful feedback to the user
            err_msg = f"❌ **Audio Validation Failed!** Could not process `{sound_file.filename}`."
            err_msg += " It might be corrupted, in an unsupported format, or require FFmpeg."
            if 'ffmpeg' in str(decode_error).lower() or 'ffprobe' in str(decode_error).lower():
                 err_msg += "\n**-> Ensure FFmpeg is installed and accessible by the bot.**"
            else:
                 err_msg += f"\n(File type: {file_extension}, Error hint: {decode_error})"
            return False, err_msg
        except Exception as validate_e:
            log.error(f"{log_prefix}: FAILED (Unexpected Pydub check error - File: '{sound_file.filename}'): {validate_e}", exc_info=True)
            await cleanup_temp()
            return False, "❌ **Audio Validation Failed!** An unexpected error occurred during audio processing."

    except discord.HTTPException as e:
        log.error(f"{log_prefix}: Error downloading temp file '{sound_file.filename}': {e}", exc_info=True)
        # No temp file exists yet, so no cleanup needed here
        return False, "❌ Error downloading the sound file from Discord."
    except Exception as e:
        log.error(f"{log_prefix}: Unexpected error during initial temp save for '{sound_file.filename}': {e}", exc_info=True)
        await cleanup_temp() # Attempt cleanup if temp file might exist partially
        return False, "❌ An unexpected server error occurred during file handling."