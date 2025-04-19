import discord
import discord.opus
import os
import sys
import struct
import ctypes
import ctypes.util
import logging
import platform

# --- Configuration ---
# If you know the exact path to your Opus DLL/SO/DYLIB, set it here.
# Otherwise, leave it as None to let the script search.
EXPLICIT_OPUS_PATH = None
# Example: EXPLICIT_OPUS_PATH = r"C:\path\to\your\opus.dll"
# Example: EXPLICIT_OPUS_PATH = "/usr/local/lib/libopus.so.0"

# --- Script Start ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

logging.info(f"--- Discord.py Opus Library Load Test ---")
logging.info(f"Python Version: {platform.python_version()}")
logging.info(f"Platform: {platform.system()} {platform.release()} ({platform.machine()})")
logging.info(f"discord.py (py-cord) Version: {discord.__version__}")
logging.info(f"discord.opus module path: {discord.opus.__file__}")

# Check 1: Is Opus already loaded somehow?
logging.info("\n[Check 1: Initial is_loaded()]")
if discord.opus.is_loaded():
    logging.info("✅ SUCCESS: discord.opus.is_loaded() returned True initially.")
    logging.info("   This means Opus was likely loaded automatically or previously.")
    exit(0)
else:
    logging.info("ℹ️ INFO: discord.opus.is_loaded() returned False initially.")

# Check 2: Try loading the explicit path if provided
opus_loaded = False
if EXPLICIT_OPUS_PATH:
    logging.info(f"\n[Check 2: Explicit Path Provided ('{EXPLICIT_OPUS_PATH}')]")
    try:
        discord.opus.load_opus(EXPLICIT_OPUS_PATH)
        if discord.opus.is_loaded():
            logging.info(f"✅ SUCCESS: Opus loaded successfully using explicit path: '{EXPLICIT_OPUS_PATH}'")
            opus_loaded = True
        else:
            # This shouldn't really happen if load_opus doesn't error, but check anyway
            logging.warning(f"⚠️ WARNING: load_opus('{EXPLICIT_OPUS_PATH}') didn't error, but is_loaded() is still False.")
    except OSError as e:
        logging.error(f"❌ FAILED to load explicit path '{EXPLICIT_OPUS_PATH}': {e}")
    except Exception as e:
        logging.error(f"❌ UNEXPECTED ERROR loading explicit path '{EXPLICIT_OPUS_PATH}': {e}", exc_info=True)

# Check 3: Simulate the _load_default() behavior if not already loaded
if not opus_loaded:
    logging.info("\n[Check 3: Simulating Internal _load_default()]")
    try:
        if sys.platform == 'win32':
            # Try loading the bundled Windows DLL
            basedir = os.path.dirname(os.path.abspath(discord.opus.__file__))
            _bitness = struct.calcsize('P') * 8
            _target = 'x64' if _bitness > 32 else 'x86'
            _filename = os.path.join(basedir, 'bin', f'libopus-0.{_target}.dll')
            logging.info(f"   Platform is Windows. Attempting to load bundled DLL: '{_filename}'")
            if os.path.exists(_filename):
                try:
                    discord.opus.load_opus(_filename)
                    if discord.opus.is_loaded():
                        logging.info(f"✅ SUCCESS: Loaded bundled Windows DLL: '{_filename}'")
                        opus_loaded = True
                    else:
                         logging.warning(f"⚠️ WARNING: load_opus (bundled DLL) didn't error, but is_loaded() is still False.")
                except OSError as e:
                    logging.error(f"❌ FAILED to load bundled Windows DLL '{_filename}': {e}")
                except Exception as e:
                     logging.error(f"❌ UNEXPECTED ERROR loading bundled DLL '{_filename}': {e}", exc_info=True)
            else:
                logging.warning(f"   INFO: Bundled DLL not found at '{_filename}'.")

        else:
            # Try using ctypes.util.find_library (Linux/macOS default)
            logging.info("   Platform is not Windows. Attempting ctypes.util.find_library('opus')...")
            found_path = ctypes.util.find_library('opus')
            if found_path:
                logging.info(f"   find_library('opus') found: '{found_path}'. Attempting to load...")
                try:
                    discord.opus.load_opus(found_path)
                    if discord.opus.is_loaded():
                        logging.info(f"✅ SUCCESS: Loaded library found by find_library: '{found_path}'")
                        opus_loaded = True
                    else:
                        logging.warning(f"⚠️ WARNING: load_opus (find_library path) didn't error, but is_loaded() is still False.")
                except OSError as e:
                    logging.error(f"❌ FAILED to load path from find_library '{found_path}': {e}")
                except Exception as e:
                    logging.error(f"❌ UNEXPECTED ERROR loading path from find_library '{found_path}': {e}", exc_info=True)
            else:
                logging.warning("   INFO: ctypes.util.find_library('opus') returned None.")

    except Exception as e:
        logging.error(f"❌ Error occurred during _load_default simulation: {e}", exc_info=True)

# Check 4: Try loading common names directly if still not loaded
if not opus_loaded:
    logging.info("\n[Check 4: Trying Common Library Names Directly]")
    common_names = ['opus', 'libopus.so.0', 'libopus.so', 'libopus.dylib', 'opus.dll']
    for name in common_names:
        logging.info(f"   Attempting to load generic name: '{name}'")
        try:
            # Clear any previous failed attempts within the module if necessary (though load_opus should handle it)
            # discord.opus._lib = None # Not typically needed, load_opus overwrites
            discord.opus.load_opus(name)
            if discord.opus.is_loaded():
                logging.info(f"✅ SUCCESS: Loaded successfully using name: '{name}'")
                opus_loaded = True
                break # Stop after first success
            else:
                logging.warning(f"⚠️ WARNING: load_opus('{name}') didn't error, but is_loaded() is still False.")
        except OSError:
            logging.info(f"   INFO: Failed to load '{name}' (Expected if not found/compatible).") # Info level for expected failures
        except Exception as e:
            logging.error(f"❌ UNEXPECTED ERROR loading '{name}': {e}", exc_info=True)
        if opus_loaded: break


# Final Summary
logging.info("\n--- Test Summary ---")
if opus_loaded:
    logging.info("✅✅✅ Opus library was successfully loaded during these tests.")
    try:
        version = discord.opus._OpusStruct.get_opus_version()
        logging.info(f"   Loaded Opus Version String: {version}")
    except Exception as e:
        logging.warning(f"   Could not retrieve Opus version string after loading: {e}")
    logging.info("   Your main bot script *should* now be able to load Opus using the same environment.")
else:
    logging.error("❌❌❌ Opus library could NOT be loaded by any tested method.")
    logging.error("    Voice functionality requiring Opus encoding (like PCMAudio playback)")
    logging.error("    will likely FAIL or be UNSTABLE in your main bot script.")
    logging.error("    -> Ensure the Opus library file ('opus.dll', 'libopus.so', etc.) matching your")
    logging.error("       Python interpreter's architecture (32/64-bit) is installed and accessible.")
    logging.error("    -> Common locations: Same folder as your script, system PATH, standard library paths.")
    logging.error("    -> You can also try setting EXPLICIT_OPUS_PATH at the top of this script.")