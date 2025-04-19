# -*- coding: utf-8 -*-
import discord
import asyncio
import logging
from typing import Optional, Dict, Any

import data_manager # To access guild settings
import config as bot_config_module

log = logging.getLogger('SoundBot.VoiceHelpers')

# Note: These functions rely on state (guild_settings, guild_leave_timers)
# being accessible, likely stored on the bot instance or passed explicitly.
# We assume they are passed via the 'bot' instance here.

def is_bot_alone(vc: Optional[discord.VoiceClient]) -> bool:
    """Checks if the bot is the only non-bot user in its voice channel."""
    if not vc or not vc.channel or not vc.guild or not vc.guild.me:
        return False
    human_members = [m for m in vc.channel.members if not m.bot]
    log.debug(f"ALONE CHECK (Guild: {vc.guild.id}, Chan: {vc.channel.name}): {len(human_members)} human(s). Members: {[m.name for m in vc.channel.members]}")
    return len(human_members) == 0

def should_bot_stay(bot: discord.Bot, guild_id: int) -> bool:
    """Checks the guild setting for whether the bot should stay in channel when idle."""
    # Assumes guild_settings is stored on the bot instance
    settings = getattr(bot, 'guild_settings', {}).get(str(guild_id), {})
    stay = settings.get("stay_in_channel", False)
    log.debug(f"Checked stay setting for guild {guild_id}: {stay}")
    return stay is True

def cancel_leave_timer(bot: discord.Bot, guild_id: int, reason: str = "unknown"):
    """Cancels the automatic leave timer for a guild if it exists."""
    # Assumes guild_leave_timers is stored on the bot instance
    guild_leave_timers = getattr(bot, 'guild_leave_timers', {})
    if guild_id in guild_leave_timers:
        timer_task = guild_leave_timers.pop(guild_id, None)
        if timer_task and not timer_task.done():
            try:
                timer_task.cancel()
                log.info(f"LEAVE TIMER: Cancelled for Guild {guild_id}. Reason: {reason}")
            except Exception as e:
                log.warning(f"LEAVE TIMER: Error cancelling timer for Guild {guild_id}: {e}")
        elif timer_task:
             log.debug(f"LEAVE TIMER: Attempted to cancel completed timer for Guild {guild_id}.")

async def start_leave_timer(bot: discord.Bot, vc: discord.VoiceClient):
    """Starts the automatic leave timer if conditions are met (bot alone, stay disabled, idle)."""
    if not vc or not vc.is_connected() or not vc.guild:
        if vc: log.warning(f"start_leave_timer called with invalid/disconnected VC for guild {vc.guild.id if vc.guild else 'Unknown'}")
        return

    guild_id = vc.guild.id
    log_prefix = f"LEAVE TIMER (Guild {guild_id}):"

    # Assumes guild_leave_timers and AUTO_LEAVE_TIMEOUT_SECONDS are accessible via bot or config
    guild_leave_timers = getattr(bot, 'guild_leave_timers', {})

    bot_config = getattr(bot, 'config', bot_config_module)
    auto_leave_timeout = getattr(bot_config, 'AUTO_LEAVE_TIMEOUT_SECONDS', 14400)

    # 1. Cancel any existing timer first
    cancel_leave_timer(bot, guild_id, reason="starting new timer check")

    # 2. Check conditions
    if not is_bot_alone(vc):
        log.debug(f"{log_prefix} Not starting timer - bot is not alone.")
        return
    if should_bot_stay(bot, guild_id):
        log.debug(f"{log_prefix} Not starting timer - 'stay' setting is enabled.")
        return
    if vc.is_playing():
         log.debug(f"{log_prefix} Not starting timer - bot is currently playing.")
         return

    log.info(f"{log_prefix} Conditions met (alone, stay disabled, idle). Starting {auto_leave_timeout}s timer.")

    async def _leave_after_delay(bot_ref: discord.Bot, voice_client_ref: discord.VoiceClient, g_id: int, timeout: int):
        original_channel = voice_client_ref.channel
        try:
            await asyncio.sleep(timeout)

            # Re-check conditions AFTER sleep
            current_vc = discord.utils.get(bot_ref.voice_clients, guild__id=g_id)
            if not current_vc or not current_vc.is_connected() or current_vc.channel != original_channel:
                 log.info(f"{log_prefix} Timer expired, but bot disconnected/moved from {original_channel.name if original_channel else 'orig chan'}. Aborting leave.")
                 return
            if not is_bot_alone(current_vc):
                 log.info(f"{log_prefix} Timer expired, but bot no longer alone in {current_vc.channel.name}. Aborting leave.")
                 return
            if should_bot_stay(bot_ref, g_id):
                 log.info(f"{log_prefix} Timer expired, but 'stay' enabled during wait. Aborting leave.")
                 return
            if current_vc.is_playing():
                log.info(f"{log_prefix} Timer expired, but bot started playing again. Aborting leave.")
                return

            # Conditions still met - Trigger Disconnect
            log.info(f"{log_prefix} Timer expired. Conditions still met in {current_vc.channel.name}. Triggering automatic disconnect.")
            await safe_disconnect(bot_ref, current_vc, manual_leave=False)

        except asyncio.CancelledError:
             log.info(f"{log_prefix} Timer explicitly cancelled.")
        except Exception as e:
             log.error(f"{log_prefix} Error during leave timer delay/check: {e}", exc_info=True)
        finally:
             # Clean up task entry
             task_obj = asyncio.current_task()
             current_timers = getattr(bot_ref, 'guild_leave_timers', {})
             if task_obj and g_id in current_timers and current_timers[g_id] is task_obj:
                 del current_timers[g_id]
                 log.debug(f"{log_prefix} Cleaned up timer task reference.")

    # Create and store the timer task
    timer_task = bot.loop.create_task(_leave_after_delay(bot, vc, guild_id, auto_leave_timeout), name=f"AutoLeave_{guild_id}")
    guild_leave_timers[guild_id] = timer_task

async def safe_disconnect(bot: discord.Bot, vc: Optional[discord.VoiceClient], *, manual_leave: bool = False):
    """Handles disconnecting the bot, considering stay settings and cleaning up tasks/timers."""
    if not vc or not vc.is_connected():
        log.debug("safe_disconnect called but VC is already disconnected or invalid.")
        return

    guild = vc.guild
    guild_id = guild.id

    # ALWAYS cancel leave timer before attempting disconnect
    cancel_leave_timer(bot, guild_id, reason="safe_disconnect called")

    # Check if disconnect should be skipped due to 'stay' setting (only if not manual)
    if not manual_leave and should_bot_stay(bot, guild_id):
        log.debug(f"Disconnect skipped for {guild.name}: 'Stay in channel' is enabled.")
        # Clean up play task if bot is idle but staying (defensive check)
        # Access playback_manager via bot instance
        playback_manager = getattr(bot, 'playback_manager', None)
        if playback_manager:
            is_playing_check = vc.is_playing()
            is_queue_empty_check = playback_manager.is_queue_empty(guild_id)
            if is_queue_empty_check and not is_playing_check:
                playback_manager.cleanup_play_task(guild_id, reason="STAY MODE idle cleanup")
        return # Don't disconnect

    # Determine if disconnect should happen
    playback_manager = getattr(bot, 'playback_manager', None)
    is_queue_empty = playback_manager.is_queue_empty(guild_id) if playback_manager else True
    is_playing = vc.is_playing()
    should_disconnect = manual_leave or (is_queue_empty and not is_playing)

    if should_disconnect:
        disconnect_reason = "Manual /leave or auto-timer" if manual_leave else "Idle, queue empty, and stay disabled"
        log.info(f"DISCONNECT: Conditions met for {guild.name} ({disconnect_reason}). Disconnecting...")
        try:
            if vc.is_playing():
                log_level = logging.WARNING if not manual_leave else logging.DEBUG
                log.log(log_level, f"DISCONNECT: Called stop() during disconnect for {guild.name} (Manual: {manual_leave}).")
                vc.stop() # This should trigger after_play handler via PlaybackManager

            await vc.disconnect(force=False)
            log.info(f"DISCONNECT: Bot disconnected from '{guild.name}'. (VC state change event will trigger final cleanup if needed)")

            # Explicit cleanup via PlaybackManager
            if playback_manager:
                playback_manager.cleanup_guild_state(guild_id, reason="safe_disconnect")

        except Exception as e:
            log.error(f"DISCONNECT ERROR: Failed disconnect from {guild.name}: {e}", exc_info=True)
    else:
         log.debug(f"Disconnect skipped for {guild.name}: Manual={manual_leave}, QueueEmpty={is_queue_empty}, Playing={is_playing}, StayEnabled={should_bot_stay(bot, guild_id)}.")


async def ensure_voice_client_ready(interaction: discord.Interaction, target_channel: discord.VoiceChannel, action_type: str = "Playback") -> Optional[discord.VoiceClient]:
    """
    Helper to connect/move VC, check permissions, and check busy status.
    Returns the VoiceClient if ready, otherwise None. Sends feedback to user.
    Relies on bot instance being available via interaction.client
    """
    bot = interaction.client
    responder = interaction.followup if interaction.response.is_done() else interaction.edit_original_response

    guild = interaction.guild
    user = interaction.user
    if not guild:
        try: await responder(content="This command must be used in a server.", ephemeral=True)
        except discord.NotFound: pass
        except Exception as e: log.warning(f"Error responding in ensure_voice_client_ready (no guild): {e}")
        return None

    guild_id = guild.id
    log_prefix = f"{action_type.upper()}:"

    # Check bot permissions
    bot_perms = target_channel.permissions_for(guild.me)
    if not bot_perms.connect or not bot_perms.speak:
        try: await responder(content=f"❌ I don't have permission to Connect or Speak in {target_channel.mention}.", ephemeral=True)
        except discord.NotFound: pass
        except Exception as e: log.warning(f"Error responding in ensure_voice_client_ready (perms): {e}")
        log.warning(f"{log_prefix} Missing Connect/Speak perms in {target_channel.name} ({guild.name}).")
        return None

    vc = discord.utils.get(bot.voice_clients, guild=guild)
    playback_manager = getattr(bot, 'playback_manager', None)

    try:
        if vc and vc.is_connected():
            # Check if playing or queue active (use PlaybackManager if available)
            is_busy = vc.is_playing() or (playback_manager and not playback_manager.is_queue_empty(guild_id))
            if is_busy:
                msg = "⏳ Bot is currently playing sounds. Please wait."
                log_msg = f"{log_prefix} Bot busy in {guild.name}, user {user.name}'s request ignored."
                try: await responder(content=msg, ephemeral=True)
                except discord.NotFound: pass
                except Exception as e: log.warning(f"Error responding in ensure_voice_client_ready (busy): {e}")
                log.info(log_msg)
                return None

            elif vc.channel != target_channel:
                should_move = (isinstance(user, discord.Member) and user.voice and user.voice.channel == target_channel) or not should_bot_stay(bot, guild_id)
                if should_move:
                     log.info(f"{log_prefix} Moving from '{vc.channel.name}' to '{target_channel.name}' for {user.name}.")
                     cancel_leave_timer(bot, guild_id, reason=f"moving for {action_type}")
                     await vc.move_to(target_channel)
                     log.info(f"{log_prefix} Moved successfully.")
                else:
                    log.debug(f"{log_prefix} Not moving from '{vc.channel.name}' to '{target_channel.name}' because stay is enabled and user isn't there.")
                    try: await responder(content=f"ℹ️ I'm currently staying in {vc.channel.mention}. Please join that channel or disable the stay setting with `/togglestay` (admin).", ephemeral=True)
                    except discord.NotFound: pass
                    except Exception as e: log.warning(f"Error responding in ensure_voice_client_ready (stay): {e}")
                    return None
        else:
            log.info(f"{log_prefix} Connecting to '{target_channel.name}' for {user.name}.")
            cancel_leave_timer(bot, guild_id, reason=f"connecting for {action_type}")
            vc = await target_channel.connect(timeout=30.0, reconnect=True)
            log.info(f"{log_prefix} Connected successfully.")

        if not vc or not vc.is_connected():
             log.error(f"{log_prefix} Failed to establish voice client for {target_channel.name} after connect/move attempt.")
             try: await responder(content="❌ Failed to connect or move to the voice channel.", ephemeral=True)
             except discord.NotFound: pass
             except Exception as e: log.warning(f"Error responding in ensure_voice_client_ready (connect failed): {e}")
             return None

        # Bot is now connected and idle in the correct channel
        cancel_leave_timer(bot, guild_id, reason=f"ensured ready for {action_type}")
        return vc

    except asyncio.TimeoutError:
         try: await responder(content="❌ Connection to the voice channel timed out.", ephemeral=True)
         except discord.NotFound: pass
         except Exception as e: log.warning(f"Error responding in ensure_voice_client_ready (timeout): {e}")
         log.error(f"{log_prefix} Connection/Move Timeout in {guild.name} to {target_channel.name}")
         return None
    except discord.errors.ClientException as e:
        msg = "⏳ Bot is busy connecting/disconnecting. Please wait a moment." if "already connect" in str(e).lower() else f"❌ Error connecting/moving: {e}. Check permissions or try again."
        try: await responder(content=msg, ephemeral=True)
        except discord.NotFound: pass
        except Exception as e_resp: log.warning(f"Error responding in ensure_voice_client_ready (ClientException): {e_resp}")
        log.warning(f"{log_prefix} Connection/Move ClientException in {guild.name}: {e}")
        return None
    except Exception as e:
        try: await responder(content="❌ An unexpected error occurred while joining the voice channel.", ephemeral=True)
        except discord.NotFound: pass
        except Exception as e_resp: log.warning(f"Error responding in ensure_voice_client_ready (unexpected): {e_resp}")
        log.error(f"{log_prefix} Connection/Move unexpected error in {guild.name}: {e}", exc_info=True)
        return None
