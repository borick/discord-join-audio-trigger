# -*- coding: utf-8 -*-
import discord
from discord.ext import commands
import os
import logging

from utils import file_helpers, voice_helpers # Import helpers
import data_manager # Import data manager functions
import config # Import config for paths

log = logging.getLogger('SoundBot.Cogs.Admin')

class AdminCog(commands.Cog):
    def __init__(self, bot: discord.Bot):
        self.bot = bot
        # Access data/state stored on the bot instance
        self.guild_settings = getattr(bot, 'guild_settings', {})

    # --- Commands ---
    @discord.slash_command(name="togglestay", description="[Admin Only] Toggle whether the bot stays in VC when idle.")
    @commands.has_permissions(manage_guild=True)
    @commands.cooldown(1, 5, commands.BucketType.guild)
    async def togglestay(self, ctx: discord.ApplicationContext):
        """Toggles the 'stay_in_channel' setting for the current guild."""
        await ctx.defer(ephemeral=True)
        if not ctx.guild_id or not ctx.guild:
            await ctx.followup.send("This command can only be used in a server.", ephemeral=True)
            return

        guild_id_str = str(ctx.guild_id)
        guild_id = ctx.guild_id
        admin = ctx.author
        log.info(f"COMMAND: /togglestay by admin {admin.name} ({admin.id}) in guild {ctx.guild.name} ({guild_id_str})")

        current_setting = self.guild_settings.get(guild_id_str, {}).get("stay_in_channel", False)
        new_setting = not current_setting

        self.guild_settings.setdefault(guild_id_str, {})['stay_in_channel'] = new_setting
        data_manager.save_guild_settings(self.guild_settings) # Save updated settings

        status_message = "ENABLED ✅ (Bot will now stay in VC when idle)" if new_setting else "DISABLED ❌ (Bot will now leave VC after being idle and alone)"
        await ctx.followup.send(f"Bot 'Stay in Channel' feature is now **{status_message}** for this server.", ephemeral=True)
        log.info(f"Guild {ctx.guild.name} ({guild_id_str}) 'stay_in_channel' set to {new_setting} by {admin.name}")

        # Trigger timer logic based on new setting
        vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
        if vc and vc.is_connected():
            if new_setting:
                voice_helpers.cancel_leave_timer(self.bot, guild_id, reason="togglestay enabled")
            else:
                # If stay disabled, check if timer should start now
                if not vc.is_playing() and voice_helpers.is_bot_alone(vc):
                    log.info(f"TOGGLESTAY: Stay disabled, bot is idle and alone. Triggering leave timer check.")
                    self.bot.loop.create_task(voice_helpers.start_leave_timer(self.bot, vc))
                elif vc.is_playing():
                    log.debug("TOGGLESTAY: Stay disabled, but bot currently playing.")
                else:
                    log.debug("TOGGLESTAY: Stay disabled, but bot not alone.")

    @discord.slash_command(name="leave", description="Make the bot leave its current voice channel.")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def leave(self, ctx: discord.ApplicationContext):
        """Forces the bot to leave the voice channel in the current guild."""
        await ctx.defer(ephemeral=True)
        guild = ctx.guild
        user = ctx.author

        if not guild:
            await ctx.followup.send("This command must be used in a server.", ephemeral=True)
            return

        log.info(f"COMMAND: /leave invoked by {user.name} ({user.id}) in guild {guild.name} ({guild.id})")
        vc = discord.utils.get(self.bot.voice_clients, guild=guild)

        if vc and vc.is_connected():
            channel_name = vc.channel.name if vc.channel else "Unknown Channel"
            log.info(f"LEAVE: Manually disconnecting from {channel_name} in {guild.name} due to /leave command...")
            # Use safe_disconnect which handles cleanup via PlaybackManager
            await voice_helpers.safe_disconnect(self.bot, vc, manual_leave=True)
            await ctx.followup.send(f"👋 Leaving {channel_name}.", ephemeral=True)
        else:
            log.info(f"LEAVE: Request by {user.name}, but bot not connected in {guild.name}.")
            await ctx.followup.send("🤷 I'm not currently in a voice channel in this server.", ephemeral=True)


    # --- Public Sound Removal ---
    async def public_sound_autocomplete(self, ctx: discord.AutocompleteContext) -> list[discord.OptionChoice]:
        """Autocomplete for public sounds."""
        return await file_helpers._generic_sound_autocomplete(ctx, file_helpers.get_public_sound_files)

    # --- Error Handlers for Admin Commands ---
    @togglestay.error
    async def admin_command_error(self, ctx: discord.ApplicationContext, error: discord.DiscordException):
        """Error handler specifically for admin commands permissions and cooldown."""
        if isinstance(error, commands.MissingPermissions):
            log.warning(f"User {ctx.author.name} tried admin command /{ctx.command.name} without Manage Guild permission.")
            await ctx.respond("🚫 You need the `Manage Server` permission to use this command.", ephemeral=True, delete_after=15)
        elif isinstance(error, commands.CommandOnCooldown):
            await ctx.respond(f"⏳ This command is on cooldown. Try again in {error.retry_after:.1f}s.", ephemeral=True, delete_after=10)
        else:
            # Let the global handler in events.py deal with other errors
            # Raise the error again so the global handler catches it
            # Or call the global handler directly if preferred:
            # events_cog = self.bot.get_cog("EventsCog")
            # if events_cog:
            #     await events_cog.on_application_command_error(ctx, error)
            # else:
            #     log.error(f"Could not find EventsCog to forward error: {error}")
            log.debug(f"Forwarding error from AdminCog to global handler: {type(error).__name__}")
            raise error # Re-raise for the global handler


def setup(bot: discord.Bot):
    bot.add_cog(AdminCog(bot))
    log.info("Admin Cog loaded.")

