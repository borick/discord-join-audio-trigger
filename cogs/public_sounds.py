# -*- coding: utf-8 -*-
import discord
from discord.ext import commands
import os
import logging
from typing import List, Optional

import config
from utils import file_helpers # For finding sounds
from core.playback_manager import PlaybackManager # Optional for type hinting

log = logging.getLogger('SoundBot.Cog.PublicSounds')

# --- Autocomplete ---
# Implement the logic directly here, similar to user_sound_autocomplete
async def public_sound_autocomplete(ctx: discord.AutocompleteContext) -> List[discord.OptionChoice]:
    """Autocomplete for public sounds."""
    try:
        sounds = file_helpers.get_public_sound_files() # Get list of public sound base names
        current_value = ctx.value.lower() if ctx.value else ""

        starts_with = []
        contains = []
        for name in sounds:
            lower_name = name.lower()
            display_name = name if len(name) <= 100 else name[:97] + "..." # Truncate if needed
            # Check if starts with or contains the user's input
            if lower_name.startswith(current_value):
                starts_with.append(discord.OptionChoice(name=display_name, value=name))
            elif current_value in lower_name:
                contains.append(discord.OptionChoice(name=display_name, value=name))

        # Sort for better presentation (starts_with first)
        starts_with.sort(key=lambda c: c.name.lower())
        contains.sort(key=lambda c: c.name.lower())

        suggestions = (starts_with + contains)[:25] # Discord limit
        return suggestions
    except Exception as e:
        log.error(f"Error during public sound autocomplete for user {ctx.interaction.user.id}: {e}", exc_info=True)
        return []


class PublicSoundsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.playback_manager: PlaybackManager = bot.playback_manager # Shortcut with type hint

    @commands.slash_command(name="publicsounds", description="Lists all available public sounds.")
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def publicsounds(self, ctx: discord.ApplicationContext):
        """Lists all available public sounds."""
        await ctx.defer(ephemeral=True)
        log.info(f"COMMAND: /publicsounds by {ctx.author.name}")

        public_sounds = file_helpers.get_public_sound_files()

        if not public_sounds:
            await ctx.followup.send("No public sounds have been added yet. Users can use `/publishsound` to share their sounds.", ephemeral=True)
            return

        # --- Pagination ---
        items_per_page = 15
        pages_content = []
        current_page_lines = []
        for i, name in enumerate(public_sounds):
            current_page_lines.append(f"- `{name}`")
            if (i + 1) % items_per_page == 0 or i == len(public_sounds) - 1:
                pages_content.append("\n".join(current_page_lines))
                current_page_lines = []

        # --- Embeds ---
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

        # --- Response ---
        if embeds:
            # TODO: Implement pagination view
            await ctx.followup.send(embed=embeds[0], ephemeral=True)
        else:
            await ctx.followup.send("Could not generate public sound list.", ephemeral=True)


    @commands.slash_command(name="playpublic", description="Plays a public sound in your current voice channel.")
    @commands.cooldown(1, 4, commands.BucketType.user)
    async def playpublic(
        self,
        ctx: discord.ApplicationContext,
        name: discord.Option(str, description="Name of the public sound to play.", required=True, autocomplete=public_sound_autocomplete)
    ):
        """Plays a specified public sound."""
        await ctx.defer() # Public defer
        author = ctx.author
        log.info(f"COMMAND: /playpublic by {author.name}, requested sound: '{name}'")

        public_path = file_helpers.find_public_sound_path(name)

        if not public_path:
            await ctx.edit_original_response(content=f"❌ Public sound `{name}` not found. Use `/publicsounds` to check available sounds.")
            return

        # Use the playback manager - **using the correct method name**
        await self.playback_manager.play_single_sound(ctx.interaction, sound_path=public_path)


    @commands.slash_command(name="removepublic", description="[Admin Only] Remove a sound from the public collection.")
    @commands.has_permissions(manage_guild=True) # Permissions check
    @commands.cooldown(1, 5, commands.BucketType.guild) # Cooldown per guild
    async def removepublic(
        self,
        ctx: discord.ApplicationContext,
        name: discord.Option(str, description="Name of the public sound to remove.", required=True, autocomplete=public_sound_autocomplete)
    ):
        """Allows server admins to remove a public sound."""
        await ctx.defer(ephemeral=True)
        admin = ctx.author
        guild_id_log = ctx.guild.id if ctx.guild else "DM_Context" # Log context guild
        log.info(f"COMMAND: /removepublic by admin {admin.name} ({admin.id}) (context guild: {guild_id_log}), target sound name: '{name}'")

        public_path = file_helpers.find_public_sound_path(name)

        if not public_path:
            await ctx.followup.send(f"❌ Public sound `{name}` not found. Use `/publicsounds` to check.", ephemeral=True)
            return

        public_base_name = os.path.splitext(os.path.basename(public_path))[0]

        # --- Security Check ---
        public_dir_abs = os.path.abspath(config.PUBLIC_SOUNDS_DIR)
        resolved_path_abs = os.path.abspath(public_path)
        if not resolved_path_abs.startswith(public_dir_abs + os.sep):
            log.critical(f"CRITICAL SECURITY ALERT: Path traversal attempt in /removepublic. Admin: {admin.id}, Input: '{name}', Resolved Path: '{resolved_path_abs}'")
            await ctx.followup.send("❌ Internal security error preventing deletion.", ephemeral=True)
            return

        # --- Deletion ---
        try:
            deleted_filename = os.path.basename(public_path)
            os.remove(public_path)
            log.info(f"ADMIN ACTION: Deleted public sound file '{deleted_filename}' by {admin.name}.")
            await ctx.followup.send(f"🗑️ Public sound `{public_base_name}` deleted successfully.", ephemeral=True)
        except OSError as e:
            log.error(f"Admin {admin.name} failed to delete public sound '{public_path}': {e}", exc_info=True)
            await ctx.followup.send(f"❌ Failed to delete public sound `{public_base_name}`: Could not remove file ({type(e).__name__}).", ephemeral=True)
        except Exception as e:
             log.error(f"Admin {admin.name} encountered unexpected error deleting public sound '{public_path}': {e}", exc_info=True)
             await ctx.followup.send(f"❌ An unexpected error occurred while deleting public sound `{public_base_name}`.", ephemeral=True)

    # Error handler specific to this cog's admin command
    @removepublic.error
    async def removepublic_error(self, ctx: discord.ApplicationContext, error: discord.DiscordException):
        if isinstance(error, commands.MissingPermissions):
            log.warning(f"User {ctx.author.name} tried /removepublic without Manage Guild permission.")
            await ctx.respond("🚫 You need the `Manage Server` permission to use this command.", ephemeral=True)
        elif isinstance(error, commands.CommandOnCooldown):
             await ctx.respond(f"⏳ This command is on cooldown for this server. Try again in {error.retry_after:.1f}s.", ephemeral=True)
        # else:
        #     # Let the global error handler in events.py handle other errors
        #     pass


def setup(bot: commands.Bot):
    bot.add_cog(PublicSoundsCog(bot))
    log.info("PublicSounds Cog loaded.")