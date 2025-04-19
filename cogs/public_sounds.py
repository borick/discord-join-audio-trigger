# -*- coding: utf-8 -*-
import discord
from discord.ext import commands
import os
import shutil
import logging
from typing import List

from utils import file_helpers # Import helpers
from core.playback_manager import PlaybackManager # Import manager
import config # Import config for paths

log = logging.getLogger('SoundBot.Cogs.PublicSounds')

class PublicSoundsCog(commands.Cog):
    def __init__(self, bot: discord.Bot):
        self.bot = bot
        # Get playback manager from bot instance
        self.playback_manager: PlaybackManager = getattr(bot, 'playback_manager')

    # --- Autocomplete ---
    async def user_sound_autocomplete(self, ctx: discord.AutocompleteContext) -> List[discord.OptionChoice]:
        """Autocomplete for user's personal sounds."""
        return await file_helpers._generic_sound_autocomplete(ctx, file_helpers.get_user_sound_files, ctx.interaction.user.id)

    async def public_sound_autocomplete(self, ctx: discord.AutocompleteContext) -> List[discord.OptionChoice]:
        """Autocomplete for public sounds."""
        return await file_helpers._generic_sound_autocomplete(ctx, file_helpers.get_public_sound_files)

    # --- Commands ---
    @discord.slash_command(name="publishsound", description="Make one of your personal sounds public for everyone.")
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def publishsound(
        self,
        ctx: discord.ApplicationContext,
        name: discord.Option(str, description="Name of YOUR personal sound to make public.", required=True, autocomplete=user_sound_autocomplete)
    ):
        """Makes one of the user's personal sounds available publicly."""
        await ctx.defer(ephemeral=True)
        author = ctx.author
        user_id = author.id
        log.info(f"COMMAND: /publishsound by {author.name} ({user_id}), target sound name: '{name}'")

        user_path = file_helpers.find_user_sound_path(user_id, name)
        if not user_path:
            await ctx.followup.send(f"❌ Personal sound `{name}` not found. Use `/mysounds` to check.", ephemeral=True); return

        source_filename = os.path.basename(user_path)
        source_base_name, source_ext = os.path.splitext(source_filename)

        public_base_name = file_helpers.sanitize_filename(name)
        if not public_base_name:
            await ctx.followup.send(f"❌ Invalid public name after sanitization (from '{name}').", ephemeral=True); return

        public_filename = f"{public_base_name}{source_ext}"
        public_path = os.path.join(config.PUBLIC_SOUNDS_DIR, public_filename)

        if file_helpers.find_public_sound_path(public_base_name):
            await ctx.followup.send(f"❌ A public sound named `{public_base_name}` already exists. Choose a different name or ask an admin to remove it.", ephemeral=True); return

        try:
            file_helpers.ensure_dir(config.PUBLIC_SOUNDS_DIR)
            shutil.copy2(user_path, public_path) # copy2 preserves metadata
            log.info(f"SOUND PUBLISHED: Copied '{user_path}' to '{public_path}' by {author.name}.")
            await ctx.followup.send(
                f"✅ Sound `{source_base_name}` published as `{public_base_name}`!\n"
                f"Others can now play it using `/playpublic name:{public_base_name}`.",
                ephemeral=True
            )
        except Exception as e:
            log.error(f"Failed to copy user sound '{user_path}' to public '{public_path}': {e}", exc_info=True)
            await ctx.followup.send(f"❌ Failed to publish `{source_base_name}`: An error occurred during copying ({type(e).__name__}).", ephemeral=True)


    @discord.slash_command(name="publicsounds", description="Lists all available public sounds.")
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def publicsounds(self, ctx: discord.ApplicationContext):
        """Lists all available public sounds."""
        await ctx.defer(ephemeral=True)
        log.info(f"COMMAND: /publicsounds by {ctx.author.name}")
        public_sounds = file_helpers.get_public_sound_files()

        if not public_sounds:
            await ctx.followup.send("No public sounds have been added yet. Use `/uploadsound make_public:True` or `/publishsound`.", ephemeral=True); return

        # Paginate
        items_per_page = 20
        pages_content = []
        current_page_lines = []
        for i, name in enumerate(public_sounds):
            current_page_lines.append(f"- `{name}`")
            if (i + 1) % items_per_page == 0 or i == len(public_sounds) - 1:
                pages_content.append("\n".join(current_page_lines))
                current_page_lines = []

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

        # TODO: Implement pagination View if num_pages > 1
        if embeds:
            await ctx.followup.send(embed=embeds[0], ephemeral=True)
        else:
            await ctx.followup.send("Could not generate public sound list.", ephemeral=True)


    @discord.slash_command(name="playpublic", description="Plays a public sound in your current voice channel.")
    @commands.cooldown(1, 4, commands.BucketType.user)
    async def playpublic(
        self,
        ctx: discord.ApplicationContext,
        name: discord.Option(str, description="Name of the public sound to play.", required=True, autocomplete=public_sound_autocomplete)
    ):
        """Plays a specified public sound."""
        await ctx.defer() # Defer publicly
        author = ctx.author
        log.info(f"COMMAND: /playpublic by {author.name}, requested sound: '{name}'")

        public_path = file_helpers.find_public_sound_path(name)
        if not public_path:
            await ctx.edit_original_response(content=f"❌ Public sound `{name}` not found. Use `/publicsounds` to check available sounds."); return

        # Use the playback manager to play the sound immediately
        await self.playback_manager.play_sound_now(ctx.interaction, public_path)


def setup(bot: discord.Bot):
    # Ensure PlaybackManager is available
    if not getattr(bot, 'playback_manager', None):
        log.critical("PlaybackManager not found on bot instance during PublicSoundsCog setup!")
        return

    bot.add_cog(PublicSoundsCog(bot))
    log.info("PublicSounds Cog loaded.")
