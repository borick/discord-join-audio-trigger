# cogs/join_sounds.py

import discord
from discord.ext import commands
import logging
import os
from typing import List, Optional

# Local application imports
import config
import data_manager
from utils import file_helpers
# Import the autocomplete function from user_sounds cog
try:
    from .user_sounds import user_sound_autocomplete
    AUTOCOMPLETE_AVAILABLE = True
except ImportError:
    log = logging.getLogger('SoundBot.Cog.JoinSounds')
    log.warning("Could not import user_sound_autocomplete from cogs.user_sounds. Set/Clear join sound commands will lack autocomplete.")
    # Define a dummy autocomplete if import fails, so commands don't break
    async def user_sound_autocomplete(ctx: discord.AutocompleteContext) -> List[discord.OptionChoice]:
        return []
    AUTOCOMPLETE_AVAILABLE = False


log = logging.getLogger('SoundBot.Cog.JoinSounds')

class JoinSoundsCog(commands.Cog):
    """Cog for managing user-specific join sounds."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # Ensure user sound config is loaded (should be done in bot.py)
        if not hasattr(bot, 'user_sound_config'):
             log.critical("JoinSoundsCog FATAL: bot.user_sound_config not found!")
             raise RuntimeError("user_sound_config not initialized on Bot before loading JoinSoundsCog")
        # No need to store it locally if we always access via self.bot.user_sound_config

    @commands.slash_command(name="setjoinsound", description="Set one of your uploaded sounds as your join sound.")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def setjoinsound(
        self,
        ctx: discord.ApplicationContext,
        name: discord.Option(
            str,
            description="Name of the personal sound to use when you join VC.",
            required=True,
            autocomplete=user_sound_autocomplete if AUTOCOMPLETE_AVAILABLE else None # Use imported autocomplete
        )
    ):
        """Sets a user's personal sound as their join sound."""
        await ctx.defer(ephemeral=True)
        author = ctx.author
        user_id_str = str(author.id)
        log_prefix = f"SETJOINSOUND (User: {author.name}/{user_id_str}):"

        log.info(f"{log_prefix} Request to set join sound to '{name}'.")

        # Find the sound file using the helper function
        sound_path = file_helpers.find_user_sound_path(author.id, name)

        if not sound_path:
            log.warning(f"{log_prefix} Sound '{name}' not found in user's personal sounds.")
            await ctx.followup.send(
                f"❌ Sound named `{name}` not found in your personal sounds."
                f"\nUse `/mysounds` to see your sounds or `/uploadsound` to add new ones.",
                ephemeral=True
            )
            return

        # Get the actual filename with extension (relative to sounds dir)
        sound_filename = os.path.basename(sound_path)
        sound_base_name = os.path.splitext(sound_filename)[0] # Get base name again for display consistency

        # Ensure the user's entry exists in the main config dict
        user_config = self.bot.user_sound_config.setdefault(user_id_str, {})

        # Update the join sound entry
        user_config['join_sound'] = sound_filename # Store filename with extension

        # Save the configuration
        data_manager.save_config(self.bot.user_sound_config)

        log.info(f"{log_prefix} Successfully set join sound to '{sound_filename}'.")
        await ctx.followup.send(
            f"✅ Your join sound has been set to **`{sound_base_name}`** (`{sound_filename}`).",
            ephemeral=True
        )

    @commands.slash_command(name="removejoinsound", description="Remove your custom join sound (uses TTS join message).")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def removejoinsound(self, ctx: discord.ApplicationContext):
        """Removes a user's custom join sound setting."""
        await ctx.defer(ephemeral=True)
        author = ctx.author
        user_id_str = str(author.id)
        log_prefix = f"REMOVEJOINSOUND (User: {author.name}/{user_id_str}):"

        log.info(f"{log_prefix} Request received.")

        user_config = self.bot.user_sound_config.get(user_id_str)

        if user_config and 'join_sound' in user_config:
            old_sound = user_config.pop('join_sound') # Remove the key
            log.info(f"{log_prefix} Removed join sound setting (was '{old_sound}').")

            # If the user's config entry is now empty (except maybe TTS defaults), remove it entirely?
            # Keep TTS defaults if they exist. Remove only if ONLY join_sound was present.
            # Let's just remove the key for now, empty entries are harmless.
            # if not user_config:
            #     if user_id_str in self.bot.user_sound_config:
            #         del self.bot.user_sound_config[user_id_str]
            #         log.info(f"{log_prefix} Removed empty user config entry.")

            # Save the configuration
            data_manager.save_config(self.bot.user_sound_config)

            await ctx.followup.send(
                f"🗑️ Your custom join sound has been removed.\n"
                f"The bot will now use a TTS announcement when you join (unless you set TTS defaults).",
                ephemeral=True
            )
        else:
            log.info(f"{log_prefix} No custom join sound was set.")
            await ctx.followup.send(
                "🤷 You don't currently have a custom join sound set.",
                ephemeral=True
            )

def setup(bot: commands.Bot):
    # Add check for user_sound_config attribute on bot
    if not hasattr(bot, 'user_sound_config'):
         log.critical("Cannot load JoinSoundsCog: bot.user_sound_config is not set.")
         # Optionally raise an error to prevent bot startup if this is critical
         # raise AttributeError("Bot object missing 'user_sound_config' during JoinSoundsCog setup.")
         return # Or just don't load the cog
    bot.add_cog(JoinSoundsCog(bot))
    log.info("JoinSounds Cog loaded.")