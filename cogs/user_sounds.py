# -*- coding: utf-8 -*-
import discord
from discord.ext import commands
import os
import logging
from typing import List, Optional

# Import helpers and managers
from utils import file_helpers, voice_helpers
from core.playback_manager import PlaybackManager
import data_manager
import config

log = logging.getLogger('SoundBot.Cogs.UserSounds')

# --- Sound Panel View ---
class UserSoundboardView(discord.ui.View):
    def __init__(self, user_id: int, playback_manager: PlaybackManager, *, timeout: Optional[float] = 600.0):
        super().__init__(timeout=timeout)
        self.user_id = user_id
        self.playback_manager = playback_manager
        self.message: Optional[discord.InteractionMessage | discord.WebhookMessage] = None
        self.populate_buttons()

    def populate_buttons(self):
        user_dir = os.path.join(config.USER_SOUNDS_DIR, str(self.user_id))
        log.debug(f"Populating sound panel for user {self.user_id} from: {user_dir}")

        if not os.path.isdir(user_dir):
            self.add_item(discord.ui.Button(label="No sounds uploaded yet!", style=discord.ButtonStyle.secondary, disabled=True, custom_id=f"usersb_placeholder_nosounds_{self.user_id}"))
            return

        sounds_found_count = 0
        max_buttons_total = 25 # Discord limit

        try:
            sound_names = file_helpers.get_user_sound_files(self.user_id)
        except Exception as e:
            log.error(f"Error getting sound files for panel population (user {self.user_id}): {e}")
            self.add_item(discord.ui.Button(label="Error Reading Sounds", style=discord.ButtonStyle.danger, disabled=True, custom_id=f"usersb_placeholder_error_{self.user_id}"))
            return

        for base_name in sound_names:
            if sounds_found_count >= max_buttons_total:
                log.warning(f"Button limit ({max_buttons_total}) reached for user {self.user_id} panel. Sound '{base_name}' skipped.")
                # Optionally add a "More..." button
                break

            sound_path = file_helpers.find_user_sound_path(self.user_id, base_name)
            if not sound_path:
                log.warning(f"Could not find path for listed sound '{base_name}' during panel population for user {self.user_id}. Skipping.")
                continue

            filename_with_ext = os.path.basename(sound_path)
            label = base_name.replace("_", " ")
            if len(label) > 78: label = label[:77] + "…"

            custom_id = f"usersb_play:{filename_with_ext}"
            if len(custom_id) > 100:
                log.warning(f"Skipping sound '{filename_with_ext}' for {self.user_id} panel: custom_id too long.")
                continue

            button = discord.ui.Button(
                label=label,
                style=discord.ButtonStyle.secondary,
                custom_id=custom_id,
                # Row calculation can be added here if needed, but default layout works up to 25
            )
            button.callback = self.user_soundboard_button_callback
            self.add_item(button)
            sounds_found_count += 1

        if sounds_found_count == 0 and not any(item.custom_id.startswith("usersb_placeholder_error_") for item in self.children):
             self.add_item(discord.ui.Button(label="No sounds uploaded yet!", style=discord.ButtonStyle.secondary, disabled=True, custom_id=f"usersb_placeholder_nosounds_{self.user_id}"))


    async def user_soundboard_button_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("✋ This is not your personal sound panel!", ephemeral=True)
            return

        custom_id = interaction.data["custom_id"]
        user = interaction.user
        log.info(f"USER PANEL: Button '{custom_id}' clicked by {user.name} on panel for {self.user_id}")

        await interaction.response.defer() # Defer publicly

        if not custom_id.startswith("usersb_play:"):
            log.error(f"Invalid custom_id format from user panel button: '{custom_id}'")
            await interaction.edit_original_response(content="❌ Internal error: Invalid button ID."); return

        sound_filename = custom_id.split(":", 1)[1]
        sound_path = os.path.join(config.USER_SOUNDS_DIR, str(self.user_id), sound_filename)

        # Use playback manager to play immediately
        await self.playback_manager.play_sound_now(interaction, sound_path)

    async def on_timeout(self):
        if self.message:
            log.debug(f"User sound panel timed out for {self.user_id} (message ID: {self.message.id})")
            owner_name = f"User {self.user_id}"
            try:
                 panel_owner = await self.message.guild.fetch_member(self.user_id) if self.message.guild else await self.playback_manager.bot.fetch_user(self.user_id)
                 if panel_owner: owner_name = panel_owner.display_name
            except Exception as e: log.warning(f"Could not fetch panel owner {self.user_id} for timeout: {e}")

            for item in self.children:
                if isinstance(item, discord.ui.Button): item.disabled = True
            try:
                await self.message.edit(content=f"🔊 **{owner_name}'s Personal Panel (Expired)**", view=self)
            except Exception as e:
                 log.warning(f"Failed to edit expired panel message {self.message.id} for {self.user_id}: {e}")
        else:
            log.debug(f"User panel timed out for {self.user_id} but no message reference was stored.")


# --- User Sounds Cog ---
class UserSoundsCog(commands.Cog):
    def __init__(self, bot: discord.Bot):
        self.bot = bot
        # Get managers and state from bot instance
        self.playback_manager: PlaybackManager = getattr(bot, 'playback_manager')
        self.user_sound_config = getattr(bot, 'user_sound_config')

    # --- Autocomplete ---
    async def user_sound_autocomplete(self, ctx: discord.AutocompleteContext) -> List[discord.OptionChoice]:
        """Autocomplete for user's personal sounds."""
        return await file_helpers._generic_sound_autocomplete(ctx, file_helpers.get_user_sound_files, ctx.interaction.user.id)

    # --- Commands ---
    @discord.slash_command(name="uploadsound", description=f"Upload a sound (personal/public). Limit: {config.MAX_USER_SOUNDS_PER_USER} personal.")
    @commands.cooldown(2, 20, commands.BucketType.user)
    async def uploadsound(
        self,
        ctx: discord.ApplicationContext,
        name: discord.Option(str, description="Short name (letters, numbers, underscore). Will be sanitized.", required=True),
        sound_file: discord.Option(discord.Attachment, description=f"Sound ({', '.join(config.ALLOWED_EXTENSIONS)}). Max {config.MAX_USER_SOUND_SIZE_MB}MB.", required=True),
        make_public: discord.Option(bool, description="Make available for everyone? (Default: False)", default=False)
    ):
        """Allows users to upload personal or public sounds."""
        await ctx.defer(ephemeral=True)
        author = ctx.author
        user_id = author.id
        log.info(f"COMMAND: /uploadsound by {author.name} ({user_id}), name: '{name}', public: {make_public}, file: '{sound_file.filename}'")

        clean_name = file_helpers.sanitize_filename(name)
        if not clean_name:
            await ctx.followup.send("❌ Invalid name provided. Please use letters, numbers, or underscores.", ephemeral=True); return
        followup_prefix = f"ℹ️ Name sanitized to `{clean_name}`.\n" if clean_name != name else ""

        file_extension = os.path.splitext(sound_file.filename)[1].lower()
        final_filename = f"{clean_name}{file_extension}"

        if make_public:
            target_dir = config.PUBLIC_SOUNDS_DIR
            file_helpers.ensure_dir(target_dir)
            if file_helpers.find_public_sound_path(clean_name):
                await ctx.followup.send(f"{followup_prefix}❌ A public sound named `{clean_name}` already exists.", ephemeral=True); return
            replacing_personal = False
            scope = "public"
        else:
            target_dir = os.path.join(config.USER_SOUNDS_DIR, str(user_id))
            file_helpers.ensure_dir(target_dir)
            existing_personal_path = file_helpers.find_user_sound_path(user_id, clean_name)
            replacing_personal = existing_personal_path is not None
            if not replacing_personal and len(file_helpers.get_user_sound_files(user_id)) >= config.MAX_USER_SOUNDS_PER_USER:
                await ctx.followup.send(f"{followup_prefix}❌ You have reached the maximum limit of {config.MAX_USER_SOUNDS_PER_USER} personal sounds.", ephemeral=True); return
            scope = "personal"

        final_path = os.path.join(target_dir, final_filename)
        success, error_msg = await file_helpers.validate_and_save_upload(ctx, sound_file, final_path, command_name="uploadsound")

        if success:
            log.info(f"Sound validation successful for {author.name}, saved to '{final_path}' ({scope})")
            if replacing_personal and not make_public and existing_personal_path:
                if existing_personal_path != final_path and os.path.exists(existing_personal_path):
                    try: os.remove(existing_personal_path); log.info(f"Removed old personal sound file '{os.path.basename(existing_personal_path)}' for user {user_id}.")
                    except Exception as e: log.warning(f"Could not remove old personal sound file '{existing_personal_path}': {e}")

            action = "updated" if replacing_personal and not make_public else "uploaded"
            play_cmd = "playpublic" if make_public else "playsound"
            list_cmd = "publicsounds" if make_public else "mysounds"
            msg = f"{followup_prefix}✅ Success! Sound `{clean_name}` {action} as {scope}.\n"
            msg += f"Use `/{play_cmd} name:{clean_name}`"
            if not make_public: msg += f", `/{list_cmd}`, `/soundpanel`, or `/publishsound name:{clean_name}`."
            else: msg += f" or list with `/{list_cmd}`."
            await ctx.followup.send(msg, ephemeral=True)
        else:
            await ctx.followup.send(f"{followup_prefix}{error_msg or '❌ An unknown error occurred during validation.'}", ephemeral=True)


    @discord.slash_command(name="mysounds", description="Lists your personal uploaded sounds.")
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def mysounds(self, ctx: discord.ApplicationContext):
        """Lists the calling user's uploaded personal sounds."""
        await ctx.defer(ephemeral=True)
        author = ctx.author
        log.info(f"COMMAND: /mysounds by {author.name} ({author.id})")
        user_sounds = file_helpers.get_user_sound_files(author.id)

        if not user_sounds:
            await ctx.followup.send("You haven't uploaded any personal sounds yet. Use `/uploadsound`!", ephemeral=True); return

        # Paginate
        items_per_page = 20
        pages_content = []
        current_page_lines = []
        for i, name in enumerate(user_sounds):
            current_page_lines.append(f"- `{name}`")
            if (i + 1) % items_per_page == 0 or i == len(user_sounds) - 1:
                pages_content.append("\n".join(current_page_lines))
                current_page_lines = []

        embeds = []
        total_sounds = len(user_sounds)
        num_pages = len(pages_content)
        for page_num, page_text in enumerate(pages_content):
            embed = discord.Embed(
                title=f"{author.display_name}'s Sounds ({total_sounds}/{config.MAX_USER_SOUNDS_PER_USER})",
                description=f"Use `/playsound`, `/soundpanel`, or `/publishsound`.\n\n{page_text}",
                color=discord.Color.blurple()
            )
            footer_text = "Use /deletesound to remove."
            if num_pages > 1: footer_text += f" | Page {page_num + 1}/{num_pages}"
            embed.set_footer(text=footer_text)
            embeds.append(embed)

        # TODO: Implement pagination View if num_pages > 1
        if embeds:
            await ctx.followup.send(embed=embeds[0], ephemeral=True)
        else:
            await ctx.followup.send("Could not generate sound list.", ephemeral=True)


    @discord.slash_command(name="deletesound", description="Deletes one of your PERSONAL sounds.")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def deletesound(
        self,
        ctx: discord.ApplicationContext,
        name: discord.Option(str, description="Name of the personal sound to delete.", required=True, autocomplete=user_sound_autocomplete)
    ):
        """Deletes one of the user's personal sounds by name."""
        await ctx.defer(ephemeral=True)
        author = ctx.author
        user_id = author.id
        log.info(f"COMMAND: /deletesound by {author.name} ({user_id}), target sound name: '{name}'")

        sound_path = file_helpers.find_user_sound_path(user_id, name)
        if not sound_path:
            await ctx.followup.send(f"❌ Personal sound `{name}` not found. Use `/mysounds`.", ephemeral=True); return

        sound_base_name = os.path.splitext(os.path.basename(sound_path))[0]

        # Security check
        user_dir_abs = os.path.abspath(os.path.join(config.USER_SOUNDS_DIR, str(user_id)))
        resolved_path_abs = os.path.abspath(sound_path)
        if not resolved_path_abs.startswith(user_dir_abs + os.sep):
            log.critical(f"CRITICAL SECURITY ALERT: Path traversal attempt in /deletesound. User: {user_id}, Input: '{name}', Resolved Path: '{resolved_path_abs}'")
            await ctx.followup.send("❌ Internal security error preventing deletion.", ephemeral=True); return

        try:
            os.remove(sound_path)
            log.info(f"Deleted PERSONAL sound file '{os.path.basename(sound_path)}' for user {user_id}.")
            await ctx.followup.send(f"🗑️ Personal sound `{sound_base_name}` deleted successfully.", ephemeral=True)
        except OSError as e:
            log.error(f"Failed to delete personal sound file '{sound_path}' for user {user_id}: {e}", exc_info=True)
            await ctx.followup.send(f"❌ Failed to delete `{sound_base_name}`: Could not remove file ({type(e).__name__}).", ephemeral=True)
        except Exception as e:
            log.error(f"Unexpected error deleting personal sound '{sound_path}' for user {user_id}: {e}", exc_info=True)
            await ctx.followup.send(f"❌ An unexpected error occurred while deleting `{sound_base_name}`.", ephemeral=True)


    @discord.slash_command(name="playsound", description="Plays one of your PERSONAL sounds in your current VC.")
    @commands.cooldown(1, 4, commands.BucketType.user)
    async def playsound(
        self,
        ctx: discord.ApplicationContext,
        name: discord.Option(str, description="Name of the personal sound to play.", required=True, autocomplete=user_sound_autocomplete)
    ):
        """Plays one of the user's personal sounds by name."""
        await ctx.defer() # Defer publicly
        author = ctx.author
        log.info(f"COMMAND: /playsound by {author.name} ({author.id}), requested sound: '{name}'")

        sound_path = file_helpers.find_user_sound_path(author.id, name)
        if not sound_path:
            await ctx.edit_original_response(content=f"❌ Personal sound `{name}` not found. Use `/mysounds` or `/soundpanel`."); return

        # Use playback manager to play immediately
        await self.playback_manager.play_sound_now(ctx.interaction, sound_path)


    @discord.slash_command(name="soundpanel", description="Displays buttons to play YOUR personal sounds in your VC.")
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def soundpanel(self, ctx: discord.ApplicationContext):
        """Displays an interactive panel with buttons for the user's personal sounds."""
        await ctx.defer() # Defer publicly
        author = ctx.author
        log.info(f"COMMAND: /soundpanel invoked by {author.name} ({author.id})")

        view = UserSoundboardView(user_id=author.id, playback_manager=self.playback_manager, timeout=600.0)

        has_playable_buttons = any(
            isinstance(item, discord.ui.Button) and not item.disabled and item.custom_id and item.custom_id.startswith("usersb_play:")
            for item in view.children
        )

        if not has_playable_buttons:
            is_placeholder = any(
                isinstance(item, discord.ui.Button) and item.disabled and item.custom_id and item.custom_id.startswith("usersb_placeholder_")
                for item in view.children
            )
            if is_placeholder:
                no_sounds_msg = "You haven't uploaded any personal sounds yet. Use `/uploadsound`!"
                error_msg = "Error loading your sounds. Please try again later."
                placeholder_id = next((item.custom_id for item in view.children if item.custom_id.startswith("usersb_placeholder_")), None)
                content = no_sounds_msg if placeholder_id and "nosounds" in placeholder_id else error_msg
                await ctx.edit_original_response(content=content, view=None)
            else:
                await ctx.edit_original_response(content="Could not generate the sound panel. No sounds found or an error occurred.", view=None)
            return

        msg_content = f"🔊 **{author.display_name}'s Personal Sound Panel** - Click to play!"
        try:
            message = await ctx.interaction.edit_original_response(content=msg_content, view=view)
            view.message = message
        except Exception as e:
            log.error(f"Failed to send soundpanel for user {author.id}: {e}", exc_info=True)
            try: await ctx.interaction.edit_original_response(content="❌ Failed to create the sound panel.", view=None)
            except Exception: pass


def setup(bot: discord.Bot):
    # Ensure dependencies are available
    if not getattr(bot, 'playback_manager', None):
        log.critical("PlaybackManager not found on bot instance during UserSoundsCog setup!")
        return
    if not hasattr(bot, 'user_sound_config'):
         log.critical("User config not found on bot instance during UserSoundsCog setup!")
         return

    bot.add_cog(UserSoundsCog(bot))
    log.info("UserSounds Cog loaded.")

