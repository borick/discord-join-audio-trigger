# -*- coding: utf-8 -*-
import discord
from discord.ext import commands
import os
import logging
import shutil
from typing import Optional, List, Any, Dict

import config
import data_manager
from utils import file_helpers # For finding, sanitizing, validating
from core.playback_manager import PlaybackManager # Optional for type hinting

log = logging.getLogger('SoundBot.Cog.UserSounds')

# --- Autocomplete Functions ---
# (Autocomplete function remains the same)
async def user_sound_autocomplete(ctx: discord.AutocompleteContext) -> List[discord.OptionChoice]:
    """Autocomplete for the user's personal sounds."""
    try:
        user_id = ctx.interaction.user.id
        sounds = file_helpers.get_user_sound_files(user_id)
        current_value = ctx.value.lower() if ctx.value else ""
        starts_with = []
        contains = []
        for name in sounds:
            lower_name = name.lower()
            display_name = name if len(name) <= 100 else name[:97] + "..."
            if lower_name.startswith(current_value):
                starts_with.append(discord.OptionChoice(name=display_name, value=name))
            elif current_value in lower_name:
                contains.append(discord.OptionChoice(name=display_name, value=name))
        starts_with.sort(key=lambda c: c.name.lower())
        contains.sort(key=lambda c: c.name.lower())
        suggestions = (starts_with + contains)[:25] # Discord limit
        return suggestions
    except Exception as e:
        log.error(f"Error during user sound autocomplete for user {ctx.interaction.user.id}: {e}", exc_info=True)
        return []

class UserSoundsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.playback_manager: PlaybackManager = bot.playback_manager # Shortcut with type hint

    # --- Commands ---
    # (Keep uploadsound, mysounds, deletesound, playsound, publishsound as they were,
    #  EXCEPT for fixing playsound's error handling context if needed, though the main issue is defer timing)

    @commands.slash_command(name="uploadsound", description=f"Upload a sound (personal). Limit: {config.MAX_USER_SOUNDS_PER_USER} sounds.")
    @commands.cooldown(2, 20, commands.BucketType.user)
    async def uploadsound(
        self,
        ctx: discord.ApplicationContext,
        name: discord.Option(str, description="Short name for the sound (letters, numbers, underscore). Will be sanitized.", required=True),
        sound_file: discord.Option(discord.Attachment, description=f"Sound ({', '.join(config.ALLOWED_EXTENSIONS)}). Max {config.MAX_USER_SOUND_SIZE_MB}MB.", required=True),
    ):
        await ctx.defer(ephemeral=True); author = ctx.author; user_id = author.id
        log.info(f"COMMAND: /uploadsound by {author.name} ({user_id}), name: '{name}', file: '{sound_file.filename}'")
        clean_name = file_helpers.sanitize_filename(name)
        if not clean_name: await ctx.followup.send("❌ Invalid name provided...", ephemeral=True); return
        followup_prefix = f"ℹ️ Name sanitized to `{clean_name}`.\n" if clean_name != name else ""
        target_dir = os.path.join(config.USER_SOUNDS_DIR, str(user_id)); file_helpers.ensure_dir(target_dir)
        existing_personal_path = file_helpers.find_user_sound_path(user_id, clean_name); replacing_personal = existing_personal_path is not None
        if not replacing_personal:
            current_sounds = file_helpers.get_user_sound_files(user_id)
            if len(current_sounds) >= config.MAX_USER_SOUNDS_PER_USER: await ctx.followup.send(f"{followup_prefix}❌ Limit reached...", ephemeral=True); return
        file_extension = os.path.splitext(sound_file.filename)[1].lower(); final_filename = f"{clean_name}{file_extension}"; final_path = os.path.join(target_dir, final_filename)
        success, error_msg = await file_helpers.validate_and_save_upload(ctx, sound_file, final_path, command_name="uploadsound")
        if success:
            log.info(f"Sound validation successful for {author.name}, saved to '{final_path}' (personal)")
            if replacing_personal and existing_personal_path and existing_personal_path != final_path:
                if os.path.exists(existing_personal_path):
                    try: os.remove(existing_personal_path); log.info(f"Removed old file...")
                    except Exception as e: log.warning(f"Could not remove old file '{existing_personal_path}': {e}")
            action = "updated" if replacing_personal else "uploaded"
            msg = f"{followup_prefix}✅ Success! Personal sound `{clean_name}` {action}.\nUse `/playsound name:{clean_name}`..."
            await ctx.followup.send(msg, ephemeral=True)
        else: await ctx.followup.send(f"{followup_prefix}{error_msg or '❌ Unknown error.'}", ephemeral=True)

    @commands.slash_command(name="mysounds", description="Lists your personal uploaded sounds.")
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def mysounds(self, ctx: discord.ApplicationContext):
        await ctx.defer(ephemeral=True); author = ctx.author; log.info(f"COMMAND: /mysounds by {author.name} ({author.id})")
        user_sounds = file_helpers.get_user_sound_files(author.id)
        if not user_sounds: await ctx.followup.send("No sounds yet...", ephemeral=True); return
        items_per_page = 15; pages_content = []; current_page_lines = []
        for i, name in enumerate(user_sounds):
            current_page_lines.append(f"- `{name}`")
            if (i + 1) % items_per_page == 0 or i == len(user_sounds) - 1: pages_content.append("\n".join(current_page_lines)); current_page_lines = []
        embeds = []; total_sounds = len(user_sounds); num_pages = len(pages_content)
        for page_num, page_text in enumerate(pages_content):
            embed = discord.Embed(title=f"{author.display_name}'s Sounds ({total_sounds}/{config.MAX_USER_SOUNDS_PER_USER})", description=f"Use `/playsound`...\n\n{page_text}", color=discord.Color.blurple())
            footer_text = "Use /deletesound to remove."; footer_text += f" | Page {page_num + 1}/{num_pages}" if num_pages > 1 else ""
            embed.set_footer(text=footer_text); embeds.append(embed)
        if embeds: await ctx.followup.send(embed=embeds[0], ephemeral=True)
        else: await ctx.followup.send("Could not generate list.", ephemeral=True)

    @commands.slash_command(name="deletesound", description="Deletes one of your PERSONAL sounds.")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def deletesound(self, ctx: discord.ApplicationContext, name: discord.Option(str, description="Name...", required=True, autocomplete=user_sound_autocomplete)):
        await ctx.defer(ephemeral=True); author = ctx.author; user_id = author.id
        log.info(f"COMMAND: /deletesound by {author.name} ({user_id}), target: '{name}'")
        sound_path = file_helpers.find_user_sound_path(user_id, name)
        if not sound_path: await ctx.followup.send(f"❌ Not found: `{name}`.", ephemeral=True); return
        sound_base_name = os.path.splitext(os.path.basename(sound_path))[0]
        user_dir_abs = os.path.abspath(os.path.join(config.USER_SOUNDS_DIR, str(user_id))); resolved_path_abs = os.path.abspath(sound_path)
        if not resolved_path_abs.startswith(user_dir_abs + os.sep): log.critical(f"SECURITY ALERT: Path traversal..."); await ctx.followup.send("❌ Security error.", ephemeral=True); return
        try: os.remove(sound_path); log.info(f"Deleted PERSONAL sound '{os.path.basename(sound_path)}' for {user_id}."); await ctx.followup.send(f"🗑️ Deleted `{sound_base_name}`.", ephemeral=True)
        except OSError as e: log.error(f"Failed delete: {e}", exc_info=True); await ctx.followup.send(f"❌ Failed delete: {type(e).__name__}.", ephemeral=True)
        except Exception as e: log.error(f"Unexpected error deleting: {e}", exc_info=True); await ctx.followup.send(f"❌ Unexpected error deleting `{sound_base_name}`.", ephemeral=True)

    @commands.slash_command(name="playsound", description="Plays one of your PERSONAL sounds in your current VC.")
    @commands.cooldown(1, 4, commands.BucketType.user)
    async def playsound(self, ctx: discord.ApplicationContext, name: discord.Option(str, description="Name...", required=True, autocomplete=user_sound_autocomplete)):
        # Defer as early as possible to avoid the 3-second timeout
        try:
            await ctx.defer() # Public defer
        except discord.NotFound:
            log.warning(f"/playsound: Interaction expired before defer could be called for {ctx.author.name}.")
            # Cannot recover interaction here, just log and exit
            return
        except Exception as defer_err:
            log.error(f"/playsound: Error during defer for {ctx.author.name}: {defer_err}", exc_info=True)
            # Cannot recover interaction, just log and exit
            return

        author = ctx.author
        log.info(f"COMMAND: /playsound by {author.name} ({author.id}), requested sound: '{name}'")
        sound_path = file_helpers.find_user_sound_path(author.id, name)

        # Use interaction.edit_original_response since we deferred publicly
        if not sound_path:
            try:
                await ctx.interaction.edit_original_response(content=f"❌ Personal sound `{name}` not found. Use `/mysounds` or `/soundpanel`.")
            except discord.NotFound: pass # Ignore if interaction is gone
            except Exception as e_resp: log.warning(f"Failed to edit response for playsound not found: {e_resp}")
            return

        await self.playback_manager.play_single_sound(ctx.interaction, sound_path=sound_path)

    @commands.slash_command(name="publishsound", description="Make one of your personal sounds public for everyone.")
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def publishsound(self, ctx: discord.ApplicationContext, name: discord.Option(str, description="Name...", required=True, autocomplete=user_sound_autocomplete)):
        await ctx.defer(ephemeral=True); author = ctx.author; user_id = author.id
        log.info(f"COMMAND: /publishsound by {author.name} ({user_id}), target: '{name}'")
        user_path = file_helpers.find_user_sound_path(user_id, name)
        if not user_path: await ctx.followup.send(f"❌ Not found: `{name}`.", ephemeral=True); return
        source_filename = os.path.basename(user_path); source_base_name, source_ext = os.path.splitext(source_filename)
        public_base_name = file_helpers.sanitize_filename(name)
        if not public_base_name: await ctx.followup.send(f"❌ Invalid name...", ephemeral=True); return
        public_filename = f"{public_base_name}{source_ext}"; public_path = os.path.join(config.PUBLIC_SOUNDS_DIR, public_filename)
        if file_helpers.find_public_sound_path(public_base_name): await ctx.followup.send(f"❌ Public sound `{public_base_name}` exists.", ephemeral=True); return
        try: file_helpers.ensure_dir(config.PUBLIC_SOUNDS_DIR); shutil.copy2(user_path, public_path); log.info(f"SOUND PUBLISHED..."); await ctx.followup.send(f"✅ Published `{public_base_name}`...", ephemeral=True)
        except Exception as e: log.error(f"Failed publish: {e}", exc_info=True); await ctx.followup.send(f"❌ Failed publish: {type(e).__name__}.", ephemeral=True)

    # ==================================
    # ===== FIX FOR /soundpanel ========
    # ==================================
    @commands.slash_command(name="soundpanel", description="Displays buttons to play YOUR personal sounds in your VC.")
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def soundpanel(self, ctx: discord.ApplicationContext):
        """Displays an interactive panel with buttons for the user's personal sounds."""
        # Defer publicly FIRST to avoid 3-second timeout
        try:
            await ctx.defer()
        except discord.NotFound:
             log.warning(f"/soundpanel: Interaction expired before defer for {ctx.author.name}")
             return # Cannot proceed if interaction is gone
        except Exception as e_defer:
             log.error(f"/soundpanel: Failed to defer for {ctx.author.name}: {e_defer}", exc_info=True)
             # Try to inform user via followup if possible, but interaction might be dead
             try: await ctx.interaction.followup.send("❌ Error initializing panel.", ephemeral=True)
             except: pass
             return

        author = ctx.author
        log.info(f"COMMAND: /soundpanel invoked by {author.name} ({author.id})")

        view = None # Initialize view to None
        try:
            log.debug(f"Creating UserSoundboardView for user {author.id}")
            view = UserSoundboardView(user_id=author.id, bot_instance=self.bot)
            log.debug(f"UserSoundboardView created. Checking for buttons...")
            msg_content = f"🔊 **{author.display_name}'s Personal Sound Panel**"

            if not view.has_buttons():
                log.warning(f"Soundpanel view for {author.id} has no playable buttons.")
                placeholder_id = view.get_placeholder_id()
                content = "You haven't uploaded any personal sounds yet..." if placeholder_id == "no_sounds" else "Error loading your sounds..."
                # *** FIX: Use interaction.edit_original_response ***
                await ctx.interaction.edit_original_response(content=content, view=None)
                log.debug(f"Sent placeholder message for soundpanel: {placeholder_id}")
                return

            msg_content += " - Click to play!"
            log.debug(f"Attempting to send sound panel view for user {author.id}...")
            # *** FIX: Use interaction.edit_original_response ***
            message = await ctx.interaction.edit_original_response(content=msg_content, view=view)
            view.message = message
            log.info(f"Successfully sent sound panel for user {author.id}.")

        except Exception as e:
            log.error(f"Failed to create/send soundpanel for user {author.id}: {e}", exc_info=True)
            try:
                # *** FIX: Use interaction.edit_original_response ***
                # Ensure view is None if we hit an error sending it
                await ctx.interaction.edit_original_response(content="❌ Failed to create the sound panel. An internal error occurred.", view=None)
            except discord.NotFound:
                log.warning(f"Interaction for soundpanel (user {author.id}) gone before error could be reported.")
            except Exception as e_resp:
                log.error(f"Failed even to send the failure message for soundpanel: {e_resp}")
    # ==================================
    # === END OF /soundpanel FIX =======
    # ==================================

# --- Sound Panel View ---
# (Keep the UserSoundboardView class definition as it was in the previous corrected version
#  including the fix to use self.bot.playback_manager.play_single_sound in the callback)
class UserSoundboardView(discord.ui.View):
    """A view containing buttons for a user's personal sounds."""
    def __init__(self, user_id: int, bot_instance: commands.Bot, *, timeout: Optional[float] = 600.0): # 10 min timeout
        log.debug(f"SOUNDPANEL VIEW INIT: Initializing view for user {user_id}")
        super().__init__(timeout=timeout)
        self.user_id = user_id
        self.bot = bot_instance # Store bot instance to access playback manager
        self.message: Optional[discord.InteractionMessage | discord.WebhookMessage] = None
        self._placeholder_id: Optional[str] = None # To track placeholder state
        try: self.populate_buttons(); log.debug(f"SOUNDPANEL VIEW INIT: Finished populating buttons for user {user_id}")
        except Exception as e_pop: log.error(f"SOUNDPANEL VIEW INIT: Error during populate_buttons for user {user_id}: {e_pop}", exc_info=True); self._add_placeholder("Error Loading Sounds", "error")
    def has_buttons(self) -> bool:
        has_btn = any(isinstance(item, discord.ui.Button) and not item.disabled and item.custom_id and item.custom_id.startswith("usersb_play:") for item in self.children)
        log.debug(f"SOUNDPANEL VIEW: has_buttons check for user {self.user_id}: {has_btn}"); return has_btn
    def get_placeholder_id(self) -> Optional[str]: return self._placeholder_id
    def populate_buttons(self):
        user_dir = os.path.join(config.USER_SOUNDS_DIR, str(self.user_id)); log.debug(f"SOUNDPANEL VIEW POPULATE: Populating for user {self.user_id} from: {user_dir}")
        if not os.path.isdir(user_dir): log.warning(f"SOUNDPANEL VIEW POPULATE: User directory not found: {user_dir}"); self._add_placeholder("No sounds uploaded yet!", "no_sounds"); return
        sounds_found_count = 0; button_row = 0; max_buttons_per_row = 5; max_rows = 5; max_buttons_total = max_buttons_per_row * max_rows
        sound_names = []
        try: sound_names = file_helpers.get_user_sound_files(self.user_id); log.debug(f"SOUNDPANEL VIEW POPULATE: Found sound names for user {self.user_id}: {sound_names}")
        except Exception as e_get_files: log.error(f"SOUNDPANEL VIEW POPULATE: Error listing files for user {self.user_id}: {e_get_files}", exc_info=True); self._add_placeholder("Error Reading Sounds", "error"); return
        if not sound_names: log.info(f"SOUNDPANEL VIEW POPULATE: No sound files found for user {self.user_id}"); self._add_placeholder("No sounds uploaded yet!", "no_sounds"); return
        for base_name in sound_names:
            log.debug(f"SOUNDPANEL VIEW POPULATE: Processing sound '{base_name}' for user {self.user_id}")
            if len(self.children) >= 25: log.warning(f"Max component limit (25)..."); break
            if sounds_found_count >= max_buttons_total: log.warning(f"Button limit ({max_buttons_total})..."); break
            sound_path = file_helpers.find_user_sound_path(self.user_id, base_name)
            if not sound_path: log.warning(f"Could not find path for '{base_name}'..."); continue
            filename_with_ext = os.path.basename(sound_path); label = base_name.replace("_", " "); label = label[:77] + "…" if len(label) > 80 else label
            custom_id = f"usersb_play:{filename_with_ext}"
            if len(custom_id) > 100: log.warning(f"Skipping '{filename_with_ext}', custom_id too long..."); continue
            try:
                button = discord.ui.Button(label=label, style=discord.ButtonStyle.secondary, custom_id=custom_id, row=button_row)
                button.callback = self.user_soundboard_button_callback; self.add_item(button); sounds_found_count += 1
                log.debug(f"SOUNDPANEL VIEW POPULATE: Added button for '{base_name}' (ID: {custom_id})")
                if sounds_found_count > 0 and sounds_found_count % max_buttons_per_row == 0:
                    button_row += 1
                    if button_row >= max_rows: log.warning(f"Row limit reached..."); break
            except Exception as e_add_button: log.error(f"Error adding button for '{base_name}': {e_add_button}", exc_info=True)
        if sounds_found_count == 0 and self._placeholder_id is None: log.info(f"No valid buttons added..."); self._add_placeholder("No sounds found/addable!", "no_sounds")
    def _add_placeholder(self, label: str, placeholder_type: str):
        log.debug(f"SOUNDPANEL VIEW: Adding placeholder '{placeholder_type}' for user {self.user_id}")
        if not self.children: self.add_item(discord.ui.Button(label=label, style=discord.ButtonStyle.secondary, disabled=True, custom_id=f"usersb_placeholder_{placeholder_type}_{self.user_id}")); self._placeholder_id = placeholder_type
        else: log.warning(f"Tried to add placeholder but view has children.")
    async def user_soundboard_button_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id: await interaction.response.send_message("✋ Not your panel!", ephemeral=True); return
        custom_id = interaction.data["custom_id"]; user = interaction.user; log.info(f"USER PANEL: Button '{custom_id}' clicked by {user.name} ({self.user_id})")
        await interaction.response.defer() # Defer for button click
        if not custom_id.startswith("usersb_play:"): log.error(f"Invalid custom_id: '{custom_id}'"); await interaction.edit_original_response(content="❌ Error: Invalid button ID."); return
        sound_filename = custom_id.split(":", 1)[1]; sound_path = os.path.join(config.USER_SOUNDS_DIR, str(self.user_id), sound_filename)
        if not os.path.exists(sound_path): log.error(f"Sound file '{sound_filename}' not found at '{sound_path}'"); await interaction.edit_original_response(content=f"❌ Error: Sound file `{sound_filename}` missing."); return
        await self.bot.playback_manager.play_single_sound(interaction, sound_path=sound_path)
    async def on_timeout(self):
        if self.message:
            log.debug(f"Panel timeout for {self.user_id} (Msg: {self.message.id})"); owner_name = f"User {self.user_id}"
            try: panel_owner = self.message.guild.get_member(self.user_id) if self.message.guild else None; panel_owner = panel_owner or (await self.bot.fetch_user(self.user_id) if self.bot.is_ready() else None); owner_name = panel_owner.display_name if panel_owner else owner_name
            except Exception as e: log.warning(f"Could not fetch owner {self.user_id} for timeout: {e}")
            for item in self.children: item.disabled = True if isinstance(item, discord.ui.Button) else item.disabled
            try: await self.message.edit(content=f"🔊 **{owner_name}'s Panel (Expired)**", view=self)
            except discord.HTTPException as e: log.warning(f"Failed edit expired panel {self.message.id}: {e}")
            except Exception as e: log.error(f"Unexpected error editing expired panel {self.message.id}: {e}", exc_info=True)
        else: log.debug(f"Panel timeout for {self.user_id}, no message ref.")


def setup(bot: commands.Bot):
    bot.add_cog(UserSoundsCog(bot))
    log.info("UserSounds Cog loaded.")