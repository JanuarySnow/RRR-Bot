"""
Copyright © Krypton 2019-Present - https://github.com/kkrypt0nn (https://krypton.ninja)
Description:
🐍 A simple template to start to code your own and personalized Discord bot in Python

Version: 6.2.0
"""

import platform
import random

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands
from discord.ext.commands import Context
import asyncio


class FeedbackForm(discord.ui.Modal, title="Feeedback"):
    feedback = discord.ui.TextInput(
        label="What do you think about this bot?",
        style=discord.TextStyle.long,
        placeholder="Type your answer here...",
        required=True,
        max_length=256,
    )

    async def on_submit(self, interaction: discord.Interaction):
        self.interaction = interaction
        self.answer = str(self.feedback)
        self.stop()


class General(commands.Cog, name="general"):
    def __init__(self, bot) -> None:
        self.bot = bot
        self.context_menu_user = app_commands.ContextMenu(
            name="Grab ID", callback=self.grab_id
        )
        self.bot.tree.add_command(self.context_menu_user)


    # User context menu command
    async def grab_id(
        self, interaction: discord.Interaction, user: discord.User
    ) -> None:
        """
        Grabs the ID of the user.

        :param interaction: The application command interaction.
        :param user: The user that is being interacted with.
        """
        embed = discord.Embed(
            description=f"The ID of {user.mention} is `{user.id}`.",
            color=0xBEBEFE,
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)


    @commands.hybrid_command(
        name="help", description="List all commands the bot has loaded."
    )
    async def help(self, context: Context) -> None:
        prefix = self.bot.config["prefix"]
        embed_pages = []
        current_page = discord.Embed(
            title="Help", description="List of available commands:", color=0xBEBEFE
        )

        fields_per_page = 5  # Adjust as needed (max 25 fields per embed)
        fields_in_current_page = 0

        for cog_name in self.bot.cogs:
            if cog_name == "owner" and not (await self.bot.is_owner(context.author)):
                continue
            cog = self.bot.get_cog(cog_name)
            commands_list = cog.get_commands()

            # Skip empty cogs
            if not commands_list:
                continue

            # Prepare data for this cog
            data = []
            for command in commands_list:
                description = command.description.partition("\n")[0]
                data.append(f"{prefix}{command.name} - {description}")

            # Split the data into chunks that fit within 1024 characters
            chunks = []
            current_chunk = ""
            for line in data:
                # Ensure we don't split commands between fields
                if len(current_chunk) + len(line) + 1 > 1000:
                    chunks.append(current_chunk)
                    current_chunk = line + "\n"
                else:
                    current_chunk += line + "\n"
            if current_chunk:
                chunks.append(current_chunk)

            # Add chunks to embeds
            for idx, chunk in enumerate(chunks):
                if fields_in_current_page >= fields_per_page:
                    embed_pages.append(current_page)
                    current_page = discord.Embed(
                        title="Help", description="List of available commands:", color=0xBEBEFE
                    )
                    fields_in_current_page = 0

                # Use cog name for the first chunk, and empty name or continuation indicator for subsequent chunks
                field_name = cog_name.capitalize() if idx == 0 else f"{cog_name.capitalize()} (cont.)"
                current_page.add_field(
                    name=field_name,
                    value=f"```{chunk.strip()}```",  # Strip to remove trailing newlines
                    inline=False
                )
                fields_in_current_page += 1

        # Add the last page if it has content
        if fields_in_current_page > 0:
            embed_pages.append(current_page)

        total_pages = len(embed_pages)
        current_page_number = 0
        message = await context.send(embed=embed_pages[current_page_number])

        # Add reactions if there's more than one page
        if total_pages > 1:
            await message.add_reaction("◀️")
            await message.add_reaction("▶️")

            def check(reaction, user):
                return (
                    user == context.author and
                    str(reaction.emoji) in ["◀️", "▶️"] and
                    reaction.message.id == message.id
                )

            while True:
                try:
                    reaction, user = await self.bot.wait_for(
                        "reaction_add", timeout=60.0, check=check
                    )

                    if str(reaction.emoji) == "▶️":
                        current_page_number = (current_page_number + 1) % total_pages
                        await message.edit(embed=embed_pages[current_page_number])
                        await message.remove_reaction(reaction, user)

                    elif str(reaction.emoji) == "◀️":
                        current_page_number = (current_page_number - 1) % total_pages
                        await message.edit(embed=embed_pages[current_page_number])
                        await message.remove_reaction(reaction, user)

                except asyncio.TimeoutError:
                    await message.clear_reactions()
                    break





async def setup(bot) -> None:
    await bot.add_cog(General(bot))
