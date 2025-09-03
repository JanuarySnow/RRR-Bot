"""
Copyright © Krypton 2019-Present - https://github.com/kkrypt0nn (https://krypton.ninja)
Description:
🐍 A simple template to start to code your own and personalized Discord bot in Python

Version: 6.2.0
"""

import json
import logging
import os
import platform
import random
import sys

import aiosqlite
import discord
from discord.ext import commands, tasks
from discord.ext.commands import Context
from dotenv import load_dotenv
from logger_config import logger

from database import DatabaseManager
import traceback
import asyncio
from base64 import b64encode
from dataclasses import dataclass, field
from datetime import datetime as dt
import logging
from typing import Literal, Optional


import httpx
from openai import AsyncOpenAI
import yaml

if not os.path.isfile(f"{os.path.realpath(os.path.dirname(__file__))}/config.json"):
    sys.exit("'config.json' not found! Please add it and try again.")
else:
    with open(f"{os.path.realpath(os.path.dirname(__file__))}/config.json") as file:
        config = json.load(file)

"""	
Setup bot intents (events restrictions)
For more information about intents, please go to the following websites:
https://discordpy.readthedocs.io/en/latest/intents.html
https://discordpy.readthedocs.io/en/latest/intents.html#privileged-intents


Default Intents:
intents.bans = True
intents.dm_messages = True
intents.dm_reactions = True
intents.dm_typing = True
intents.emojis = True
intents.emojis_and_stickers = True
intents.guild_messages = True
intents.guild_reactions = True
intents.guild_scheduled_events = True
intents.guild_typing = True
intents.guilds = True
intents.integrations = True
intents.invites = True
intents.messages = True # `message_content` is required to get the content of the messages
intents.reactions = True
intents.typing = True
intents.voice_states = True
intents.webhooks = True

Privileged Intents (Needs to be enabled on developer portal of Discord), please use them only if you need them:
intents.members = True
intents.message_content = True
intents.presences = True
"""


intents = discord.Intents.default()
intents.bans = True
intents.dm_messages = True
intents.dm_reactions = True
intents.dm_typing = True
intents.emojis = True
intents.emojis_and_stickers = True
intents.guild_messages = True
intents.guild_reactions = True
intents.guild_scheduled_events = True
intents.guild_typing = True
intents.guilds = True
intents.integrations = True
intents.invites = True
intents.messages = True # `message_content` is required to get the content of the messages
intents.reactions = True
intents.typing = True
intents.voice_states = True
intents.webhooks = True
"""
Uncomment this if you want to use prefix (normal) commands.
It is recommended to use slash commands and therefore not use prefix commands.

If you want to use prefix commands, make sure to also enable the intent below in the Discord developer portal.
"""
intents.message_content = True
intents.members = True
intents.presences = True
intents.guild_messages = True
intents.guild_reactions = True
intents.guild_scheduled_events = True
intents.guild_typing = True

# Setup both of the loggers

VISION_MODEL_TAGS = ("gpt-4", "gpt-4o-mini", "text-embedding-3-small", "claude-3", "gemini", "pixtral", "llava", "vision", "vl")
PROVIDERS_SUPPORTING_USERNAMES = ("openai", "x-ai")

ALLOWED_FILE_TYPES = ("image", "text")

EMBED_COLOR_COMPLETE = discord.Color.dark_green()
EMBED_COLOR_INCOMPLETE = discord.Color.orange()

STREAMING_INDICATOR = " ⚪"
EDIT_DELAY_SECONDS = 1

MAX_MESSAGE_NODES = 100

import sys
import builtins
import sys
from logger_config import logger

def _sanitize(s: str) -> str:
    # drop NULs and other non-printables except common whitespace
    return ''.join(ch for ch in s if ch.isprintable() or ch in '\n\r\t')

def print_and_log(*args, **kwargs):
    message = " ".join(map(str, args))
    message = _sanitize(message)
    logger.info(message)

# Monkey-patch print
builtins.print = print_and_log

def log_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        return sys.__excepthook__(exc_type, exc_value, exc_traceback)
    # This records full stack info; no stdout writes
    logger.exception("Unhandled Exception", exc_info=(exc_type, exc_value, exc_traceback))

sys.excepthook = log_exception


@dataclass
class MsgNode:
    text: Optional[str] = None
    images: list = field(default_factory=list)

    role: Literal["user", "assistant"] = "assistant"
    user_id: Optional[int] = None

    has_bad_attachments: bool = False
    fetch_parent_failed: bool = False

    parent_msg: Optional[discord.Message] = None

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)



class DiscordBot(commands.Bot):
    def __init__(self) -> None:
        super().__init__(
            command_prefix=commands.when_mentioned_or(config["prefix"]),
            intents=intents,
            help_command=None,
        )
        self.logger = logger
        self.config = config
        self.database = None
        self.httpx_client = httpx.AsyncClient()
        self.parsed = None

        self.msg_nodes = {}
        self.last_task_time = 0
        self.prompt = "You are a helpful and pleasant simracer who always wants to help people get faster, you love the Mazda mx5 and love racing in the Real Rookie Racing community, you will offer simracing and setup advice for Assetto Corsa, and will talk about many subjects happily without necessarily always referring back to Assetto Corsa or simracing in general unless asked about it, you are aware of the users Buggy, Iosh, Cheesus, Potato from Real Rookie Racing, and Potato is your creator, the others ( CHeesus, Iosh, Buggy ) are all quite fast racers and you admire them greatly, but love to tease them in a good natured way, you wont always refer to them in your messages but will occasionally"
        self.cfg = self.get_gpt_config()

    async def init_db(self) -> None:
        async with aiosqlite.connect(
            f"{os.path.realpath(os.path.dirname(__file__))}/database/database.db"
        ) as db:
            with open(
                f"{os.path.realpath(os.path.dirname(__file__))}/database/schema.sql"
            ) as file:
                await db.executescript(file.read())
            await db.commit()

    async def load_cogs(self) -> None:
        """
        The code in this function is executed whenever the bot will start.
        """
        for file in os.listdir(f"{os.path.realpath(os.path.dirname(__file__))}/cogs"):
            if file.endswith(".py"):
                extension = file[:-3]
                try:
                    await self.load_extension(f"cogs.{extension}")
                    self.logger.info(f"Loaded extension '{extension}'")
                except Exception as e:
                    exception = f"{type(e).__name__}: {e}"
                    self.logger.error(
                        f"Failed to load extension {extension}\n{exception}"
                    )
                    traceback.print_exc()

    @tasks.loop(minutes=1.0)
    async def status_task(self) -> None:
        """
        Setup the game status task of the bot.
        """
        statuses = ["Crashing into Buggy", "Crashing into ZeroToHero", "Banning Iosh"]
        await self.change_presence(activity=discord.Game(random.choice(statuses)))

    @status_task.before_loop
    async def before_status_task(self) -> None:
        """
        Before starting the status changing task, we make sure the bot is ready
        """
        await self.wait_until_ready()

    async def setup_hook(self) -> None:
        """
        This will just be executed when the bot starts the first time.
        """
        self.logger.info(f"Logged in as {self.user.name}")
        self.logger.info(f"discord.py API version: {discord.__version__}")
        self.logger.info(f"Python version: {platform.python_version()}")
        self.logger.info(
            f"Running on: {platform.system()} {platform.release()} ({os.name})"
        )
        self.logger.info("-------------------")
        self.get_gpt_config()
        await self.init_db()
        await self.load_cogs()
        self.status_task.start()
        self.database = DatabaseManager(
            connection=await aiosqlite.connect(
                f"{os.path.realpath(os.path.dirname(__file__))}/database/database.db"
            )
        )


    def get_gpt_config(self,filename="config.yaml"):
        with open(filename, "r") as file:
            return yaml.safe_load(file)

    async def on_message(self, message: discord.Message) -> None:
        """
        The code in this event is executed every time someone sends a message, with or without the prefix

        :param message: The message that was sent.
        """
        if message.author == self.user or message.author.bot:
            return
        if self.user.mentioned_in(message):
            channelbot = self.get_channel(1328800009189195828)
            channelmember = self.get_channel(1317982679517626388)
            if message.channel != channelbot and message.channel != channelmember:
                return
            await self.do_chat_message(message)
        else:
            await self.process_commands(message)

    async def do_chat_message(self, new_msg):
        role_ids = set(role.id for role in getattr(new_msg.author, "roles", ()))
        channel_ids = set(id for id in (new_msg.channel.id, getattr(new_msg.channel, "parent_id", None), getattr(new_msg.channel, "category_id", None)) if id)

        self.cfg = self.get_gpt_config()

        allow_dms = self.cfg["allow_dms"]
        permissions = self.cfg["permissions"]
        is_dm = new_msg.channel.type == discord.ChannelType.private
        (allowed_user_ids, blocked_user_ids), (allowed_role_ids, blocked_role_ids), (allowed_channel_ids, blocked_channel_ids) = (
            (perm["allowed_ids"], perm["blocked_ids"]) for perm in (permissions["users"], permissions["roles"], permissions["channels"])
        )

        allow_all_users = not allowed_user_ids if is_dm else not allowed_user_ids and not allowed_role_ids
        is_good_user = allow_all_users or new_msg.author.id in allowed_user_ids or any(id in allowed_role_ids for id in role_ids)
        is_bad_user = not is_good_user or new_msg.author.id in blocked_user_ids or any(id in blocked_role_ids for id in role_ids)

        allow_all_channels = not allowed_channel_ids
        is_good_channel = allow_dms if is_dm else allow_all_channels or any(id in allowed_channel_ids for id in channel_ids)
        is_bad_channel = not is_good_channel or any(id in blocked_channel_ids for id in channel_ids)

        if is_bad_user or is_bad_channel:
            return

        provider, model = self.cfg["model"].split("/", 1)
        base_url = self.cfg["providers"][provider]["base_url"]
        api_key = self.cfg["providers"][provider].get("api_key", "sk-no-key-required")
        openai_client = AsyncOpenAI(base_url=base_url, api_key=api_key)

        accept_images = any(x in model.lower() for x in VISION_MODEL_TAGS)
        accept_usernames = any(x in provider.lower() for x in PROVIDERS_SUPPORTING_USERNAMES)

        max_text = self.cfg["max_text"]
        max_images = self.cfg["max_images"] if accept_images else 0
        max_messages = self.cfg["max_messages"]

        use_plain_responses = self.cfg["use_plain_responses"]
        max_message_length = 2000 if use_plain_responses else (4096 - len(STREAMING_INDICATOR))

        # Build message chain and set user warnings
        messages = []
        user_warnings = set()
        curr_msg = new_msg

        while curr_msg != None and len(messages) < max_messages:
            curr_node = self.msg_nodes.setdefault(curr_msg.id, MsgNode())

            async with curr_node.lock:
                if curr_node.text == None:
                    cleaned_content = curr_msg.content.removeprefix(self.user.mention).lstrip()

                    good_attachments = {type: [att for att in curr_msg.attachments if att.content_type and type in att.content_type] for type in ALLOWED_FILE_TYPES}

                    curr_node.text = "\n".join(
                        ([cleaned_content] if cleaned_content else [])
                        + [embed.description for embed in curr_msg.embeds if embed.description]
                        + [(await self.httpx_client.get(att.url)).text for att in good_attachments["text"]]
                    )

                    curr_node.images = [
                        dict(type="image_url", image_url=dict(url=f"data:{att.content_type};base64,{b64encode((await self.httpx_client.get(att.url)).content).decode('utf-8')}"))
                        for att in good_attachments["image"]
                    ]

                    curr_node.role = "assistant" if curr_msg.author == self.user else "user"

                    curr_node.user_id = curr_msg.author.id if curr_node.role == "user" else None

                    curr_node.has_bad_attachments = len(curr_msg.attachments) > sum(len(att_list) for att_list in good_attachments.values())

                    try:
                        if (
                            curr_msg.reference == None
                            and self.user.mention not in curr_msg.content
                            and (prev_msg_in_channel := ([m async for m in curr_msg.channel.history(before=curr_msg, limit=1)] or [None])[0])
                            and prev_msg_in_channel.type in (discord.MessageType.default, discord.MessageType.reply)
                            and prev_msg_in_channel.author == (self.user if curr_msg.channel.type == discord.ChannelType.private else curr_msg.author)
                        ):
                            curr_node.parent_msg = prev_msg_in_channel
                        else:
                            is_public_thread = curr_msg.channel.type == discord.ChannelType.public_thread
                            parent_is_thread_start = is_public_thread and curr_msg.reference == None and curr_msg.channel.parent.type == discord.ChannelType.text

                            if parent_msg_id := curr_msg.channel.id if parent_is_thread_start else getattr(curr_msg.reference, "message_id", None):
                                if parent_is_thread_start:
                                    curr_node.parent_msg = curr_msg.channel.starter_message or await curr_msg.channel.parent.fetch_message(parent_msg_id)
                                else:
                                    curr_node.parent_msg = curr_msg.reference.cached_message or await curr_msg.channel.fetch_message(parent_msg_id)

                    except (discord.NotFound, discord.HTTPException):
                        logging.exception("Error fetching next message in the chain")
                        curr_node.fetch_parent_failed = True

                if curr_node.images[:max_images]:
                    content = ([dict(type="text", text=curr_node.text[:max_text])] if curr_node.text[:max_text] else []) + curr_node.images[:max_images]
                else:
                    content = curr_node.text[:max_text]

                if content != "":
                    message = dict(content=content, role=curr_node.role)
                    if accept_usernames and curr_node.user_id != None:
                        message["name"] = str(curr_node.user_id)

                    messages.append(message)

                if len(curr_node.text) > max_text:
                    user_warnings.add(f"⚠️ Max {max_text:,} characters per message")
                if len(curr_node.images) > max_images:
                    user_warnings.add(f"⚠️ Max {max_images} image{'' if max_images == 1 else 's'} per message" if max_images > 0 else "⚠️ Can't see images")
                if curr_node.has_bad_attachments:
                    user_warnings.add("⚠️ Unsupported attachments")
                if curr_node.fetch_parent_failed or (curr_node.parent_msg != None and len(messages) == max_messages):
                    user_warnings.add(f"⚠️ Only using last {len(messages)} message{'' if len(messages) == 1 else 's'}")

                curr_msg = curr_node.parent_msg

        logging.info(f"Message received (user ID: {new_msg.author.id}, attachments: {len(new_msg.attachments)}, conversation length: {len(messages)}):\n{new_msg.content}")

        system_prompt = self.prompt
        system_prompt_extras = [f"Today's date: {dt.now().strftime('%B %d %Y')}."]
        if accept_usernames:
            system_prompt_extras.append("User's names are their Discord IDs and should be typed as '<@ID>'.")

        full_system_prompt = "\n".join([system_prompt] + system_prompt_extras)
        messages.append(dict(role="system", content=full_system_prompt))

        # Generate and send response message(s) (can be multiple if response is long)
        curr_content = finish_reason = edit_task = None
        response_msgs = []
        response_contents = []

        embed = discord.Embed()
        for warning in sorted(user_warnings):
            embed.add_field(name=warning, value="", inline=False)

        kwargs = dict(model=model, messages=messages[::-1], stream=True, extra_body=self.cfg["extra_api_parameters"])
        try:
            async with new_msg.channel.typing():
                async for curr_chunk in await openai_client.chat.completions.create(**kwargs):
                    if finish_reason != None:
                        break

                    finish_reason = curr_chunk.choices[0].finish_reason

                    prev_content = curr_content or ""
                    curr_content = curr_chunk.choices[0].delta.content or ""

                    new_content = prev_content if finish_reason == None else (prev_content + curr_content)

                    if response_contents == [] and new_content == "":
                        continue

                    if start_next_msg := response_contents == [] or len(response_contents[-1] + new_content) > max_message_length:
                        response_contents.append("")

                    response_contents[-1] += new_content

                    if not use_plain_responses:
                        ready_to_edit = (edit_task == None or edit_task.done()) and dt.now().timestamp() - self.last_task_time >= EDIT_DELAY_SECONDS
                        msg_split_incoming = finish_reason == None and len(response_contents[-1] + curr_content) > max_message_length
                        is_final_edit = finish_reason != None or msg_split_incoming
                        is_good_finish = finish_reason != None and finish_reason.lower() in ("stop", "end_turn")

                        if start_next_msg or ready_to_edit or is_final_edit:
                            if edit_task != None:
                                await edit_task

                            embed.description = response_contents[-1] if is_final_edit else (response_contents[-1] + STREAMING_INDICATOR)
                            embed.color = EMBED_COLOR_COMPLETE if msg_split_incoming or is_good_finish else EMBED_COLOR_INCOMPLETE

                            if start_next_msg:
                                reply_to_msg = new_msg if response_msgs == [] else response_msgs[-1]
                                response_msg = await reply_to_msg.reply(embed=embed, silent=True)
                                response_msgs.append(response_msg)

                                self.msg_nodes[response_msg.id] = MsgNode(parent_msg=new_msg)
                                await self.msg_nodes[response_msg.id].lock.acquire()
                            else:
                                edit_task = asyncio.create_task(response_msgs[-1].edit(embed=embed))

                            self.last_task_time = dt.now().timestamp()

                if use_plain_responses:
                    for content in response_contents:
                        reply_to_msg = new_msg if response_msgs == [] else response_msgs[-1]
                        response_msg = await reply_to_msg.reply(content=content, suppress_embeds=True)
                        response_msgs.append(response_msg)

                        self.msg_nodes[response_msg.id] = MsgNode(parent_msg=new_msg)
                        await self.msg_nodes[response_msg.id].lock.acquire()

        except Exception:
            logging.exception("Error while generating response")

        for response_msg in response_msgs:
            self.msg_nodes[response_msg.id].text = "".join(response_contents)
            self.msg_nodes[response_msg.id].lock.release()

        # Delete oldest MsgNodes (lowest message IDs) from the cache
        if (num_nodes := len(self.msg_nodes)) > MAX_MESSAGE_NODES:
            for msg_id in sorted(self.msg_nodes.keys())[: num_nodes - MAX_MESSAGE_NODES]:
                async with self.msg_nodes.setdefault(msg_id, MsgNode()).lock:
                    self.msg_nodes.pop(msg_id, None)


    async def on_command_completion(self, context: Context) -> None:
        """
        The code in this event is executed every time a normal command has been *successfully* executed.

        :param context: The context of the command that has been executed.
        """
        full_command_name = context.command.qualified_name
        split = full_command_name.split(" ")
        executed_command = str(split[0])
        if context.guild is not None:
            self.logger.info(
                f"Executed {executed_command} command in {context.guild.name} (ID: {context.guild.id}) by {context.author} (ID: {context.author.id})"
            )
        else:
            self.logger.info(
                f"Executed {executed_command} command by {context.author} (ID: {context.author.id}) in DMs"
            )

    async def on_command_error(self, context: Context, error) -> None:
        """
        The code in this event is executed every time a normal valid command catches an error.

        :param context: The context of the normal command that failed executing.
        :param error: The error that has been faced.
        """
        if isinstance(error, commands.CommandOnCooldown):
            minutes, seconds = divmod(error.retry_after, 60)
            hours, minutes = divmod(minutes, 60)
            hours = hours % 24
            embed = discord.Embed(
                description=f"**Please slow down** - You can use this command again in {f'{round(hours)} hours' if round(hours) > 0 else ''} {f'{round(minutes)} minutes' if round(minutes) > 0 else ''} {f'{round(seconds)} seconds' if round(seconds) > 0 else ''}.",
                color=0xE02B2B,
            )
            await context.send(embed=embed)
        elif isinstance(error, commands.NotOwner):
            embed = discord.Embed(
                description="You are not the owner of the bot!", color=0xE02B2B
            )
            await context.send(embed=embed)
            if context.guild:
                self.logger.warning(
                    f"{context.author} (ID: {context.author.id}) tried to execute an owner only command in the guild {context.guild.name} (ID: {context.guild.id}), but the user is not an owner of the bot."
                )
            else:
                self.logger.warning(
                    f"{context.author} (ID: {context.author.id}) tried to execute an owner only command in the bot's DMs, but the user is not an owner of the bot."
                )
        elif isinstance(error, commands.MissingPermissions):
            embed = discord.Embed(
                description="You are missing the permission(s) `"
                + ", ".join(error.missing_permissions)
                + "` to execute this command!",
                color=0xE02B2B,
            )
            await context.send(embed=embed)
        elif isinstance(error, commands.BotMissingPermissions):
            embed = discord.Embed(
                description="I am missing the permission(s) `"
                + ", ".join(error.missing_permissions)
                + "` to fully perform this command!",
                color=0xE02B2B,
            )
            await context.send(embed=embed)
        elif isinstance(error, commands.MissingRequiredArgument):
            embed = discord.Embed(
                title="Error!",
                # We need to capitalize because the command arguments have no capital letter in the code and they are the first word in the error message.
                description=str(error).capitalize(),
                color=0xE02B2B,
            )
            await context.send(embed=embed)
        else:
            raise error

load_dotenv()


bot = DiscordBot()
bot.run(os.getenv("TOKEN"))

ALLOWED_CHANNELS = {
    "global": ["1134963371553337478", "1328800009189195828", "1328117523740229792", "1094610718222979123"],  # Channels for most commands
    "command_name": ["1328117523740229792"]  # Specific channel for one command
}

@bot.before_invoke
async def check_channel(ctx):
    allowed_channels = ALLOWED_CHANNELS.get(ctx.command.name, ALLOWED_CHANNELS["global"])
    if allowed_channels and str(ctx.channel.id) not in allowed_channels:
        await ctx.send(f"This command cannot be used in this channel.")
        raise commands.CheckFailure  # Prevent command execution

