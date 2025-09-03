from __future__ import annotations
import functools
import aiohttp
from bs4 import BeautifulSoup
import discord
import math
from discord.ext import commands, tasks
from discord.ext.commands import Context
from discord.ui import Button, View
from discord import MessageFlags, utils
import json
from urllib.parse import urlparse
import os
import random
import calendar
import statsparser
import requests
from collections import defaultdict
from scipy.stats import chi2
import asyncio
from zoneinfo import ZoneInfo
from urllib.parse import urljoin
from logger_config import logger
import difflib
import logging
from base64 import b64encode
from dataclasses import dataclass, field
from datetime import timedelta, timezone, datetime,date, time as dtime
from typing import List, Literal, Optional, Tuple, Dict, Any
import scipy.stats as stats
from faker import Faker
from PIL import Image, ImageDraw, ImageFont, ImageOps
from PIL import features
import json
from urllib.parse import quote
import os
import difflib
import pytz
import championship
import serialize
import time
import tempfile
import discord, pathlib, itertools
from typing   import Iterable, Optional
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import builtins
import aiofiles
from zoneinfo import ZoneInfo
import datetime
datetime.now         = datetime.datetime.now
datetime.fromtimestamp = datetime.datetime.fromtimestamp
datetime.combine = datetime.datetime.combine
datetime.fromisoformat = datetime.datetime.fromisoformat
datetime.strptime = datetime.datetime.strptime
datetime.utcfromtimestamp = datetime.datetime.utcfromtimestamp
from statistics import mean
import httpx
from openai import AsyncOpenAI
import yaml

TICKET_FORUM=1404222656164790434

GITHUB_API = "https://api.github.com"
GITHUB_OWNER = os.getenv("GITHUB_OWNER")  # e.g. "your-user-or-org"
GITHUB_REPO = os.getenv("GITHUB_REPO")    # e.g. "your-repo"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # PAT with public_repo or repo scope

TICKETS_JSON = "tickets.json"

class Tickets(commands.Cog, name="Tickets"):
    def __init__(self, bot) -> None:
        self.bot = bot
        self.tickets: list[dict[str, Any]] = []
        self.logger = logger
        logger.info("Initializing Tickets cog")

    async def create_github_issue(
        self,
        title: str,
        body: str,
        labels: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        """
        Creates a GitHub issue. Returns the JSON response (includes 'html_url' and 'number').
        """
        if not (GITHUB_OWNER and GITHUB_REPO and GITHUB_TOKEN):
            raise RuntimeError(
                "Missing GITHUB_OWNER, GITHUB_REPO, or GITHUB_TOKEN environment variables."
            )

        payload: Dict[str, Any] = {"title": title, "body": body}
        if labels:
            payload["labels"] = labels

        headers = {
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "discord-ticket-bot",
        }

        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(
                f"{GITHUB_API}/repos/{GITHUB_OWNER}/{GITHUB_REPO}/issues",
                json=payload,
                headers=headers,
            )
            if r.status_code != 201:
                # include response body for debugging
                raise RuntimeError(
                    f"GitHub issue create failed: {r.status_code} {r.text}"
                )
            return r.json()

    @commands.hybrid_command(name="createticket", description="Create a support ticket")
    async def createticket(self, ctx: commands.Context, type: str | None = None):
        forum_channel = self.bot.get_channel(TICKET_FORUM)

        if not isinstance(forum_channel, discord.ForumChannel):
            await ctx.send("Configured channel is not a forum channel.")
            return

        ticket_type = type or "General"
        message_content = (
            f"Ticket created by {ctx.author.mention}\n"
            f"Type: {ticket_type}"
        )

        # Create the forum thread (starter message included). With `content=`,
        # Discord.py returns a ThreadWithMessage(thread=..., message=...)
        created = await forum_channel.create_thread(
            name=f"{ctx.author.name}'s Ticket - {ticket_type}",
            content=message_content,
            applied_tags=[],
        )

        # Unpack robustly whether it's ThreadWithMessage or just Thread
        if hasattr(created, "thread"):  # ThreadWithMessage
            thread: discord.Thread = created.thread
            starter_message = created.message  # not used here, but available
        else:
            thread: discord.Thread = created
            starter_message = None

        # Build a direct link to the thread (for forum threads this is guild_id/thread_id)
        guild_id = ctx.guild.id if ctx.guild else "@me"
        thread_url = f"https://discord.com/channels/{guild_id}/{thread.id}"

        # Create the GitHub issue
        gh_title = f"[Ticket] {ctx.author.name} - {ticket_type}"
        gh_body = (
            f"**Discord Ticket**\n"
            f"- **User:** {ctx.author} ({ctx.author.id})\n"
            f"- **Guild:** {ctx.guild.name if ctx.guild else 'DM'}\n"
            f"- **Thread:** {thread_url}\n\n"
            f"Starter message:\n\n{message_content}"
        )

        try:
            gh_issue = await self.create_github_issue(
                title=gh_title,
                body=gh_body,
                labels=[ticket_type] if ticket_type else None,
            )
            gh_url = gh_issue.get("html_url")
            gh_number = gh_issue.get("number")
        except Exception as e:
            logger.exception("Failed to create GitHub issue: %s", e)
            gh_url = None
            gh_number = None

        # Post the link into the thread for visibility
        if gh_url:
            await thread.send(f"Linked GitHub issue: {gh_url}")
        else:
            await thread.send(
                "Failed to create the GitHub issue. A moderator may need to create it manually."
            )

        # Save ticket data locally
        record = {
            "thread_id": thread.id,
            "channel_id": thread.parent_id,
            "guild_id": thread.guild.id if thread.guild else None,
            "user_id": ctx.author.id,
            "type": ticket_type,
            "timestamp": utils.utcnow().isoformat(),
            "github_issue_url": gh_url,
            "github_issue_number": gh_number,
            "thread_url": thread_url,
        }
        self.append_ticket_record(record)

        await ctx.send(f"Ticket created: {thread.mention}")




    def load_ticket_data(self) -> None:
        try:
            if not os.path.exists(TICKETS_JSON):
                self.tickets = []
                return
            with open(TICKETS_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                self.tickets = data
            else:
                # migrate single-object → list
                self.tickets = [data]
        except Exception as e:
            logger.exception("Error loading %s: %s", TICKETS_JSON, e)
            self.tickets = []

    def append_ticket_record(self, record: dict[str, Any]) -> None:
        """
        Append a new ticket record and write the whole list back.
        """
        self.tickets.append(record)
        try:
            with open(TICKETS_JSON, "w", encoding="utf-8") as f:
                json.dump(self.tickets, f, indent=2)
        except Exception as e:
            logger.exception("Error writing %s: %s", TICKETS_JSON, e)




async def setup(bot) -> None:
    await bot.add_cog(Tickets(bot))
