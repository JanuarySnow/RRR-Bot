from __future__ import annotations
import functools
import aiohttp
from bs4 import BeautifulSoup
import discord
import math
from discord.ext import commands, tasks
from discord.ext.commands import Context
from discord import ui, ButtonStyle
from discord.ui import Button, View
from discord import MessageFlags, utils
from discord import app_commands
import json
from typing import Optional
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

CMD_NAME = "announcewithrolebuttons"
WHITELIST = {"announcewithrolebuttons"}
GUILD_ID: int = 917204555459100703
EU_TIME = dtime(19, 0, tzinfo=ZoneInfo("Europe/London"))
US_TIME = dtime(20, 0, tzinfo=ZoneInfo("US/Central"))
SAT_TIME = dtime(20, 0, tzinfo=ZoneInfo("Europe/London"))

def linkify_discord_refs(guild: discord.Guild, text: str) -> str:
    """Turn '#channel' and '@RoleName' into clickable mentions for embeds.
       Doesn’t ping anyone; just renders as links."""
    if not guild or not text:
        return text

    # Build quick lookup maps
    chan_map = {c.name.lower(): c.id for c in guild.channels if isinstance(c, (discord.TextChannel, discord.ForumChannel, discord.VoiceChannel, discord.CategoryChannel, discord.StageChannel, discord.Thread)) or hasattr(c, "name")}
    role_map = {r.name.lower(): r.id for r in guild.roles}

    # Already-correct tokens pass through (e.g., <#123>, <@&456>)
    # Replace #channel-name (letters, numbers, underscores, hyphens)
    def repl_channel(m: re.Match) -> str:
        name = m.group(1).lower()
        cid = chan_map.get(name)
        return f"<#{cid}>" if cid else m.group(0)  # leave unchanged if not found

    # Replace @RoleName (avoid @everyone/@here)
    def repl_role(m: re.Match) -> str:
        name = m.group(1).strip().lower()
        if name in ("everyone", "here"):
            return m.group(0)
        rid = role_map.get(name)
        return f"<@&{rid}>" if rid else m.group(0)

    # Important: don’t touch existing <#id> or <@&id>
    text = re.sub(r'(?<![<\w])#([A-Za-z0-9_\-]+)', repl_channel, text)
    text = re.sub(r'(?<![<\w])@([^\s@#<>]+)', repl_role, text)
    return text

class RoleButtonView(discord.ui.View):
    """
    Persistent role-grant button. Stores role_id (not Role) so it survives restarts.
    Remember to register it on startup: bot.add_view(RoleButtonView())
    """
    def __init__(self, role_id: int | None = None):
        super().__init__(timeout=None)
        self.role_id = role_id

    @discord.ui.button(
        label="Grab that Role!",
        style=discord.ButtonStyle.primary,
        custom_id="announce_with_role_button"  # keep stable for persistence
    )
    async def on_click(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return

        if not self.role_id:
            await interaction.response.send_message(
                "Sorry, this button isn’t wired up yet. Please ping a moderator.",
                ephemeral=True
            )
            return

        role = interaction.guild.get_role(self.role_id)
        if role is None:
            await interaction.response.send_message("Role not found anymore.", ephemeral=True)
            return

        member = interaction.user
        if role in getattr(member, "roles", []):
            await interaction.response.send_message(f"You already have {role.mention}.", ephemeral=True)
            return

        await member.add_roles(role, reason="Role-button announcement")
        await interaction.response.send_message(f"You’ve been given {role.mention}!", ephemeral=True)

class AnnouncementModal(discord.ui.Modal, title="Create Announcement"):
    def __init__(self, *, target_channel_id: int, button_role_id: int, ping_role_id: int | None):
        super().__init__(timeout=300)
        self.target_channel_id = target_channel_id
        self.button_role_id = button_role_id
        self.ping_role_id = ping_role_id

        self.embed_title = discord.ui.TextInput(
            label="Embed Title",
            style=discord.TextStyle.short,
            required=True,
            max_length=256,
            placeholder="Tekly iRacing Season Announcement",
        )
        self.embed_desc = discord.ui.TextInput(
            label="Announcement (multi-line)",
            style=discord.TextStyle.paragraph,
            required=True,
            placeholder="Write your full announcement here.\nBlank lines are OK.",
        )
        self.image_url = discord.ui.TextInput(
            label="Image URL (optional)",
            style=discord.TextStyle.short,
            required=False,
            placeholder="https://example.com/banner.png",
        )
        self.thumb_url = discord.ui.TextInput(
            label="Thumbnail URL (optional)",
            style=discord.TextStyle.short,
            required=False,
            placeholder="https://example.com/logo.png",
        )

        self.add_item(self.embed_title)
        self.add_item(self.embed_desc)
        self.add_item(self.image_url)
        self.add_item(self.thumb_url)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        # Linkify channels/roles in the embed description (no pings)
        linked_desc = linkify_discord_refs(interaction.guild, str(self.embed_desc.value))

        embed = discord.Embed(
            title=str(self.embed_title.value).strip(),
            description=linked_desc,
            color=discord.Color.blurple(),
            timestamp=discord.utils.utcnow(),
        )
        if self.image_url.value:
            embed.set_image(url=str(self.image_url.value).strip())
        if self.thumb_url.value:
            embed.set_thumbnail(url=str(self.thumb_url.value).strip())

        # Content ping: ONLY the optional ping_role (never the button role)
        content = None
        allowed_mentions = discord.AllowedMentions.none()
        # @everyone special-case: its "role id" equals the guild id
        if self.ping_role_id == interaction.guild.id:
            content = "@everyone"
            allowed_mentions = discord.AllowedMentions(everyone=True, roles=False, users=False)
        else:
            ping_role = interaction.guild.get_role(self.ping_role_id)
            if ping_role:
                content = ping_role.mention
                allowed_mentions = discord.AllowedMentions(roles=[ping_role], users=False, everyone=False)

        # Post to target channel
        target_channel = interaction.client.get_channel(self.target_channel_id)
        if target_channel is None:
            await interaction.response.send_message("Target channel not found.", ephemeral=True)
            return

        # Button view uses the button role id
        view = RoleButtonView(role_id=self.button_role_id)

        await target_channel.send(
            content=content,
            embed=embed,
            view=view,
            allowed_mentions=allowed_mentions
        )
        await interaction.response.send_message(
            f"Announcement sent to {target_channel.mention}.",
            ephemeral=True
        )

class ChampionshipCommands(commands.Cog, name="potato"):
    def __init__(self, bot) -> None:
        self.bot = bot
        self.parsed = None

    async def cog_load(self):
        # This runs after the cog is fully loaded
        self.parser = getattr(self.bot, "parsed", None)
        if self.parsed is None:
            logger.warning("Stats parser not found yet!")
        # 1) register persistent button view
        try:
            self.bot.add_view(RoleButtonView())
        except Exception:
            pass

        # 2) only one sync per process
        if getattr(self.bot, "_did_announce_sync", False):
            return

        guild = discord.Object(id=GUILD_ID)

        try:
            # 3) check if the command already exists in that guild
            existing = await self.bot.tree.fetch_commands(guild=guild)
            if any(cmd.name == CMD_NAME for cmd in existing):
                # already present — skip sync
                self.bot._did_announce_sync = True
                print(f"[announce] {CMD_NAME} already registered in guild {GUILD_ID}; skipping sync.")
                return

            # 4) not present — do a fast guild-only sync
            await self.bot.tree.sync(guild=guild)
            self.bot._did_announce_sync = True
            print(f"[announce] Synced {CMD_NAME} to guild {GUILD_ID}.")
        except Exception as e:
            print(f"[announce] Guild sync check/sync failed: {e}")

    async def is_next_event_season(self, champtype, target_weekday: int) -> bool:
        today = date.today()
        today_weekday = today.weekday()

        # Calculate the date of the next occurrence of the target weekday
        days_until_target = (target_weekday - today_weekday) % 7
        next_target_date = today + timedelta(days=days_until_target)

        for champ in self.parsed.championships.values():
            if champtype in champ.type or champtype == champ.type:
                future_events = sorted(
                    (date.fromisoformat(event.date) for event in champ.schedule if date.fromisoformat(event.date) >= today)
                )
                if future_events:
                    next_event_date = future_events[0]
                    return next_event_date == next_target_date

        return False

    async def send_announcement(self, channel: discord.TextChannel,  announcement):
        # 4) Attach the file + embed in a single send call
        await channel.send(announcement)

    @commands.hybrid_command(name="testracesessionannounce", description="testracesessionannounce")
    async def testracesessionannounce(self, ctx, type:str):
        if type not in ["mx5", "touringcar", "formula", "gt3", "worldtour", "test", "wcw"]:
            await ctx.send("Invalid type. Please use one of the following: mx5, touringcar, formula, gt3, worldtour, test, wcw")
        else:
            isseason = await self.find_if_season_day(type, None)
            if isseason:
                await ctx.send(type + " would be called a season race if it were announced today")
            else:
                await ctx.send(type + " would NOT be called a season race if it were announced today")

    @commands.hybrid_command(name="testracesessionannouncewithdate", description="testracesessionannouncewithdate")
    async def testracesessionannouncewithdate(self, ctx, type:str, giventime:str):
        if type not in ["mx5", "touringcar", "formula", "gt3", "worldtour", "test", "wcw"]:
            await ctx.send("Invalid type. Please use one of the following: mx5, touringcar, formula, gt3, worldtour, test, wcw")
        else:
            isseason = await self.find_if_season_day(type, giventime)
            if isseason:
                await ctx.send(type + " would be called a season race if it were announced on " + giventime)
            else:
                await ctx.send(type + " would NOT be called a season race if it were announced on " + giventime)

    # This asynchronous function checks whether any championship in the provided
    # list has an event scheduled for today (in UTC) or, for some cross-boundary cases,
    # tomorrow (but which still belong to today’s race day, depending on the sport).
    async def find_if_season_day(self, type, giventime) -> bool:
        # Use UTC since event.date was created from a UTC-converted datetime.
        today_utc = None
        tomorrow_utc = None
        if type == "test":
            return True
        elif type == "testopen":
            return False
        if giventime:
            try:
                # Convert the Discord timestamp string into a datetime object
                today_utc = datetime.fromisoformat(giventime).date()
            except ValueError:
                logger.info(f"Invalid date format: {giventime}. Please use YYYY-MM-DD format.")
                return False
        else:
            today_utc = datetime.now(timezone.utc).date()
            tomorrow_utc = today_utc + timedelta(days=1)

        for champ in self.parsed.championships.values():
            logger.info(f"Checking championship: {champ.name}")
            for event in champ.schedule:
                # event.date is stored as an ISO string, e.g. "2025-06-06"
                event_date = date.fromisoformat(event.date)
                logger.info(f"Found event '{event.name}' scheduled on {event_date} (UTC)")
                # Direct match: event falls on today's date (UTC)
                logger.info(f"Comparing event date {event_date} with today {today_utc} and tomorrow {tomorrow_utc} or given date {giventime}")
                if event_date == today_utc and type in champ.type:
                    logger.info("Event occurs today!")
                    return True

                # For events that fall on tomorrow’s UTC date, apply extra checks
                # depending on the championship type. These conditions help account for
                # races that might straddle midnight (ET vs. London time) without changing
                # your championship code.
                if event_date == tomorrow_utc:
                    # Monday = MX5, Tuesday = touringcar, Thursday = Formula, Friday = GT3.
                    if today_utc.weekday() == 0 and "mx5" in champ.type and type in champ.type:
                        logger.info("Found an MX5 event that belongs to today's race day (Monday scenario)!")
                        logger.info("Found an MX5 event that belongs to today's race day")
                        return True
                    if today_utc.weekday() == 1 and "touringcar" in champ.type and type in champ.type:
                        logger.info("Found a touringcar event that belongs to today's race day (Tuesday scenario)!")
                        logger.info("Found a touringcar event that belongs to today's race")
                        return True
                    if today_utc.weekday() == 3 and "formula" in champ.type and type in champ.type:
                        logger.info("Found a Formula event that belongs to today's race day (Thursday scenario)!")
                        logger.info("Found a Formula event that belongs to today's race")
                        return True
                    if today_utc.weekday() == 4 and "gt3" in champ.type and type in champ.type:
                        logger.info("Found a GT3 event that belongs to today's race day (Friday scenario)!")
                        logger.info("Found a GT3 event that belongs to today's day")
                        return True
        return False
    
    def get_current_time(self, timezone):
        # Get the current system time in UTC
        utc_time = datetime.now(pytz.utc)
        
        # Convert UTC time to the specified timezone
        local_timezone = pytz.timezone(timezone)
        local_time = utc_time.astimezone(local_timezone)
        
        return local_time

        
    @tasks.loop(seconds=60.0)
    async def fetch_time(self):
        self.currenteutime = self.get_current_time("Europe/London")
        self.currentnatime = self.get_current_time("US/Central")

    def clear_session_flags(self):
        """
        Set every `*euraceannounced` and `*naraceannounced` flag to False,
        plus your one “saturdayraceannounced” if you’re using that.
        """
        # EU flags
        for day in self.session_days:
            setattr(self, f"{day}euraceannounced", False)
        # NA flags
        for day in self.session_days:
            setattr(self, f"{day}naraceannounced", False)
        # If you have a special saturday flag without region suffix:
        setattr(self, "saturdayraceannounced", False)


    @tasks.loop(time=EU_TIME)
    async def eu_race_slot(self):
        # fires Mon/Tue/Thu/Fri at 19:00 Europe/London
        now = self.get_current_time("Europe/London")
        wd  = now.weekday()
        if wd == 0:      # Monday
            await self.on_race_start(region="EU", event="mx5")
        elif wd == 1:    # Tuesday
            await self.on_race_start(region="EU", event="touringcar")
        elif wd == 3:    # Thursday
            await self.on_race_start(region="EU", event="formula")
        elif wd == 4:    # Friday
            await self.on_race_start(region="EU", event="gt3")

    @tasks.loop(time=US_TIME)
    async def na_race_slot(self):
        # fires Mon/Tue/Thu/Fri at 20:00 CST
        now = self.get_current_time("US/Central")
        wd  = now.weekday()
        if wd == 0:
            await self.on_race_start(region="NA", event="mx5")
        elif wd == 1:
            await self.on_race_start(region="NA", event="touringcar")
        elif wd == 3:
            await self.on_race_start(region="NA", event="formula")
        elif wd == 4:
            await self.on_race_start(region="NA", event="gt3")

    @tasks.loop(time=SAT_TIME)
    async def sat_special_slot(self):
        # fires Saturday at 20:00 Europe/London
        now = self.get_current_time("Europe/London")
        if now.weekday() == 5:
            await self.on_race_start(region="EU", event="worldtour")

    async def on_race_start(self, region: str, event: str):
        roles = []
        leaguechannel = 1317629640793264229
        announcestr = ""
        tz = ZoneInfo("Europe/London") if region == "EU" else ZoneInfo("US/Central")
        today = datetime.now(tz).weekday()            # 0=Mon .. 6=Sun
        day_name = self.session_days[today]
        flag_attr = f"{day_name}{region.lower()}raceannounced"
        # for a “normal” weekday race (Mon–Fri):
        if event in {"mx5", "touringcar", "formula", "gt3"}:
            flag_attr = f"{day_name}{region.lower()}raceannounced"
        # for your Saturday “worldtour” slot you said you have a single flag:
        elif event == "worldtour":
            flag_attr = "saturdayraceannounced"
        else:
            # nothing to do for unknown event
            return
        if getattr(self, flag_attr, False):
            logger.info("returning early as already announced for this region, even though it should only occur at that time")
            return  # already announced today for this region
        self.clear_session_flags()
        if event == "mx5":
            leaguechannel = 1366724512632148028
            if region == "EU":
                roles.append(1117573763869978775)
            else:
                roles.append(1117573512064946196)
        elif event == "test":
            leaguechannel = 1366724512632148028
            roles.append(1320448907976638485)
        elif event == "gt3":
            leaguechannel = 1366724548719804458
            if region == "EU":
                roles.append(1117574027645558888)
            else:
                roles.append(1117573957634228327)
        elif event == "touringcar":
            if region == "EU":
                roles.append(1358914901153681448)
            else:
                roles.append(1358915346362531940)
            leaguechannel = 1366782207238209548
        elif event == "formula":
            leaguechannel = 1366755399566491718
            if region == "EU":
                roles.append(1358915606115651684)
            else:
                roles.append(1358915647634936058)
        elif event == "worldtour":
            leaguechannel = 1410620564120277063
            roles.append(1396558471175864430)
        isseason = await self.find_if_season_day(event, None)
        if event == "worldtour":
            isseason = True
        setattr(self, flag_attr, True)
        self.save_race_announcement_data()
        role_mentions = " ".join([f"<@&{role_id}>" for role_id in roles])
        announcestr += role_mentions
        announcestr += "The Race session has started! check out : <#" + str(leaguechannel) + "> for more info!"
        if event == "formula":
            if isseason:
                if region == "EU":
                    announcestr += " This is NOT a season race today for the EU folks, it is an OPEN race! but it IS a season race for the NA folks later!"
                else:
                    announcestr += " This is a season race today!"
            else:
                announcestr += " This is NOT a season race today, it is an OPEN race! for BOTH EU and NA!"
        else:
            if isseason:
                announcestr += " This is a season race today!"
            else:
                announcestr += " This is NOT a season race today, it is an OPEN race!"
        parent_channel = self.bot.get_channel(1382026220388225106)
        if parent_channel is None:
            logger.info("No valid channel available to send the announcement.")
            return
        await self.send_announcement(parent_channel, announcestr)

        
    @tasks.loop(seconds=600.0)
    async def check_for_announcements(self):
        logger.info("check for announcements task running")
        global ON_READY_FIRST_ANNOUNCE_CHECK
        if ON_READY_FIRST_ANNOUNCE_CHECK:
            ON_READY_FIRST_ANNOUNCE_CHECK = False
            logger.info("ON_READY_FIRST_ANNOUNCE_CHECK is True, returning from first announcement check")
            return
        cst_timezone = pytz.timezone("US/Central")
        now_cst = self.currentnatime.astimezone(cst_timezone)  # Ensure it's CST-aware
        current_cst_day = now_cst.strftime("%A")
        if 8 <= now_cst.hour < 10:
            race_map = {
                "Monday": "mx5",
                "Tuesday": "touringcar",
                "Wednesday": "wcw",
                "Thursday": "formula",
                "Friday": "gt3",
                "Saturday": "worldtour"
            }
            if current_cst_day in race_map and not getattr(self, f"{current_cst_day.lower()}announced", False):
                logger.info("announcing raceday for " + current_cst_day)
                logger.info("announcing raceday for " + current_cst_day)
                await self.announce_raceday(race_map[current_cst_day])
            
                # Reset all flags
                for day in race_map.keys():
                    setattr(self, f"{day.lower()}announced", day == current_cst_day)
                for day in race_map.keys():
                    logger.info("setting " + day.lower() + "announced to " + str(getattr(self, f"{day.lower()}announced", False)))
                    logger.info("setting " + day.lower() + "announced to " + str(getattr(self, f"{day.lower()}announced", False)))
                self.save_announcement_data()
            else:
                pass

    @commands.hybrid_command(name="testracedayannounce", description="testracedayannounce")
    @commands.is_owner()
    async def testracedayannounce(self, ctx, type: str):
        await self.announce_raceday(type)
        await ctx.send("Announced raceday for " + type)

    @commands.hybrid_command(name="cancelrace", description="cancelrace")
    async def cancelrace(self, ctx):
        cst_timezone = pytz.timezone("US/Central")
        now_cst = self.currentnatime.astimezone(cst_timezone)  # Ensure it's CST-aware
        current_cst_day = now_cst.strftime("%A")
        race_map = {
            "Monday": "mx5",
            "Tuesday": "touringcar",
            "Wednesday": "wcw",
            "Thursday": "formula",
            "Friday": "gt3",
            "Saturday": "worldtour"
        }
        if current_cst_day in race_map and not getattr(self, f"{current_cst_day.lower()}announced", False):
            logger.info("cancelling raceday for " + current_cst_day)
            logger.info("announcing raceday for " + current_cst_day)
        
            # Reset all flags
            for day in race_map.keys():
                setattr(self, f"{day.lower()}announced", day == current_cst_day)
            for day in race_map.keys():
                logger.info("setting " + day.lower() + "announced to " + str(getattr(self, f"{day.lower()}announced", False)))
                logger.info("setting " + day.lower() + "announced to " + str(getattr(self, f"{day.lower()}announced", False)))
            self.save_announcement_data()

        tzeu = ZoneInfo("Europe/London")
        tzna = ZoneInfo("US/Central")
        todayeu = datetime.now(tzeu).weekday()  
        todayna = datetime.now(tzna).weekday()
        day_nameeu = self.session_days[todayeu]
        day_namena = self.session_days[todayna]
        flag_attreu = f"{day_nameeu}{"eu"}raceannounced"
        flag_attrna = f"{day_namena}{"na"}raceannounced"
        
        if todayeu == 5:
            flag_attreu = "saturdayraceannounced"
        if getattr(self, flag_attreu, False):
            setattr(self, flag_attreu, True)
        if getattr(self, flag_attrna, False):
            setattr(self, flag_attrna, True)
        self.save_race_announcement_data()
        await ctx.send("cancelling race announcements for today only")
        
    async def announce_raceday(self, type):
        roles = []
        leaguechannel = 1317629640793264229
        announcestr = ""
        if type == "wcw":
            roles.append(1117574168611930132)
            roles.append(1332356298179870821)
            role_mentions = " ".join([f"<@&{role_id}>" for role_id in roles])
            announcestr += role_mentions
            announcestr += "It's Wildcard Wednesday! stay tuned for futher information later on when we reveal what the surprise event is!"
            leaguechannel = 1366786850760429671
            parent_channel = self.bot.get_channel(1382026220388225106)
            await self.send_announcement(parent_channel, announcestr)
            return
        else:
            if type == "mx5":
                leaguechannel = 1366724512632148028
                roles.append(1117573512064946196)
                roles.append(1117573763869978775)
            elif type == "test":
                leaguechannel = 1328800009189195828
            elif type == "testopen":
                leaguechannel = 1328800009189195828
            elif type == "testmx5open":
                leaguechannel = 1328800009189195828
            elif type == "gt3":
                leaguechannel = 1366724548719804458
                roles.append(1117573957634228327)
                roles.append(1117574027645558888)
            elif type == "touringcar":
                leaguechannel = 1366782207238209548
                roles.append(1358914901153681448)
                roles.append(1358915346362531940)
            elif type == "formula":
                leaguechannel = 1366755399566491718
                roles.append(1358915606115651684)
                roles.append(1358915647634936058)
            elif type == "worldtour":
                leaguechannel = 1410620564120277063
                roles.append(1396558471175864430)
            else:
                logger.info("Invalid type provided for raceday announcement.")
                return

            role_mentions = " ".join([f"<@&{role_id}>" for role_id in roles])
            announcestr += role_mentions
            embed = await self.get_raceday_announce_string(type, leaguechannel)
        if type == "test" or type == "testopen" or type == "testmx5open":
            parent_channel = self.bot.get_channel(1328800009189195828)
        else:
            parent_channel = self.bot.get_channel(1382026220388225106)
        if parent_channel is None:
            logger.info("No valid channel available to send the announcement.")
            return
        await parent_channel.send(content=role_mentions, embed=embed)

    async def get_raceday_announce_string(self, type, leaguechannel):
        logger.info("get raceday announce string for type " + type )
        is_season = await self.find_if_season_day(type, None)
        if type == "worldtour":
            is_season = True
        if is_season:
            embed = await self._build_season_raceday_embed(type, leaguechannel)
        else:
            embed = await self._build_open_raceday_embed(type, leaguechannel)
        return embed

    async def _build_open_raceday_embed(self, series_type: str, leaguechannel) -> discord.Embed:
        # determine server URLs
        mapping = {
            "mx5":    (self.mx5euopenserver,    self.mx5naopenserver),
            "touringcar":    (self.gt4euopenserver,    self.gt4naopenserver),
            "formula":(self.formulaeuopenserver,self.formulanaopenserver),
            "gt3":    (self.gt3euopenserver,    self.gt3naopenserver),
            "worldtour": (self.worldtourserver, self.worldtourserver),
            "test": (self.mx5euopenserver, self.mx5naopenserver),
            "testopen": (self.mx5euopenserver, self.mx5naopenserver),
            "testmx5open": (self.mx5euopenserver, self.mx5naopenserver),
        }
        try:
            base_eu, base_na = mapping[series_type]
        except KeyError:
            raise ValueError(f"Unknown series: {series_type}")

        # scrape track name
        data = self.scrape_event_details_and_map(base_eu)
        if not data:
            raise RuntimeError("Failed to scrape track info")

        # compute next session times
        slot_map = {"mx5open":0, "touringcaropen":1, "formulaopen":3, "gt3open":4, "worldtouropen":5, "worldtour": 5, "testopen":0, "test":0, "testmx5open":0}
        if series_type != "testopen" and series_type != "testmx5open":
            event_type = series_type + "open"
        else:
            event_type = "testopen"
        wd = slot_map[event_type]
        now = datetime.now(timezone.utc)
        eu_dt = self._next_slot(now, wd, ZoneInfo("Europe/London"), 19, 0)
        if event_type == "worldtour" or event_type == "worldtouropen":
            eu_dt = self._next_slot(now, wd, ZoneInfo("Europe/London"), 20, 0)
        na_dt = self._next_slot(now, wd, timezone(timedelta(hours=-6)),    19, 0)
        eu_ts = self._to_discord_timestamp(eu_dt, "f")
        na_ts = self._to_discord_timestamp(na_dt, "f")
        print("series type is " + series_type)
        if event_type == "worldtour" or event_type == "worldtouropen":
            na_ts = "no NA worldtour race"
        if event_type == "gt4" or event_type == "gt4open":
            series_type == "Touring Car"
        if series_type == "gt4" or series_type == "gt4open":
            series_type == "Touring Car"
        # build the embed
        emb = discord.Embed(
            title=f"{series_type.upper()} Open Raceday Tonight! 🏁",
            colour=discord.Colour.dark_teal()
        )
        track_to_use = None
        if data["downloads"]["track"] is None:
            track_to_use = "Track comes with the game!"
        else:
            track_to_use = f"[Click here]({data["downloads"]["track"]})"
        live_timing_eu = f"{base_eu}/live-timing"
        live_timing_na = f"{base_na}/live-timing"
        emb.add_field(name="EU Session Start",   value=eu_ts,                       inline=True)
        emb.add_field(name="NA Session Start",   value=na_ts,                       inline=True)
        emb.add_field(name="Track",              value=data["track_name"],          inline=False)
        emb.add_field(name="Track Download",          value=track_to_use,  inline=True)
        emb.add_field(name="Join EU Server",     value=f"[Click here]({live_timing_eu})",  inline=True)
        emb.add_field(
            name="Join NA Server",
            value=f"[Click here]({live_timing_na})" if event_type != "worldtour" and event_type != "worldtouropen" else "no NA worldtour server",
            inline=True
        )

        emb.add_field(
            name="Incident Reports and help: Any problems during the race, log a ticket in the channel below!",
            value=f"<#1156789473309368330>",
            inline=True
        )

        emb.add_field(name="Information Channel", value=f"<#{leaguechannel}>", inline=False)
        emb.add_field(name="💖 Support Tekly Racing",
            value=(
                "If you like racing with us, please consider helping with the server costs for "
                "Tekly Racing here: <https://ko-fi.com/teklysimracing>\n"
                "Or grab some cool Tekly merch: <https://store.tekly.racing>\n"
                "Check out our instagram : <https://www.instagram.com/teklyracing/>\n"
                "Real Rookie Racing Youtube : <https://www.youtube.com/@RealRookieRacing/>"
            ),
            inline=False
        )

        # footer / reminder
        emb.add_field(
            name="Rules and Regulations",
            value=("Read our Wiki for our general rules, regulations, and series information here : <https://wiki.tekly.racing/en/home> \n"
             " and grab the roles you want from the Channels & Roles section at the top of the Discord, so you can be notified of events and announcements!"
            ),
            inline=False
        )

        return emb
        
    async def _build_season_raceday_embed(self, series_type: str, leaguechannel) -> discord.Embed:
        # determine server URLs
        mapping = {
            "mx5":    (self.mx5eurrrserver,    self.mx5narrrserver),
            "formula":(self.formulanararserver),
            "gt3":    (self.gt3eurrrserver,    self.gt3narrrserver),
            "worldtour": (self.worldtourserver, self.worldtourserver),
            "test": (self.mx5eurrrserver, self.mx5narrrserver),
            "testopen": (self.mx5eurrrserver, self.mx5narrrserver),
        }
        standings_mapping = {
            "mx5":    (1366725812002492416),
            "gt3":    (1366725954852098078),
            "formula":(1366760700713898106),
            "worldtour": (1366759482914508893),
            "test": (1366759482914508893),
        }
        register_mapping = {
            "mx5":    (1366725292302925895),
            "gt3":    (1366725904952197161),
            "formula":(1366760737829027880),
            "worldtour": (1366759542041350294),
            "test": (1366759542041350294),
        }
        schedule_mapping = {
            "mx5":    (1366724891751088129),
            "gt3":    (1366725727604445305),
            "formula":(1366760782867599462),
            "worldtour": (1366759596638601248),
            "test": (1366759596638601248),
        }
        try:
            base_eu, base_na = mapping[series_type]
        except KeyError:
            raise ValueError(f"Unknown series: {series_type}")

        # scrape track name
        data = self.scrape_event_details_and_map(base_eu)
        if not data:
            raise RuntimeError("Failed to scrape track info")

        # compute next session times
        slot_map = {"mx5":0, "touringcar":1, "formula":3, "gt3":4, "worldtour":5, "test":0}

        wd = slot_map[series_type]
        now = datetime.now(timezone.utc)
        eu_dt = self._next_slot(now, wd, ZoneInfo("Europe/London"), 19, 0)
        if series_type == "worldtour":
            eu_dt = self._next_slot(now, wd, ZoneInfo("Europe/London"), 20, 0)
        na_dt = self._next_slot(now, wd, timezone(timedelta(hours=-6)),    19, 0)
        eu_ts = self._to_discord_timestamp(eu_dt, "f")
        na_ts = self._to_discord_timestamp(na_dt, "f")
        if series_type == "worldtour":
            emb = discord.Embed(
                title=f"{series_type.upper()} iRacing Season Raceday Tonight! 🏁",
                colour=discord.Colour.dark_teal()
            )
            iracingtime = self._next_slot(now, wd, ZoneInfo("Europe/London"), 21, 0)
            iracingtime = self._to_discord_timestamp(iracingtime, "f")
            emb.add_field(name="Session Start", value=iracingtime, inline=True)
            emb.add_field(name="Information Channel for schedule and details:", value=f"<#{leaguechannel}>", inline=False)
            emb.add_field(
            name="Incident Reports and help: Any problems during the race, log a ticket in the channel below!",
            value=f"<#1156789473309368330>",
            inline=True
            )

            emb.add_field(
               name="💖 Support Tekly Racing",
                value=(
                "If you like racing with us, please consider helping with the server costs for "
                "Tekly Racing here: <https://ko-fi.com/teklysimracing>\n"
                "Or grab some cool Tekly merch: <https://store.tekly.racing>"
                ),
                inline=False
            )
            return emb

        # build the embed
        emb = discord.Embed(
            title=f"{series_type.upper()} Season Raceday Tonight! 🏁",
            colour=discord.Colour.dark_teal()
        )
        live_timing_eu = f"{base_eu}/live-timing"
        live_timing_na = f"{base_na}/live-timing"

        emb.add_field(name="EU Session Start",   value=eu_ts,                       inline=True)
        emb.add_field(name="NA Session Start",   value=na_ts,                       inline=True)
        emb.add_field(name="Track",              value=data["track_name"],          inline=False)
        emb.add_field(
            name="Track Download",
            value=f"[Click here]({data['downloads']['track']})",
            inline=True
        )
        emb.add_field(
            name="Join EU Server",
            value=f"[Click here]({live_timing_eu})",
            inline=True
        )
        emb.add_field(
            name="Join NA Server",
            value=f"[Click here]({live_timing_na})" if series_type != "worldtour" else "no NA worldtour server",
            inline=True
        )
        if series_type == "mx5":
            emb.add_field(
                name="Join NA RAR Server",
                value=f"[Click here]({self.mx5nararserver}/live-timing)",
                inline=True
            )

        emb.add_field(name="Information Channel", value=f"<#{leaguechannel}>", inline=False)

        # **FIXED**: wrap IDs in <#…> so they become clickable
        emb.add_field(
            name="Standings",
            value=f"<#{standings_mapping[series_type]}>",
            inline=True
        )
        emb.add_field(
            name="Registration",
            value=f"<#{register_mapping[series_type]}>",
            inline=True
        )
        emb.add_field(
            name="Schedule",
            value=f"<#{schedule_mapping[series_type]}>",
            inline=True
        )

        emb.add_field(
            name="Incident Reports and help: Any problems during the race, log a ticket in the channel below!",
            value=f"<#1156789473309368330>",
            inline=True
        )

        emb.add_field(
            name="💖 Support Tekly Racing",
            value=(
                "If you like racing with us, please consider helping with the server costs for "
                "Tekly Racing here: <https://ko-fi.com/teklysimracing>\n"
                "Or grab some cool Tekly merch: <https://store.tekly.racing>"
            ),
            inline=False
        )
        

        emb.add_field(
            name="Rules and Regulations",
            value=("Read our Wiki for our general rules, regulations, and series information here : <https://wiki.tekly.racing/en/home> \n"
             " and grab the roles you want from the Channels & Roles section at the top of the Discord, so you can be notified of events and announcements!"
            ),
            inline=False
        )


        return emb
    
    def format_race_line(self, day_offset, label, season_flag, track_name):
        today = date.today()
        if today.weekday() == 0:  # Monday
            monday = today
        else:  # Sunday
            monday = today + timedelta(days=1)

        race_type = "SEASON" if season_flag else "OPEN"
        race_day = monday + timedelta(days=day_offset)
        return f"{label} {race_day.day} of {race_day.strftime('%B')}: {race_type} race at {track_name}"


    @commands.hybrid_command(name="thisweek", description="thisweek")
    async def thisweek(self, ctx):
        today = date.today()
        weekday = today.weekday()  # Monday=0 ... Sunday=6

        # Race data placeholders
        race_data = {
            0: {"label": "Monday", "track": None, "season": False},
            1: {"label": "Tuesday", "track": None, "season": False},
            2: {"label": "Wednesday", "track": "wildcard Wednesday race – surprise track and car combo", "season": None},
            3: {"label": "Thursday", "track": None, "season": False},
            4: {"label": "Friday", "track": None, "season": False},
            5: {"label": "Saturday", "track": None, "season": False},
            6: {"label": "Sunday", "track": "no race", "season": None},
        }

        weekday_series = {
            0: "MX5",
            1: "Touring cars",
            3: "Formula Mazda",
            4: "GT3",
            5: "World Tour - BMW M2"
        }


        mappingopen = {
            "mx5":    self.mx5euopenserver,
            "touringcar":    self.gt4euopenserver,
            "formula": self.formulaeuopenserver,
            "gt3":    self.gt3euopenserver,
            "worldtour": self.worldtourserver
        }
        seasonmapping = {
            "mx5":    self.mx5eurrrserver,
            "formula": self.formulanararserver,
            "gt3":    self.gt3eurrrserver,
            "worldtour": self.worldtourserver
        }
        daymapping = {
            "mx5": 0,  # Monday
            "touringcar": 1,  # Tuesday
            "formula": 3,  # Thursday
            "gt3": 4,  # Friday
            "worldtour": 5,  # Saturday
        }
        types = ["mx5", "touringcar", "formula", "gt3", "worldtour"]

        for racetype in types:
            isseason = await self.is_next_event_season(racetype, daymapping[racetype])
            server = seasonmapping.get(racetype) if isseason else mappingopen.get(racetype)
            data = self.scrape_event_details_and_map(server)

            if racetype == "mx5":
                race_data[0]["track"] = data["track_name"]
                race_data[0]["season"] = isseason
            elif racetype == "touringcar":
                race_data[1]["track"] = data["track_name"]
                race_data[1]["season"] = isseason
            elif racetype == "formula":
                race_data[3]["track"] = data["track_name"]
                race_data[3]["season"] = isseason
            elif racetype == "gt3":
                race_data[4]["track"] = data["track_name"]
                race_data[4]["season"] = isseason
            elif racetype == "worldtour":
                race_data[5]["track"] = data["track_name"]
                race_data[5]["season"] = isseason

        # Build the output only for upcoming days
        printoutput = ""
        for offset in range(weekday, 7):
            day = today + timedelta(days=(offset - weekday))
            info = race_data[offset]
            label = info["label"]

            # Series name prefix if defined
            series = weekday_series.get(offset)
            if offset == 2:  # Wednesday
                printoutput += f"{label} {day.day} of {day.strftime('%B')}: wildcard Wednesday race: surprise track and car combo\n"
            elif offset == 6:  # Sunday
                printoutput += f"{label} {day.day} of {day.strftime('%B')}: no race\n"
            else:
                race_type = "SEASON" if info["season"] else "OPEN"
                printoutput += f"{label} {day.day} of {day.strftime('%B')}: {race_type} race ({series}) at {info['track']}\n"


        await ctx.send(printoutput.strip())

    @app_commands.command(
    name="announcewithrolebuttons",
    description="Open a modal to create an announcement embed with a role-grant button."
    )
    @app_commands.describe(
        grant_role="Role to grant when the button is pressed",
        ping_role="Role to ping in the announcement (optional)",
        test="Post to test channel instead of live"
    )
    @app_commands.guilds(discord.Object(id=GUILD_ID))  # keep guild-only
    @app_commands.checks.has_permissions(manage_roles=True)
    async def announcewithrolebuttons(
        self,
        interaction: discord.Interaction,
        grant_role: discord.Role,
        ping_role: Optional[discord.Role] = None,
        test: bool = False
    ):
        if interaction.guild is None:
            await interaction.response.send_message("Use this in a server.", ephemeral=True)
            return

        # choose channel
        channel_id = 1328800009189195828 if test else 1102816381348626462

        modal = AnnouncementModal(
            target_channel_id=channel_id,
            button_role_id=grant_role.id,                 # ← was grant_role_id
            ping_role_id=(ping_role.id if ping_role else None)
        )
        await interaction.response.send_modal(modal)

async def setup(bot) -> None:
    await bot.add_cog(ChampionshipCommands(bot))  