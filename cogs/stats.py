from __future__ import annotations
import aiohttp
import discord
import math
from discord.ext import commands, tasks, voice_recv
from discord.ext.commands import Context
from discord.ui import Button, View
import json
import os
import random
import calendar
import statsparser
import requests
import asyncio
from zoneinfo import ZoneInfo
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

# one executor for the whole cog / bot
BLOCKING_IO_EXECUTOR = ThreadPoolExecutor(max_workers=2)

gt3ids = ["ks_audi_r8_lms_2016","bmw_z4_gt3", "ks_ferrari_488_gt3", "ks_lamborghini_huracan_gt3",
         "ks_mclaren_650_gt3", "ks_mercedes_amg_gt3", "ks_nissan_gtr_gt3", "ks_porsche_911_gt3_r_2016"]

gt4ids = ["gt4_alpine_a110", "gt4_ford_mustang","gt4_ginetta_g55", "gt4_mclaren_570s", "gt4_porsche_cayman_718", "gt4_toyota_supra"]

formulaids = ["rss_formula_hybrid_v12-r","rss_formula_rss_4", "rss_formula_rss_3_v6", "rss_formula_rss_2_v6_2020", "rss_formula_rss_2_v8", "rss_formula_hybrid_2021", "rss_formula_hybrid_2018"]

async def _run_blocking(func, *args):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(BLOCKING_IO_EXECUTOR, func, *args)

ON_READY_FIRST_RUN_DOWNLOAD = True
ON_READY_FIRST_TIME_SCAN = True
ON_READY_FIRST_TIME_QUALY_SCAN = True
ALREADY_BETTING_CLOSED = False
ON_READY_FIRST_DISTRIBUTE_COIN = True
ALREADY_ANNOUNCED_BETTING = False
ON_READY_FIRST_ANNOUNCE_CHECK = True

EU_TIME = dtime(19, 0, tzinfo=ZoneInfo("Europe/London"))
US_TIME = dtime(20, 0, tzinfo=ZoneInfo("US/Central"))
SAT_TIME = dtime(20, 0, tzinfo=ZoneInfo("Europe/London"))

_FAMILY_RX = re.compile(r"^([a-z]+[0-9]*?)(?=eu|na|_|$)", re.I)

_TS_RX = re.compile(r"<t:(\d+):")

def _raw_ts(discord_ts: str) -> Optional[int]:
    m = _TS_RX.search(discord_ts or "")
    return int(m.group(1)) if m else None

def _family(ch_type: str) -> str:
    """
    'mx5euopen'  -> 'mx5'
    'gt3eurar'   -> 'gt3'
    'gt4naopen'  -> 'gt4'
    'formulaeur' -> 'formula'
    'worldtour'  -> 'worldtour'
    """
    m = _FAMILY_RX.match(ch_type)
    return m.group(1).lower() if m else ch_type.lower()

def _label(ch_type: str) -> str:
    """'mx5narar' → 'NA'   ·   'gt3eurrr' → 'EU'   ·   fall‑back ⇒ ch_type"""
    return "NA" if "na" in ch_type.lower() else "EU" if "eu" in ch_type.lower() else ch_type

ALLOWED_CHANNELS = {
    "global": ["1134963371553337478", "1328800009189195828", "1098040977308000376"],  # Channels for most commands
    "tracklookup": ["1134963371553337478","1328800009189195828","1098040977308000376","1085906626852163636"],
    "votefortrack": ["1134963371553337478","1328800009189195828","1098040977308000376","1085906626852163636"],
    "save_track_data_to_json": ["1134963371553337478","1328800009189195828","1098040977308000376","1085906626852163636"],
    "select_track": ["1134963371553337478","1328800009189195828","1098040977308000376","1085906626852163636"],
    "handle_vote": ["1134963371553337478","1328800009189195828","1098040977308000376","1085906626852163636"]
    }

class VoteView(discord.ui.View):
    def __init__(self, embed, timeout=14400, create_callback=None):
        super().__init__(timeout=timeout)
        self.embed = embed
        self.create_callback = create_callback
        # Add score buttons from 1 to 5
        for rating in range(1, 6):
            button = discord.ui.Button(
                label=f"{rating} Stars",
                style=discord.ButtonStyle.primary,
                custom_id=f"vote_{rating}"  # Optional: helps with debugging/logging
            )
            # Bind the button's callback with the rating.
            button.callback = self._generate_callback(rating)
            self.add_item(button)

    def _generate_callback(self, rating: int):
        async def callback(interaction: discord.Interaction):
            # Delegate to your existing vote handling logic.
            await self.create_callback(interaction, rating)
        return callback

    async def on_timeout(self):
        # When the view times out, disable all the children (buttons)
        for child in self.children:
            child.disabled = True

        # Update the embed footer (or you could add a new field) to indicate voting has ended.
        self.embed.set_footer(text="Voting time expired. You can no longer cast a vote.")
        # Edit the original message to update the view and embed.
        if hasattr(self, "message"):
            try:
                await self.message.edit(embed=self.embed, view=self)
            except Exception as e:
                logger.info(f"Error updating message on timeout: {e}")

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

class Bet():
    def __init__(self, betterid, amount, racerguid, odds):
        self.amount = amount
        self.racerguid = racerguid
        self.odds = odds
        self.better = betterid

class EventBet():
    def __init__(self, eventname, track, odds, car, guidtoname, nametoguid, servername):
        self.eventname = eventname
        self.track = track
        self.odds = odds
        self.timestamp = datetime.now()
        self.closed = False
        self.car = car
        self.bets = []
        self.guidtoname = guidtoname
        self.nametoguid = nametoguid
        self.servername = servername

    def to_dict(self):
        return {
            "eventname": self.eventname,
            "track": self.track,
            "odds": self.odds,
            "timestamp": self.timestamp.isoformat(),  # Convert datetime to ISO 8601 string
            "closed": self.closed,
            "car": self.car,
            "bets": [bet.__dict__ for bet in self.bets],  
            "guidtoname": self.guidtoname,
            "servername" : self.servername,
            "nametoguid": self.nametoguid
        }

    @staticmethod
    def from_dict(data):
        # Convert guidtoname and nametoguid keys/values to lowercase
        guidtoname = {guid.lower(): name.lower() for guid, name in data["guidtoname"].items()}
        nametoguid = {name.lower(): guid.lower() for name, guid in data["nametoguid"].items()}

        # Create EventBet instance
        event_bet = EventBet(
            eventname=data["eventname"],  # Ensure event name is lowercase
            track=data["track"],          # Ensure track name is lowercase
            odds={guid.lower(): odds for guid, odds in data["odds"].items()},  # Lowercase keys for odds
            car=data["car"],              # Ensure car name is lowercase
            guidtoname=guidtoname,
            nametoguid=nametoguid,
            servername=data["servername"]
        )
        # Convert each bet dictionary back into a Bet object, mapping `better` to `betterid`
        event_bet.bets = [
            Bet(
                betterid=bet_data["better"],  # Map `better` to `betterid`
                amount=bet_data["amount"],
                racerguid=bet_data["racerguid"],
                odds=bet_data["odds"]
            )
            for bet_data in data["bets"]
        ]
        event_bet.timestamp = datetime.fromisoformat(data["timestamp"])  # Convert timestamp back to datetime
        event_bet.closed = data["closed"]  # Restore closed status
        return event_bet


# Here we name the cog and create a new class for the cog.
class Stats(commands.Cog, name="stats"):
    def __init__(self, bot) -> None:
        self.bot = bot
        print("loading stats cog")
        self.parsed = statsparser.parser()
        self.user_data = self.load_user_data()
        self.currenteventbet = None
        self.load_current_event_bet()
        self.fetch_results_list.start()
        self.justadded = []
        self.logger = logger
        self.session_days = [
            "monday", "tuesday", "wednesday",
            "thursday", "friday", "saturday", "sunday",
            ]
        self.currenteutime = None
        self.currentnatime = None
        self.mondayannounced = False
        self.tuesdayannounced = False
        self.wednesdayannounced = False
        self.thursdayannounced = False
        self.fridayannounced = False
        self.saturdayannounced = False
        self.sundayannounced = False
        self.mondayeuraceannounced = False
        self.tuesdayeuraceannounced = False
        self.wednesdayeuraceannounced = False
        self.thursdayeuraceannounced = False
        self.fridayeuraceannounced = False
        self.saturdayraceannounced = False
        self.sundayeuraceannounced = False
        self.mondaynaraceannounced = False
        self.tuesdaynaraceannounced = False
        self.wednesdaynaraceannounced = False
        self.thursdaynaraceannounced = False
        self.fridaynaraceannounced = False
        self.sundaynaraceannounced = False
        self.load_announcement_data()
        self.load_race_announcement_data()
        self.eu_race_slot.start()
        self.na_race_slot.start()
        self.sat_special_slot.start()
        self.timetrialserver = 'https://timetrial.ac.tekly.racing'

        self.mx5euopenserver = "https://eu.mx5.ac.tekly.racing"
        self.mx5naopenserver = "https://na.mx5.ac.tekly.racing"
        self.mx5eurrrserver = "https://eu.mx5.rrr.ac.tekly.racing"
        self.mx5narrrserver = "https://na.mx5.rrr.ac.tekly.racing"
        self.mx5nararserver = "https://na.mx5.rar.ac.tekly.racing"
        self.gt3euopenserver = "https://eu.gt3.ac.tekly.racing"
        self.gt3naopenserver = "https://na.gt3.ac.tekly.racing"
        self.gt3eurrrserver = "https://eu.gt3.rrr.ac.tekly.racing"
        self.gt3narrrserver = "https://na.gt3.rrr.ac.tekly.racing"
        self.worldtourserver = "https://worldtour.ac.tekly.racing"
        self.gt4euopenserver = "https://eu.gt4.ac.tekly.racing"
        self.gt4naopenserver = "https://na.gt4.ac.tekly.racing"
        self.formulaeuopenserver = "https://eu.f3.ac.tekly.racing"
        self.formulanaopenserver = "https://na.f3.ac.tekly.racing"
        self.formulanararserver = "https://na.f1.rar.ac.tekly.racing"
        self.testserver = "https://eu.wcw.ac.tekly.racing"
        self.servers = ( self.mx5euopenserver, self.mx5naopenserver, self.mx5eurrrserver, self.mx5narrrserver, self.mx5nararserver,
                        self.gt3euopenserver, self.gt3naopenserver, self.gt3eurrrserver, self.gt3narrrserver,
                        self.worldtourserver, self.gt4euopenserver, self.gt4naopenserver,
                        self.formulaeuopenserver, self.formulanaopenserver,
                         self.formulanararserver)
        self.blacklist = ["2025_1_4_21_37_RACE.json", "2025_1_4_22_2_RACE.json",
                          "2024_12_21_21_58_RACE.json", "2024_12_21_21_32_RACE.json",
                          "2025_2_17_20_30_RACE.json", "2025_2_17_20_57_RACE.json",
                          "2025_2_22_22_0_RACE.json", "2025_2_22_21_35_RACE.json", "2025_4_8_19_42_RACE.json"]

        self.servertodirectory = {
            self.mx5euopenserver: "mx5euopen",
            self.mx5naopenserver: "mx5naopen",
            self.mx5eurrrserver: "mx5eurrr",
            self.mx5narrrserver: "mx5narrr",
            self.mx5nararserver: "mx5narar",
            self.gt3euopenserver: "gt3euopen",
            self.gt3naopenserver: "gt3naopen",
            self.gt3eurrrserver: "gt3eurrr",
            self.gt3narrrserver: "gt3narrr",
            self.worldtourserver: "worldtour",
            self.gt4euopenserver: "gt4euopen",
            self.gt4naopenserver: "gt4naopen",
            self.formulaeuopenserver: "formulaeuopen",
            self.formulanaopenserver: "formulanaopen",
            self.formulanararserver: "formulanarar"

        }
        self.servertoseriesname = {
            self.mx5euopenserver: "MX5 EU Open Race",
            self.mx5naopenserver: "MX5 NA Open Race",
            self.mx5eurrrserver: "MX5 EU RRR Season Race",
            self.mx5narrrserver: "MX5 NA RRR Season Race",
            self.mx5nararserver: "MX5 NA RAR Season Race",
            self.gt3euopenserver: "GT3 EU Open Race",
            self.gt3naopenserver: "GT3 NA Open Race",
            self.gt3eurrrserver: "GT3 EU RRR Season Race",
            self.gt3narrrserver: "GT3 NA RRR Season Race",
            self.worldtourserver: "World Tour Race",
            self.gt4euopenserver: "GT4 EU Open Race",
            self.gt4naopenserver: "GT4 NA Open Race",
            self.formulaeuopenserver: "Formula 3 EU Open Race",
            self.formulanaopenserver: "Formula 3 NA Open Race",
            self.formulanararserver: "Formula 1 NA RAR Season Race"

        }
        self.servertoresultsthread = {
            self.mx5euopenserver: 1366724741175443456,
            self.mx5naopenserver: 1366724741175443456,
            self.mx5eurrrserver: 1366724741175443456,
            self.mx5narrrserver: 1366724741175443456,
            self.mx5nararserver: 1366724741175443456,
            self.gt3euopenserver: 1366725581470830625,
            self.gt3naopenserver: 1366725581470830625,
            self.gt3eurrrserver: 1366725581470830625,
            self.gt3narrrserver: 1366725581470830625,
            self.worldtourserver: 1366759692294160394,
            self.gt4euopenserver: 1367102297988923514,
            self.gt4naopenserver: 1367102297988923514,
            self.formulaeuopenserver: 1366760813494276156,
            self.formulanaopenserver: 1366760813494276156,
            self.formulanararserver: 1366760813494276156

        }
        self.servertoschedulethread = {
            self.mx5euopenserver: 1366781901796409464, 
            self.mx5naopenserver: 1366781901796409464,
            self.mx5eurrrserver: 1366724891751088129,
            self.mx5narrrserver: 1366724891751088129,
            self.mx5nararserver: 1366724891751088129,
            self.gt4euopenserver: 1366782309659049984,
            self.gt4naopenserver: 1366782309659049984,
            self.gt3euopenserver: 1366776293344940042,
            self.gt3naopenserver: 1366776293344940042,
            self.gt3eurrrserver: 1366725727604445305,
            self.gt3narrrserver: 1366725727604445305,
            self.worldtourserver: 1366759596638601248,
            self.formulaeuopenserver: 1366781229336100905,
            self.formulanaopenserver: 1366781229336100905,
            self.formulanararserver: 1366760782867599462,

        }

        self.servertostandingsthread = {
            self.mx5euopenserver: 1366725812002492416,
            self.mx5naopenserver: 1366725812002492416,
            self.mx5eurrrserver: 1366725812002492416,
            self.mx5narrrserver: 1366725812002492416,
            self.mx5nararserver: 1366725812002492416,
            self.gt3euopenserver: 1366725954852098078,
            self.gt3naopenserver: 1366725954852098078,
            self.gt3eurrrserver: 1366725954852098078,
            self.gt3narrrserver: 1366725954852098078,
            self.worldtourserver: 1366759596638601248,
            self.formulaeuopenserver: 1366760700713898106,
            self.formulanaopenserver: 1366760700713898106,
            self.formulanararserver: 1366760700713898106,

        }
        self.servertoparentchannel = {
            self.mx5euopenserver: 1366724512632148028,
            self.mx5naopenserver: 1366724512632148028,
            self.mx5eurrrserver: 1366724512632148028,
            self.mx5narrrserver: 1366724512632148028,
            self.mx5nararserver: 1366724512632148028,
            self.gt3euopenserver: 1366724548719804458,
            self.gt3naopenserver: 1366724548719804458,
            self.gt3eurrrserver: 1366724548719804458,
            self.gt3narrrserver: 1366724548719804458,
            self.worldtourserver: 1366757441768915034,
            self.gt4euopenserver: 1366782207238209548,
            self.gt4naopenserver: 1366782207238209548,
            self.formulaeuopenserver: 1366755399566491718,
            self.formulanaopenserver: 1366755399566491718,
            self.formulanararserver: 1366755399566491718
        }
        self.download_queue = []
        self.mx5openrace = None
        self.gt3openrace = None
        self.gt4openrace = None
        self.formulaopenrace = None
        self.mx5openracemessage = None
        self.gt3openracemessage = None
        self.gt4openracemessage = None
        self.formulaopenracemessage = None
        self.load_open_race_data()
        logger.info("Stats cog loaded")
        self.check_sessions_task.start()
        self.check_open_races_task.start()
        self.distribute_coins.start()
        self.fetch_time.start()
        self.check_for_announcements.start()
        self.bot.loop.create_task(self._first_load())
        self.base_dir: Path = Path(__file__).parent.parent
        self.dir_flags = self.base_dir / "flags"
        self.dir_fonts = self.base_dir / "fonts"
        self.dir_logos = self.base_dir / "logos"
        self.dir_output = self.base_dir / "output"
        self.dir_presets = self.base_dir / "presets"
        self.dir_results = self.base_dir / "results"
        self.dir_templates = self.base_dir / "templates"
        self.default_settings: Dict[str, Any] = {
            "x_name": 80,
            "y_start": 150,
            "line_spacing": 35,
            "logo_offset_x": 10,
            "logo_offset_y": -5,
            "logo_fixed_x": 500,
            "logo_size": 40,
            "font_size": 28,
            "font_path": str(self.dir_fonts / "BaijamJuree-Medium.ttf"),
        }
        self.directory_to_series = {
            dir_name: self.servertoseriesname[server]
            for server, dir_name in self.servertodirectory.items()
        }

    async def _first_load(self):
        await self.bot.wait_until_ready()          # be safe; optional
        self.currenteutime = self.get_current_time("Europe/London")
        self.currentnatime = self.get_current_time("US/Central")
        await self.deserializeall_internal()

    def load_announcement_data(self):
        try:
            with open("raceannouncements.json", "r") as file:
                data = json.load(file)
            print("Loaded data:", data)  # DEBUG PRINT
            # Update the flags based on JSON data
            self.mondayannounced = data["mondayannounced"]["announced"]
            self.tuesdayannounced = data["tuesdayannounced"]["announced"]
            self.wednesdayannounced = data["wednesdayannounced"]["announced"]
            self.thursdayannounced = data["thursdayannounced"]["announced"]
            self.fridayannounced = data["fridayannounced"]["announced"]
            self.saturdayannounced = data["saturdayannounced"]["announced"]
            self.sundayannounced = data["sundayannounced"]["announced"]
            print("self.wednesdayannounced on first load = " + str(self.wednesdayannounced))  # DEBUG PRINT
        except FileNotFoundError:
            print("raceannouncements.json not found. Using default values.")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")

    def load_race_announcement_data(self):
        try:
            with open("racesessionannouncements.json", "r") as file:
                data = json.load(file)
            print("Loaded data:", data)  # DEBUG PRINT
            # Update the flags based on JSON data
            self.mondayeuraceannounced = data["mondayeuraceannounced"]["announced"]
            self.tuesdayeuraceannounced = data["tuesdayeuraceannounced"]["announced"]
            self.wednesdayeuraceannounced = data["wednesdayeuraceannounced"]["announced"]
            self.thursdayeuraceannounced = data["thursdayeuraceannounced"]["announced"]
            self.fridayeuraceannounced = data["fridayeuraceannounced"]["announced"]
            self.saturdayraceannounced = data["saturdayraceannounced"]["announced"]
            self.sundayeuraceannounced = data["sundayeuraceannounced"]["announced"]
            self.mondaynaraceannounced = data["mondaynaraceannounced"]["announced"]
            self.tuesdaynaraceannounced = data["tuesdaynaraceannounced"]["announced"]
            self.wednesdaynaraceannounced = data["wednesdaynaraceannounced"]["announced"]
            self.thursdaynaraceannounced = data["thursdaynaraceannounced"]["announced"]
            self.fridaynaraceannounced = data["fridaynaraceannounced"]["announced"]
            self.sundaynaraceannounced = data["sundaynaraceannounced"]["announced"]
            print("self.wednesdayraceannounced on first load = " + str(self.wednesdayannounced))  # DEBUG PRINT
        except FileNotFoundError:
            print("racesessionannouncements.json not found. Using default values.")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")


    def save_announcement_data(self):
        data = {
            "mondayannounced": {"announced": self.mondayannounced},
            "tuesdayannounced": {"announced": self.tuesdayannounced},
            "wednesdayannounced": {"announced": self.wednesdayannounced},
            "thursdayannounced": {"announced": self.thursdayannounced},
            "fridayannounced": {"announced": self.fridayannounced},
            "saturdayannounced": {"announced": self.saturdayannounced},
            "sundayannounced": {"announced": self.sundayannounced},
        }
        try:
            with open("raceannouncements.json", "w") as file:
                json.dump(data, file, indent=4)
            print("Saved data:", data)  # DEBUG PRINT
        except Exception as e:
            print(f"Error saving announcement data: {e}")

    def save_race_announcement_data(self):
        data = {
            "mondayeuraceannounced": {"announced": self.mondayeuraceannounced},
            "tuesdayeuraceannounced": {"announced": self.tuesdayeuraceannounced},
            "wednesdayeuraceannounced": {"announced": self.wednesdayeuraceannounced},
            "thursdayeuraceannounced": {"announced": self.thursdayeuraceannounced},
            "fridayeuraceannounced": {"announced": self.fridayeuraceannounced},
            "saturdayraceannounced": {"announced": self.saturdayraceannounced},
            "sundayeuraceannounced": {"announced": self.sundayeuraceannounced},
            "mondaynaraceannounced": {"announced": self.mondaynaraceannounced},
            "tuesdaynaraceannounced": {"announced": self.tuesdaynaraceannounced},
            "wednesdaynaraceannounced": {"announced": self.wednesdaynaraceannounced},
            "thursdaynaraceannounced": {"announced": self.thursdaynaraceannounced},
            "fridaynaraceannounced": {"announced": self.fridaynaraceannounced},
            "sundaynaraceannounced": {"announced": self.sundaynaraceannounced},
        }
        try:
            with open("racesessionannouncements.json", "w") as file:
                json.dump(data, file, indent=4)
            print("Saved data:", data)  # DEBUG PRINT
        except Exception as e:
            print(f"Error saving racesessionannouncements data: {e}")


    def save_open_race_data(self):
        print("Saving")
        data = {
            "mx5open": self.mx5openrace,
            "gt4open": self.gt4openrace,
            "formulaopen": self.formulaopenrace,
            "gt3open": self.gt3openrace,
            "mx5openracemessage": self.mx5openracemessage,
            "gt4openracemessage": self.gt4openracemessage,
            "formulaopenracemessage": self.formulaopenracemessage,
            "gt3openracemessage": self.gt3openracemessage
        }
        try:
            with open("openraces.json", "w") as file:
                json.dump(data, file, indent=4)
            print("Saved data:", data)  # DEBUG PRINT
        except Exception as e:
            print(f"Error saving openrace data: {e}")

    def load_open_race_data(self):
        try:
            with open("openraces.json", "r") as file:
                data = json.load(file)
            print("Loaded data:", data)  # DEBUG PRINT
            # Update the flags based on JSON data
            self.mx5openrace = data["mx5open"]
            self.gt4openrace = data["gt4open"]
            self.formulaopenrace = data["formulaopen"]
            self.gt3openrace = data["gt3open"] # DEBUG PRINT
            self.mx5openracemessage = data["mx5openracemessage"]
            self.gt4openracemessage = data["gt4openracemessage"]
            self.formulaopenracemessage = data["formulaopenracemessage"]
            self.gt3openracemessage = data["gt3openracemessage"]
        except FileNotFoundError:
            print("openraces.json not found. Using default values.")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")

        
    def load_user_data(self):
        try:
            with open('user_data.json', 'r') as file:
                data = json.load(file)
                if isinstance(data, dict):
                    return data
                else:
                    return {}
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def save_user_data(self):
        with open('user_data.json', 'w') as file:
            json.dump(self.user_data, file, indent=4)


    def get_steam_guid(self, ctx, query: str = None):
        user_id = str(ctx.author.id)

        # Scenario One: No additional string
        if query is None:
            logger.info("no steamid provided, looking up Discord ID")
            if user_id in self.user_data:
                return self.user_data[user_id]["guid"]
            else:
                return None
        # Scenario Two: Steam GUID provided
        else:
            logger.info("steamid provided, it is " + query)
            if query in self.parsed.racers.keys():
                return query
        return None
    
    @commands.hybrid_command(name="serializeall", description="serializeall")
    @commands.is_owner()
    async def serializeall(self, ctx):
        await self.serializeall_internal()

    async def serializeall_internal(self):
        serialize.serialize_all_data(self.parsed)

    @commands.hybrid_command(name="deserializeall", description="deserializeall")
    @commands.is_owner()
    async def deserializeall(self, ctx):
        await self.deserializeall_internal()
    
    async def deserializeall_internal(self):
        def log(msg):
            # timestamped print helper
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] {msg}")
        t0 = time.perf_counter()
        log("⏳  Deserialising JSON → objects …")
        self.parsed = serialize.deserialize_all_data()
        t1 = time.perf_counter()
        log(f"✅  Deserialised in {t1 - t0:0.3f}s")

        log("⏳  Calculating raw pace percentages …")
        self.parsed.calculate_raw_pace_percentages_for_all_racers()
        t2 = time.perf_counter()
        log(f"✅  Pace calc done in {t2 - t1:0.3f}s")

        log("⏳  Calculating rankings …")
        self.parsed.calculate_rankings()
        t3 = time.perf_counter()
        log(f"✅  Rankings done in {t3 - t2:0.3f}s")

        log("⏳  Loading track ratings …")
        self.parsed.loadtrackratings()
        t4 = time.perf_counter()
        log(f"✅  Track ratings loaded in {t4 - t3:0.3f}s")

        log(f"🏁  Total elapsed {t4 - t0:0.3f}s")
    
    @commands.hybrid_command(name="clearbets", description="clearbets")
    @commands.is_owner()
    async def clearbets(self, ctx):
        if self.currenteventbet:
            for bet in self.currenteventbet.bets:
                betterguid = bet.better
                for betterid in self.user_data:
                    if self.user_data[betterid]["guid"] == betterguid:
                        self.user_data[betterid]["spudcoins"] += bet.amount
            self.currenteventbet.bets = []
            self.save_current_event_bet()
            self.save_user_data()
            await ctx.send("Cleared bets for current event")

    @commands.hybrid_command(name="cleareventbet", description="cleareventbet")
    @commands.is_owner()
    async def cleareventbet(self, ctx):
        if self.currenteventbet:
            await self.clear_event_bet()
            await ctx.send("Cleared entire event for betting")

    async def clear_event_bet(self):
        if self.currenteventbet:
            self.currenteventbet = None
            self.save_current_event_bet()

    @commands.hybrid_command(name="top10collided", description="top10collided")
    async def top10collided(self, ctx, query: str = None):
        steam_guid = self.get_steam_guid(ctx, query)
        if steam_guid:
            racer = self.parsed.get_racer(steam_guid)
            if racer:
                # Sort the collision racers by the number of collisions in descending order
                top10collided = sorted(
                    racer.collisionracers.items(),
                    key=lambda item: item[1],
                    reverse=True
                )[:10]  # Get the top 10

                # Create an embed with the top 10 collided racers
                embed = discord.Embed(
                    title="Top 10 Collided Racers",
                    description="Here are the racers you collided with the most:",
                    color=discord.Color.blue()
                )
                
                for other_racer, collisions in top10collided:
                    embed.add_field(
                        name=other_racer.name,  # Racer's name
                        value=f"{collisions} collisions",  # Collision count
                        inline=False
                    )
                
                await ctx.send(embed=embed)
            else:
                await ctx.send("Could not find racer data for the provided Steam GUID.")
        else:
            await ctx.send("Invalid query. Provide a valid Steam GUID or /register your Steam GUID to your Discord name.")

    
    @commands.hybrid_command(name="changeprompt", description="changeprompt")
    async def changeprompt(self, ctx, newprompt:str):
        self.bot.prompt = newprompt
        await ctx.send("changed chat prompt")

    @commands.hybrid_command(name="register", description="register steamid")
    async def register(self, ctx, steam_guid: str):
        user_id = str(ctx.author.id)
        self.user_data[user_id] = {}
        self.user_data[user_id]["guid"] = steam_guid
        self.user_data[user_id]["spudcoins"] = 1000
        self.user_data[user_id]["activebets"] = []
        self.save_user_data()
        await ctx.send(f'Registered Steam GUID {steam_guid} for Discord user {ctx.author.name}')

    @commands.hybrid_command(name="mycoins", description="get my coin amount")
    async def mycoins(self, ctx, query: str = None):
        steam_guid = self.get_steam_guid(ctx, query)
        if steam_guid:
            user_id = str(ctx.author.id)
            coins = self.user_data[user_id]["spudcoins"]
            await ctx.send(f'You have {coins} spudcoins!')


    def calculate_win_probabilities(self,elo_ratings, k=math.log(6) / 800):
        """
        Converts ELO ratings into win probabilities for a multi-competitor race.
        :param elo_ratings: List of ELO ratings.
        :param k: Scaling factor (default makes 400-point difference ~10x strength).
        :return: List of win probabilities that sum to 1.
        """
        reducer = 4
        # Calculate each racer's weight
        weights = [math.exp(k * elo) / reducer for elo in elo_ratings]
        total_weight = sum(weights)
        # The win probability is proportional to the racer's weight
        probabilities = [w / total_weight for w in weights]
        return probabilities

    def calculate_odds(self,probabilities):
        """
        Converts probabilities into basic decimal odds (without loading/margin).
        :param probabilities: List of win probabilities.
        :return: List of odds.
        """
        odds = [1 / p if p > 0 else float('inf') for p in probabilities]

        for i in range(len(odds)):
            odds[i] = odds[i] / 2.0
            odds[i] = max(1.5, odds[i])
            odds[i] = min(20, odds[i])

        return odds
    
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
            await self.on_race_start(region="EU", event="gt4")
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
            await self.on_race_start(region="NA", event="gt4")
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
        if event in {"mx5", "gt4", "formula", "gt3"}:
            flag_attr = f"{day_name}{region.lower()}raceannounced"
        # for your Saturday “worldtour” slot you said you have a single flag:
        elif event == "worldtour":
            flag_attr = "saturdayraceannounced"
        else:
            # nothing to do for unknown event
            return
        if getattr(self, flag_attr, False):
            print("returning early as already announced for this region, even though it should only occur at that time")
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
        elif event == "gt4":
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
            leaguechannel = 1366757441768915034
            roles.append(1117574229894901871)
        isseason = await self.find_if_season_day()
        setattr(self, flag_attr, True)
        self.save_race_announcement_data()
        role_mentions = " ".join([f"<@&{role_id}>" for role_id in roles])
        announcestr += role_mentions
        announcestr += "The Race session has started! check out : <#" + str(leaguechannel) + "> for more info!"
        if isseason:
            announcestr += " This is a season race today!"
        else:
            announcestr += " This is NOT a season race today, it is an OPEN race!"
        parent_channel = self.bot.get_channel(1102816381348626462)
        if parent_channel is None:
            logger.info("No valid channel available to send the announcement.")
            return
        await self.send_announcement(parent_channel, announcestr)

        
    @tasks.loop(seconds=180.0)
    async def check_for_announcements(self):
        global ON_READY_FIRST_ANNOUNCE_CHECK
        if ON_READY_FIRST_ANNOUNCE_CHECK:
            ON_READY_FIRST_ANNOUNCE_CHECK = False
            return
        cst_timezone = pytz.timezone("US/Central")
        now_cst = self.currentnatime.astimezone(cst_timezone)  # Ensure it's CST-aware
        current_cst_day = now_cst.strftime("%A")
        if 8 <= now_cst.hour < 10:
            race_map = {
                "Monday": "mx5",
                "Tuesday": "gt4",
                "Wednesday": "wcw",
                "Thursday": "formula",
                "Friday": "gt3",
                "Saturday": "worldtour"
            }
            if current_cst_day in race_map and not getattr(self, f"{current_cst_day.lower()}announced", False):
                await self.announce_raceday(race_map[current_cst_day])
            
                # Reset all flags
                for day in race_map.keys():
                    setattr(self, f"{day.lower()}announced", day == current_cst_day)
                self.save_announcement_data()
            else:
                pass

    @commands.hybrid_command(name="testracedayannounce", description="testracedayannounce")
    @commands.is_owner()
    async def testracedayannounce(self, ctx, type: str):
        await self.announce_raceday(type)
        await ctx.send("Announced raceday for " + type)

    
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
        else:
            if type == "mx5":
                leaguechannel = 1366724512632148028
                roles.append(1117573512064946196)
                roles.append(1117573763869978775)
            elif type == "test":
                leaguechannel = 1366724512632148028
                roles.append(1320448907976638485)
            elif type == "gt3":
                leaguechannel = 1366724548719804458
                roles.append(1117573957634228327)
                roles.append(1117574027645558888)
            elif type == "gt4":
                leaguechannel = 1366782207238209548
                roles.append(1358914901153681448)
                roles.append(1358915346362531940)
            elif type == "formula":
                leaguechannel = 1366755399566491718
                roles.append(1358915606115651684)
                roles.append(1358915647634936058)
            elif type == "worldtour":
                leaguechannel = 1366757441768915034
                roles.append(1117574229894901871)
            isseason = await self.find_if_season_day()

            role_mentions = " ".join([f"<@&{role_id}>" for role_id in roles])
            announcestr += role_mentions
            announcestr += "It's raceday! check out : <#" + str(leaguechannel) + "> for more info!"
            if isseason:
                announcestr += " This is a season race today!"
            else:
                announcestr += " This is NOT a season race today, it is an OPEN race!"
        parent_channel = self.bot.get_channel(1102816381348626462)
        if parent_channel is None:
            logger.info("No valid channel available to send the announcement.")
            return
        await self.send_announcement(parent_channel, announcestr)

    async def send_announcement(self, channel: discord.TextChannel,  announcement):
        # 4) Attach the file + embed in a single send call
        await channel.send(announcement)

    async def find_if_season_day(self):
        for elem in self.parsed.championships.values():
            for event in elem.schedule:
                if event.date == date.today().isoformat():
                    return True
        return False
    
    def normalize_odds(self, odds, mean=1.5, std_dev=0.5):
        """
        Normalize odds to keep them within a reasonable range.
        The predefined mean and standard deviation squash extreme odds towards more common/expected values.
        :param odds: List of odds.
        :param mean: Mean of the normal distribution (default 1.5).
        :param std_dev: Standard deviation of the normal distribution (default 0.5).
        :return: List of normalized odds.
        """
        return [stats.norm.pdf(o, loc=mean, scale=std_dev) for o in odds]

    @commands.hybrid_command(name="testracebetting", description="testracebetting")
    @commands.is_owner()
    async def testracebetting(self, ctx, amount:int):
        guids = []
        fakenames = []
        elo_ratings = []
        for i in range(amount):
            randomint = random.randint(800, 2100)
            elo_ratings.append(randomint)
            guids.append(i)
            fakenames.append(Faker().name())
        win_probs = self.calculate_win_probabilities(elo_ratings)
        odds = self.calculate_odds(win_probs)
        odds_dict = {guid: round(odds[i], 2) for i, guid in enumerate(guids)}
        guidtonamedict = {}
        nametoguiddict = {}
        guidtoelodict = {}
        for i, guid in enumerate(guids):
            guidtonamedict[guid] = fakenames[i]
            nametoguiddict[fakenames[i]] = guid
            guidtoelodict[guid] = elo_ratings[i]
        newbetevent = EventBet("TESTRACE", "yer mums", odds_dict, "yer da", guidtonamedict, nametoguiddict, "pretend server")
        self.currenteventbet = newbetevent
        await self.announce_fake_betting_event("yer mums", "yer da", odds_dict, guidtonamedict, guidtoelodict)
        self.save_current_event_bet()

    @commands.hybrid_command(name="checksessions", description="checksessions")
    async def checksessions(self, ctx):
        await self.check_sessions()

    @commands.hybrid_command(name="checkopenservers", description="checkopenservers")
    async def checkopenservers(self, ctx):
        await self.check_open_servers()


    @commands.hybrid_command(name="topratedtracks", description="Displays the top-rated tracks")
    async def topratedtracks(self, ctx):
        # Create a dictionary to combine ratings of tracks with the same `highest_priority_name`
        combined_tracks = {}

        for track in self.parsed.contentdata.tracks:
            if len(track.ratings) >= 3:  # Only include tracks with 3 or more ratings
                if track.highest_priority_name in combined_tracks:
                    # Combine average ratings (weighted based on the number of ratings)
                    existing_avg = combined_tracks[track.highest_priority_name]["average_rating"]
                    existing_count = combined_tracks[track.highest_priority_name]["count"]
                    combined_avg = (existing_avg * existing_count + track.average_rating * len(track.ratings)) / (existing_count + len(track.ratings))
                    combined_tracks[track.highest_priority_name] = {
                        "average_rating": combined_avg,
                        "count": existing_count + len(track.ratings)
                    }
                else:
                    combined_tracks[track.highest_priority_name] = {
                        "average_rating": track.average_rating,
                        "count": len(track.ratings)
                    }

        # Convert combined_tracks to a list of tuples for sorting
        sorted_tracks = sorted(combined_tracks.items(), key=lambda item: item[1]["average_rating"], reverse=True)

        # Take the top 10 tracks, or fewer if there aren't enough
        top_tracks = sorted_tracks[:10]

        # Create the embed
        embed = discord.Embed(title="Top Rated Tracks", description="Tracks with at least 3 ratings", color=discord.Color.blue())
        
        # Add tracks to the embed
        for track, data in top_tracks:
            embed.add_field(name=track, value=f"Rating: {round(data['average_rating'], 2)}", inline=False)

        embed.set_footer(text="Track Ratings")
        
        # Send the embed in the Discord channel
        await ctx.send(embed=embed)

    @commands.hybrid_command(name="pitboxsearch", description="pitboxsearch")
    async def pitboxsearch(self, ctx, amount: int):
        bigtracks = {}
        for track in self.parsed.contentdata.tracks:
            for variant in track.variants:
                if variant.pitboxes == '':
                    continue
                if variant.pitboxes == '0':
                    continue
                if not variant.pitboxes:
                    continue
                if int(variant.pitboxes) >= amount:
                    if track.highest_priority_name in bigtracks:
                        bigtracks[track].append(variant)
                    else:
                        bigtracks[track] = [variant]

        # Count total track variants
        total_variants = sum(len(variants) for variants in bigtracks.values())
        if total_variants > 100:
            await ctx.send("Too many track variants to display. Please narrow your search.")
            return

        retstring = ""
        for track in bigtracks:
            for variant in bigtracks[track]:
                line = (
                    f"{track.highest_priority_name} variant : {variant.name} has "
                    f"{variant.pitboxes} pitboxes\n"
                )
                if len(retstring) + len(line) > 2000:
                    # Send the accumulated message if it reaches the limit
                    await ctx.send(retstring)
                    retstring = ""  # Reset for the next chunk
                retstring += line

        if retstring:  # Send any remaining message
            await ctx.send(retstring)

    async def cog_before_invoke(self, ctx):
        allowed_channels = ALLOWED_CHANNELS.get(ctx.command.name, ALLOWED_CHANNELS.get("global", []))
        if allowed_channels and str(ctx.channel.id) not in allowed_channels:
            await ctx.send("This command cannot be used in this channel.")
            raise commands.CheckFailure

    @tasks.loop(seconds=86400.0)
    async def distribute_coins(self):
        global ON_READY_FIRST_DISTRIBUTE_COIN
        if ON_READY_FIRST_DISTRIBUTE_COIN:
            ON_READY_FIRST_DISTRIBUTE_COIN = False
            return
        for user_id in self.user_data:
            self.user_data[user_id]["spudcoins"] += 1
        self.save_user_data()

    @tasks.loop(seconds=600.0)
    async def check_open_races_task(self):
        await self.check_open_servers()
    
    @tasks.loop(seconds=120.0)
    async def check_sessions_task(self):
        await self.check_sessions()

    async def announce_betting_event(self, track, car, odds_dict):
        print("announcing betting event")
        channel = self.bot.get_channel(1328800009189195828)

        # Create an embed for the announcement
        embed = discord.Embed(
            title="🏎️ **Betting Event Now Open!** 🏁",
            description=f"**Track**: {self.parsed.get_track_name(track)} 🌍\n**Car**: {self.parsed.contentdata.get_car(car).name} 🚗\n\n**Betting is now open for 5 minutes!** Place your bets wisely! 🔥",
            color=discord.Color.blue()
        )
        
        # Add driver odds to the embed
        for driver_guid, odds in odds_dict.items():
            driver_name = f"Driver {self.parsed.get_racer(driver_guid).name}"  # Replace with actual driver name retrieval logic if available
            driver_rating = self.parsed.racers[driver_guid].rating  # Retrieve the driver's ELO rating
            embed.add_field(
                name=f"🎯 {driver_name}",
                value=f"Odds: **{odds}**\nELO Rating: **{driver_rating}**",  # Add the ELO rating here
                inline=False
            )
        
        # Send the embed to the channel
        await channel.send(embed=embed)

    async def announce_fake_betting_event(self, track, car, odds_dict, guidtonamedict, guidtoelodict):
        
        channel = self.bot.get_channel(1328800009189195828)

        # Create an embed for the announcement
        embed = discord.Embed(
            title="🏎️ **Betting Event Now Open!** 🏁",
            description=f"**Track**: {track} 🌍\n**Car**: {car} 🚗\n\n**Betting is now open for 8 minutes!** Place your bets wisely! 🔥",
            color=discord.Color.blue()
        )
        
        # Add driver odds and ELO rating to the embed
        for driver_guid, odds in odds_dict.items():
            driver_name = f"Driver {guidtonamedict[driver_guid]}"  # Replace with actual driver name retrieval logic if available
            driver_rating = guidtoelodict[driver_guid]  # Retrieve the driver's ELO rating
            embed.add_field(
                name=f"🎯 {driver_name}",
                value=f"Odds: **{odds}**\nELO Rating: **{driver_rating}**",  # Add the ELO rating here
                inline=False
            )

        # Send the embed to the channel
        await channel.send(embed=embed)

    async def check_open_servers(self):
        for server in self.servers:
            data = await self.get_live_timing_data("regularcheck", server)
            if not data:
                continue
            if data["Name"] == "Practice":
                track = data["Track"]
                if server == self.mx5naopenserver:
                    if track != self.mx5openrace:
                        print("updated mx5 open race track to " + track)
                        self.mx5openrace = track
                        await self.update_open_event(server,"mx5open", track)
                elif server == self.gt3naopenserver:
                    if track != self.gt3openrace:
                        self.gt3openrace = track
                        print("updated gt3 open race track to " + track)
                        await self.update_open_event(server,"gt3open", track)
                elif server == self.gt4naopenserver:
                    if track != self.gt4openrace:
                        self.gt4openrace = track
                        print("updated gt4 open race track to " + track)
                        await self.update_open_event(server,"gt4open", track)
                elif server == self.formulanaopenserver:
                    if track != self.formulaopenrace:
                        self.formulaopenrace = track
                        print("updated formula open race track to " + track)
                        await self.update_open_event(server, "formulaopen", track)


    async def check_sessions(self):
        global ON_READY_FIRST_TIME_QUALY_SCAN
        global ALREADY_BETTING_CLOSED
        if ON_READY_FIRST_TIME_QUALY_SCAN:
            ON_READY_FIRST_TIME_QUALY_SCAN = False
            return
        channel = self.bot.get_channel(1328800009189195828)
        if self.currenteventbet:
            # Get the current time in UTC
            # Get the current time as a naive datetime
            now = datetime.now()

            # Remove timezone info from the timestamp to make it naive
            timestamp_naive = self.currenteventbet.timestamp.replace(tzinfo=None)

            # Check if 5 minutes have passed since the timestamp
            if not ALREADY_BETTING_CLOSED:
                if now - timestamp_naive >= timedelta(minutes=8):
                    self.currenteventbet.closed = True
                    logger.info(f"Betting for the current event has been closed. Event timestamp: {self.currenteventbet.timestamp}")
                    if not ALREADY_BETTING_CLOSED:
                        await channel.send("BETTING IS NOW CLOSED FOR THE EVENT!")
                        ALREADY_BETTING_CLOSED = True
                return
        else:
            for server in self.servers:
                data = await self.get_live_timing_data("regularcheck", server)
                if not data:
                    continue
                if data["Name"] == "Qualify":
                    print("qualy session")
                    print("in server " + self.servertodirectory[server])
                    if server == self.mx5nararserver:
                        continue
                    if data["ConnectedDrivers"] is None:
                        print("connecteddrivers is none in server")
                        continue
                    if len(data["ConnectedDrivers"]) < 1:
                        print("connecteddrivers is less than 1")
                        continue
                    racerguids = []
                    racer_data = {}
                    car = None
                    for driver in data["ConnectedDrivers"]:
                        racerguid = driver["CarInfo"]["DriverGUID"]
                        car = driver["CarInfo"]["CarModel"]
                        racerobj = self.parsed.get_racer(racerguid)
                        if racerobj:
                            racer_data[racerguid] = racerobj.rating
                            racerguids.append(racerguid)
                    elo_ratings = list(racer_data.values())
                    guids = list(racer_data.keys())
                    win_probs = self.calculate_win_probabilities(elo_ratings)
                    odds = self.calculate_odds(win_probs)
                    odds_dict = {guid: round(odds[i], 2) for i, guid in enumerate(guids)}
                    guidtonamedict = {}
                    nametoguiddict = {}
                    servername = self.servertodirectory[server]
                    for guid in guids:
                        guidtonamedict[guid] = self.parsed.get_racer(guid).name.lower()
                        nametoguiddict[self.parsed.get_racer(guid).name.lower()] = guid
                    newbetevent = EventBet(data["ServerName"], data["Track"], odds_dict, car, guidtonamedict, nametoguiddict, servername)
                    self.currenteventbet = newbetevent
                    logger.info("new event bet created for " + data["ServerName"] + " at " + data["Track"])
                    logger.info("odds are " + str(odds_dict))

                    print("about to announce betting event")
                    await self.announce_betting_event(data["Track"], car, odds_dict)
                    self.save_current_event_bet()

    def _to_discord_timestamp(self, dt: datetime, style: str = "f") -> str:
        """Return a Discord timestamp tag <t:…:style>"""
        return f"<t:{int(dt.timestamp())}:{style}>"
    
    def _next_slot(self,
        now_utc: datetime,
        target_weekday: int,       # 0=Mon,1=Tue,…,6=Sun
        local_tz: ZoneInfo,
        hour: int,
        minute: int = 0
    ) -> datetime:
        """
        Return the next datetime (in UTC) that falls on `target_weekday`
        at local time `hour:minute` in `local_tz`.  If today is that weekday
        but time has already passed, move 7 days ahead.
        """
        local_now = now_utc.astimezone(local_tz)
        today     = local_now.date()
        days_ahead = (target_weekday - local_now.weekday()) % 7
        candidate_date = today + timedelta(days=days_ahead)
        candidate_local = datetime.combine(candidate_date, dtime(hour, minute), tzinfo=local_tz)

        if candidate_local <= local_now:
            candidate_date += timedelta(days=7)
            candidate_local = datetime.combine(candidate_date, dtime(hour, minute), tzinfo=local_tz)

        return candidate_local.astimezone(timezone.utc)

    async def update_open_event(self, server, event_type, track_id):
        # 1. grab old message ID
        msg_attr   = f"{event_type}racemessage"
        old_msg_id = getattr(self, msg_attr, None)
        # 2. find forum/thread
        thread_id = self.servertoschedulethread[server]
        forum_id  = self.servertoparentchannel[server]
        forum = self.bot.get_channel(forum_id) \
            or await self.bot.fetch_channel(forum_id)
        thread = forum.get_thread(thread_id) \
             or await self.bot.fetch_channel(thread_id)
        if not thread:
            print(f"Could not find forum/thread for {event_type} on {server}")
            return

        # 3. build page URL base
        base_url     = server  # or map your server key to actual URL
        page_url     = f"{base_url}/track/{track_id}"

        # 4. scrape everything
        download_url = championship._scrape_download_url(page_url)
        image_paths  = championship.scrape_track_images(base_url, track_id)
        track_name   = championship._scrape_track_name(base_url, track_id)
        slot_map = {
            "mx5open":    0,  # Monday
            "gt4open":    1,  # Tuesday
            "formulaopen":3,  # Thursday
            "gt3open":    4,  # Friday
        }
        wd = slot_map[event_type]
        now_utc = datetime.now(timezone.utc)
        tz_eu   = ZoneInfo("Europe/London")
        tz_na   = timezone(timedelta(hours=-6))  # CST

        # compute next EU 19:00
        next_eu_utc = self._next_slot(now_utc, wd, tz_eu, 19, 0)
        # compute next NA 20:00
        next_na_utc = self._next_slot(now_utc, wd, tz_na, 19, 0)

        # build description with your helper
        eu_ts = self._to_discord_timestamp(next_eu_utc, "f")
        na_ts = self._to_discord_timestamp(next_na_utc, "f")

        emb = discord.Embed(
            title=f"🏁 Upcoming Open Race • {track_name}",
            description=(
                f"**EU session start**: {eu_ts}\n"
                f"**NA session start**: {na_ts}"
            ),
            colour=discord.Colour.dark_teal()
        )

        # 7. download field (or fallback text)
        if download_url:
            emb.add_field(name="Download track", value=f"[Click here]({download_url})", inline=False)
        else:
            emb.add_field(
                name="Download track",
                value="Comes with the game; no download needed.",
                inline=False
            )
        # 8. attach first image (if any)
        file = None
        if isinstance(image_paths, dict) and image_paths:
            img = next(iter(image_paths.values()))
            p   = Path(img)
            if p.is_file():
                file = discord.File(str(p), filename=p.name)
                emb.set_image(url=f"attachment://{p.name}")

        # 9. edit vs. send
        if old_msg_id:
            try:
                prev = await thread.fetch_message(int(old_msg_id))
                if file:
                    await prev.edit(embed=emb, attachments=[file])
                else:
                    await prev.edit(embed=emb)
                new_id = prev.id
            except discord.NotFound:
                fresh = await thread.send(embed=emb, file=file) if file else await thread.send(embed=emb)
                new_id = fresh.id
        else:
            print("fresh message")
            fresh  = await thread.send(embed=emb, file=file) if file else await thread.send(embed=emb)
            new_id = fresh.id

        # 10. persist
        setattr(self, msg_attr, str(new_id))
        print(self.mx5openrace)
        self.save_open_race_data()


    def save_current_event_bet(self):
        if self.currenteventbet is None:
            # Clear the file if there is no current event bet
            with open('current_bet_event.json', 'w') as file:
                file.write("")  # Writing an empty string clears the file
            logger.info("The current event bet is None. Cleared the file.")
        else:
            # Save the current event bet to the file
            with open('current_bet_event.json', 'w') as file:
                json.dump(self.currenteventbet.to_dict(), file, indent=4)
            logger.info("Saved the current event bet to the file.")


    def load_current_event_bet(self):
        try:
            # Try to open the file
            with open('current_bet_event.json', 'r') as file:
                # Check if the file is empty
                if file.read().strip() == "":
                    logger.info("The file is empty. Setting currenteventbet to None.")
                    self.currenteventbet = None
                    return
                
                # Move the cursor back to the start of the file
                file.seek(0)
                
                # Load JSON data
                data = json.load(file)

                # Deserialize into EventBet object
                self.currenteventbet = EventBet.from_dict(data)
                self.currenteventbet.timestamp = datetime.fromisoformat(data["timestamp"])  # Convert string back to datetime

        except FileNotFoundError:
            logger.info("File not found. Setting currenteventbet to None.")
            self.currenteventbet = None

        except json.JSONDecodeError:
            logger.info("File is invalid or corrupted. Setting currenteventbet to None.")
            self.currenteventbet = None # Convert string back to datetime

    @commands.hybrid_command(name="getcurrentbetevent", description="getcurrentbetevent")
    async def getcurrentbetevent(self, ctx):
        if self.currenteventbet:
            await ctx.send("Current event betting is " + self.currenteventbet.eventname + " at " + self.currenteventbet.track)
            for guid, odds in self.currenteventbet.odds.items():
                await ctx.send("Driver " + self.currenteventbet.guidtoname[guid] + " has odds of " + str(odds))
            for bet in self.currenteventbet.bets:
                await ctx.send("Bet of " + str(bet.amount) + " on " + self.currenteventbet.guidtoname[bet.racerguid] + " by " + self.parsed.get_racer(bet.better).name)
        else:
            await ctx.send("No current event bet")

    @commands.hybrid_command(name="bet", description="bet on winner")
    async def bet(self, ctx, winnername, amount):
        winnername = winnername.lower()
        amount = int(amount)
        if amount < 0:
            await ctx.send(f"{ctx.author.mention}, you cannot bet a negative amount.")
            return
        if amount > 100:
            await ctx.send(f"{ctx.author.mention}, you cannot bet more than 100.")
            return
        if self.currenteventbet is None:
            await ctx.send(f"{ctx.author.mention}, no current event betting is open.")
            return
        if self.currenteventbet.closed:
            await ctx.send(f"{ctx.author.mention}, betting is now closed for the current event.")
            return
        steam_guid = self.get_steam_guid(ctx, None)
        if steam_guid:
            for bet in self.currenteventbet.bets:
                if bet.better == str(steam_guid):
                    await ctx.send(f"{ctx.author.mention}, you have already placed a bet for this event!")
                    return
            if winnername not in self.currenteventbet.nametoguid:
                await ctx.send(f"{ctx.author.mention}, this driver is not in the event.")
                return
            winnerguid = self.currenteventbet.nametoguid[winnername]
            odds = self.currenteventbet.odds[winnerguid]
            if amount > self.user_data[str(ctx.author.id)]["spudcoins"]:
                await ctx.send(f"{ctx.author.mention}, you do not have enough spudcoins to place this bet.")
                return
            self.user_data[str(ctx.author.id)]["spudcoins"] -= amount
            newbet = Bet(str(steam_guid), amount, winnerguid, odds)
            self.currenteventbet.bets.append(newbet)
            self.save_current_event_bet()
            self.save_user_data()
            await ctx.send(f"{ctx.author.mention}, you have bet {amount} on {self.currenteventbet.guidtoname[winnerguid]} at odds of {odds}.")
        else:
            await ctx.send(f"{ctx.author.mention}, you have not registered a Steam GUID.")
            return


    @commands.hybrid_command(name="unregister", description="unregister steamid")
    async def unregister(self, ctx): 
        user_id = str(ctx.author.id) 
        if user_id in self.user_data: 
            del self.user_data[user_id] 
            self.save_user_data() 
            await ctx.send(f'Removed registration for Discord user {ctx.author.name}') 
        else: 
            await ctx.send(f'No registration found for Discord user {ctx.author.name}')

    @commands.hybrid_command(name="showlink", description="show linked steamid for user")
    async def showlink(self, ctx): 
        user_id = str(ctx.author.id) 
        if user_id in self.user_data: 
            steam_guid = self.user_data[user_id]["guid"]
            await ctx.send(f'Steam GUID linked to {ctx.author.name} is {steam_guid}') 
        else: 
            await ctx.send(f'No Steam GUID linked to Discord user {ctx.author.name}')

    
    @commands.hybrid_command(name="testoutput", description="show linked steamid for user")
    @commands.is_owner()
    async def testoutput(self, ctx, guid:str):
        self.parsed.test_output(guid)

    @commands.hybrid_command(name="mostimproved", description="show most improved racers, 3 or 6 months")
    async def mostimproved(self, ctx, time:int=3):
        improvedlist = self.parsed.most_improved(time)
        embed = discord.Embed(title="Most improved racers over " + str(time) + " months", description="Most improved racers", color=discord.Color.blue()) 
        for racer,improvement in improvedlist.items():
            embed.add_field(name=racer.name, value=str(round(improvement, 2)), inline=False)
        await ctx.send(embed=embed)


    @commands.hybrid_command(name="successfulgt3", description="show which gt3 is the most successful")
    @commands.is_owner()
    async def successfulgt3(self, ctx):
        sorted_cars = self.parsed.successfulgt3()
        embed = discord.Embed(title="Average finishing position of each GT3 car", description="GT3 car avg. position ( normalized for racer rating )", color=discord.Color.blue()) 
        for data in sorted_cars: 
            embed.add_field(name=data[0].name, value=str(round(data[1], 2)), inline=False) 
            embed.set_footer(text="GT3 Performance report") 
        await ctx.send(embed=embed)

    @commands.hybrid_command(name="mytracks", description="show each tracks average positions")
    async def mytracks(self, ctx, query: str = None):
        steam_guid = self.get_steam_guid(ctx, query)
        if steam_guid:
            tracks_report = self.parsed.get_racer_tracks_report(steam_guid)
            embed = discord.Embed(title="Track Average Positions", description=f"Average finishing positions for racer `{self.parsed.racers[steam_guid].name}`", color=discord.Color.blue()) 
            for track, avg_position in tracks_report.items(): 
                embed.add_field(name=track, value=f"{avg_position}", inline=False) 
                embed.set_footer(text="Track Performance Report") 
            await ctx.send(embed=embed)

    @commands.hybrid_command(name="worsttracks", description="show each tracks average positions")
    async def worsttracks(self, ctx, query: str = None):
        steam_guid = self.get_steam_guid(ctx, query)
        if steam_guid:
            tracks_report = self.parsed.get_racer_tracks_report(steam_guid, True)
            embed = discord.Embed(title="Worst tracks Average Positions", description=f"Average finishing positions for racer `{self.parsed.racers[steam_guid].name}`", color=discord.Color.blue()) 
            for track, avg_position in tracks_report.items(): 
                embed.add_field(name=track, value=f"{avg_position}", inline=False) 
                embed.set_footer(text="Track Performance Report") 
            await ctx.send(embed=embed)
        else:
            await ctx.send('Invalid query. Provide a valid Steam GUID or /register your steam guid to your Discord name.')

    @commands.hybrid_command(name="randomcombo", description="get my stats")
    async def randomcombo(self, ctx):
        tracklist = self.parsed.contentdata.tracks
        carlist = self.parsed.contentdata.cars
        chosentrack = random.choice(tracklist)
        chosencar = random.choice(carlist)

        await ctx.send("Car: " + chosencar.name + " ( id : " + chosencar.id  + " )"  + "\n" + 
                       "Track : " + chosentrack.highest_priority_name + " ( id: " + chosentrack.highest_priority_id + " ) ")


    @commands.hybrid_command(name="lastraces", description="get my stats for last x races")
    async def lastraces(self, ctx, num:int = 1, query: str = None):
        steam_guid = self.get_steam_guid(ctx, query)
        if num > 5:
            await ctx.send('Invalid query. please select a number smaller than 6')
            return
        if steam_guid:
            racer = self.parsed.racers[steam_guid]
            print(racer.name)
            mostrecentdict = self.parsed.get_summary_last_races(racer, num)

            # Create an embed
            embed = discord.Embed(title="Last Races Summary", description=f"Summary for the last {num} races")

            # Loop through mostrecentdict and add fields to the embed
            for result, (position, rating_change) in mostrecentdict.items():
                result_date = datetime.fromisoformat(result.date)
                race_date = result_date.strftime("%d %B %Y")
                track_name = result.track.parent_track.highest_priority_name
                embed.add_field(name="Race Summary", value=f"Race on {race_date} at {track_name}, finished in position: {position}, and gained/lost rating: {rating_change}", inline=False)
            await ctx.send(embed=embed)
        else:
            await ctx.send('Invalid query. Provide a valid Steam GUID or /register your steam guid to your Discord name.')
    
    @commands.hybrid_command(name="madracereport", description="get my stats")
    async def madracereport(self, ctx, query: str = None):
        steam_guid = self.get_steam_guid(ctx, query)
        if steam_guid:
            retstring = ""
            racer = self.parsed.racers[steam_guid]
            for incidentplotkey in racer.incidentplot:
                numincidents = racer.incidentplot[incidentplotkey]
                if numincidents > 15:
                    date = incidentplotkey
                    for result in self.parsed.raceresults:
                        if result.date == date:
                            track = result.track.parent_track.highest_priority_name
                            for entry in result.entries:
                                if entry.racer == racer:
                                    position = entry.finishingposition
                                    rating_change = entry.ratingchange
                                    car = entry.car.name
                                    retstring += "raced at track " + track + " on " + date + " in car " + car + " finished in position " + str(position) + " and gained/lost rating " + str(rating_change) + " and incidents were " + str(round(numincidents, 2)) + "\n" 
                                
            await ctx.send(retstring)
        else:
            await ctx.send('Invalid query. Provide a valid Steam GUID or /register your steam guid to your Discord name.')

    @commands.hybrid_command(name="mystats", description="get my stats")
    async def mystats(self, ctx, query: str = None):
        steam_guid = self.get_steam_guid(ctx, query)
        if steam_guid:
            racer = self.parsed.racers[steam_guid]
            user = ctx.author

            embed = discord.Embed(title="Racer Stats", description="User Stats for " + racer.name, color=discord.Color.blue())
            embed.set_thumbnail(url=user.display_avatar.url)
            embed.add_field(name="🏁 **Total races**", value=racer.get_num_races(), inline=True)
            embed.add_field(name="🥈 **ELO**", value=f"{racer.rating} (Rank: {self.parsed.get_elo_rank(racer) + 1}/{len(self.parsed.elorankings)})", inline=True)
            embed.add_field(name="🏆 **Total wins**", value=f"{racer.wins} (Rank: {self.parsed.get_wins_rank(racer) + 1}/{len(self.parsed.wins_rankings)})", inline=True)
            embed.add_field(name="🥉 **Total podiums**", value=f"{racer.podiums} (Rank: {self.parsed.get_podiums_rank(racer) + 1}/{len(self.parsed.podiums_rankings)})", inline=True)
            embed.add_field(name="⚠️ **Average incidents/race**", value=f"{racer.averageincidents} (Rank: {self.parsed.get_safety_rank(racer) + 1}/{len(self.parsed.safety_rankings)})", inline=True)
            embed.add_field(name="🛣️ **Most successful track**", value=racer.mostsuccesfultrack.name, inline=True)
            embed.add_field(name="🔄 **Total race laps**", value=racer.totallaps, inline=True)
            embed.add_field(name="💥 **Most collided with other racer**", value=racer.mosthitotherdriver.name, inline=True)
            embed.add_field(name="⏱️ **Lap Time Consistency**", value=f"{racer.laptimeconsistency:.2f}% (Rank: {self.parsed.get_laptime_consistency_rank(racer) + 1}/{len(self.parsed.laptimeconsistencyrankings)})" if racer.laptimeconsistency is not None else "No data", inline=True)
            embed.add_field(name="🏎️ **Average Pace % Compared to Top Lap Times in MX-5**", value=f"{racer.pace_percentage_mx5:.2f}% (Rank: {self.parsed.get_pace_mx5_rank(racer) + 1}/{len(self.parsed.pacerankingsmx5)})" if racer.pace_percentage_mx5 is not None else "No data", inline=True)
            embed.add_field(name="🚗 **Average Pace % Compared to Top Lap Times in GT3**", value=f"{racer.pace_percentage_gt3:.2f}% (Rank: {self.parsed.get_pace_gt3_rank(racer) + 1}/{len(self.parsed.pacerankingsgt3)})" if racer.pace_percentage_gt3 is not None else "No data", inline=True)

            await ctx.send(embed=embed)
        else:
            await ctx.send('Invalid query. Provide a valid Steam GUID or /register your steam guid to your Discord name.')

    @commands.hybrid_command(name="livetiming", description="check for latest results")
    async def livetiming(self, ctx, server, raw:str=None):
        if server == "" or server is None:
            await ctx.send("Please provide a server name from one of the following: mx5eu, mx5na, mx5napro, gt3eu, gt3na, worldtour, timetrial")
            return
        servertouse = None
        if server == "mx5eu":
            servertouse = self.mx5euserver
        elif server == "mx5na":
            servertouse = self.mx5naserver
        elif server == "mx5napro":
            servertouse = self.mx5naproserver
        elif server == "gt3eu":
            servertouse = self.gt3euserver
        elif server == "gt3na":
            servertouse = self.gt3naserver
        elif server == "worldtour":
            servertouse = self.worldtourserver
        elif server == "timetrial":
            servertouse = self.timetrialserver
        else:
            await ctx.send("Please provide a server name from one of the following: mx5eu, mx5na, mx5napro, gt3eu, gt3na, worldtour, timetrial")
            return
        serverdata = await self.get_live_timing_data(server, servertouse)
        if not serverdata:
            await ctx.send("Error fetching data from server")
            return
        await self.print_live_timings(ctx, serverdata, raw!="raw")

    async def get_live_timing_data(self, server, servertouse):
        user_agent = "https://github.com/JanuarySnow/RRR-Bot"
        headers  = {"User-Agent":user_agent}
        try:
        # This will prevent your bot from stopping everything when doing a web request - see: https://discordpy.readthedocs.io/en/stable/faq.html#how-do-i-make-a-web-request
            livetimingsurl = servertouse + "/api/live-timings/leaderboard.json"
            async with aiohttp.ClientSession() as session:
                async with session.get(
                        livetimingsurl, 
                        headers=headers) as request:
                    if request.status == 200:
                        return await request.json(content_type='application/json')
                    else:
                        logger.info("error fetching from " + server)
        except Exception as e:
            logger.info(f"Failed loading live timing data: {e}")
            return None
        return None
    

    async def print_live_timings(self, ctx, data, pretty=False):

        # Prepare a list to store driver data tuples
        driver_data_list = []

        # Extract the DisconnectedDrivers list
        drivers = data.get('DisconnectedDrivers', [])

        for driver in drivers:
            car_info = driver.get('CarInfo', {})
            driver_name = car_info.get('DriverName', 'Unknown')
            num_laps = driver.get('TotalNumLaps', 0)

            # Access the Cars dictionary to get the best lap times for each car
            cars = driver.get('Cars', {})

            for car_model, car_data in cars.items():
                best_lap_ns = car_data.get('BestLap', None)
                if best_lap_ns and best_lap_ns > 0:
                    # Convert nanoseconds to minutes:seconds:milliseconds
                    total_seconds = best_lap_ns / 1e9
                    minutes = int(total_seconds // 60)
                    seconds = int(total_seconds % 60)
                    milliseconds = int((total_seconds * 1000) % 1000)
                    best_lap_formatted = f"{minutes:02d}:{seconds:02d}.{milliseconds:03d}"
                else:
                    # Assign a very high value for sorting purposes
                    best_lap_ns = float('inf')
                    best_lap_formatted = "N/A"
                num_laps = car_data.get('NumLaps')

                car_name = car_data.get('CarName', car_model)
                # Append a tuple to the list: (best lap time in ns for sorting, formatted time, driver name, car name, number of laps)
                driver_data_list.append((best_lap_ns, best_lap_formatted, driver_name, car_name, num_laps))

        # Sort the list by best lap time (fastest to slowest)
        driver_data_list.sort(key=lambda x: x[0])

        output_lines = []
        for idx, (lap_time_ns, lap_time_str, driver_name, car_name, num_laps) in enumerate(driver_data_list, start=1):
            line = f"{driver_name},{car_name},{lap_time_str},{num_laps}"
            output_lines.append(line)

        # Discord's message character limit
        max_message_length = 2000

        # Combine lines into messages within the character limit
        messages = []
        current_message = ''
        for line in output_lines:
            if len(current_message) + len(line) + 1 > max_message_length:
                messages.append(current_message)
                current_message = line
            else:
                current_message = f"{current_message}\n{line}" if current_message else line
        if current_message:
            messages.append(current_message)

        # Prepare the embed pages
        embed_pages = []
        total_pages = len(messages)

        for page_number, message_content in enumerate(messages):
            embed = discord.Embed(
                title=f"Race Timing Results (Page {page_number + 1}/{total_pages})",
                color=0x00ff00,
                description=message_content
            )
            embed_pages.append(embed)

        current_page_number = 0
        message = await ctx.send(embed=embed_pages[current_page_number])

        # Add reactions if there's more than one page
        if total_pages > 1:
            await message.add_reaction("◀️")
            await message.add_reaction("▶️")

            def check(reaction, user):
                return (
                    user == ctx.author and
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


    @commands.hybrid_command(name="forcerefreshalldata", description="forcescanresults")
    @commands.is_owner()
    async def forcerefreshalldata(self, ctx):
        logger.info("force refreshing all data")
        await ctx.defer()
        await _run_blocking(self.parsed.refresh_all_data) 
        await ctx.send("Finished processing results")

    @commands.hybrid_command(name="testdeserialization", description="testdeserialization")
    @commands.is_owner()
    async def testdeserialization(self, ctx):
        steam_guid = self.get_steam_guid(ctx, None)
        racer = self.parsed.racers[steam_guid]
        if racer:
            print(racer.name)
            print(str(len(racer.entries)))


    async def votefortrack(self, ctx=None, track_override=None, channel: discord.TextChannel = None):
        def save_track_data_to_json(track, json_file="trackratings.json"):
            if os.path.exists(json_file):
                try:
                    with open(json_file, "r") as file:
                        if os.path.getsize(json_file) == 0:
                            data = {}
                        else:
                            data = json.load(file)
                except json.JSONDecodeError:
                    logger.info(f"{json_file} contains invalid JSON. Overwriting with new data.")
                    data = {}
            else:
                data = {}

            data[str(track.id)] = {
                "average_rating": track.average_rating,
                "ratings": track.ratings
            }
            with open(json_file, "w") as file:
                json.dump(data, file, indent=4)

        def select_track():
            used_tracks = [track for track in self.parsed.contentdata.tracks if track.timesused > 1]
            unused_tracks = [track for track in self.parsed.contentdata.tracks if track.timesused == 0]

            # Convert the user ID to a string (button voters use string keys)
            used_tracks = [track for track in used_tracks if ctx and str(ctx.author.id) not in track.ratings.keys()]
            unused_tracks = [track for track in unused_tracks if ctx and str(ctx.author.id) not in track.ratings.keys()]

            used_tracks.sort(key=lambda t: t.timesused, reverse=True)
            top_candidates = [track for track in used_tracks if len(track.ratings) < 10][:10]
            if top_candidates:
                return random.choice(top_candidates)
            if unused_tracks:
                return random.choice(unused_tracks)
            return None

        track = track_override or select_track()
        if not track:
            if ctx:
                await ctx.send("No tracks available for voting.")
            else:
                logger.info("No tracks available for voting.")
            return

        if not hasattr(track, "ratings"):
            track.ratings = {}

        embed = discord.Embed(
            title="Vote for the Track",
            description=f"ID: {track.id}\nName: {track.highest_priority_name}",
            color=discord.Color.blue()
        )
        embed.set_footer(text="Click a button below to submit your rating!")

        async def handle_vote(interaction: discord.Interaction, rating: int):
            user_id = str(interaction.user.id)  # Ensure user IDs are strings
            existing_rating = track.ratings.get(user_id)
            if existing_rating is not None:
                if existing_rating == rating:
                    await interaction.response.send_message(
                        f"You have already rated this track {rating} stars.",
                        ephemeral=True
                    )
                    return
                else:
                    track.ratings[user_id] = rating
                    track.average_rating = sum(track.ratings.values()) / len(track.ratings)
                    save_track_data_to_json(track)
                    await interaction.response.send_message(
                        f"Your vote has been changed to {rating} stars. The current average rating for {track.highest_priority_name} is now {track.average_rating:.2f}.",
                        ephemeral=True
                    )
            else:
                track.ratings[user_id] = rating
                track.average_rating = sum(track.ratings.values()) / len(track.ratings)
                save_track_data_to_json(track)
                await interaction.response.send_message(
                    f"Thanks for voting! The current average rating for {track.highest_priority_name} is now {track.average_rating:.2f}.",
                    ephemeral=True
                )

        # Create the VoteView, which binds handle_vote to your button callbacks.
        view = VoteView(embed, timeout=14400, create_callback=handle_vote)

        # Send the message using either the command context or a provided channel
        if ctx:
            message = await ctx.send(embed=embed, view=view)
        else:
            # In case ctx is None, ensure a fallback channel was provided.
            if not channel:
                # Replace this with your actual default channel ID.
                default_channel_id = 123456789012345678
                channel = self.bot.get_channel(default_channel_id)
                if channel is None:
                    logger.info("No valid channel available to send the vote embed.")
                    return
            message = await channel.send(embed=embed, view=view)

        # Link the message back to the view (e.g. for on_timeout handling)
        view.message = message

    @commands.hybrid_command(name="votetrack", description="Vote for track")
    async def voterandomtrack(self, ctx):
        await self.votefortrack(ctx, None)

    @commands.hybrid_command(name="allwinners", description="allwinners")
    async def allwinners(self, ctx):
        retstring = self.parsed.getallwinners()
        for i in range(math.ceil(len(retstring) / 4096)):
            embed = discord.Embed(title='Winners:')
            embed.description = (retstring[(4096*i):(4096*(i+1))])
            await ctx.send(embed=embed)

    def file_exists_in_results(self, filename):
        for root, _, files in os.walk("results"):
            if filename in files:
                return True
        return False

    async def check_one_server_for_results(self, server, query):
        user_agent = "https://github.com/JanuarySnow/RRR-Bot"
        headers = {"User-Agent": user_agent}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(query, headers=headers) as request:
                    if request.status == 200:
                        data = await request.json(content_type='application/json')
                        if data["results"] is None:
                            return None
                        data["results"].sort(key=lambda elem: datetime.fromisoformat(elem["date"]), reverse=True)

                        for result in data["results"]:
                            download_url = result["results_json_url"]
                            filename = os.path.basename(download_url)
                            directory = self.servertodirectory[server]
                            filepath = os.path.join("results", directory, filename)

                            # Only add to the download queue if the file doesn't already exist
                            if filename in self.blacklist:
                                continue
                            if not self.file_exists_in_results(filename):
                                self.download_queue.append((server, download_url))
 
                        return data
                    else:
                        logger.info(f"Error fetching from {server}")
                        return None
        except Exception as e:
            logger.info(f"Failed loading check_one_server_for_results: {e}")
            return None  

    async def download_files_from_queue(self):
        user_agent = "https://github.com/JanuarySnow/RRR-Bot"
        headers = {"User-Agent": user_agent}

        while self.download_queue:
            logger.info("size of download queue = " + str(len(self.download_queue)))
            server, download_url = self.download_queue.pop(0)
            download_url = server + download_url
            logger.info("downloading from " + download_url)
            directory = self.servertodirectory[server]
            filename = os.path.basename(download_url)
            filepath = os.path.join("results", directory, filename)

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(download_url, headers=headers) as request:
                        if request.status == 200:
                            data = await request.json(content_type='application/json')

                            # Ensure the directory exists
                            os.makedirs(os.path.join("results", directory), exist_ok=True)

                            # Save the JSON data to a file
                            with open(filepath, 'w') as json_file:
                                json.dump(data, json_file, indent=4)
                                logger.info(f"JSON data saved to {filepath}")
                                self.justadded.append((filepath, server))
                        else:
                            logger.info(f"Error fetching from {download_url}")
            except Exception as e:
                logger.info(f"Failed loading download_files_from_queue: {e}")
            if len(self.download_queue) >= 1:
                logger.info("waiting for next download")
                await asyncio.sleep(20)
            else:
                logger.info("no more files to download, exiting")
                break

    @commands.hybrid_command(name='euvsna', description="compare EU vs NA")
    async def euvsna(self, ctx):
        euracers = self.parsed.get_eu_racers()
        naracers = self.parsed.get_na_racers()
        logger.info("size of euracers = " + str(len(euracers)))
        logger.info("size of naracers = " + str(len(naracers)))

        average_eu_elo = sum(racer.rating for racer in euracers) / len(euracers)
        average_na_elo = sum(racer.rating for racer in naracers) / len(naracers)
        logger.info("average eu elo = " + str(average_eu_elo))
        logger.info("average na elo = " + str(average_na_elo))

        average_eu_clean = sum(racer.averageincidents for racer in euracers) / len(euracers)
        average_na_clean = sum(racer.averageincidents for racer in naracers) / len(naracers)
        logger.info("average eu clean = " + str(average_eu_clean))
        logger.info("average na clean = " + str(average_na_clean))

        average_pace_percentage_gt3_eu = sum(racer.pace_percentage_gt3 for racer in euracers if racer.pace_percentage_gt3 is not None) / len(euracers)
        average_pace_percentage_gt3_na = sum(racer.pace_percentage_gt3 for racer in naracers if racer.pace_percentage_gt3 is not None) / len(naracers)
        average_pace_percentage_mx5_eu = sum(racer.pace_percentage_mx5 for racer in euracers if racer.pace_percentage_mx5 is not None) / len(euracers)
        average_pace_percentage_mx5_na = sum(racer.pace_percentage_mx5 for racer in naracers if racer.pace_percentage_mx5 is not None) / len(naracers)
        logger.info("average pace percentage gt3 eu = " + str(average_pace_percentage_gt3_eu))
        logger.info("average pace percentage gt3 na = " + str(average_pace_percentage_gt3_na))
        logger.info("average pace percentage mx5 eu = " + str(average_pace_percentage_mx5_eu))
        logger.info("average pace percentage mx5 na = " + str(average_pace_percentage_mx5_na))


        embed = discord.Embed(
                title="EU VS NA",
                color=discord.Color.blue()
            )

        
        embed.add_field(name="🏆 Average EU racer ELO 🏆", value=(f"🔴 {round(average_eu_elo, 2)}" if average_na_elo > average_eu_elo else f"🟢 {round(average_eu_elo, 2)}") or "\u200b", inline=False)
        embed.add_field(name="🏆 Average NA racer ELO 🏆", value=(f"🟢 {round(average_na_elo, 2)}" if average_na_elo > average_eu_elo else f"🔴 {round(average_na_elo, 2)}") or "\u200b", inline=False)

        embed.add_field(name="🚗 Average EU racer Incidents per race 🚗", value=(f"🔴 {round(average_eu_clean, 2)}" if average_na_clean < average_eu_clean else f"🟢 {round(average_eu_clean, 2)}") or "\u200b", inline=False)
        embed.add_field(name="🚗 Average NA racer Incidents per race 🚗", value=(f"🟢 {round(average_na_clean, 2)}" if average_na_clean < average_eu_clean else f"🔴 {round(average_na_clean, 2)}") or "\u200b", inline=False)

        embed.add_field(name="⏱️ Average EU Racer pace percentage GT3 ⏱️", value=(f"🔴 {round(average_pace_percentage_gt3_eu, 2)}" if average_pace_percentage_gt3_na > average_pace_percentage_gt3_eu else f"🟢 {round(average_pace_percentage_gt3_eu, 2)}") or "\u200b", inline=False)
        embed.add_field(name="⏱️ Average NA Racer pace percentage GT3 ⏱️", value=(f"🟢 {round(average_pace_percentage_gt3_na, 2)}" if average_pace_percentage_gt3_na > average_pace_percentage_gt3_eu else f"🔴 {round(average_pace_percentage_gt3_na, 2)}") or "\u200b", inline=False)

        embed.add_field(name="⏱️ Average EU Racer pace percentage MX5 ⏱️", value=(f"🔴 {round(average_pace_percentage_mx5_eu, 2)}" if average_pace_percentage_mx5_na > average_pace_percentage_mx5_eu else f"🟢 {round(average_pace_percentage_mx5_eu, 2)}") or "\u200b", inline=False)
        embed.add_field(name="⏱️ Average NA Racer pace percentage MX5 ⏱️", value=(f"🟢 {round(average_pace_percentage_mx5_na, 2)}" if average_pace_percentage_mx5_na > average_pace_percentage_mx5_eu else f"🔴 {round(average_pace_percentage_mx5_na, 2)}") or "\u200b", inline=False)
        await ctx.send(embed=embed)


    @commands.hybrid_command(name='forcetimedtask', description="force timed task")
    async def forcetimedtask(self, ctx):
        await self.fetch_results_list()

    @commands.hybrid_command(name='forcetimedtaskdelayed', description="force timed task")
    async def forcetimedtaskdelayed(self, ctx):
        await self.fetch_results_list_delayed()

    async def vote_for_track_results(self, numdone):
        logger.info("starting vote for track results")
        last_x_elements = self.parsed.raceresults[-numdone:]
        last_track = None
        for result in last_x_elements:
            if last_track == result.track:
                continue
            last_track = result.track
            trackobj = result.track.parent_track
            logger.info("voting for track " + trackobj.highest_priority_name)
            default_channel = self.bot.get_channel(1085906626852163636)
            await self.votefortrack(ctx=None, track_override=trackobj, channel=default_channel)
            await asyncio.sleep(5)


    async def post_results(self, numdone):
        last_x_elements = self.parsed.raceresults[-numdone:]
        channel_id = 1328800009189195828  # Your target channel

        for result in last_x_elements:
            server = result.server
            parentchannel = self.servertoparentchannel[server]
            announcethread = self.servertoresultsthread[server]
            serverdirectory = self.servertodirectory[server]
            series_name = self.servertoseriesname[server]
            winner_name = result.entries[0].racer.name if result.entries else None
            
            iso_timestamp = result.date
            dt = datetime.strptime(iso_timestamp, "%Y-%m-%dT%H:%M:%SZ")

            formatted_date = dt.strftime("%m/%d/%Y %H:%M")  # Now includes HH:MM

            # Properly encode the result URL
            encoded_url = quote(result.url, safe='')
            simresultsurl = f"http://simresults.net/remote?result={encoded_url}"
            trackname = result.track.parent_track.highest_priority_name

            # Create embed with additional details
            embed = discord.Embed(
                title=f"🏎️ Race Results for : {trackname}",
                description=f"📅 **Date:** {formatted_date}\n🔗 [View Full Results]({simresultsurl})",
                color=discord.Color.gold()
            )
            winnerguid = None
            winnerdiscordid = None
            if winner_name:
                winnerguid = result.entries[0].racer.guid
                for key in self.user_data:
                    guid = self.user_data[key]["guid"]
                    if guid == winnerguid:
                        winnerdiscordid = key
                        break
            if winner_name and winnerdiscordid:
                winner_name = f"<@{winnerdiscordid}>"
            embed.add_field(name="🏆 Winner", value=winner_name, inline=False)
            embed.add_field(name="🏁 Series", value=series_name, inline=False)

            # Send the embed
            parent_channel = self.bot.get_channel(parentchannel)  # Get the parent forum channel
            thread = parent_channel.get_thread(announcethread) if parent_channel else None
            if thread is None:
                logger.info("No valid channel available to send the announcement.")
                return
            await thread.send(embed=embed)
            
    async def fetch_results_list_delayed(self):
        numdone = 0
        channel = self.bot.get_channel(1328800009189195828)
        async with channel.typing():
            
            serverstocheck = self.servers
            for server in serverstocheck:
                query = server + "/api/results/list.json?q=Type:\"RACE\"&sort=date&page=0"
                await self.check_one_server_for_results(server,query)
            await self.download_files_from_queue()
        if len(self.justadded) == 0:
            pass
        else:
            for elem in self.justadded:
                await channel.send("Added " + elem[0] + " from server " + elem[1])
            async with channel.typing():
                for elem in self.justadded:
                    await self.parsed.add_one_result(elem[0], os.path.basename(elem[0]), elem[1], elem[1] + "/results/download/" + os.path.basename(elem[0]))
                    numdone += 1
                    await asyncio.sleep(3)
                await self.update_standings_internal()
                await self.serializeall_internal()
            await channel.send("All results have been processed and data has been refreshed")
            await self.post_results(numdone)
            await self.create_results_images(self.justadded)
            self.justadded.clear()
        await self.process_bet_results()

    @tasks.loop(seconds=600.0)
    async def fetch_results_list(self):
        numdone = 0
        global ON_READY_FIRST_TIME_SCAN
        if ON_READY_FIRST_TIME_SCAN:
            ON_READY_FIRST_TIME_SCAN = False
            return
        channel = self.bot.get_channel(1328800009189195828)
        async with channel.typing():
            us_timezone = pytz.timezone("America/New_York")

            # Get the current time in the US timezone
            current_time_us = datetime.now(us_timezone)

            # Get the current day
            current_day = current_time_us.strftime("%A")
            serverstocheck = []
            if current_day == "Monday":
                serverstocheck = [self.mx5euopenserver, self.mx5naopenserver, self.mx5eurrrserver, self.mx5narrrserver, self.mx5nararserver]
            elif current_day == "Tuesday":
                serverstocheck = [self.gt4euopenserver, self.gt4naopenserver]
            elif current_day == "Wednesday":
                return
            elif current_day == "Thursday":
                serverstocheck = [self.formulaeuopenserver, self.formulanaopenserver, self.formulanararserver]
            elif current_day == "Friday":
                serverstocheck = [self.gt3euopenserver, self.gt3eurrrserver, self.gt3naopenserver, self.gt3narrrserver]
            elif current_day == "Saturday":
                serverstocheck = [self.worldtourserver]
            else:
                print("Error: Invalid weekday detected.")
            for server in serverstocheck:
                query = server + "/api/results/list.json?q=Type:\"RACE\"&sort=date&page=0"
                await self.check_one_server_for_results(server,query)
            await self.download_files_from_queue()
        if len(self.justadded) == 0:
            pass
        else:
            for elem in self.justadded:
                await channel.send("Added " + elem[0] + " from server " + elem[1])
            async with channel.typing():
                for elem in self.justadded:
                    await self.parsed.add_one_result(elem[0], os.path.basename(elem[0]), elem[1], elem[1] + "/results/download/" + os.path.basename(elem[0]))
                    numdone += 1
                    await asyncio.sleep(3)
                await self.update_standings_internal()
                await self.serializeall_internal()
            await channel.send("All results have been processed and data has been refreshed")
            await self.post_results(numdone)
            await self.create_results_images(self.justadded)
            await self.vote_for_track_results(numdone)
            self.justadded.clear()
        await self.process_bet_results()

    @commands.hybrid_command(name='checkforbetresults', description="checkforbetresults")
    async def checkforbetresults(self, ctx):
        await self.process_bet_results()

            

    async def process_bet_results(self):
        global ALREADY_BETTING_CLOSED
        channel = self.bot.get_channel(1328800009189195828)
        if self.currenteventbet is None:
            return
        logger.info("processing bet results")
        # Get the current time with timezone information (UTC)
        now = datetime.now(timezone.utc)

        # Define the 4-hour window
        four_hours_ago = now - timedelta(hours=4)

        # Filter results from the last 4 hours of the current day
        recent_results = []
        for result in self.parsed.raceresults:
            # Parse the ISO-8601 date string to a datetime object
            result_time = datetime.fromisoformat(result.date.replace("Z", "+00:00"))
            
            # Check if the result is within the last 4 hours and on the same day
            if result_time.date() == now.date() and four_hours_ago <= result_time <= now:
                recent_results.append(result)

        # Sort results by date (chronologically)
        recent_results.sort(key=lambda result: datetime.fromisoformat(result.date.replace("Z", "+00:00")))
        recent_results_matches = []
        current_bet_track = self.currenteventbet.track
        for result in recent_results:
            if result.track.parent_track.id == current_bet_track:
                recent_results_matches.append(result)

        # If there are no recent results, return early
        if not recent_results_matches:
            logger.info("No recent results that match the current betting round found in the last 4 hours.")
            return

        # Sort matching results chronologically
        recent_results_matches.sort(key=lambda result: datetime.fromisoformat(result.date.replace("Z", "+00:00")))

        # Get the earliest result (first in the sorted list)
        earliest_result = recent_results_matches[0]

        # Get the winner's racer GUID (first entry in self.entries)
        if earliest_result.entries:
            winner_guid = earliest_result.entries[0].racer.guid
            earliest_result_track = earliest_result.track.name
            earliest_result_car = earliest_result.entries[0].car.name
            logger.info(f"The winner's racer GUID is: {winner_guid}")
            logger.info(f"The winner's track is: {earliest_result_track}")
            logger.info(f"The winner's car is: {earliest_result_car}")

            winningbets = []
            for bet in self.currenteventbet.bets:
                chosenwinner = bet.racerguid
                if chosenwinner == winner_guid:
                    # Add the winnings to the user's total
                    betterdiscordid = None
                    for key in self.user_data:
                        guid = self.user_data[key]["guid"]
                        if guid == bet.better:
                            betterdiscordid = key
                            break
                    winnings = bet.amount * bet.odds
                    self.user_data[betterdiscordid]["spudcoins"] += winnings
                    winningbets.append({
                        "betterdiscordid": betterdiscordid,
                        "amount": bet.amount,
                        "winnings": winnings,
                        "total_coins": self.user_data[betterdiscordid]["spudcoins"],
                        "racerguid": bet.racerguid
                    })

            # If there are winning bets, create an embed
            if winningbets:
                if channel is None:
                    logger.info("Channel 'bot-testing' not found.")
                    return

                # Create an embed message
                # Create the embed
                embed = discord.Embed(
                    title="🏁 Race Results 🏎️",
                    description=f"**Track:** {earliest_result_track}\n**Car:** {earliest_result_car}\n**Winner:** {earliest_result.entries[0].racer.name}",
                    color=discord.Color.green()
                )

                # Loop through the top 5 positions (or fewer if there are less than 5 entries)
                for position, entry in enumerate(earliest_result.entries[:5], start=1):
                    embed.add_field(
                        name=f"🏅 Position {position}:",
                        value=f"Racer: `{entry.racer.name}`",
                        inline=False
                    )

                await channel.send(embed=embed)
                # Add each winning bet to the embed
                for bet in winningbets:
                    embed = discord.Embed(
                        title="🏆 Betting Results 🎲",
                        color=discord.Color.gold()
                    )
                    embed.add_field(
                        name="🎉 Congratulations! You won!",
                        value=(
                            f"Bet Amount: `{bet['amount']}` Spudcoins\n"
                            f"Racer Bet On: `{self.parsed.get_racer(bet['racerguid']).name}`\n"
                            f"Winnings: `{bet['winnings']}` Spudcoins\n"
                            f"New Total: `{bet['total_coins']}` Spudcoins"
                        ),
                        inline=False
                    )

                    mention = f"<@{bet['betterdiscordid']}>"
                    await channel.send(
                        f"{mention} Here are your winnings!",
                        embed=embed
                    )
            else:
                logger.info("No winning bets found.")
                await channel.send("No winning bets found for recent processed results.")
        else:
            logger.info("The earliest result has no entries.")
        await self.clear_event_bet()
        ALREADY_BETTING_CLOSED = False
        self.save_user_data()

    @commands.hybrid_command(name='carlookup', description="get car info")
    async def carlookup(self, ctx, *, input_string: str, guid:str = None):
        
        # Try to match the input string as a track ID
        matched_car = None
        for elem in self.parsed.contentdata.cars:
            base_id = elem.id
            if input_string == base_id:
                matched_car = elem
                break
            carname = elem.name
            if input_string == carname:
                matched_car = elem

        # If no direct matches are found, fall back to fuzzy matching
        if not matched_car:
            matches = self.parsed.find_and_list_cars(input_string)
            if not matches:
                await ctx.send('No matching cars found.')
                return

            # Check if there's only one match with 100% confidence
            if len(matches) == 1:
                match = matches[0]
                matched_car = self.parsed.contentdata.get_car(match["id"])
                if matched_car:
                    embed = self.create_car_embed(matched_car, guid)
                    await ctx.send(embed=embed)
                else:
                    await ctx.send('No car found')
                    return

            # Create buttons for the top 3 matches
            view = View()

            async def button_callback(interaction: discord.Interaction, match):
                matched_car = self.parsed.contentdata.get_car(match["id"])

                if matched_car:
                        embed = self.create_car_embed(matched_car, guid)
                        await interaction.response.send_message(embed=embed)
                else:
                    await interaction.response.send_message('No matching car found.')

            # Add buttons to the view with their respective callbacks
            for match in matches[:3]:
                button = Button(label=match["input_match"], style=discord.ButtonStyle.primary)
                button.callback = lambda interaction, m=match: button_callback(interaction, m)
                view.add_item(button)

            await ctx.send('Select what car you want to see:', view=view)
        else:
            embed = self.create_car_embed(matched_car, guid)
            await ctx.send(embed=embed)

    @commands.hybrid_command(name='myrecords', description="See if I hold any track records")
    async def myrecords(self, ctx, guid: str = None):
        steam_guid = self.get_steam_guid(ctx, guid)
        if steam_guid:
            racer = self.parsed.racers[steam_guid]
            fastests = self.parsed.get_fastest_laps_for_racer(racer)
            if len(fastests) > 0:
                embed = discord.Embed(
                    title=f"{racer.name}'s Track Records",
                    description="Here are the fastest laps recorded at each track:",
                    color=discord.Color.purple()
                )

                for track_variant, records in fastests.items():
                    record_texts = []
                    for car_class, record in records.items():
                        if not record["time"] or not record["car"]:
                            continue

                        # Convert laptime from milliseconds to a readable format
                        total_seconds = float(record["time"]) / 1000.0
                        minutes = int(total_seconds // 60)
                        seconds = total_seconds % 60

                        record_text = f"**{car_class}**: ⏱️ {minutes}:{seconds:06.3f} - Set with {record['car'].name}"
                        record_texts.append(record_text)

                    if record_texts:
                        embed.add_field(
                            name=f"{track_variant.name}",
                            value="\n".join(record_texts),
                            inline=False
                        )

                embed.set_footer(text="Track Records Report")
                await ctx.send(embed=embed)
            else:
                await ctx.send(f'No track records found for racer `{racer.name}`.')
        else:
            await ctx.send('Invalid query. Provide a valid Steam GUID or /register your steam GUID to your Discord name.')

    @commands.hybrid_command(name="makeresultsimage",description="makeresultsimage")
    async def cmd_results(
        self,
        ctx: commands.Context) -> None:
        """Generate an image from an ACC JSON result file.

        **Required**
        • `json_file`  – The race result JSON attachment.

        **Optional**
        • `template`   – PNG/JPG background to draw on (defaults to first PNG in /templates).
        • `preset`     – Name of a saved JSON preset in /presets (without extension).
        • `custom_text`– Extra headline text placed per preset coords.
        • `track_text` – Override the auto‑detected track name.
        """
        await ctx.typing()

        # ------------------------------------------------------------------ #
        #                           save attachments                         #
        # ------------------------------------------------------------------ #
        json_path = Path("/share/RRR-Bot/RRR-Bot/results/formulanarar/2025_5_16_2_56_RACE.json")
        

        # ------------------------------------------------------------------ #
        #                      merge JSON + template → image                 #
        # ------------------------------------------------------------------ #
        try:
            output_path = await asyncio.to_thread(
                self._generate_image,json_path)
        except Exception as e:
            return await ctx.send(f"❌ Failed to generate image: `{e}`")

        # ------------------------------------------------------------------ #
        await ctx.send(file=discord.File(fp=output_path, filename=output_path.name))


    @commands.hybrid_command(name='top10mx5pace', description="get MX5 pace top 10 rankings")
    async def top10mx5pace(self, ctx):
        rankings = self.parsed.pacerankingsmx5[:10]  # Get top 10 rankings
        embed = discord.Embed(
            title="Top 10 MX5 Pace Rankings",
            color=discord.Color.blue()
        )

        for index, racer in enumerate(rankings):
            embed.add_field(
                name=f"{index + 1}. {racer.name}",
                value=f"**Pace Percentage**: {racer.pace_percentage_mx5}%",
                inline=False
            )

        await ctx.send(embed=embed)

    def _load_preset(self, name: str | None) -> Dict[str, Any]:
        if not name:
            return {}
        preset_file = self.dir_presets / f"{name}.json"
        if not preset_file.exists():
            raise FileNotFoundError(f"Preset '{name}' not found.")
        with open(preset_file, "r", encoding="utf‑8") as fp:
            return json.load(fp)

    def _generate_image(
        self,json_path: Path):
        # ------------------------ settings merge ------------------------- #
        settings = self.default_settings.copy()
        settings.update(self._load_preset("VERTICAL"))

        # ------------------ read race JSON + detect track ---------------- #
        with open(json_path, "r", encoding="utf‑8") as f:
            data = json.load(f)

        event_name = data.get("EventName", "Unknown Track")

        # ----------------------- sort driver info ----------------------- #
        results = data.get("Result", [])
        sorted_results = sorted(
            results,
            key=lambda x: (
                x.get("Disqualified", False),
                -x.get("NumLaps", 0),
                float(x.get("TotalTime", float("inf"))),
            ),
        )
        if len(sorted_results) == 0:
            print("empty result in generate results image")
            return
        # GUID → Nation map (so we can draw correct flag)
        guid_to_nation: Dict[str, str] = {}
        for car in data.get("Cars", []):
            guid = car.get("Driver", {}).get("Guid", "")
            nation = car.get("Driver", {}).get("Nation", "")
            if guid:
                guid_to_nation[guid] = nation

        driver_data = [
            {
                "DriverName": r["DriverName"],
                "GridPosition": r.get("GridPosition", 0),
                "CarModel": r["CarModel"],
                "Nation": guid_to_nation.get(r.get("DriverGuid", ""), ""),
            }
            for r in sorted_results
        ]
        templatedict = {
            "gt3" : "/share/RRR-Bot/RRR-Bot/templates/GT3_TEMP.png",
            "mx5" : "/share/RRR-Bot/RRR-Bot/templates/MX5_TEMP.png",
            "gt4" : "/share/RRR-Bot/RRR-Bot/templates/GT3_TEMP.png",
            "formula" : "/share/RRR-Bot/RRR-Bot/templates/F3_TEMP.png",
            "other" : "/share/RRR-Bot/RRR-Bot/templates/NORMAL_TEMP.png",
        }
        for item in driver_data:
            carmodel = item["CarModel"]
            print("carmodel = " + carmodel)
            if carmodel == "amr_v8_vantage_gt3_sprint_acc":
                template_path = templatedict["gt3"]
            elif carmodel in gt3ids:
                template_path = templatedict["gt3"]
            elif carmodel == "ks_mazda_mx5_cup":
                template_path = templatedict["mx5"]
            elif carmodel in gt4ids:
                template_path = templatedict["gt3"]
            elif carmodel in formulaids:
                template_path = templatedict["formula"]
            else:
                template_path = templatedict["other"]

        # ------------------------- load template ------------------------ #
        image = Image.open(template_path).convert("RGBA")
        draw = ImageDraw.Draw(image)


        main_font_path = "/share/RRR-Bot/RRR-Bot/fonts/BaiJamjuree-Bold.ttf"
        track_font_path="/share/RRR-Bot/RRR-Bot/fonts/Microgramma D Extended Bold.ttf"
        time_font_path="/share/RRR-Bot/RRR-Bot/fonts/BaiJamjuree-Regular.ttf"
        font_arrow_path= "/share/RRR-Bot/RRR-Bot/fonts/BaiJamjuree-Bold.ttf"

        try:
            font_main = ImageFont.truetype(main_font_path, size=settings["font_size"])
            logger.info(f"Successfully loaded {main_font_path} with size={settings["font_size"]}")
        except Exception as e:
            logger.info(f"Failed loading {main_font_path}: {e}")
            font_main = ImageFont.load_default()

        try:
            font_track = ImageFont.truetype(track_font_path, size=settings["font_size"] * 1.4)
        except:
            font_track = ImageFont.load_default()

        try:
            font_time = ImageFont.truetype(time_font_path, size=20)
        except:
            font_time = ImageFont.load_default()

        try:
            font_arrow = ImageFont.truetype(font_arrow_path, size=20)
        except:
            font_arrow = ImageFont.load_default()
        # --------------------------- draw track ------------------------- #
        draw.text(
            (30, 70),
            event_name,
            font=font_track,
            fill="white",
        )

        # -------------------------- driver list ------------------------- #
        y = settings["y_start"]
        for index, item in enumerate(driver_data):
            # names
            draw.text((settings["x_name"], y), item["DriverName"], font=font_main, fill="white")

            # timing column
            total_time_ms = sorted_results[index].get("TotalTime", 0)
            num_laps = sorted_results[index].get("NumLaps", 0)
            max_laps = sorted_results[0].get("NumLaps", 0)
            dnf = num_laps < max_laps - 2

            if dnf:
                time_text = "DNF"
            elif index == 0:
                leader_time = total_time_ms
                if total_time_ms >= 60_000:
                    time_text = datetime.utcfromtimestamp(total_time_ms / 1_000).strftime("%M:%S.%f")[:-3]
                else:
                    time_text = f"{total_time_ms / 1_000:.3f}"
            else:
                if num_laps < max_laps:
                    time_text = "+1 Lap"
                else:
                    gap_seconds = (total_time_ms - leader_time) / 1_000
                    time_text = f"+ {gap_seconds:.3f}"

            draw.text((775, y + 3), time_text, font=font_time, fill="white")

            # position change arrow/number (skipped for DNF)
            if not dnf:
                delta = (index + 1) - item.get("GridPosition", 0)
                if delta > 0:
                    arrow, colour = f"▼ {abs(delta)}", "red"
                elif delta < 0:
                    arrow, colour = f"▲ {abs(delta)}", "lime"
                else:
                    arrow, colour = "-", "white"
                draw.text((settings["x_name"] + 500, y + 5), arrow[0], font=font_arrow, fill=colour)
                draw.text((settings["x_name"] + 515, y + 1), arrow[1:], font=font_main, fill=colour)

            # national flag (fallback to TS.png if not found)
            nation_code = (item.get("Nation", "") or "TS").upper()
            flag_path = next(
                (
                    f
                    for f in self.dir_flags.glob("*.png")
                    if f.stem.upper() == nation_code
                ),
                self.dir_flags / "TS.png",
            )
            flag = Image.open(flag_path).convert("RGBA")
            flag = ImageOps.contain(flag, (40, 40))
            flag_y = y + (font_main.size - flag.height) // 2 + 8
            image.paste(flag, (90, flag_y), flag)

            # manufacturer logo
            model = item["CarModel"].lower()
            logo_files = [f.stem.lower() for f in self.dir_logos.glob("*.png")]
            guess_brand = next(
                (
                    match
                    for part in model.split("_")
                    for match in difflib.get_close_matches(part, logo_files, n=1, cutoff=0.7)
                ),
                model.split("_")[0],
            )
            try:
                logo_path = next(
                    p for p in self.dir_logos.glob("*.png") if p.stem.lower() == guess_brand.lower()
                )
                logo = Image.open(logo_path).convert("RGBA")
                logo = ImageOps.contain(logo, (settings["logo_size"], settings["logo_size"]))
                image.paste(
                    logo,
                    (settings["logo_fixed_x"] + settings["logo_offset_x"], y + settings["logo_offset_y"]),
                    logo,
                )
            except StopIteration:
                draw.text(
                    (
                        settings["logo_fixed_x"] + settings["logo_offset_x"],
                        y + settings["logo_offset_y"],
                    ),
                    guess_brand.upper(),
                    font=font_main,
                    fill="white",
                )

            y += settings["line_spacing"]


        # --------------------------- date stamp ------------------------- #
        timestamp_str = data.get("Date") or next(
            (data.get(k) for k in data if "date" in k.lower() or "time" in k.lower()),
            None,
        )
        try:
            dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            date_text = dt.strftime("%B %d, %Y")
        except Exception:
            date_text = timestamp_str or "Unknown Date"
        draw.text((60, image.height - 36), date_text, font=font_arrow, fill="white")

        #---------------------------- event name ------------------------- #
        folder = Path(json_path).parent.name  
        series_name = self.directory_to_series.get(folder, "Unknown Series")
        draw.text((image.width - 330, image.height - 36), series_name, font=font_arrow, fill="white")
        # ----------------------------- save ----------------------------- #
        filename_safe = f"{event_name}_{date_text}".replace(" ", "_").replace(",", "").lower()
        out_path = self.dir_output / f"{filename_safe}.png"
        image.save(out_path)
        self.send_results(str(out_path), folder)
        return out_path
    

    async def send_results(self, final_image_path: str, folder: Path) -> None:
        """
        Sends the results image as an attachment with an embed message.
        """
        # 1) Create the embed
        embed = discord.Embed(
            title="Here are the results of the recent race!",
            description="Check out the attached image above!.",
            color=0x00FF00  # (Optional) pick some color
        )
        announcechannel = self.bot.get_channel(1102816381348626462)
        # 2) You can add fields, footers, or any other embed info if desired:
        # embed.add_field(name="Some Field", value="Value")
        # embed.set_footer(text="Powered by MyRaceBot")

        # 3) Create the File object for your image
        race_image = discord.File(final_image_path, filename="race_results.png")

        # 4) Attach the file + embed in a single send call
        await announcechannel.send(
            embed=embed,
            file=race_image
        )
        for key,value in self.servertodirectory.items():
            if value == folder:
                server = key
        if not server:
            return

        #now for the results thread too
        parentchannel = self.servertoparentchannel[server]
        announcethread = self.servertoresultsthread[server]
        thread = parentchannel.get_thread(announcethread) if parentchannel else None
        if thread is None:
            logger.info("No valid channel available to send the announcement.")
            return
        await thread.send(
            embed=embed,
            file=race_image
        )

        # 5) Optionally, you can delete the image file after sending
        if os.path.exists(final_image_path):
            os.remove(final_image_path)


    async def create_results_images(self, files):   
        for file in files:
            try:
                output_path = await asyncio.to_thread(
                    self._generate_image,
                    json_path=file,
                )
            except Exception as e:
                print(f"❌ Failed to generate image: `{e}`")
                return


    @commands.hybrid_command(name='richest', description="Top 10 Racers with Most Spudcoins")
    async def richest(self, ctx):
        # Extract spudcoin data from self.user_data and sort by spudcoins
        sorted_users = sorted(
            self.user_data.items(),
            key=lambda item: item[1].get('spudcoins', 0),
            reverse=True
        )[:10]  # Get top 10 users
        
        embed = discord.Embed(
            title="Top 10 Racers with Most Spudcoins",
            color=discord.Color.green()
        )

        for index, (discord_id, user_data) in enumerate(sorted_users):
            guid = user_data.get('guid')  # Retrieve GUID from user data
            spudcoins = user_data.get('spudcoins', 0)

            # Use the GUID to access the racer and get their name
            racer = self.parsed.racers.get(guid)
            racer_name = racer.name if racer else "Unknown Racer"

            embed.add_field(
                name=f"{index + 1}. {racer_name}",
                value=f"**Spudcoins**: {spudcoins:,}",  # Format spudcoins with thousand separators
                inline=False
            )

        await ctx.send(embed=embed)



    @commands.hybrid_command(name='top10gt3pace', description="get GT3 pace top 10 rankings")
    async def top10gt3pace(self, ctx):
        rankings = self.parsed.pacerankingsgt3[:10]  # Get top 10 rankings
        embed = discord.Embed(
            title="Top 10 GT3 Pace Rankings",
            color=discord.Color.green()
        )

        for index, racer in enumerate(rankings):
            embed.add_field(
                name=f"{index + 1}. {racer.name}",
                value=f"**Pace Percentage**: {racer.pace_percentage_gt3}%",
                inline=False
            )

        await ctx.send(embed=embed)

    @commands.hybrid_command(name='top10trackusage', description="get track info")
    async def top10trackusage(self, ctx):
        usagedict = {}
        for result in self.parsed.raceresults:
            trackused = result.track
            if trackused in usagedict:
                usagedict[trackused] += 1
            else:
                usagedict[trackused] = 1
        sortedusagedict = dict(sorted(usagedict.items(), key=lambda item: item[1], reverse=True))
        retstring = ""
        index = 0
        for elem in sortedusagedict:
            trackname = self.parsed.get_track_name(self.parsed.get_parent_track_from_variant(elem.id).id)
            if trackname is None:
                trackname = elem.id
            retstring += trackname + " : " + str(sortedusagedict[elem]) + " times" + "\n"
            index += 1
            if index == 10:
                break
        await ctx.send(retstring)

    @commands.hybrid_command(name='tracklookup', description="get track info")
    async def tracklookup(self, ctx, *, input_string: str, guid:str = None):
        
        # Try to match the input string as a track ID
        matched_track = None
        for elem in self.parsed.contentdata.tracks:
            for variant in elem.variants:
                base_id = elem.id

                if input_string == base_id:
                    matched_track = variant.parent_track
                    break
                if input_string + ";" + input_string == base_id:
                    matched_track = variant.parent_track
                    break
                variantname = variant.name
                if input_string == variantname:
                    matched_track = variant.parent_track

        # If no direct matches are found, fall back to fuzzy matching
        if not matched_track:
            matches = self.parsed.find_and_list_variants(input_string)
            if not matches:
                await ctx.send('No matching track variants found.')
                return

            # Check if there's only one match with 100% confidence
            if len(matches) == 1:
                match = matches[0]
                matched_track = self.parsed.contentdata.get_base_track(match["id"])
                if matched_track:
                    highest_priority_variant = None
                    for variant in matched_track.variants:
                        if variant.name == matched_track.highest_priority_name or variant.id == matched_track.highest_priority_id:
                            highest_priority_variant = variant
                            break  
                    if highest_priority_variant:
                        embed = self.create_variant_embed(highest_priority_variant, guid)
                        await ctx.send(embed=embed)
                        await self.votefortrack(ctx, matched_track)
                    else:
                        await ctx.send('No highest priority variant found for the matching track.')
                else:
                    await ctx.send('No matching track variants found.')
                return

            # Create buttons for the top 3 matches
            view = View()

            async def button_callback(interaction: discord.Interaction, match):
                matched_track = self.parsed.contentdata.get_base_track(match["id"])

                if matched_track:
                    highest_priority_variant = matched_track.highest_priority_name
                    for variant in matched_track.variants:
                        if variant.name == matched_track.highest_priority_name or variant.variant_id == matched_track.highest_priority_id:
                            highest_priority_variant = variant
                            break

                    if highest_priority_variant:
                        embed = self.create_variant_embed(highest_priority_variant, guid)
                        await interaction.response.send_message(embed=embed)
                        await self.votefortrack(ctx, matched_track)
                    else:
                        await interaction.response.send_message('No highest priority variant found for the matching track.')
                else:
                    await interaction.response.send_message('No matching track variants found.')

            # Add buttons to the view with their respective callbacks
            for match in matches[:3]:
                button = Button(label=match["input_match"], style=discord.ButtonStyle.primary)
                button.callback = lambda interaction, m=match: button_callback(interaction, m)
                view.add_item(button)

            await ctx.send('Select what track you want to see:', view=view)
        else:
            # Select the highest priority variant if direct match is found
            highest_priority_variant = None
            for variant in matched_track.variants:
                if variant.name == matched_track.highest_priority_name or variant.id == matched_track.highest_priority_id:
                    highest_priority_variant = variant
                    break

            if highest_priority_variant:
                embed = self.create_variant_embed(highest_priority_variant, guid)
                await ctx.send(embed=embed)
                await self.votefortrack(ctx, matched_track)
            else:
                await ctx.send('No highest priority variant found for the matching track.')

    def create_car_embed(self, car, guid:str=None):
        embed = discord.Embed(
            title=f"Car: {car.name}",
            description=car.description or "N/A",
            color=discord.Color.green()
        )
        embed.add_field(name="🏷️ Tags", value=", ".join(car.tags) if car.tags else "N/A", inline=True)
        embed.add_field(name="🚗 Brand", value=car.brand or "N/A", inline=True)
        embed.add_field(name="🏎️ Class", value=car.carclass or "N/A", inline=True)
        embed.add_field(name="🇺🇳 Country", value=car.country or "N/A", inline=True)
        #embed.add_field(name="📏 Torque Curve", value=car.torquecurve or "N/A", inline=True)
        #embed.add_field(name="📏 Power Curve", value=car.powercurve or "N/A", inline=True)
        #embed.add_field(name="⚙️ Specs", value=car.specs or "N/A", inline=True)
        embed.add_field(name="🖋️ Author", value=car.author or "N/A", inline=True)
        embed.add_field(name="🔢 Version", value=car.version or "N/A", inline=True)
        embed.add_field(name="🌐 URL", value=car.url or "N/A", inline=True)
        embed.add_field(name="📅 Year", value=str(car.year) if car.year else "N/A", inline=True)

        embed.set_footer(text="Car Information Report")
        return embed

    


    def create_variant_embed(self, variant, guid:str=None):
        embed = discord.Embed(
            title=f"Track Variant: {variant.name}",
            description=variant.description,
            color=discord.Color.blue()
        )
        numused = self.parsed.get_times_track_used(variant)
        embed.add_field(name="🏷️ Tags", value=", ".join(variant.tags) if variant.tags else "N/A", inline=True)
        embed.add_field(name="🌍 GeoTags", value=", ".join(variant.geotags) if variant.geotags else "N/A", inline=True)
        embed.add_field(name="🇺🇳 Country", value=variant.country or "N/A", inline=True)
        embed.add_field(name="🏙️ City", value=variant.city or "N/A", inline=True)
        embed.add_field(name="📏 Length", value=variant.length or "N/A", inline=True)
        embed.add_field(name="📏 Width", value=variant.width or "N/A", inline=True)
        embed.add_field(name="🚗 Pitboxes", value=variant.pitboxes or "N/A", inline=True)
        embed.add_field(name="🏃 Run", value=variant.run or "N/A", inline=True)
        embed.add_field(name="🖋️ Author", value=variant.author or "N/A", inline=True)
        embed.add_field(name="🔢 Version", value=variant.version or "N/A", inline=True)
        embed.add_field(name="🌐 URL", value=variant.url or "N/A", inline=True)
        embed.add_field(name="📅 Year", value=str(variant.year) if variant.year else "N/A", inline=True)
        embed.add_field(name="🔢 Times used", value=str(numused), inline=True)
        embed.add_field(name="🔢 Track Rating", value=str(round(variant.parent_track.average_rating, 2)), inline=True)
        for elem in variant.parent_track.variants:
            fastest_mx5_lap = elem.get_fastest_lap_in_mx5(guid)
            fastest_gt3_lap = elem.get_fastest_lap_in_gt3(guid)

            if fastest_mx5_lap and fastest_mx5_lap.racerguid:
                total_seconds = float(fastest_mx5_lap.time / 1000.0)
                minutes = int(total_seconds // 60)
                seconds = total_seconds % 60
                if guid:
                    embed.add_field(
                    name=f"{self.parsed.racers[fastest_mx5_lap.racerguid].name}'s fastest ever MX5 lap at: {elem.name}",
                    value=f"⏱️ {minutes}:{seconds:06.3f}",
                    inline=False
                    )
                else:
                    embed.add_field(
                    name=f"fastest ever MX5 lap at: {elem.name} by : {self.parsed.racers[fastest_mx5_lap.racerguid].name}",
                    value=f"⏱️ {minutes}:{seconds:06.3f}",
                    inline=False
                    )
            if fastest_gt3_lap and fastest_gt3_lap.racerguid:
                total_seconds = float(fastest_gt3_lap.time / 1000.0)
                minutes = int(total_seconds // 60)
                seconds = total_seconds % 60
                if guid:
                    embed.add_field(
                        name=f"{self.parsed.racers[fastest_gt3_lap.racerguid].name}'s fastest ever GT3 lap at: {elem.name}",
                        value=f"⏱️ {minutes}:{seconds:06.3f}",
                        inline=False
                    )
                else:
                    embed.add_field(
                        name=f"fastest ever GT3 lap at: {elem.name} by {self.parsed.racers[fastest_gt3_lap.racerguid].name}",
                        value=f"⏱️ {minutes}:{seconds:06.3f}",
                        inline=False
                    )
            avg_mx5 = elem.get_average_lap_in_mx5(guid)
            if avg_mx5 is not None:
                total_s = avg_mx5 / 1000.0
                m = int(total_s // 60)
                s = total_s % 60
                if guid:
                    embed.add_field(
                        name=f"{self.parsed.racers[guid].name}'s average MX-5 lap at {elem.name}",
                        value=f"⏱️ {m}:{s:06.3f}",
                        inline=False
                    )
                else:
                    embed.add_field(
                        name=f"overall average MX-5 lap at {elem.name}",
                        value=f"⏱️ {m}:{s:06.3f}",
                        inline=False
                    )

            avg_gt3 = elem.get_average_lap_in_gt3(guid)
            if avg_gt3 is not None:
                total_s = avg_gt3 / 1000.0
                m = int(total_s // 60)
                s = total_s % 60
                if guid:
                    embed.add_field(
                        name=f"{self.parsed.racers[guid].name}'s average GT3 lap at {elem.name}",
                        value=f"⏱️ {m}:{s:06.3f}",
                        inline=False
                    )
                else:
                    embed.add_field(
                        name=f"overall average GT3 lap at {elem.name}",
                        value=f"⏱️ {m}:{s:06.3f}",
                        inline=False
                    )

        return embed


    
    @commands.hybrid_command(name="mytrackrecord", description="get users fastest lap at track")
    async def mytrackrecord(self, ctx: commands.Context, input_string: str, guid: str = None) -> None:
        steam_guid = self.get_steam_guid(ctx, guid)
        if steam_guid:
            racer = self.parsed.racers[steam_guid]
            await self.tracklookup(ctx, input_string=input_string, guid=steam_guid)
        else:
            await ctx.send('Invalid query. Provide a valid Steam GUID or /register your steam guid to your Discord name.')

    @commands.hybrid_command(name="rrreloprogression", description="rrreloprogression")
    async def rrreloprogression(self, ctx: Context) -> None:
        self.parsed.create_average_elo_progression_chart()
        file = discord.File("average_elo_progression_chart.png", filename="average_elo_progression_chart.png") 
        embed = discord.Embed( title="RRR ELO Progression", description=f"How RRR has improved over the years!", color=discord.Color.green() ) 
        embed.set_image(url="attachment://average_elo_progression_chart.png") 
        await ctx.send(embed=embed, file=file)

    
    @commands.hybrid_command(name="mypaceprogression", description="show improvement over time")
    async def mypaceprogression(self, ctx: Context, guid:str=None) -> None:
        steam_guid = self.get_steam_guid(ctx, guid)
        if steam_guid:
            racer = self.parsed.racers[steam_guid]
            if not racer.paceplot:
                await ctx.send("Racer hasnt done enough races yet")
                return
            self.parsed.create_progression_chart(racer, racer.paceplot)
            file = discord.File("progression_chart.png", filename="progression_chart.png") 
            embed = discord.Embed( title="Racer Pace Progression", description=f"Pace Progression Over Time for {racer.name}", color=discord.Color.green() ) 
            embed.set_image(url="attachment://progression_chart.png") 
            await ctx.send(embed=embed, file=file)
        else:
            await ctx.send('Invalid query. Provide a valid Steam GUID or /register your steam guid to your Discord name.')

    
    @commands.hybrid_command(name="mysafetyprogression", description="show improvement over time")
    async def mysafetyprogression(self, ctx: Context, guid:str=None) -> None:
        steam_guid = self.get_steam_guid(ctx, guid)
        if steam_guid:
            racer = self.parsed.racers[steam_guid]
            if not racer.paceplot:
                await ctx.send("Racer hasnt done enough races yet")
                return
            self.parsed.create_progression_chart(racer, racer.incidentplot)
            file = discord.File("progression_chart.png", filename="progression_chart.png") 
            embed = discord.Embed( title="Racer Safety Progression", description=f"Safety Progression Over Time for {racer.name}", color=discord.Color.green() ) 
            embed.set_image(url="attachment://progression_chart.png") 
            await ctx.send(embed=embed, file=file)
        else:
            await ctx.send('Invalid query. Provide a valid Steam GUID or /register your steam guid to your Discord name.')

    @commands.hybrid_command(name="myprogression", description="show improvement over time")
    async def myprogression(self, ctx: Context, guid:str=None) -> None:
        steam_guid = self.get_steam_guid(ctx, guid)
        if steam_guid:
            racer = self.parsed.racers[steam_guid]
            if not racer.progression_plot:
                await ctx.send("Racer hasnt done enough races yet")
                return
            self.parsed.create_progression_chart(racer, racer.progression_plot)
            file = discord.File("progression_chart.png", filename="progression_chart.png") 
            embed = discord.Embed( title="Racer Progression", description=f"Progression Over Time for {racer.name}", color=discord.Color.green() ) 
            embed.set_image(url="attachment://progression_chart.png") 
            await ctx.send(embed=embed, file=file)
        else:
            await ctx.send('Invalid query. Provide a valid Steam GUID or /register your steam guid to your Discord name.')

    
    @commands.hybrid_command(name="dickpic", description="get dickpic")
    async def dickpic(self, ctx: commands.Context) -> None:

        # Load the local image
        with open("iosh.png", "rb") as file:
            image = discord.File(file)
        
        # Create an embed
        embed = discord.Embed(
            title="Here is a dick!",
            description="",
            color=0x00ff00
        )
        embed.set_image(url="attachment://iosh.png")
        
        # Send the embed with the image
        await ctx.send(file=image, embed=embed)


    @commands.hybrid_command(name="top10wins", description="get top 10 wins")
    async def top10wins(self, ctx: commands.Context) -> None:
        if self.parsed:
            retstring = "Top 10 winners :" + "\n"
            winrankings = self.parsed.wins_rankings
            index = 1
            for elem in winrankings:
                retstring += str(index) + " : " + elem.name + " : " + str(elem.wins) + " wins" + "\n"
                index += 1
                if index == 11:
                    break
            await ctx.send(retstring)

    @commands.hybrid_command(name="monthreport", description="get monthlyreport")
    async def monthreport(self, ctx: commands.Context, datemonth:str, guid:str = None) -> None:
        steam_guid = self.get_steam_guid(ctx, guid)
        if steam_guid:
            year = datemonth[-2:]
            month = datemonth[:-2]
            report = self.parsed.month_report(steam_guid, month, year)
            retstr = "Month report for: " + self.parsed.racers[steam_guid].name + " in : " + month + " , " + "20" + year
            retstr += "\n"
            retstr += "**rating at start of month :** " + str(report[0])
            retstr += "\n"
            for elem in report[2]:
                retstr += elem.track.parent_track.highest_priority_name + " : " + elem.car.name + " : finished : " + str(elem.finishingposition) + " rating change: " + str(elem.ratingchange)
                retstr += "\n"
            retstr += "**rating at end of month :** " + str(report[1])
            retstr += "\n"
            retstr += "**total change in december :** " + str(round(report[1] - report[0], 2) )
            await ctx.send(retstr)
        else:
            await ctx.send('Invalid query. Provide a valid Steam GUID or /register your steam guid to your Discord name.')

    @commands.hybrid_command(name="top10podiums", description="get top 10 podums")
    async def top10podiums(self, ctx: commands.Context) -> None:
        if self.parsed:
            retstring = "Top 10 podium finishes:" + "\n"
            winrankings = self.parsed.podiums_rankings
            index = 1
            for elem in winrankings:
                retstring += str(index) + " : " + elem.name + " : " + str(elem.podiums) + " wins" + "\n"
                index += 1
                if index == 11:
                    break
            await ctx.send(retstring)

    @commands.hybrid_command(name="scatterplot", description="scatter plot of racers")
    async def scatterplot(self, ctx: commands.Context, only_recent = False) -> None: 
        self.parsed.plot_racers_scatter()
        file = discord.File("scatter_plot.png", filename="scatter_plot.png") 
        embed = discord.Embed( title="Scatter Plot", description=f"Cleanliness vs ELO scatter", color=discord.Color.green() ) 
        embed.set_image(url="attachment://scatter_plot.png") 
        await ctx.send(embed=embed, file=file)

    @commands.hybrid_command(name="rrrstats", description="get overall top 10s")
    async def rrrstats(self, ctx: commands.Context, only_recent = False) -> None:
        if self.parsed:
            stats = self.parsed.get_overall_stats(only_recent)
            embed = discord.Embed(
                title="Overall Stats " + ("(Recently Active Racers)" if only_recent else ""),
                color=discord.Color.blue()
            )

            def format_rankings(rankings, value_formatter):
                formatted_lines = [value_formatter(entry) for entry in rankings]
                return "\n".join(formatted_lines)

            def elo_formatter(entry):
                return f"{entry['rank']}. {entry['name']} - **Rating**: {entry['rating']}"

            def safety_formatter(entry):
                return f"{entry['rank']}. {entry['name']} - **Average Incidents**: {entry['averageincidents']:.2f}"

            def consistency_formatter(entry):
                return f"{entry['rank']}. {entry['name']} - **Consistency**: {entry['laptimeconsistency']:.2f}%"

            # Add top 10 ELO rankings
            elo_rankings = format_rankings(stats['elos'], elo_formatter)
            embed.add_field(name="🏆 Top 10 ELO Rankings 🏆", value=elo_rankings or "\u200b", inline=False)

            # Add top 10 clean racers
            safety_rankings = format_rankings(stats['safety'], safety_formatter)
            embed.add_field(name="🚗 Top 10 Clean Racers 🚗", value=safety_rankings or "\u200b", inline=False)

            # Add top 10 lap time consistency rankings
            laptime_consistency_rankings = format_rankings(stats['laptime_consistency'], consistency_formatter)
            embed.add_field(name="⏱️ Top 10 Lap Time Consistency ⏱️", value=laptime_consistency_rankings or "\u200b", inline=False)

            await ctx.send(embed=embed)
        else:
            await ctx.send("ERROR: Overall results have not been parsed yet")

    def car_embed(self, champ) -> Tuple[discord.Embed, List[discord.File]]:
        """
        Overview embed for a championship.

        • Lists every car, each with its download-URL and a short spec line
        (Power / Weight if available).
        • Shows preview images for **up to four** cars. If there are more
        than four cars → no images at all.
        • Returns (embed, [files…]) so the caller can send:
            embed, files = car_embed(champ)
            await thread.send(embed=embed, files=files)
        """
        cars      = champ.available_cars
        print("size ofa vaialble cars = " + str(len(cars)))
        first_evt = min(champ.schedule, key=lambda ev: ev.date)

        # ─── build description ─────────────────────────────────────────────
        desc_lines: list[str] = []
        for c in cars:
            dl_url = champ.car_download_links.get(c.id)
            dl = f"[Download]({dl_url})" if dl_url else "—"
            print("download link = " + dl)
            specs = []
            if c.specs:
                if bhp := c.specs.get("bhp"):
                    specs.append(f"{bhp} hp")
                if w := c.specs.get("weight"):
                    specs.append(f"{w} kg")
            specs_str = " • ".join(specs)
            line = f"• **{c.name}** — {dl}"
            if specs_str:
                line += f"  ({specs_str})"
            desc_lines.append(line)

        # strip leading EU/NA from the championship name
        display_name = re.sub(r'^\s*(EU|NA)\s+', '', champ.name, flags=re.IGNORECASE)

        emb = discord.Embed(
            title       = f"🏁 {display_name}",
            colour      = discord.Colour.blue(),
            description = "\n".join(desc_lines),
        )
        emb.add_field(
            name="Events",
            value=f"{len(champ.schedule)} races",
            inline=False,
        )

        # ─── attach up to 4 previews ────────────────────────────────────────
        files: list[discord.File] = []
        if len(cars) <= 4:
            for idx, c in enumerate(cars):
                if not c.imagepath:
                    continue
                p = Path(c.imagepath)
                print("path = " + str(p))
                # if it's relative, resolve it against cwd
                if not p.is_absolute():
                    p = Path.cwd() / c.imagepath
                if p.is_file():
                    df = discord.File(str(p), filename=p.name)
                    files.append(df)
                    # first image becomes the embed's hero
                    if idx == 0:
                        emb.set_image(url=f"attachment://{p.name}")

        return emb, files

    def _first_map_under(self,track_id: str) -> Optional[pathlib.Path]:
        """
        Look for *any* “map.png / preview.png / …” file saved by your scraper
        under  contentmedia/tracks/<track_id>/**  and return the first hit.
        """
        for p in (championship._MEDIA_ROOT / "tracks" / track_id).rglob("map.png"):
            return p
        for p in (championship._MEDIA_ROOT / "tracks" / track_id).rglob("preview.png"):
            return p
        for p in (championship._MEDIA_ROOT / "tracks" / track_id).rglob("*.png"):
            return p
        return None

    def event_embeds(self,
        events: Iterable, 
        root: str = "contentmedia"
    ) -> List[Tuple[discord.Embed, Optional[discord.File]]]:
        """
        • Adds **Race N** prefix (based on chronological order).
        • Shows variant name only when it differs from parent id/default.
        • Adds a “Download track” field.
        • Image lookup:
            1. ev.track.imagepath  (only if the file exists)
            2. first PNG under …/tracks/<track_id>/…
        """
        out: List[Tuple[discord.Embed, Optional[discord.File]]] = []
        def _multiplier(val: int | float) -> str:
            """250 → '2.5'   100 → '1'   45 → '0.45'"""
            return f"{val/100:.2f}".rstrip("0").rstrip(".")
                # make sure the list is in chronological order
        events = sorted(events, key=lambda e: e.date)

        for idx, ev in enumerate(events, 1):
            parent      = ev.track.parent_track
            base_name   = parent.highest_priority_name or parent.id
            variant_raw = ev.track.id.split(";")[-1]         # e.g. “default” or “gp”
            show_var    = variant_raw.lower() not in {
                parent.id.lower(),
                "default",
                "_base",
            }

            title = f"🏁 Race {idx} • {base_name}"
            if show_var:
                title += f" / {ev.track.name}"

            # readable date (still keep the discord timestamp below)
            pretty_date = datetime.fromisoformat(ev.date).strftime("%d %b %Y")

            desc_lines = [
                f"**Date**: {pretty_date}",
                f"**Session start**: {ev.sessionstarttime}",
                f"**Practice** {ev.practicelength} min",
                f"**Quali** {ev.qualifyinglength} min",
                # this will be overwritten if you’re doing a lap-based race
                f"**Race** {ev.raceonelength} min",
                f"Fuel ×{_multiplier(ev.fuelrate)}",
                f"Tyre ×{_multiplier(ev.tirewear)}",
                f"Damage ×{_multiplier(ev.damage)}",
            ]

            # lap-based instead of time-based?
            if ev.raceonelength == 0 and getattr(ev, "racelaps", 0) > 0:
                desc_lines[4] = f"**Race** {ev.racelaps} laps"

            # now insert Race 2 immediately after the first Race line
            if ev.doublerace:
                # pick either minutes or laps
                if ev.racetwolength > 0:
                    second = f"{ev.racetwolength} min"
                elif getattr(ev, "racelaps", 0) > 0:
                    second = f"{ev.racelaps} laps"
                else:
                    second = f"{ev.racetwolength} min"
                desc_lines.insert(
                    5,           # right after desc_lines[4]
                    f"**Race 2** {second}"
                )

            emb = discord.Embed(
                title       = title,
                description = "\n".join(desc_lines),
                colour      = discord.Colour.dark_teal(),
            )
            # download link
            if ev.track_download_link:
                emb.add_field(
                    name="Download track",
                    value=f"[Click here]({ev.track_download_link})",
                    inline=False,
                )

            # ─────────── image handling ────────────
            img_path: Optional[pathlib.Path] = None

            if ev.track.imagepath:
                p = pathlib.Path(ev.track.imagepath)
                if p.is_file():
                    img_path = p

            if img_path is None:                       # fallback search
                img_path = self._first_map_under(parent.id)

            file: Optional[discord.File] = None
            if img_path and img_path.is_file():
                file = discord.File(img_path, filename=img_path.name)
                emb.set_image(url=f"attachment://{img_path.name}")

            out.append((emb, file))

        return out
    
    @commands.hybrid_command(name="forceupdatestandings", description="forceupdatestandings")
    async def forceupdatestandings(self,ctx: commands.Context):
        await self.update_standings_internal()
        await ctx.send("Standings updated")

    async def update_standings_internal(self):

        for elem in self.parsed.championships.values():
            server   = {v: k for k, v in self.servertodirectory.items()}.get(elem.type)
            threadID = self.servertostandingsthread[server]
            forumID  = self.servertoparentchannel[server]

            forum  = self.bot.get_channel(forumID)              # discord.ForumChannel
            thread = forum.get_thread(threadID) 
            print(f"updating standings for {elem.name}")
            elem.update_standings()
            standings = elem.standings
            # 1) build the embed
            emb = discord.Embed(
                title   = f"🏆  {elem.name} — Driver Standings",
                colour  = discord.Colour.gold(),
            )
            lines = []
            for idx, (driver_name, pts) in enumerate(standings.items(), start=1):
                lines.append(f"**{idx}.** {driver_name} — {pts} pts")

            emb.description = "\n".join(lines)
            if elem.standingsmessage and elem.standingsmessage != "":
                msg = await thread.fetch_message(int(elem.standingsmessage))
                await msg.edit(embed=emb)
            else:
                sent = await thread.send(embed=emb)
                elem.standingsmessage = str(sent.id)
        await self.serializeall_internal()


    @commands.hybrid_command(name="sendschedule", description="sendschedule")
    async def sendschedule(self,ctx: commands.Context,type: str):
        await self.send_schedule_embeds(ctx, type)

    async def send_schedule_embeds(self, ctx: commands.Context, ch_type: str) -> None:
        """
        • If a sister schedule (EU/NA) already exists → do **nothing**.
        • Otherwise post a full schedule and automatically add the
        other region’s start‑time in the description (± 6 h),
        except for family ‘worldtour’.
        """

        # ─── 0. locate the Championship object ──────────────────────────────
        champ: Optional[championship.Championship] = self.parsed.championships.get(ch_type)
        if not champ:
            await ctx.send(f"❌ No championship of type **{ch_type}** registered.")
            return

        fam = _family(ch_type)                          # mx5 / gt3 / …
        if fam == "worldtour":                          # special case → no sister
            sister = None
        else:
            sister = next(
                (
                    c for t, c in self.parsed.championships.items()
                    if _family(t) == fam and t != ch_type
                    and any(getattr(ev, "schedulemessage", None) for ev in c.schedule)
                ),
                None,
            )

        # ─── 1. if sister already has schedule messages → do nothing ─────────
        if sister:
            await ctx.send(
                f"ℹ️  Schedule for **{fam.upper()}** already posted – "
                "nothing to do."
            )
            return

        # ─── 2. figure out forum / thread objects (your mappings) ────────────
        server   = {v: k for k, v in self.servertodirectory.items()}.get(ch_type)
        threadID = self.servertoschedulethread.get(server,         1368551209400795187)
        forumID  = self.servertoparentchannel.get(server,          1368551150537670766)

        forum  = self.bot.get_channel(forumID)                 # discord.ForumChannel
        thread = forum.get_thread(threadID) if forum else None
        if thread is None:
            await ctx.send("❌ Could not find the announcement thread.")
            return

        # ─── 3. region helpers ───────────────────────────────────────────────
        this_region  = "EU" if "eu" in ch_type.lower() else "NA"
        other_region = "NA" if this_region == "EU" else "EU"
        add_other    = fam != "worldtour"                      # only mx5 / gt3 / …

        # -------------------------------------------------------------------- #
        # 4. build + send the overview card
        # -------------------------------------------------------------------- #
        ovw_emb, ovw_files = self.car_embed(champ)
        if ovw_files:
            if len(ovw_files) == 1:
                msg = await thread.send(embed=ovw_emb, file=ovw_files[0])
            else:
                msg = await thread.send(embed=ovw_emb, files=ovw_files)
        else:
            msg = await thread.send(embed=ovw_emb)
        champ.infomessage = str(msg.id)

        # -------------------------------------------------------------------- #
        # 5. per‑event cards
        # -------------------------------------------------------------------- #
        for ev, (emb, f) in zip(
                sorted(champ.schedule, key=lambda e: e.date),
                self.event_embeds(champ.schedule),
        ):
            # ───── rebuild the “Session start” row so *both* regions sit on ONE line
            if add_other:
                ts_main  = ev.sessionstarttime                      # '<t:…:f>'
                raw_main = _raw_ts(ts_main)
                if raw_main:
                    # EU is always six hours *ahead* of NA
                    delta      = 7 * 3600 if this_region == "EU" else -7 * 3600
                    raw_other  = raw_main + delta
                    ts_other   = f"<t:{raw_other}:f>"

                    main_lbl   = f"{this_region} session start"
                    other_lbl  = f"{other_region} session start"

                    # ▸ split the description into individual lines
                    lines = emb.description.split("\n")

                    # helper that lower‑cases and swaps NBSP → normal space
                    def _norm(s: str) -> str:
                        return s.replace("\u00a0", " ").lower()

                    # locate the original “Session start” line
                    for i, l in enumerate(lines):
                        if _norm(l).startswith("**session start**"):
                            # replace it with   "**EU session start**: …   **NA session start**: …"
                            lines[i] = f"**{main_lbl}**: {ts_main}"
                            lines.insert(i + 1, f"**{other_lbl}**: {ts_other}")
                            break
                    else:
                        # fail‑safe: append a new combined line
                        lines.append(
                            f"**{main_lbl}**: {ts_main}   **{other_lbl}**: {ts_other}"
                        )

                    emb.description = "\n".join(lines)

            # ───── send the embed (and attachment, if any) ─────────────
            msg = await thread.send(embed=emb, file=f) if f else await thread.send(embed=emb)
            ev.schedulemessage = str(msg.id)

        await ctx.send(f"🗓️  Schedule for **{ch_type}** posted.")

    @commands.hybrid_command(name="registerchampionship", description="Register a new championship with an attached file")
    async def registerchampionship(self,ctx: commands.Context,attachment: discord.Attachment,  type: str):
        file_name = attachment.filename
        if not file_name.endswith(".json"):
            await ctx.send("Please upload a valid JSON file.")
            return
        if not file_name:
            await ctx.send("Please upload a file.")
            return
        rolesuser = ctx.author.roles
        if not any(role.id == 1099807643918422036 for role in rolesuser):
            await ctx.send("You are not allowed to register championships")
            return
        allowedtypes = ["mx5euopen", "mx5naopen", "mx5eurrr", "mx5narrr", "mx5narar", "gt3naopen", "gt3eurrr",
                         "gt4euopen", "gt4naopen", "formulaeuopen", "formulanaopen",
                         "formulnarrr", "formulanarar", "worldtour"]
        if type not in allowedtypes:
            await ctx.send("Invalid type. Allowed types are: " + ", ".join(allowedtypes))
            return
        reverse_lookup = {v: k for k, v in self.servertodirectory.items()}
        server = reverse_lookup.get(type)

        champ_dict = None
        try:
            data_bytes = await attachment.read()
            champ_dict = json.loads(data_bytes.decode("utf‑8"))
        except Exception as exc:
            return await ctx.send(f"Could not parse JSON: {exc}")
        if type in self.parsed.championships:
            await ctx.send("Championship already registered")
            return
        # Getting the server name from a type
        baseurl = server
        with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json") as tf:
            json.dump(champ_dict, tf, indent=2)
            tmp_name = tf.name
        try:
            champ = championship.create_championship(tmp_name, baseurl, self.parsed.contentdata, type)
            self.parsed.championships[type] = champ     
        finally:
            await self.serializeall_internal()
            await ctx.send(f"Championship " + type + " has been registered!")
            os.remove(tmp_name)

        

    @commands.hybrid_command(name="unregisterchampionship", description="unregisterchampionship ")
    async def unregisterchampionship(self, ctx: commands.Context, type:str) -> None:
        rolesuser = ctx.author.roles
        if not 1099807643918422036 in rolesuser:
            await ctx.send("You are not allowed to register championships")
            return

    @commands.hybrid_command(name="dumptracks", description="dump all tracks")
    @commands.is_owner()
    async def dumptracks(self, context: Context) -> None:
        for track in self.parsed.contentdata.tracks:
            logger.info(track.highest_priority_name)

    @commands.hybrid_command(name="exportall", description="exportall")
    @commands.is_owner()
    async def exportall(self, context: Context) -> None:
        track_json_output = json.dumps([track.to_dict() for track in self.parsed.contentdata.tracks], indent=4)
        # Save to file
        with open("export/tracks.json", "w") as f:
            f.write(track_json_output)

        car_json_output = json.dumps([car.to_dict() for car in self.parsed.contentdata.cars], indent=4)
        # Save to file
        with open("export/cars.json", "w") as f:
            f.write(car_json_output)

        racer_json_output = json.dumps([racer.to_dict() for racer in self.parsed.racers.values()], indent=4)
        # Save to file
        with open("export/racers.json", "w") as f:
            f.write(racer_json_output)

        result_json_output = json.dumps([result.to_dict() for result in self.parsed.raceresults], indent=4)
        # Save to file
        with open("export/results.json", "w") as f:
            f.write(result_json_output)


    @commands.hybrid_command(name="rrrdirty", description="get dirtiest drivers")
    @commands.is_owner()
    async def rrrdirty(self, context: Context) -> None:
        if self.parsed:
            retstr = self.parsed.get_dirty_drivers()
            await context.send(retstr)
        else:
            await context.send("ERROR:Overall results have not been parsed yet")

# And then we finally add the cog to the bot so that it can load, unload, reload and use it's content.
async def setup(bot) -> None:
    await bot.add_cog(Stats(bot))