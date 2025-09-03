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

class Simgrid(commands.Cog, name="Simgrid"):
    def __init__(self, bot) -> None:
        self.bot = bot
        logger.info("simgrid cog init id=%s", id(self))
        logger.info("loading simgrid cog")
        self.simgrid_championship_id = 17708
        self.simgrid_user_agent = "https://github.com/JanuarySnow/RRR-Bot"
        self.simgrid_api_token = os.getenv("SIMGRID_API_TOKEN", "").strip()
        self.simgrid_base_url = os.getenv("SIMGRID_API_BASE", "https://www.thesimgrid.com/api/v1")
        self.simgrid_forum = 1412215127360671826
        self.simgrid_threads = {
            "schedule":   1412215209220767744,
            "next_race":  1412215237616336896,
            "leaderboard":1412215381019459666,
        }
        self._simgrid_msg_ids = {}  # key: thread_key ("schedule"/...), value: message_id (int)
        #self.fetch_iracing_data.start()

    # ---------- SimGrid (GridOS) integration: config ----------
    # You can set these once (e.g., in __init__) instead of every run; this code guards defaults if missing.
    def _ensure_simgrid_defaults(self):
        import os
        if not hasattr(self, "simgrid_base_url"):
            # Try the docs host's API path first; change to your real API host if different.
            # Common alternatives you might try:
            #   https://api.thesimgrid.com
            #   https://gridos-api.thesimgrid.com
            self.simgrid_base_url = os.getenv("SIMGRID_API_BASE", "https://www.thesimgrid.com/api/v1")
        if not hasattr(self, "simgrid_api_token"):
            self.simgrid_api_token = os.getenv("SIMGRID_API_TOKEN", "").strip()
        if not hasattr(self, "simgrid_user_agent"):
            self.simgrid_user_agent = "https://github.com/JanuarySnow/RRR-Bot"
        if not hasattr(self, "simgrid_championship_id"):
            self.simgrid_championship_id = 17708  # your championship id
        # Discord thread IDs (provided)
        if not hasattr(self, "simgrid_threads"):
            self.simgrid_threads = {
                "schedule":   1412211889584209940,
                "next_race":  1412211943132893314,
                "leaderboard":1412212004013211658,
            }
        # In-memory upsert cache for message IDs we post/edit in threads
        if not hasattr(self, "_simgrid_msg_ids"):
            self._simgrid_msg_ids = {}  # key: thread_key ("schedule"/...), value: message_id (int)

    def _simgrid_headers(self) -> dict:
        # SimGrid expects Bearer token auth
        headers = {
            "User-Agent": self.simgrid_user_agent,
            "Accept": "application/json",
        }
        if self.simgrid_api_token:
            headers["Authorization"] = f"Bearer {self.simgrid_api_token}"
        return headers

    # ---------- HTTP helpers with robust retry/backoff ----------
    async def _http_get_json(self, session, url: str, *, max_retries: int = 3, timeout_s: int = 20):
        import asyncio, random, aiohttp
        from aiohttp import ClientResponseError

        for attempt in range(1, max_retries + 1):
            try:
                async with session.get(url, headers=self._simgrid_headers(), timeout=aiohttp.ClientTimeout(total=timeout_s)) as resp:
                    status = resp.status
                    # Handle rate limiting
                    if status == 429:
                        retry_after = resp.headers.get("Retry-After")
                        try:
                            delay = float(retry_after) if retry_after else 2 ** attempt
                        except Exception:
                            delay = 2 ** attempt
                        if attempt < max_retries:
                            await asyncio.sleep(delay)
                            continue
                        else:
                            text = await resp.text()
                            raise ClientResponseError(request_info=resp.request_info, history=resp.history,
                                                    status=429, message=f"Rate limited: {text}", headers=resp.headers)
                    # Retry transient 5xx
                    if 500 <= status <= 599 and attempt < max_retries:
                        await asyncio.sleep(1.5 * attempt)
                        continue

                    resp.raise_for_status()
                    # Be tolerant of content-type quirks
                    try:
                        return await resp.json(content_type=None)
                    except Exception:
                        # Try text -> json
                        import json
                        text = await resp.text()
                        return json.loads(text)
            except asyncio.TimeoutError:
                if attempt >= max_retries:
                    raise
                await asyncio.sleep(1.0 * attempt)
            except Exception:
                if attempt >= max_retries:
                    raise
                await asyncio.sleep(0.75 * attempt + random.random() * 0.25)

    # ---------- API calls (paths mirror your samples/docs) ----------
    # ---------- URL helpers ----------
    def _simgrid_join(self, *parts: str) -> str:
        base = self.simgrid_base_url.rstrip("/")
        tail = "/".join(p.strip("/") for p in parts if p is not None)
        return f"{base}/{tail}"

    def _simgrid_with_params(self, path: str, params: dict) -> str:
        from urllib.parse import urlencode
        base = self.simgrid_base_url.rstrip("/")
        if params:
            return f"{base}/{path.lstrip('/')}?{urlencode(params, doseq=True)}"
        return f"{base}/{path.lstrip('/')}"

    async def fetch_simgrid_championship(self, championship_id: int):
        import aiohttp, traceback
        url = self._simgrid_join("championships", str(championship_id))
        try:
            async with aiohttp.ClientSession() as session:
                return await self._http_get_json(session, url)
        except Exception as e:
            try:
                logger.exception(f"SimGrid championship fetch failed: {e}")
            except NameError:
                print("SimGrid championship fetch failed:", e, traceback.format_exc())
            return None

    async def fetch_simgrid_races(self, championship_id: int):
        import aiohttp, traceback
        # Correct endpoint: /races?championship_id=ID
        url = self._simgrid_with_params("races", {"championship_id": championship_id})
        try:
            async with aiohttp.ClientSession() as session:
                data = await self._http_get_json(session, url)
                # Expect a list
                return data if isinstance(data, list) else (data.get("races") if isinstance(data, dict) else None)
        except Exception as e:
            try:
                logger.exception(f"SimGrid races fetch failed: {e}")
            except NameError:
                print("SimGrid races fetch failed:", e, traceback.format_exc())
            return None

    async def fetch_simgrid_entrylist(self, championship_id: int, *, class_ids: list[int] | None = None):
        import aiohttp, traceback
        params = {"format": "json"}
        if class_ids:
            params["championship_car_class_ids[]"] = class_ids
        path = f"championships/{championship_id}/entrylist"
        url = self._simgrid_with_params(path, params)
        try:
            async with aiohttp.ClientSession() as session:
                data = await self._http_get_json(session, url)
                # Typically returns {"entries":[...], "forceEntryList": 1} or a bare array for some outputs
                return data if isinstance(data, dict) else {"entries": data} if isinstance(data, list) else None
        except Exception as e:
            try:
                logger.exception(f"SimGrid entry list fetch failed: {e}")
            except NameError:
                print("SimGrid entry list fetch failed:", e, traceback.format_exc())
            return None

    async def fetch_simgrid_participating_users(self, championship_id: int):
        import aiohttp, traceback
        url = self._simgrid_join("championships", str(championship_id), "participating_users")
        try:
            async with aiohttp.ClientSession() as session:
                data = await self._http_get_json(session, url)
                return data if isinstance(data, list) else []
        except Exception as e:
            try:
                logger.warning(f"SimGrid participating_users fetch failed: {e}")
            except NameError:
                print("SimGrid participating_users fetch failed:", e, traceback.format_exc())
            return []

    # ---------- Formatting helpers (unchanged) ----------
    def _parse_iso_utc(self, s: str):
        from datetime import datetime
        return datetime.fromisoformat(s.replace("Z", "+00:00"))

    def _to_london(self, dt):
        from zoneinfo import ZoneInfo
        return dt.astimezone(ZoneInfo("Europe/London"))

    def _fmt_when(self, starts_at_iso: str) -> tuple[str, str]:
        from datetime import datetime, timezone
        start = self._parse_iso_utc(starts_at_iso)
        start_local = self._to_london(start)
        now_local = datetime.now(timezone.utc).astimezone(start_local.tzinfo)
        abs_s = start_local.strftime("%a, %d %b %Y • %H:%M %Z")
        delta = start_local - now_local
        seconds = int(delta.total_seconds())
        if seconds >= 0:
            days, rem = divmod(seconds, 86400)
            hours, rem = divmod(rem, 3600)
            mins, _ = divmod(rem, 60)
            if days > 0: rel = f"in {days}d {hours}h"
            elif hours > 0: rel = f"in {hours}h {mins}m"
            else: rel = f"in {mins}m"
        else:
            seconds = -seconds
            days, rem = divmod(seconds, 86400)
            hours, rem = divmod(rem, 3600)
            mins, _ = divmod(rem, 60)
            if days > 0: rel = f"{days}d {hours}h ago"
            elif hours > 0: rel = f"{hours}h {mins}m ago"
            else: rel = f"{mins}m ago"
        return abs_s, rel

    def _build_schedule_embed(self, champ: dict | None, races: list[dict]) -> "discord.Embed":
        import discord
        title = f"Schedule — {champ.get('name')}" if champ else "Schedule"
        emb = discord.Embed(title=title, colour=discord.Colour.blurple())
        if champ:
            if champ.get("start_date"):
                abs_s, _ = self._fmt_when(champ["start_date"])
                emb.add_field(name="Season start", value=abs_s, inline=True)
            if champ.get("host_name"):
                emb.add_field(name="Host", value=champ["host_name"], inline=True)
            if champ.get("game_name"):
                emb.add_field(name="Game", value=champ["game_name"], inline=True)
            if champ.get("image"):
                emb.set_thumbnail(url=champ["image"])

        if not races:
            emb.description = "No races found."
            emb.set_footer(text="SimGrid • auto-updated")
            return emb

        try:
            races_sorted = sorted(races, key=lambda r: self._parse_iso_utc(r["starts_at"]))
        except Exception:
            races_sorted = races

        lines = []
        for r in races_sorted:
            when_abs, when_rel = self._fmt_when(r["starts_at"])
            name = r.get("display_name") or r.get("race_name") or "Race"
            track = r.get("track", "Unknown Track")
            results = "✅ results" if r.get("results_available") else "—"
            lines.append(f"• **{name}** — {track}\n  {when_abs} ({when_rel}) · {results}")

        chunk = "\n".join(lines)
        if len(chunk) <= 1000:
            emb.add_field(name="Races", value=chunk, inline=False)
        else:
            buf, cur = [], ""
            for line in lines:
                if len(cur) + len(line) + 1 <= 900:
                    cur += ("\n" if cur else "") + line
                else:
                    buf.append(cur); cur = line
            if cur: buf.append(cur)
            for i, part in enumerate(buf, 1):
                emb.add_field(name=f"Races (part {i})", value=part, inline=False)

        emb.set_footer(text="SimGrid • auto-updated")
        return emb

    def _build_next_race_embed(self, champ: dict | None, races: list[dict], entries: dict | None, participating_users: list[dict] | None) -> "discord.Embed":
        import discord
        emb = discord.Embed(title="Next Race", colour=discord.Colour.green())

        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        future = []
        for r in races:
            try:
                dt = self._parse_iso_utc(r["starts_at"])
                if dt >= now:
                    future.append((dt, r))
            except Exception:
                continue
        if future:
            future.sort(key=lambda t: t[0])
            next_r = future[0][1]
        else:
            # fallback: most recent past race
            try:
                past = sorted(races, key=lambda r: self._parse_iso_utc(r["starts_at"]), reverse=True)
                next_r = past[0] if past else None
            except Exception:
                next_r = races[0] if races else None

        if not next_r:
            emb.description = "No upcoming race."
            emb.set_footer(text="SimGrid • auto-updated")
            return emb

        when_abs, when_rel = self._fmt_when(next_r["starts_at"])
        emb.add_field(name="Championship", value=(champ.get("name") if champ else "—"), inline=False)
        emb.add_field(name="Race", value=next_r.get("display_name") or next_r.get("race_name") or "Race", inline=True)
        emb.add_field(name="Track", value=next_r.get("track", "—"), inline=True)
        emb.add_field(name="When", value=f"{when_abs} ({when_rel})", inline=False)

        registered = None
        if entries and isinstance(entries.get("entries"), list):
            registered = len(entries["entries"])
        elif participating_users:
            registered = len(participating_users)
        if registered is not None:
            emb.add_field(name="Registered", value=str(registered), inline=True)

        if champ and champ.get("image"):
            emb.set_thumbnail(url=champ["image"])
        emb.set_footer(text="SimGrid • auto-updated")
        return emb

    def _build_leaderboard_embed(self, standings: dict | list | None) -> "discord.Embed":
        import discord
        emb = discord.Embed(title="Leaderboard", colour=discord.Colour.gold())
        if not standings:
            emb.description = "Standings endpoint not available (or no data)."
            emb.set_footer(text="SimGrid • auto-updated")
            return emb

        rows = []
        if isinstance(standings, dict):
            rows = standings.get("standings") or standings.get("results") or standings.get("data") or []
        elif isinstance(standings, list):
            rows = standings

        if not rows:
            emb.description = "No standings data found."
            emb.set_footer(text="SimGrid • auto-updated")
            return emb

        lines = []
        for i, row in enumerate(rows[:15], 1):
            pos = row.get("position") or row.get("rank") or i
            name = row.get("driver_name") or row.get("team_name") or row.get("name") or row.get("display_name") or "—"
            pts = row.get("points") or row.get("score") or row.get("total_points") or "—"
            lines.append(f"**{pos:>2}**  {name} — {pts} pts")

        emb.add_field(name="Top 15", value="\n".join(lines) if lines else "—", inline=False)
        emb.set_footer(text="SimGrid • auto-updated")
        return emb

    # ---------- Discord upsert helper (same as before) ----------
    async def _upsert_thread_embed(self, thread_id: int, thread_key: str, embed: "discord.Embed"):
        import discord
        allowed = discord.AllowedMentions.none()
        thread = self.bot.get_channel(thread_id)
        if thread is None:
            try:
                thread = await self.bot.fetch_channel(thread_id)
            except Exception as e:
                try:
                    logger.warning(f"Could not fetch thread {thread_id}: {e}")
                except NameError:
                    print(f"Could not fetch thread {thread_id}: {e}")
                return

        msg_id = self._simgrid_msg_ids.get(thread_key)
        if msg_id:
            try:
                msg = await thread.fetch_message(msg_id)
                await msg.edit(embed=embed, allowed_mentions=allowed)
                return
            except Exception:
                pass

        try:
            sent = await thread.send(embed=embed, allowed_mentions=allowed)
            self._simgrid_msg_ids[thread_key] = sent.id
        except Exception as e:
            try:
                logger.exception(f"Failed to send to thread {thread_id}: {e}")
            except NameError:
                print(f"Failed to send to thread {thread_id}:", e)

    # ---------- Periodic task (uses corrected endpoints) ----------
    from discord.ext import tasks

    @tasks.loop(seconds=600.0)
    async def fetch_iracing_data(self):
        self._ensure_simgrid_defaults()
        cid = self.simgrid_championship_id
        try:
            champ = await self.fetch_simgrid_championship(cid)
            races = await self.fetch_simgrid_races(cid) or []
            entrylist = await self.fetch_simgrid_entrylist(cid)
            part_users = await self.fetch_simgrid_participating_users(cid)
            standings = None  # still best-effort; wire up if/when you have a correct endpoint

            schedule_embed = self._build_schedule_embed(champ, races)
            next_embed = self._build_next_race_embed(champ, races, entrylist, part_users)
            leaderboard_embed = self._build_leaderboard_embed(standings)

            await self._upsert_thread_embed(self.simgrid_threads["schedule"], "schedule", schedule_embed)
            await self._upsert_thread_embed(self.simgrid_threads["next_race"], "next_race", next_embed)
            await self._upsert_thread_embed(self.simgrid_threads["leaderboard"], "leaderboard", leaderboard_embed)

        except Exception as e:
            try:
                logger.exception(f"fetch_iracing_data failed: {e}")
            except NameError:
                import traceback
                print("fetch_iracing_data failed:", e, traceback.format_exc())

# Optional: kick it off in your cog's on_ready or __init__
# self.fetch_iracing_data.start()

async def setup(bot) -> None:
    await bot.add_cog(Simgrid(bot))
