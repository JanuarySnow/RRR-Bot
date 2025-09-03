import math
from typing import Dict
import content_data
import racer
import statsparser
from logger_config import logger
from datetime import datetime
import uuid
import aiohttp
import asyncio
import requests
import racer
import content_data
import re, uuid, json, requests, asyncio
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from pathlib import Path
from typing import Optional


_ts_re = re.compile(r"<t:(\d+):")      # grabs the integer part

def discord_ts_to_dt(tag: str) -> datetime:
    _ts_re = re.compile(r"<t:(\d+):")  
    """
    Convert a Discord timestamp   <t:1718292000:f>   →   2025-06-13 18:00:00+00:00
    """
    m = _ts_re.match(tag)
    if not m:
        raise ValueError(f"Not a Discord timestamp: {tag!r}")
    return datetime.fromtimestamp(int(m.group(1)), tz=timezone.utc)

_MEDIA_ROOT = Path("contentmedia")          # root folder on disk
def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _download(url: str, dest: Path, timeout: int = 30) -> Path | None:
    """
    Download *url* to *dest* (only if it is missing) and return *dest*.
    Silently returns **None** if the remote file is a placeholder (‘#’ etc.)
    or the request fails.
    """
    if not url or url in {"#", "javascript:void(0)"}:
        return None

    _ensure_dir(dest.parent)
    if dest.exists():                       # already on disk: skip
        return dest

    try:
        with requests.get(url, timeout=timeout, stream=True) as r:
            r.raise_for_status()
            with open(dest, "wb") as fp:
                for chunk in r.iter_content(32_768):
                    fp.write(chunk)
        return dest
    except requests.RequestException:
        return None

def _scrape_download_url(page_url: str, timeout: int = 30) -> str | None:
    """
    Scrape a Tekly ‘/car/…’ or ‘/track/…’ page (public view) and
    return the real download link from the green button.

    ─ How it works ─
    • The public page always contains at least one
        <a class="btn btn-success …">Download …</a>
      element.
    • If the server admin hasn’t filled in a URL yet, that anchor’s
      href is literally "#"  (or sometimes 'javascript:void(0)').
    • We scan btn‑success anchors in document order and return the first
      whose href is *not* "#".
    • If every button is a placeholder we return None.
    """
    resp = requests.get(page_url, timeout=timeout)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    for btn in soup.select("a.btn.btn-success[href]"):
        href = btn["href"].strip()
        if href and href not in {"#", "javascript:void(0)"}:
            return urljoin(page_url, href)   # handles relative links too

    # no usable link configured
    return None

def _scrape_track_name(base_url: str, track_id: str) -> str | None:
    """
    Fetches the human-readable track name (including layout) from Tekly.
    Tries the <h3.card-title> first (layout-specific), then falls back to <h1>.
    """
    page_url = f"{base_url}/track/{track_id}"
    resp     = requests.get(page_url, timeout=10)
    resp.raise_for_status()
    soup     = BeautifulSoup(resp.text, "html.parser")

    # layout-specific title, if present
    h3 = soup.select_one("h3.card-title.mb-0")
    if h3 and (txt := h3.get_text(strip=True)):
        return txt

    # generic track name
    h1 = soup.select_one("h1.text-center.mb-0")
    if h1 and (txt := h1.get_text(strip=True)):
        return txt

    # fallback to prettified id
    return track_id.replace("_", " ").title()

def _scrape_car_media(
    page_url: str,
    root_dir: Path = _MEDIA_ROOT / "cars",
    timeout:  int  = 30,
) -> str | None:
    """
    Return a *local* path to the hero preview image.

    • **NEW**: if the image is already on disk we return immediately
      without touching the network.
    """
    car_id = page_url.rstrip("/").split("/")[-1]
    dest   = root_dir / car_id / "preview.jpg"

    # ─── early‑exit — file already there ─────────────────────────────
    if dest.is_file():
        return str(dest)

    # (otherwise fall through and fetch once)
    resp = requests.get(page_url, timeout=timeout)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    img_url = None
    hero = soup.select_one("#hero-skin[src]")
    if hero:
        img_url = urljoin(page_url, hero["src"])
    else:
        for img in soup.select("img.car-image[src]"):
            if "badge" not in img["src"]:
                img_url = urljoin(page_url, img["src"])
                break
    if not img_url:
        return None

    saved = _download(img_url, dest)
    return str(saved) if saved else None


def scrape_track_images(base_url: str, track_id: str) -> dict[str, str]:
    """
    Download map / preview images *only if the files are missing*.
    Returns a mapping  {variant_id: local_path}
    """
    page_root = _MEDIA_ROOT / "tracks" / track_id          # local root
    # quick check – if **any** .png already exists we assume the track
    # has been scraped before and skip hitting the website altogether
    if any(page_root.rglob("*.png")):
        return {p.parent.name.replace("_base", track_id): str(p)
                for p in page_root.rglob("*.png")}

    # (otherwise fetch once)
    page_url = f"{base_url}/track/{track_id}"
    soup     = BeautifulSoup(requests.get(page_url, timeout=10).text,
                             "html.parser")
    images = {}
    for img in soup.select("img[class*=track-map-][src]"):
        classes = img.get("class", [])
        variant = next((c.removeprefix("track-map-")
                        for c in classes if c.startswith("track-map-")), "")
        var_id  = f"{track_id};{variant or track_id}"

        img_url = urljoin(page_url, img["src"])
        fname   = Path(urlparse(img_url).path).name
        dest    = _MEDIA_ROOT / "tracks" / track_id / (variant or "_base") / fname
        saved   = _download(img_url, dest)
        if saved:
            images[var_id] = str(saved)

    # also grab the hero ‘preview.png’ (class image-track)
    hero = soup.select_one("img.image-track[src]")
    if hero:
        img_url = urljoin(page_url, hero["src"])
        fname   = Path(urlparse(img_url).path).name
        dest    = _MEDIA_ROOT / "tracks" / track_id / "_base" / fname
        saved   = _download(img_url, dest)
        if saved:
            images[f"{track_id};{track_id}"] = str(saved)

    return images

def scrape_championship_standings(base_url: str, championship_id: str, debug: bool = False) -> Dict[str, int]:
    """
    Fetches the driver standings for a Tekly championship.
    If the “Driver Standings” tab exists, returns {driver_name: points}.
    Otherwise falls back to the “Entrants” tab and returns each name with 0 points.
    """
    url = f"{base_url}/championship/{championship_id}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    if debug:
        print(f"Fetching standings from: {url}")
        print(f"Response status code: {resp.status_code}")
        print(f"Response content preview:\n{resp.text[:500]}")
    soup = BeautifulSoup(resp.text, "html.parser")

    # 1) Try to find the Driver Standings table
    drivers_pane = soup.select_one("div.tab-pane#drivers table")
    if debug:
        print(f"Drivers pane found: {bool(drivers_pane)}")
        print(f"Number of rows found: {len(drivers_pane.select('tbody tr')) if drivers_pane else 0}")
    if drivers_pane:
        # Figure out which column is “Points”
        header_cells = drivers_pane.select("thead th")
        if debug:
            headers = [th.get_text(strip=True) for th in header_cells]
            print(f"Header columns: {headers}")
        points_idx = None
        for idx, th in enumerate(header_cells):
            if th.get_text(strip=True).lower() == "points":
                points_idx = idx
                break
        if points_idx is None:
            # fallback to the 3rd column if we didn't find “Points”
            points_idx = 2
            if debug:
                print("Warning: 'Points' column not found, defaulting to index 2")

        standings: Dict[str, int] = {}
        for tr in drivers_pane.select("tbody tr"):
            cols = tr.find_all("td")
            # make sure we have at least enough columns
            if len(cols) > max(1, points_idx):
                name = cols[1].get_text(strip=True)
                raw_points = cols[points_idx].get_text(strip=True)
                if debug:
                    print(f"Parsed: {name} - {raw_points}")
                try:
                    standings[name] = int(raw_points)
                except ValueError:
                    standings[name] = 0
        return standings

    # 2) Fallback: grab all Entrants, zero out their points
    entrants_pane = soup.select_one("div.tab-pane#entrants table")
    if debug:
        print(f"Entrants pane found: {bool(entrants_pane)}")
        print(f"Number of entrant rows: {len(entrants_pane.select('tbody tr')) if entrants_pane else 0}")
    standings: Dict[str, int] = {}
    if entrants_pane:
        for tr in entrants_pane.select("tbody tr"):
            cols = tr.find_all("td")
            if len(cols) >= 2:
                name = cols[1].get_text(strip=True)
                standings[name] = 0
    return standings

def _to_discord_timestamp(dt: datetime, style: str = "f") -> str:
    """Return a Discord timestamp tag <t:…:style>"""
    return f"<t:{int(dt.timestamp())}:{style}>"

class Event:
    def __init__(self, name: str, date: str, track:str, doublerace:bool, practicelength:int, qualifyinglength:int, raceonelength:int, racetwolength:int, location: str, sessionstarttime: str, track_download_link: str = None):

        self.name = name
        self.date = date
        self.location = location
        self.track = track
        self.doublerace = doublerace
        self.practicelength = practicelength
        self.qualifyinglength = qualifyinglength
        self.sessionstarttime  = sessionstarttime
        self.raceonelength = raceonelength
        self.racetwolength = racetwolength
        self.fuelrate = 100
        self.racelaps = 0
        self.damage = 100
        self.tirewear = 100
        self.id = str(uuid.uuid4())
        self.result = None
        self.track_download_link = track_download_link
        self.resultmessage = None
        self.schedulemessage = None

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "date": self.date,
            "location": self.location,
            "track": self.track.id,
            "doublerace": self.doublerace,
            "practicelength": self.practicelength,
            "qualifyinglength": self.qualifyinglength,
            "raceonelength": self.raceonelength,
            "racetwolength": self.racetwolength,
            "fuelrate": self.fuelrate,
            "damage": self.damage,
            "tirewear": self.tirewear,
            "result": self.result.id if self.result else None,
            "sessionstarttime": self.sessionstarttime,
            "track_download_link": self.track_download_link,
            "resultmessage": self.resultmessage,
            "schedulemessage": self.schedulemessage,
            "racelaps" : self.racelaps,
        }


class Championship:
    def __init__(self, name: str, racers: list[racer.Racerprofile], schedule: list[Event], open: bool = False, type: str = "gt3euopen"):
        self.name = name
        self.racers = racers
        self.schedule = []
        self.id = ""
        self.standings = {} # racer guid -> int position
        self.open = open
        self.type = type
        self.completed = False
        self.available_cars = []  # List of available cars for the championship
        self.car_download_links = {}  # Dictionary to store car download links
        self.standingsmessage = None
        self.infomessage = None
        self.baseurl = None

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "racers": [racer.guid for racer in self.racers],  # Extract GUIDs correctly
            "schedule": [event.to_dict() for event in self.schedule],
            "open": self.open,
            "type": self.type,
            "completed": self.completed,
            "standings": {racer: position for racer, position in self.standings.items()},
            "available_cars": [car.id for car in self.available_cars],
            "car_download_links": self.car_download_links,
            "standingsmessage": self.standingsmessage,
            "infomessage": self.infomessage,
            "baseurl": self.baseurl,
        }
    
    def update_standings(self) -> None:#
        debug = False
        self.standings = scrape_championship_standings(self.baseurl, self.id, debug)


    def get_next_race(self, *, now: Optional[datetime] = None):
        if now is None:
            now = datetime.now(timezone.utc)

        next_evt       = None
        next_evt_start = None          # keep the datetime so we can compare

        for event in self.schedule:
            try:
                start_dt = discord_ts_to_dt(event.sessionstarttime)
            except ValueError:
                continue               # bad tag? just ignore

            if start_dt > now and (
                next_evt_start is None or start_dt < next_evt_start
            ):
                next_evt       = event
                next_evt_start = start_dt

        return next_evt



# championship_loader.py  (⇦ put this next to championship.py)

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from content_data import Contentdata                # your existing helper


def create_championship(json_file,base_url: str, contentdata, champ_type: str = "gt3euopen") -> Championship:
    """
    Parse a Tekly‑style championship export (the file you just uploaded)
    into the in‑memory Championship / Event objects you already use.

    Parameters
    ----------
    json_file : str | Path
        Path to the *.json* file that was just uploaded / passed in.
    contentdata : Contentdata
        Your global Contentdata instance (so we can look up or create
        TrackVariant objects that the Events will point at).
    champ_type : str, optional
        Whatever you normally store in `Championship.type`.
    """
    data = json.loads(Path(json_file).read_text(encoding="utf‑8"))

    # ───────────────────────── championship shell ─────────────────────────
    champ = Championship(
        name=data["Name"],
        racers=[],
        schedule=[],
        open=False,
        type=champ_type,
    )
    champ.id = data["ID"]
    champ.baseurl = base_url
    car_ids = set()                                   # avoid duplicates
    for cls in data.get("Classes", []):
        car_ids.update(cls.get("AvailableCars", []))

    for car_id in car_ids:
        base_car = contentdata.get_car(car_id) or \
                   contentdata.create_basic_car(car_id)

        # ▸ download‑URL  – only if we don't have it yet
        if not getattr(base_car, "download_url", None):
            car_page = f"{base_url}/car/{car_id}"
            base_car.download_url = _scrape_download_url(car_page)

        # ▸ preview image – only if imagepath missing *or* file vanished
        if not (base_car.imagepath and Path(base_car.imagepath).is_file()):
            car_page = f"{base_url}/car/{car_id}"
            base_car.imagepath = _scrape_car_media(car_page)

        champ.available_cars.append(base_car)
        champ.car_download_links[car_id] = base_car.download_url                  # keep the real UUID

    # ─────────────────────────────   events   ─────────────────────────────
    for ev_json in data["Events"]:
        sched_dt = _iso_to_dt(ev_json["Scheduled"])          # datetime obj
        race_setup = ev_json["RaceSetup"]
        sessions    = race_setup["Sessions"]

        # --- track lookup / creation -------------------------------------
        base_id   = race_setup["Track"]
        layout_id = race_setup.get("TrackLayout", "") or base_id
        variant_id = f"{base_id};{layout_id}"
        track_var = contentdata.get_track(variant_id) or \
                    contentdata.create_basic_track(base_id, layout_id)

        # ▸ download‑URL  – only once
        if not getattr(track_var, "download_url", None):
            track_var.download_url = _scrape_download_url(
                f"{base_url}/track/{base_id}"
            )

        # ▸ map / preview images  – only if not scraped before
        if not (track_var.imagepath and Path(track_var.imagepath).is_file()):
            img_map = scrape_track_images(base_url, base_id)
            for var_id, local_path in img_map.items():
                v = contentdata.get_track(var_id)
                if v:
                    v.imagepath = local_path

        # --- figure out session lengths ----------------------------------
        practice_len    = sessions.get("PRACTICE", {}).get("Time", 0)
        qualifying_len  = sessions.get("QUALIFY", {}).get("Time", 0)
        race_one_len    = sessions.get("RACE",    {}).get("Time", 0)
        race_two_len    = sessions.get("RACE_2",  {}).get("Time", 0)
        double_race     = "ReversedGridRacePositions" in race_setup and race_setup["ReversedGridRacePositions"] == -1
        if race_two_len == 0 and double_race:
            race_two_len = race_one_len


        # --- build the Event object --------------------------------------
        ev = Event(
            name      = ev_json.get("Name", "") or track_var.name,  # fallback
            date      = sched_dt.date().isoformat(),
            track     = track_var,
            doublerace= double_race,
            practicelength   = practice_len,
            qualifyinglength = qualifying_len,
            raceonelength    = race_one_len,
            racetwolength    = race_two_len,
            location  = track_var.country or "",   # if you have it
            sessionstarttime = _to_discord_timestamp(sched_dt, "f"),
            track_download_link = track_var.download_url,
        )

        if race_one_len == 0 and sessions["RACE"]["Laps"] > 0:
            ev.racelaps = sessions["RACE"]["Laps"]


        # extra realism sliders
        ev.fuelrate = race_setup["FuelRate"]
        ev.damage   = race_setup["DamageMultiplier"]
        ev.tirewear = race_setup["TyreWearRate"]

        champ.schedule.append(ev)
    champ.update_standings()
    return champ


# ────────────────────────── helpers ──────────────────────────
def _iso_to_dt(ts: str) -> datetime:
    """
    Convert ISO‑8601 strings like ``2025‑05‑05T14:00:00‑04:00`` to *aware*
    `datetime`s in UTC (feel free to change if you prefer local).
    """
    dt = datetime.fromisoformat(ts)
    return dt.astimezone(timezone.utc)
