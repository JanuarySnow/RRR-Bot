import math
import pandas as pd
from discord.ext import commands, tasks
from discord.ext.commands import Context
from dateutil.parser import parse
import json
from operator import itemgetter
import os
import io
import matplotlib.pyplot as plt
import matplotlib.dates as mdates 
from datetime import datetime
import matplotlib.ticker as mtick
import numpy as np
from fuzzywuzzy import fuzz 
from fuzzywuzzy import process
import content_data
import racer
import result
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from adjustText import adjust_text
from logger_config import logger
from collections import defaultdict, Counter
from dataclasses import dataclass
from typing import Iterable, Optional
from math import isfinite
from statistics import fmean
from datetime import datetime, timedelta
from collections import Counter
try:
    from scipy.stats import chi2
except ImportError:
    chi2 = None  # handle below with a tiny fallback if needed
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Iterable, Tuple, Optional, Set

def _iso_to_dt(s: str) -> datetime:
    # robust to "Z" suffix and naive strings
    if isinstance(s, str):
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    return s  # already a datetime

def _safe_round(x, n=2):
    return round(x, n) if x is not None else None


@dataclass
class RacerHistory:
    first_seen: datetime
    races: Set[datetime]  # all race datetimes (UTC/naive consistently)

class RetentionTracker:
    def __init__(self):
        self.histories: Dict[str, RacerHistory] = {}
    @staticmethod
    def _as_date(dt) -> datetime:
        # Ensure datetime; if you only have YYYY-MM-DD strings, parse them here.
        if isinstance(dt, datetime):
            return dt
        # Try common formats; adjust to your pipeline
        return datetime.fromisoformat(str(dt))

    def register_race(self, guid: str, race_dt) -> None:
        if not guid:
            return
        dt = self._as_date(race_dt)
        hist = self.histories.get(guid)
        if hist is None:
            self.histories[guid] = RacerHistory(first_seen=dt, races={dt})
        else:
            # keep earliest first_seen
            if dt < hist.first_seen:
                hist.first_seen = dt
            hist.races.add(dt)

    def to_jsonable(self) -> dict:
        """Return a JSON-safe dict representing the tracker."""
        out = []
        for guid, hist in self.histories.items():
            out.append({
                "guid": guid,
                "first_seen": hist.first_seen.isoformat(),
                "races": sorted(d.isoformat() for d in hist.races),
            })
        return {"version": 1, "histories": out}
    
    @classmethod
    def from_jsonable(cls, data: dict) -> "RetentionTracker":
        """Create a tracker from JSON-safe dict produced by to_jsonable()."""
        t = cls()
        items = data.get("histories", [])
        for rec in items:
            guid = rec["guid"]
            fs = datetime.fromisoformat(rec["first_seen"])
            races = set(datetime.fromisoformat(s) for s in rec.get("races", []))
            # Ensure first_seen is at least the min of races (defensive)
            if races:
                fs = min(fs, min(races))
            t.histories[guid] = RacerHistory(first_seen=fs, races=races or {fs})
        return t


    # ----- Cohort helpers -----
    @staticmethod
    def _ym(dt: datetime) -> str:
        return f"{dt.year:04d}-{dt.month:02d}"

    def _cohort_guids_by_month(self) -> Dict[str, List[str]]:
        cohorts = defaultdict(list)
        for guid, hist in self.histories.items():
            cohorts[self._ym(hist.first_seen)].append(guid)
        return cohorts

    # ----- Retention logic -----
    def _returned_within(self, hist: RacerHistory, horizon_days: int, min_extra_races: int = 1) -> bool:
        start = hist.first_seen
        end = start + timedelta(days=horizon_days)
        # Count races strictly after first_seen and <= end
        extra = sum(1 for d in hist.races if (d > start and d <= end))
        return extra >= min_extra_races

    def cohort_retention_table(
        self,
        horizons_days: Iterable[int] = (7, 30, 60, 90, 180),
        min_extra_races: int = 1,
        cohort_filter: Optional[Tuple[datetime, datetime]] = None,  # (start_inclusive, end_exclusive) on first_seen
    ) -> List[Dict]:
        """
        Returns list of {cohort:'YYYY-MM', new_count:int, r7:float, r30:float,...}
        If cohort_filter is provided, only cohorts whose month lies fully inside the filter are included.
        """
        cohorts = self._cohort_guids_by_month()
        rows = []
        for cohort, guids in sorted(cohorts.items()):
            # optional filter by absolute dates
            if cohort_filter:
                c_year, c_month = map(int, cohort.split("-"))
                c_first = datetime(c_year, c_month, 1)
                # end-of-month start for next month
                c_next = datetime(c_year + (1 if c_month == 12 else 0),
                                  1 if c_month == 12 else c_month + 1, 1)
                if not (c_first >= cohort_filter[0] and c_next <= cohort_filter[1]):
                    continue

            new_count = len(guids)
            row = {"cohort": cohort, "new_count": new_count}
            if new_count == 0:
                for h in horizons_days:
                    row[f"r{h}"] = 0.0
                rows.append(row)
                continue

            for h in horizons_days:
                retained = sum(
                    1 for g in guids
                    if self._returned_within(self.histories[g], h, min_extra_races=min_extra_races)
                )
                row[f"r{h}"] = retained / new_count
            rows.append(row)
        return rows

    def window_retention_compare(
        self,
        new_start: datetime, new_end: datetime,   # include [new_start, new_end)
        prev_start: datetime, prev_end: datetime,
        horizon_days: int = 180,
        min_extra_races: int = 1,
    ) -> Dict[str, float]:
        """
        Compares % of *new* racers (first_seen in window) who returned within horizon.
        Returns dict with {new_rate, prev_rate, delta_pp}.
        """
        def rate_for_window(s: datetime, e: datetime) -> float:
            cohort_guids = [
                g for g, hist in self.histories.items()
                if (hist.first_seen >= s and hist.first_seen < e)
            ]
            if not cohort_guids:
                return 0.0
            retained = sum(
                1 for g in cohort_guids
                if self._returned_within(self.histories[g], horizon_days, min_extra_races)
            )
            return retained / len(cohort_guids)

        new_rate = rate_for_window(new_start, new_end)
        prev_rate = rate_for_window(prev_start, prev_end)
        return {
            "new_rate": new_rate,
            "prev_rate": prev_rate,
            "delta_pp": (new_rate - prev_rate) * 100.0
        }

@dataclass
class RacerSafetySnapshot:
    name: str
    incidents: float
    km: float

def league_mean_incidents_per_km(racers: Iterable[RacerSafetySnapshot], min_km: float = 50.0) -> float:
    pool = [r for r in racers if r.km >= min_km and r.incidents >= 0]
    if not pool:
        return 0.5 / 100.0  # fallback 0.005 per km (1 per 200 km)
    # mean of individual rates, weighted by km is usually better:
    total_inc = sum(r.incidents for r in pool)
    total_km  = sum(r.km for r in pool)
    return (total_inc / total_km) if total_km > 0 else 0.005

def eb_adjusted_rate(incidents: float, km: float, league_mean: float, K0: float = 500.0) -> float:
    # incidents per km with pseudokilometers K0 at league_mean
    k0 = league_mean * K0
    denom = km + K0
    if denom <= 0:
        return league_mean  # if truly zero exposure
    return (incidents + k0) / denom

# add next to poisson_upper_rate
def poisson_lower_rate(incidents: float, km: float, conf: float = 0.95) -> float | None:
    """
    Lower (1-α) bound for λ (incidents per km) under Poisson with exposure km.
    Returns None if km <= 0.
    """
    if km <= 0:
        return None
    x = max(0, int(round(incidents)))
    if chi2 is not None:
        # For x = 0, this gives 0 (chi2.ppf(α, 0) = 0)
        return 0.5 * chi2.ppf(1.0 - conf, 2 * x) / km
    # Fallback approximation without SciPy:
    if x == 0:
        return 0.0
    return max(0.0, (x - 0.5) / km)  # crude; install scipy for accuracy


def poisson_upper_rate(incidents: float, km: float, conf: float = 0.95) -> Optional[float]:
    # Returns the (1-α) upper confidence bound for λ (incidents per km)
    if km <= 0:
        return None
    x = max(0, int(round(incidents)))
    if chi2 is not None:
        return 0.5 * chi2.ppf(conf, 2*(x + 1)) / km
    # Very small fallback approximation if SciPy not available:
    # For x=0, upper ≈ -ln(1-conf)/km; for x>0, add a small continuity bump.
    import math
    if x == 0:
        return -math.log(1.0 - conf) / km
    return (x + 1.5) / km  # crude; install scipy for accuracy

class parser():
    GT3_IDS = {
        "ks_audi_r8_lms_2016", "bmw_z4_gt3", "ks_ferrari_488_gt3", "ks_lamborghini_huracan_gt3", "GT3",
        "ks_mclaren_650_gt3", "ks_mercedes_amg_gt3", "ks_nissan_gtr_gt3",
        "ks_porsche_911_gt3_r_2016", "amr_v8_vantage_gt3_sprint_acc", "ks_porsche_911_gt3_rs", "lotus_evora_gtc",
        "vm_bmw_m4_gt3", "vm_mclaren_720_gt3_acc", "vm_lexus_rcf_gt3_acc", "vm_amr_v8_vantage_gt3_acc",
        "vm_crsh_porsche_gt3_2019_endurance", "vm_amg_evo_gt3_2020", "vm_bentley_continental_gt3_18_acc",
        "vm_ferrari_488_gt3_acc", "vm_bmw_m4_gt3", "vm_wec_gte_chevrolet_corvette_c8r", "vm_mclaren_720_gt3_acc",
        "vm_lexus_rcf_gt3_acc", "vm_crsh_porsche_gt3_2019_endurance", "vm_amg_evo_gt3_2020",
        "ks_nissan_gtr", "ks_porsche_911_gt3_r_2016", "ks_porsche_911_gt3_rs", "ks_lamborghini_huracan_gt3",
        "ks_mclaren_650_gt3", "ks_mercedes_amg_gt3", "ks_audi_r8_lms_2016", "ks_ferrari_488_gt3",
        "mclaren_mp412c_gt3", "ks_porsche_911_gt3_cup_2017", "mercedes_sls_gt3", "mclaren_mp412c_gt3",
        "honda_nsx_gt3_endurance_acc", "ks_audi_r8_lms_2016", "ks_ferrari_488_gt3", "ks_lamborghini_huracan_gt3",
        "ks_mclaren_650_gt3", "ks_mercedes_amg_gt3", "ks_nissan_gtr_gt3", "ks_audi_r8_lms", "ks_audi_r18_etron_quattro"
    }
    GT4_IDS = {
        "ks_porsche_cayman_gt4_clubsport", "ks_maserati_gt_mc_gt4",
        "gt4_mercedes_amg_evo", "gt4_ginetta_g55", "gt4_bmw_m4_g82",
        "gt4_toyota_supra", "gt4_saleen_s1", "gt4_ford_mustang", "gt4_bmw_m4",
        "gt4_aston_martin_vantage", "gt4_ktm_xbow", "gt4_mclaren_570s", "gt4_alpine_a110",
        "gt4_camaro", "gt4_sin_r1", "gt4_ginetta_g55", "gt4_toyota_supra", "gt4_ford_mustang",
        "gt4_alpine_a110", "gt4_porsche_cayman_718", "lotus_2_eleven_gt4", "ks_maserati_gt_mc_gt4",
        "lotus_2_eleven_gt4", "ks_porsche_cayman_gt4_clubsport", "ks_maserati_gt_mc_gt4",
        "gt4_camaro", "gt4_sin_r1", "gt4_aston_martin_vantage", "fsr_ford_mustang_s", "fsr_toyota_s"
    }
    TCR_IDS = {
        "acme_hyundai_i20_rally1_22_gravel", "acme_ford_puma_rally1_22", "acme_ford_puma_rally1_22_gravel",
        "acme_toyota_yaris_rally1_22", "acme_toyota_yaris_rally1_22_gravel", "acme_hyundai_i20_rally1_22",
        "tcr_audi_rs3_dsg", "tcr_cupra_leon_dsg", "tcr_seat_leon_eurocup", "tcr_volkswagen_golf_gti_mk7dsg",
        "pm3dm_volvo_s40_btcc", "pm3dm_nissan_primera_btcc", "fw_cupra_tcr_2024", "fw_elantra_tcr_2024",
        "tcr_audi_rs3_dsg", "tcr_cupra_leon_dsg", "ks_audi_r8_lms", "tcr_seat_leon_eurocup"
    }
    TRACK_SPECIALS = {
        "pagani_zonda_r", "ferrari_599xxevo", "pg_euronascarford", "pg_euronascarfj", "pg_euronascar"
    }
    ALWAYS_COLLAPSE = {"GT3", "GT4", "TCR", "TRACK_SPECIALS"}
    SKIP_MODELS = {"ford_transit", "tmc_dingus_d2004", "ferrari_f40", "tky_chair"}
    ZERO = "00000000-0000-0000-0000-000000000000"
    def __init__(self):
        self.raceresults = []

        self.racers = {} # guid to racer object
        self.usedtracks = {} # guid to trackvariant object
        self.usedcars = {}
        self.safety_rankings = []
        self.safety_rating_rankings = []
        self.safety_rankingsperkm = []
        self.logger = logger
        self.safety_rankingsgt3 = []
        self.safety_rankingsmx5 = []

        self.wins_rankings = []
        self.wins_rankingsgt3 = []
        self.wins_rankingsmx5 = []
        self.podiums_rankings = []
        self.podiums_rankingsgt3 = []
        self.podiums_rankingsmx5 = []

        self.elorankings = []
        self.qualifyingrankings = []
        self.laptimeconsistencyrankings = []
        self.laptimeconsistencyrankingsmx5 = []
        self.laptimeconsistencyrankingsgt3 = []
        self.positionconsistencyrankings = []
        self.pacerankingsmx5 = []
        self.pacerankingsgt3 = []
        self.averageelorankingsovertime = {}
        self.contentdata = None
        self.championships = {}
        self.completedchampionships = []
        self.blacklist = ["2025_1_4_21_37_RACE.json", "2025_1_4_22_2_RACE.json",
                          "2024_12_21_21_58_RACE.json", "2024_12_21_21_32_RACE.json",
                          "2025_2_17_20_30_RACE.json", "2025_2_17_20_57_RACE.json",
                          "2025_2_22_22_0_RACE.json", "2025_2_22_21_35_RACE.json", "2025_4_8_19_42_RACE.json"]
        self.retention = RetentionTracker()


    def get_all_result_files(self):
        datalist = []

    # Traverse the entire directory tree
        for root, dirs, files in os.walk("results/"):
            if "testserver" in root.split(os.sep):  # Check if "testserver" is part of the path
                continue

            for filename in files:
                if filename.endswith(".json"):
                    if filename in self.blacklist:
                        continue
                    filepath = os.path.join(root, filename)
                    with open(filepath, encoding="utf8") as f:
                        data = json.load(f)
                        if data.get("TrackName") == "sportsdrome_figure_8_north":
                            continue
                        if data.get("TrackName") == "sportsdrome_figure_8_south":
                            continue
                        if data.get("Type") == "RACE":
                            data["Filename"] = filename
                            # Parse the date string to a datetime object
                            race_time = datetime.fromisoformat(data["Date"].replace("Z", "+00:00"))
                            # Determine the region based on the race time
                            data["directory"] = os.path.basename(os.path.dirname(filepath))
                            if race_time.hour < 24 and race_time.hour >= 12:
                                data["Region"] = "EU"
                            else:
                                data["Region"] = "NA"
                            datalist.append(data)
        return datalist
    
    
    async def add_one_result(self, filepath, filename, server, url):
        logger.info("adding one result " + filename)
        logger.info("from url " + url)
        with open(filepath, encoding="utf8") as f:
            if "testserver" in filepath.split(os.sep):
                logger.info(f"Skipping file in 'testserver' folder: {filename}")
                return
            logger.info("Parsing file: " + filepath)
            data = json.load(f)
            if data.get("Type") == "RACE":
                if self.ismulticlass(data):
                    self.handle_potential_multiclass(data, filepath, filename, server, url)
                    return
                
                data["Filename"] = filename
                resultobj = result.Result()
                resultobj.filename = data["Filename"]
                resultobj.server = server
                resultobj.url = url
                resultobj.directory = os.path.basename(os.path.dirname(filepath))
                # Parse the date string to a datetime object
                race_time = datetime.fromisoformat(data["Date"].replace("Z", "+00:00"))
                # Determine the region based on the race time
                if race_time.hour < 24 and race_time.hour >= 12:
                    data["Region"] = "EU"
                else:
                    data["Region"] = "NA"
                logger.info("Region determined: " + data["Region"])
                logger.info("parsing result for date " + data["Date"])
                self.parse_one_result(resultobj, data)

                for car in data["Result"]:
                    if not car["DriverGuid"] or car["DriverGuid"] == "" :
                        continue
                    guid = car["DriverGuid"]
                    racer = self.racers[guid]
                    racer.calculate_averages()
        self.calculate_raw_pace_percentages_for_all_racers()
        self.calculate_rankings()
        logger.info("done adding one result")

    def _logical_class_for_model(self, model: str) -> str | None:
        """Map a car model name to a logical class bucket label, or None if it should be skipped."""
        if not model:
            return None
        m = model.lower()
        if m in self.SKIP_MODELS:
            return None
        if m in self.GT3_IDS:
            return "GT3"
        if m in self.TCR_IDS or "_tcr_" in m:
            return "TCR"
        if m in self.GT4_IDS or "gt4" in m:
            return "GT4"
        if m.startswith("etrc_"):
            return "TRUCK"
        if m in self.TRACK_SPECIALS:
            return "TRACK_SPECIALS"
        # Default: treat each unique model as its own class bucket
        return m

    def _effective_car_classid(self, entry: dict) -> str | None:
        """
        Pick a stable ClassID for a car entry:
        - Prefer outer ClassID if present and not ZERO; otherwise fall back to Driver.ClassID if available.
        """
        outer = entry.get("ClassID")
        if outer and outer != self.ZERO:
            return outer
        inner = (entry.get("Driver") or {}).get("ClassID")
        if inner and inner != self.ZERO:
            return inner
        return None

    def get_valid_event_classids(self, data: dict) -> set[str]:
        """
        Determine all ClassIDs that appear at least twice in the event's car list.
        This can be used to identify valid class groupings in an event.
        """
        from collections import Counter
        counts = Counter()
        for car in data.get("Cars", []) or []:
            cid_outer = car.get("ClassID")
            cid_inner = (car.get("Driver") or {}).get("ClassID")
            for cid in (cid_outer, cid_inner):
                if cid and cid != self.ZERO:
                    counts[cid] += 1
        return {cid for cid, count in counts.items() if count >= 2}

    def _build_split_context(self, data: dict) -> dict:
        """
        Build mappings needed for class splitting:
        - carid2logical: maps CarId to its logical class (bucket label)
        - guid2carid: maps Driver GUID to CarId
        - classid2logical_majority: for each valid ClassID (appearing on >=2 cars),
        determine the majority logical class of those cars.
        """
        from collections import defaultdict, Counter
        carid2logical = {}
        guid2carid = {}
        classid_counts = Counter()
        classid_logical_counts = defaultdict(lambda: Counter())

        for car in data.get("Cars", []) or []:
            if not isinstance(car, dict):
                continue
            drv = car.get("Driver") or {}
            guid = drv.get("Guid")
            name = drv.get("Name", "")
            car_id = car.get("CarId")
            model = (car.get("Model") or "").lower()
            if not guid or car_id is None:
                continue  # skip spectators or invalid entries
            logical = self._logical_class_for_model(model)
            if not logical:
                continue  # skip models that are explicitly skipped
            if name == "Broadcast" or name == "RRR":
                # Skip broadcast/pace car entries from context
                continue
            # Populate mapping for this car
            carid2logical[car_id] = logical
            guid2carid[guid] = car_id
            # Tally ClassID usage (consider both outer and inner class IDs)
            cid_outer = car.get("ClassID")
            cid_inner = drv.get("ClassID")
            for cid in (cid_outer, cid_inner):
                if cid and cid != self.ZERO:
                    classid_counts[cid] += 1
                    classid_logical_counts[cid][logical] += 1

        # Only consider ClassIDs that appear on 2 or more cars as separate classes
        valid_classids = {cid for cid, cnt in classid_counts.items() if cnt >= 2}
        # Determine the predominant logical class for each valid ClassID
        classid2logical_majority = {}
        for cid in valid_classids:
            if classid_logical_counts[cid]:
                # Choose the logical class with the highest count for this ClassID
                majority_class = max(classid_logical_counts[cid].items(), key=lambda kv: kv[1])[0]
                classid2logical_majority[cid] = majority_class

        return {
            "carid2logical": carid2logical,
            "guid2carid": guid2carid,
            "classid2logical_majority": classid2logical_majority
        }

    def _logical_class_of_item(self, item: dict, ctx: dict) -> str | None:
        """
        Determine the logical class for an arbitrary entry (e.g., a lap, result, or event entry).
        Priority:
        1. If CarId is present, use carid2logical mapping.
        2. Else if DriverGuid is present, resolve to CarId then logical class.
        3. Else if ClassID is a valid class (with a majority class in context), use that.
        4. If none apply, return None.
        """
        carid2logical = ctx["carid2logical"]
        guid2carid = ctx["guid2carid"]
        classid2logical_majority = ctx["classid2logical_majority"]

        # 1) CarId -> direct logical class
        car_id = item.get("CarId")
        if car_id is not None and car_id in carid2logical:
            return carid2logical[car_id]
        # 2) DriverGuid -> CarId -> logical class
        guid = item.get("DriverGuid") or (item.get("Driver") or {}).get("Guid")
        if guid and guid in guid2carid:
            resolved_car = guid2carid[guid]
            if resolved_car in carid2logical:
                return carid2logical[resolved_car]
        # 3) ClassID -> majority logical class (only if ClassID is considered valid)
        cid = item.get("ClassID")
        if cid and cid in classid2logical_majority:
            logical = classid2logical_majority[cid]
            return logical
        # 4) Unable to determine class
        return None

    def get_classes_from_result(self, data: dict) -> set[str]:
        """
        Decide if this result is multi-class and return the set of logical bucket labels to split on.
        Rules enforced:
        - Ignore cars with no GUID, skip models, or driver name in {"Broadcast","RRR"}.
        - Effective ClassID = outer if non-zero else inner if non-zero; otherwise ignored.
        - A ClassID is valid only if it appears on >=2 cars AND that class turned >=1 lap.
        - After validating ClassIDs, collapse them into logical buckets (GT3/GT4/TCR/TRUCK/...).
        - If all valid ClassIDs land in the same bucket (skill split), treat as single-class.
        - Otherwise return the set of buckets.
        """
        from collections import defaultdict, Counter

        ZERO = self.ZERO

        # ---- 1) Collect eligible participants ----
        participants = []
        for car in (data.get("Cars") or []):
            if not isinstance(car, dict):
                continue
            drv = car.get("Driver") or {}
            guid = drv.get("Guid")
            name = (drv.get("Name") or "").strip()
            model = (car.get("Model") or "").lower()
            if not guid:
                continue
            bucket = self._logical_class_for_model(model)
            if not bucket:
                continue  # skip models you flagged (spectator/safety etc.)
            if name in {"Broadcast", "RRR"}:
                continue  # skip broadcast/pace entries entirely
            participants.append(car)

        if not participants:
            return set()

        # ---- 2) Group by effective ClassID (outer non-zero; else inner non-zero) ----
        classid2cars: dict[str, list] = defaultdict(list)
        carid2cid: dict[int, str] = {}
        guid2cid: dict[str, str] = {}

        for car in participants:
            cid = self._effective_car_classid(car)  # returns None when ZERO/absent
            if not cid:
                continue
            classid2cars[cid].append(car)
            car_id = car.get("CarId")
            drv = car.get("Driver") or {}
            guid = drv.get("Guid")
            if car_id is not None:
                carid2cid[car_id] = cid
            if guid:
                guid2cid[guid] = cid

        if not classid2cars:
            # No non-zero ClassIDs to consider → single class
            return set()

        # ---- 3) Filter ClassIDs by ">= 2 cars" ----
        candidate_cids = {cid for cid, cars in classid2cars.items() if len(cars) >= 2}
        if len(candidate_cids) < 2:
            # Fewer than two valid ClassIDs -> single class
            return set()

        # ---- 4) Count laps per ClassID (only among the candidate ClassIDs) ----
        laps = data.get("Laps") or []
        laps_by_cid = Counter()

        for lap in laps:
            if not isinstance(lap, dict):
                continue
            # prefer lap.ClassID if it is non-zero
            lc = lap.get("ClassID")
            resolved = None
            if lc and lc != ZERO and lc in candidate_cids:
                resolved = lc
            else:
                # fallback via CarId
                car_id = lap.get("CarId")
                if car_id is not None:
                    cid = carid2cid.get(car_id)
                    if cid in candidate_cids:
                        resolved = cid
                if not resolved:
                    # fallback via DriverGuid
                    guid = lap.get("DriverGuid")
                    if guid:
                        cid = guid2cid.get(guid)
                        if cid in candidate_cids:
                            resolved = cid
            if resolved:
                laps_by_cid[resolved] += 1

        # Keep ClassIDs that actually turned >=1 lap
        valid_cids = {cid for cid in candidate_cids if laps_by_cid.get(cid, 0) > 0}
        if len(valid_cids) < 2:
            return set()

        # ---- 5) Collapse valid ClassIDs into logical buckets ----
        # Majority bucket per CID; then see how many distinct buckets remain.
        def bucket_of_model(model: str) -> str | None:
            return self._logical_class_for_model(model)

        cid_major_bucket: dict[str, str] = {}
        for cid in valid_cids:
            bcounts = Counter()
            for car in classid2cars[cid]:
                b = bucket_of_model(car.get("Model", ""))
                if b:
                    bcounts[b] += 1
            if not bcounts:
                # If somehow no bucket, skip this CID (shouldn't happen due to participant filter)
                continue
            cid_major_bucket[cid] = max(bcounts.items(), key=lambda kv: kv[1])[0]

        buckets = set(cid_major_bucket.values())
        # If everyone maps to the same bucket (e.g., GT4 skill split), treat as single-class
        if len(buckets) < 2:
            return set()

        return buckets


    def ismulticlass(self, data: dict) -> bool:
        """
        Check if the given result data represents a multi-class event.
        Prints the detected classes and returns True if more than one class is present.
        """
        classes = self.get_classes_from_result(data)
        if len(classes) > 1:
            return True
        return False

    def filter_result_json_for_class(self, data: dict, bucket_label: str, ctx: dict) -> dict:
        """
        Create a filtered copy of the result JSON data containing only the entries 
        belonging to the specified class (bucket_label).
        """
        filtered = {k: v for k, v in data.items() if k not in ("Cars", "Result", "Laps", "Events")}
        cars   = data.get("Cars")   or []
        result = data.get("Result") or []
        laps   = data.get("Laps")   or []
        events = data.get("Events") or []

        # Filter Cars: keep only those in the desired bucket
        filtered["Cars"] = [
            car for car in cars 
            if isinstance(car, dict)
            and self._logical_class_for_model(car.get("Model")) == bucket_label
            and (car.get("Driver") or {}).get("Guid")
        ]

        # Helper to filter lists by bucket using the context
        def keep_items(items):
            return [
                item for item in (items or []) 
                if isinstance(item, dict) and self._logical_class_of_item(item, ctx) == bucket_label
            ]

        filtered["Result"] = keep_items(result)
        filtered["Laps"]   = keep_items(laps)

        # Filter Events: only keep events where both parties belong to this bucket
        filtered_events = []
        carid2logical = ctx["carid2logical"]
        for evt in events or []:
            if not isinstance(evt, dict):
                continue
            c1 = evt.get("CarId")
            oc = evt.get("OtherCarId")
            b1 = carid2logical.get(c1) or self._logical_class_of_item(evt, ctx)
            b2 = carid2logical.get(oc) if oc is not None else b1
            if b2 is None and oc is not None:
                # If OtherCarId wasn't directly in carid2logical, try to resolve it
                b2 = self._logical_class_of_item({"CarId": oc}, ctx)
            if b1 == bucket_label and b2 == bucket_label:
                filtered_events.append(evt)
        filtered["Events"] = filtered_events

        return filtered

    def handle_potential_multiclass_from_refresh(self, data: dict):
        """
        If the result is multi-class, handle each class separately (for data refresh scenario).
        """
        split_buckets = sorted(self.get_classes_from_result(data))  # e.g., ["GT3", "TCR", ...]
        if len(split_buckets) < 2:
            # Single-class, nothing to do
            return

        ctx = self._build_split_context(data)
        for bucket_label in split_buckets:
            class_data = self.filter_result_json_for_class(data, bucket_label, ctx)
            # Print class summary: number of racers and laps in this class
            num_racers = len(class_data.get("Cars", []))
            laps_by_driver = {}
            for lap in class_data.get("Laps", []):
                if not isinstance(lap, dict):
                    continue
                guid = lap.get("DriverGuid") or (lap.get("Driver") or {}).get("Guid")
                if guid:
                    laps_by_driver[guid] = laps_by_driver.get(guid, 0) + 1
            max_laps = max(laps_by_driver.values()) if laps_by_driver else 0
            # Prepare class-specific data for parsing
            class_data["Filename"] = f"{data.get('Filename', '')}_{bucket_label}"
            class_data["directory"] = data.get("directory", "")
            class_data["ChampionshipID"] = data.get("ChampionshipID", "")
            # Create a result object and parse it for this class
            resultobj = result.Result()
            resultobj.filename = class_data["Filename"]
            resultobj.directory = class_data["directory"]
            resultobj.championshipid = class_data["ChampionshipID"]
            self.parse_one_result(resultobj, class_data)
            # Integrate updated Elo or other metrics as needed
            self.add_average_elo_step(data["Date"])

    def handle_potential_multiclass(self, data: dict, filepath: str, filename: str, server, url):
        """
        If the result is multi-class, handle each class separately (for initial file parsing scenario).
        """
        split_buckets = sorted(self.get_classes_from_result(data))  # e.g., ["GT3", "GT4", ...]
        if len(split_buckets) < 2:
            # Single-class, nothing to do
            return

        ctx = self._build_split_context(data)
        for bucket_label in split_buckets:
            class_data = self.filter_result_json_for_class(data, bucket_label, ctx)
            # Print class summary: number of racers and laps in this class
            num_racers = len(class_data.get("Cars", []))
            laps_by_driver = {}
            for lap in class_data.get("Laps", []):
                if not isinstance(lap, dict):
                    continue
                guid = lap.get("DriverGuid") or (lap.get("Driver") or {}).get("Guid")
                if guid:
                    laps_by_driver[guid] = laps_by_driver.get(guid, 0) + 1
            max_laps = max(laps_by_driver.values()) if laps_by_driver else 0
            # Prepare class-specific data for parsing
            class_data["Filename"] = f"{filename}_{bucket_label}"
            # Determine region (example: EU vs NA based on time of day)
            try:
                from datetime import datetime
                race_dt = datetime.fromisoformat(data["Date"].replace("Z", "+00:00"))
                class_data["Region"] = "EU" if 12 <= race_dt.hour < 24 else "NA"
            except Exception:
                class_data["Region"] = ""
            class_data["directory"] = os.path.basename(os.path.dirname(filepath))
            # Create a result object and parse it for this class
            resultobj = result.Result()
            resultobj.filename = class_data["Filename"]
            resultobj.server   = server
            resultobj.url      = url
            resultobj.directory = class_data["directory"]
            self.parse_one_result(resultobj, class_data)
            # Update racer stats for this class result
            for car in class_data.get("Result", []) or []:
                guid = car.get("DriverGuid")
                if guid:
                    self.racers[guid].calculate_averages()

        # Recalculate overall pace percentages and rankings after adding all classes
        self.calculate_raw_pace_percentages_for_all_racers()
        self.calculate_rankings()


    
    def get_cars_and_racers_from_result(self, resultobject, data):
        
        for car in data["Result"]:
            if not car["DriverGuid"] or car["DriverGuid"] == "" :
                return
            guid = car["DriverGuid"]
            racerobj = None
            newname = ""
            if guid == '76561198103373944':
                newname = "Buggy"
            else:
                newname = car["DriverName"]
            if not guid in self.racers:
                racerobj = racer.Racerprofile(newname, guid)
                self.racers[guid] = racerobj
            else:
                self.racers[guid].name = newname
                racerobj = self.racers[guid]
            
            race_dt = data["Date"]  # ensure this is a datetime or ISO string; the tracker parses ISO
            self.retention.register_race(guid, race_dt)
            carid = car["CarModel"]
            car = None
            if not carid in self.usedcars:
                newcar = self.contentdata.get_car(carid)
                if newcar == None:
                    newcar = self.contentdata.create_basic_car(carid)
                self.usedcars[carid] = newcar
                car = newcar
            else:
                car = self.usedcars[carid]
            entry = result.Entry(racerobj, car, resultobject.track, data["Date"])
            resultobject.entries.append(entry)

    def get_track_from_result(self, result, data):
        baseid = data["TrackName"]
        if "TrackConfig" in data and data["TrackConfig"] != "":
            # this is a variant
            trackconfig = data["TrackConfig"]
            combinedid = baseid + ";" + trackconfig
            trackvariant = None
            if combinedid in self.usedtracks:
                trackvariant = self.usedtracks[combinedid]
            else:
                trackvariant = self.contentdata.get_track(combinedid)
                if trackvariant == None:
                    trackvariant = self.contentdata.create_basic_track(baseid, data["TrackConfig"])
                self.usedtracks[combinedid] = trackvariant
            result.track = trackvariant
            base = trackvariant.parent_track
            base.timesused += 1
        else:
            #this is a track without a variant id
            combinedid = baseid + ";" + baseid
            if combinedid in self.usedtracks:
                result.track = self.usedtracks[combinedid]
                base = result.track.parent_track
                base.timesused += 1
            else:
                # not in used tracks
                #does it exist in content library?
                trackvariant = self.contentdata.get_track(combinedid)
                if trackvariant == None:
                    trackvariant = self.contentdata.create_basic_track(baseid, "")
                    self.usedtracks[combinedid] = trackvariant
                    result.track = trackvariant
                    base = result.track.parent_track
                    base.timesused += 1
                else:
                    self.usedtracks[combinedid] = trackvariant
                    result.track = trackvariant
                    base = result.track.parent_track
                    base.timesused += 1
        
        if result.track == None:
            logger.info("found a NONE track")   
            logger.info("track id in result file = " + data["TrackName"])
            if "TrackConfig" in data and data["TrackConfig"] != "":
                logger.info("and config = " + data["TrackConfig"])
            else:
                logger.info("no config")

    
    def parse_one_result(self, result, data):
        result.date = data["Date"]
        result.issecond = self.is_second_race(result)
        result.is_endurance_race(data)
        result.set_region(data)
        result.calculate_is_mx5_or_gt3(data)
        result.get_race_duration(data)
        self.get_track_from_result(result, data)
        self.get_cars_and_racers_from_result(result, data)
        result.calculate_laps(data)
        result.calculate_positions(data)
        result.calculate_collisions(data)
        result.finalize_entries()
        result.date = data["Date"]
        self.raceresults.append(result)

    def refresh_all_data(self):
        self.clear_old_data()
        self.contentdata = content_data.Contentdata()
        logger.info("loaded content data")
        self.contentdata.load_cars()
        logger.info("loaded cars")
        self.contentdata.load_tracks()
        logger.info("loaded tracks")
        logger.info("loaded parser")
        datalist = self.get_all_result_files()
        datalist = sorted(datalist, key=lambda d: d["Date"])
        for data in datalist:
            if self.ismulticlass(data):
                self.handle_potential_multiclass_from_refresh(data)
                continue
            resultobj = result.Result()
            resultobj.filename = data["Filename"]
            resultobj.directory = data["directory"]
            resultobj.championshipid = data["ChampionshipID"]
            self.parse_one_result(resultobj, data)
            self.add_average_elo_step(data["Date"])
        for elem in self.racers.keys():
            racer = self.racers[elem]
            racer.calculate_averages()
        self.calculate_raw_pace_percentages_for_all_racers()
        self.calculate_rankings()
        self.loadtrackratings()