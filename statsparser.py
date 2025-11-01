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


    def get_summary_last_races(self, racer, num):
        """
        Returns a *ordered list* of tuples:
        [
            (result, {
                "position": int,
                "rating_change": float,
                "sr_change": float | None,   # delta SR for that race
            }),
            ...
        ]
        newest first.
        """
        # Sort rating changes (historyofratingchange: {result -> delta})
        rated = sorted(
            racer.historyofratingchange.items(),
            key=lambda kv: _iso_to_dt(kv[0].date),
            reverse=True
        )
        last = rated[:num]

        # Precompute SR 'before' for each race date using the SR timeline
        # safetyratingplot: {entry.date(str ISO): sr_after(float)}
        sr_points = sorted(
            racer.safetyratingplot.items(),
            key=lambda kv: _iso_to_dt(kv[0])  # kv[0] is ISO date string key
        )
        prev_sr_by_date = {}
        prev = None
        for date_str, sr_after in sr_points:
            prev_sr_by_date[date_str] = prev  # SR before this race
            prev = sr_after

        out = []
        for result, rating_change in last:
            pos = result.get_position_of_racer(racer)
            car = result.get_car_of_racer(racer)
            date_key = result.date  # matches keys stored in safetyratingplot
            sr_after  = racer.safetyratingplot.get(date_key)
            sr_before = prev_sr_by_date.get(date_key)
            sr_delta  = _safe_round(sr_after - sr_before, 2) if (sr_after is not None and sr_before is not None) else None
            out.append((result, {
                "position": pos,
                "car": car,
                "rating_change": _safe_round(rating_change, 2),
                "sr_change": sr_delta,
            }))
        return out

    
    def get_racer_name(self,guid:str):
        for racerkey in self.racers.keys():
            racer = self.racers[racerkey]
            if guid == racer.guid:
                return racer.name
        return None
    
    def get_racer(self,guid:str):
        for racerkey in self.racers.keys():
            racer = self.racers[racerkey]
            if guid == racer.guid:
                return racer
        return None
    
    def get_parent_track_from_variant(self, id:str):
        for track in self.contentdata.tracks:
            for variant in track.variants:
                if id == variant.id:
                    return track
        return None
    
    def get_track_name(self, id:str):

        for track in self.contentdata.tracks:
            if id == track.id:
                return track.highest_priority_name
        return None
    
    def get_track_variants(self, id:str):
        for track in self.contentdata.tracks:
            if id == track.id:
                return track.variants
        return None

    def calculate_rankings(self):
        safetydict = {}
        safetydictgt3 = {}
        safetydictmx5 = {}
        safetydictperkm = {}
        safetyratingdict = {}
        winsdict = {}
        podiumsdict = {}
        elodict = {}
        winsdictgt3 = {}
        podiumsdictgt3 = {}

        winsdictmx5 = {}
        podiumsdictmx5 = {}

        laptimeconsistencydict = {}
        laptimeconsistencydictgt3 = {}
        laptimeconsistencydictmx5 = {}
        raceconsistencydict = {}
        qualifyingdict = {}
        pacedictgt3 = {}
        pacedictmx5 = {}
        for racerid in self.racers.keys():
            racer = self.racers[racerid]
            if racer.numraces >= 5:
                safetydict[racer] = racer.averageincidents
                if racer.averageincidentsgt3:
                    safetydictgt3[racer] = racer.averageincidentsgt3
                if racer.averageincidentsmx5:
                    safetydictmx5[racer] = racer.averageincidentsmx5
                safetydictperkm[racer] = racer.incidentsperkm
                winsdict[racer] = racer.wins
                podiumsdict[racer] = racer.podiums
                elodict[racer] = racer.rating
                safetyratingdict[racer] = racer.safety_rating

                winsdictgt3[racer] = racer.gt3wins
                podiumsdictgt3[racer] = racer.gt3podiums
                qualifyingdict[racer] = racer.qualifyingrating

                winsdictmx5[racer] = racer.mx5wins
                podiumsdictmx5[racer] = racer.mx5podiums
                if racer.laptimeconsistency != None:
                    laptimeconsistencydict[racer] = racer.laptimeconsistency
                if racer.laptimeconsistencygt3:
                    laptimeconsistencydictgt3[racer] = racer.laptimeconsistencygt3
                if racer.laptimeconsistencymx5:
                    laptimeconsistencydictmx5[racer] = racer.laptimeconsistencymx5
                raceconsistencydict[racer] = racer.raceconsistency
                if racer.pace_percentage_gt3 and  racer.pace_percentage_gt3 > 0.0:
                    pacedictgt3[racer] = racer.pace_percentage_gt3
                if racer.pace_percentage_mx5 and racer.pace_percentage_mx5 > 0.0:
                    pacedictmx5[racer] = racer.pace_percentage_mx5

        self.safety_rankings = [racer for racer in sorted(safetydict, key=safetydict.get, reverse=False)]
        self.safety_rankingsgt3 = [racer for racer in sorted(safetydictgt3, key=safetydictgt3.get, reverse=False)]
        self.safety_rankingsmx5 = [racer for racer in sorted(safetydictmx5, key=safetydictmx5.get, reverse=False)]
        self.safety_rankingsperkm = [racer for racer in sorted(safetydictperkm, key=safetydictperkm.get, reverse=False)]
        self.wins_rankings = [racer for racer in sorted(winsdict, key=winsdict.get, reverse=True)]
        self.podiums_rankings = [racer for racer in sorted(podiumsdict, key=podiumsdict.get, reverse=True)]
        self.elorankings = [racer for racer in sorted(elodict, key=elodict.get, reverse=True)]
        self.safety_rating_rankings = [racer for racer in sorted(safetyratingdict, key=safetyratingdict.get, reverse=True)]
        self.qualifyingrankings = [racer for racer in sorted(qualifyingdict, key=qualifyingdict.get, reverse=True)]
        self.laptimeconsistencyrankings = [racer for racer in sorted(laptimeconsistencydict, key=laptimeconsistencydict.get, reverse=True)]
        self.laptimeconsistencyrankingsgt3 = [racer for racer in sorted(laptimeconsistencydictgt3, key=laptimeconsistencydictgt3.get, reverse=True)]
        self.laptimeconsistencyrankingsmx5 = [racer for racer in sorted(laptimeconsistencydictmx5, key=laptimeconsistencydictmx5.get, reverse=True)]
        self.positionconsistencyrankings = [racer for racer in sorted(raceconsistencydict, key=raceconsistencydict.get, reverse=True)]
        self.pacerankingsgt3 = [racer for racer in sorted(pacedictgt3, key=pacedictgt3.get, reverse=True)]
        self.pacerankingsmx5 = [racer for racer in sorted(pacedictmx5, key=pacedictmx5.get, reverse=True)]
        
        self.wins_rankingsgt3 = [racer for racer in sorted(winsdictgt3, key=winsdictgt3.get, reverse=True)]
        self.podiums_rankingsgt3 = [racer for racer in sorted(podiumsdictgt3, key=podiumsdictgt3.get, reverse=True)]
        

        self.wins_rankingsmx5 = [racer for racer in sorted(winsdictmx5, key=winsdictmx5.get, reverse=True)]
        self.podiums_rankingsmx5 = [racer for racer in sorted(podiumsdictmx5, key=podiumsdictmx5.get, reverse=True)]
    
    def custom_scorer(self, query, choices):
        scores = []
        for choice in choices:
            if query.lower() == choice.lower():
                # Prioritize exact matches
                score = 100
            elif query.lower() in choice.lower():
                # Prioritize significant partial matches
                score = 90
            else:
                # Use fuzzy matching
                score = fuzz.ratio(query, choice)
                # Boost score for exact keyword match
                if query.lower() in choice.lower():
                    score += 20
                # Check for whole word match
                query_words = query.lower().split()
                choice_words = choice.lower().split()
                for word in query_words:
                    if word in choice_words:
                        score += 5
            scores.append((choice, score))
        return scores
    
    def get_fastest_laps_for_racer(self, racer):
        fastestlist = {}
        for track in self.contentdata.tracks:
            for variant in track.variants:
                # Initialize the dictionary for this variant if not already present
                if variant not in fastestlist:
                    fastestlist[variant] = {}

                # Retrieve and store the fastest MX5 lap
                fastestmx5 = variant.get_fastest_lap_in_mx5()
                if fastestmx5 != None:
                    if fastestmx5.racerguid == racer.guid:
                        fastestlist[variant]["MX5"] = {
                            "time": fastestmx5.time,
                            "car": self.contentdata.get_car(fastestmx5.car)
                        }

                # Retrieve and store the fastest GT3 lap
                fastestgt3 = variant.get_fastest_lap_in_gt3()
                if fastestgt3 != None:
                    if fastestgt3.racerguid == racer.guid:
                        fastestlist[variant]["GT3"] = {
                            "time": fastestgt3.time,
                            "car": self.contentdata.get_car(fastestgt3.car)
                        }

        return fastestlist


    def most_improved(self, time):
        current_date = datetime.now()
        date_months_ago = current_date - relativedelta(months=time)
        improvementdict = {}  # racer, improvement

        for racerid in self.racers.keys():
            racer = self.racers[racerid]
            if racer.numraces >= 5:
                closest_date = None
                min_diff = None
                for date_str in racer.progression_plot.keys():
                    date = datetime.fromisoformat(date_str)
                    # Make date_months_ago timezone-aware if date is timezone-aware
                    if date.tzinfo is not None and date_months_ago.tzinfo is None:
                        date_months_ago = date_months_ago.replace(tzinfo=date.tzinfo)
                    diff = abs((date - date_months_ago).days)
                    if min_diff is None or diff < min_diff:
                        min_diff = diff
                        closest_date = date_str
                
                if closest_date is not None:
                    starting_rating = racer.progression_plot[closest_date]
                    current_rating = racer.rating
                    improvement = current_rating - starting_rating
                    improvementdict[racer] = improvement

        # Sort the racers by improvement in descending order and get the top 5
        sorted_improvement = sorted(improvementdict.items(), key=lambda x: x[1], reverse=True)
        top_5_improvements = dict(sorted_improvement[:5])

        return top_5_improvements

    def successfulgt3(self):
        gt3dict = {}
        for racerguid in self.racers.keys():
            racer = self.racers[racerguid]
            for entry in racer.entries:
                entrycar = entry.car
                position = entry.finishingposition
                rating = racer.rating
                if entrycar.id in result.gt3ids:
                    if entrycar not in gt3dict:
                        gt3dict[entrycar] = {'total_weighted_position': 0, 'total_rating': 0}
                    gt3dict[entrycar]['total_weighted_position'] += position * rating
                    gt3dict[entrycar]['total_rating'] += rating

        # Calculate the weighted average position for each car
        weighted_avg_positions = {car: data['total_weighted_position'] / data['total_rating'] for car, data in gt3dict.items()}
        # Sort by weighted average position
        sorted_cars = sorted(weighted_avg_positions.items(), key=lambda x: x[1])
        return sorted_cars

    
    def find_and_list_cars(self, input_string, threshold=60):
        # Convert input string to lower case
        input_string = input_string.lower()
        
        # Collect all track variants from contentdata
        all_cars = self.contentdata.cars

        # Apply custom scorer to prioritize exact matches and whole word matches
        scores = self.custom_scorer(input_string, [car.name for car in all_cars])
        
        # Find high confidence matches
        matches = [(name, score) for name, score in scores if score >= threshold]

        # Sort matches by score in descending order
        matches.sort(key=lambda x: x[1], reverse=True)
        # Get the parent track and list all its variants
        result = []
        matchesfound = 0
        for match in matches:
            car_name = match[0]
            for car in all_cars:
                if car.name == car_name:
                    result.append({
                        'input_match': car.name,
                        'id': car.id,
                        'confidence': match[1]
                    })
                    if match[1] == 100:
                        matchesfound = 3
                    matchesfound += 1
                    break
            if matchesfound >= 3:
                break
        return result

    def find_and_list_variants(self, input_string, threshold=60):
        # Convert input string to lower case
        input_string = input_string.lower()
        
        # Collect all track variants from contentdata
        all_tracks = self.contentdata.tracks

        # Apply custom scorer to prioritize exact matches and whole word matches
        scores = self.custom_scorer(input_string, [variant.highest_priority_name.lower() for variant in all_tracks])
        
        # Find high confidence matches
        matches = [(name, score) for name, score in scores if score >= threshold]

        # Sort matches by score in descending order
        matches.sort(key=lambda x: x[1], reverse=True)
        # Get the parent track and list all its variants
        result = []
        matchesfound = 0
        for match in matches:
            track_name = match[0]
            for track in all_tracks:
                if track.highest_priority_name.lower() == track_name:
                    result.append({
                        'input_match': track.highest_priority_name,
                        'id': track.id,
                        'confidence': match[1],
                        'all_variants': [v.name for v in track.variants]
                    })
                    if match[1] == 100:
                        matchesfound = 5
                    matchesfound += 1
                    break
            logger.info("matches found = " + str(matchesfound))
            if matchesfound >= 5:
                break
        return result

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
    
    def get_eu_racers(self):
        euracers = []
        for racer in self.racers.values():
            if racer.geteuorna() == "EU" and racer.numraces >= 5:
                euracers.append(racer)
        return euracers
    
    def get_na_racers(self):
        euracers = []
        for racer in self.racers.values():
            if racer.geteuorna() == "NA" and racer.numraces >= 5:
                euracers.append(racer)
        return euracers
    
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

    def getallwinners(self):
        winnerdict = {}
        retstring = ""
        for result in self.raceresults:
            if len(result.entries) > 0:
                first = result.entries[0].racer
                if not first in winnerdict:
                    winnerdict[first] = 1
        for elem in winnerdict.keys():
            retstring += elem.name
            retstring += ":"
            retstring += elem.guid
            retstring += ","
        return retstring


    def parse_one_time_trial(self, result):
        results = result["Result"]
        retarray = []
        info = {}
        trackname = ""
        trackbase = result["TrackName"]
        trackvariant = result["TrackConfig"]
        trackdata = self.contentdata.get_track(trackbase+";"+trackvariant)
        
        if trackdata is None:
            trackname = trackbase + ";" + trackvariant
        else:
            trackname = trackdata.name
        info["trackname"] = trackname
        info["date"] = result["Date"]
        for elem in results:
            arraydict = {}
            cardata = self.contentdata.get_car(elem["CarModel"])
            carname = ""
            if cardata is None:
                carname = elem["CarModel"]
            else:
                carname = cardata.name
            arraydict["car"] = carname
            arraydict["drivername"] = elem["DriverName"]
            arraydict["fastestlap"] = elem["BestLap"]
            #count laps for this combo
            numlaps = 0
            for lap in result["Laps"]:
                if lap["CarModel"] == elem["CarModel"] and lap["DriverGuid"] == elem["DriverGuid"]:
                    numlaps += 1
            arraydict["numlaps"] = numlaps
            retarray.append(arraydict)
        return info, retarray

    from datetime import datetime, timedelta

    def is_second_race(self, current_result):
        current_dt = datetime.fromisoformat(current_result.date.replace("Z", "+00:00"))
        for prior_result in reversed(self.raceresults):
            if not hasattr(prior_result, "date") or not prior_result.date:
                continue
            try:
                prior_dt = datetime.fromisoformat(prior_result.date.replace("Z", "+00:00"))
            except ValueError:
                continue

            time_diff = abs((current_dt - prior_dt).total_seconds())
            if time_diff <= 2 * 3600:  # within 2 hours
                return True  # this is a second race
            elif current_dt > prior_dt:
                break  # too far apart, and all future results will be further back

        return False

    
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

    def clear_old_data(self):
        
        self.raceresults.clear()
        self.contentdata = None
        self.racers.clear()
        self.usedtracks.clear()
        self.usedcars.clear()
        self.safety_rankings.clear()
        self.safety_rankingsgt3.clear()
        self.safety_rankingsmx5.clear()

        self.wins_rankings.clear()
        self.wins_rankingsgt3.clear()
        self.wins_rankingsmx5.clear()
        self.podiums_rankings.clear()
        self.podiums_rankingsmx5.clear()
        self.podiums_rankingsgt3.clear()

        self.elorankings.clear()
        self.laptimeconsistencyrankings.clear()
        self.laptimeconsistencyrankingsmx5.clear()
        self.laptimeconsistencyrankingsgt3.clear()
        self.positionconsistencyrankings.clear()
        self.pacerankingsmx5.clear()
        self.pacerankingsgt3.clear()
        self.safety_rankingsperkm.clear()
        self.averageelorankingsovertime.clear()
        self.retention = RetentionTracker()

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

    def retention_by_elo(self, horizon_days=90):
        data = []
        for guid, hist in self.retention.histories.items():
            racer = self.racers.get(guid)
            if not racer:
                continue
            elo = getattr(racer, "elo", None)
            if elo is None:
                continue
            returned = self.retention._returned_within(hist, horizon_days, min_extra_races=1)
            data.append((elo, returned))

        stats = defaultdict(lambda: {"total":0, "retained":0})
        for elo, returned in data:
            b = int(elo // 100 * 100)
            stats[b]["total"] += 1
            if returned:
                stats[b]["retained"] += 1

        results = []
        for b in sorted(stats):
            total = stats[b]["total"]
            kept = stats[b]["retained"]
            pct = kept / total if total > 0 else 0
            results.append({"elo_bin": b, "total": total, "retained": kept, "retention_rate": pct})
        return results
    
    def churn_rate_by_elo_bin(self, horizon_days: int = 90, bins=None, anchor=None):
        """
        Bin by Elo-at-last, compute churn rates per bin.
        Returns list of dicts sorted by bin: [{'elo_bin': 1200, 'total': n, 'churned': k, 'churn_rate': k/n}, ...]
        """
        snap = self.churn_snapshot_by_elo(horizon_days=horizon_days, anchor=anchor)
        per_racer = snap['per_racer']

        if bins is None:
            # Adjust to your range; 100-pt bins
            bins = np.arange(800, 2300, 100)

        stats = defaultdict(lambda: {"total": 0, "churned": 0})
        for guid, info in per_racer.items():
            elo = info["elo_at_last"]
            # floor to nearest 100 using your simple scheme
            b = int(elo // 100 * 100)
            stats[b]["total"] += 1
            if info["churned"]:
                stats[b]["churned"] += 1

        out = []
        for b in sorted(stats):
            tot = stats[b]["total"]
            ch  = stats[b]["churned"]
            out.append({
                "elo_bin": b,
                "total": tot,
                "churned": ch,
                "churn_rate": (ch / tot) if tot else 0.0
            })
        return out
    
    def churn_snapshot_by_elo(self, horizon_days: int = 90, anchor=None):
        """
        Returns:
        {
            'anchor': datetime (naive UTC),
            'horizon_days': int,
            'per_racer': { guid: {'last_seen': dt, 'elo_at_last': float, 'churned': bool} }
        }
        """
        import datetime as _dt
        DT, TD, UTC = _dt.datetime, _dt.timedelta, _dt.timezone.utc

        def _norm(dt):
            if isinstance(dt, DT):
                return dt.astimezone(UTC).replace(tzinfo=None) if dt.tzinfo else dt
            # handle strings incl. Z
            d = _dt.datetime.fromisoformat(str(dt).replace("Z", "+00:00"))
            return d.astimezone(UTC).replace(tzinfo=None) if d.tzinfo else d

        # 1) Build per-racer chronological entries (date, ratingchange)
        per_guid_entries = {}
        for res in self.raceresults:
            for e in res.entries:
                g = e.racer.guid
                per_guid_entries.setdefault(g, []).append((_norm(e.date), float(e.ratingchange)))

        # 2) For each racer, sort by date, cum-sum from 1500 to get elo_at_last
        per_racer = {}
        for guid, hist in self.retention.histories.items():
            entries = sorted(per_guid_entries.get(guid, []), key=lambda x: x[0])
            if not entries:
                # no entries (shouldn't happen if in histories), fall back to first_seen and base 1500
                last_seen = _norm(hist.first_seen)
                elo_last  = 1500.0
            else:
                elo = 1500.0
                last_seen = entries[-1][0]
                for dt, delta in entries:
                    elo += delta
                elo_last = elo

            per_racer[guid] = {"last_seen": last_seen, "elo_at_last": elo_last}

        # 3) Anchor = latest observed race unless provided
        if anchor is None:
            max_dt = None
            for g, info in per_racer.items():
                d = info["last_seen"]
                if (max_dt is None) or (d > max_dt):
                    max_dt = d
            anchor = max_dt or DT.now()  # naive UTC

        anchor = _norm(anchor)
        H = TD(days=horizon_days)

        # 4) Churn flag: last_seen + H <= anchor  => churned
        for guid, info in per_racer.items():
            info["churned"] = (info["last_seen"] + H) <= anchor

        return {"anchor": anchor, "horizon_days": horizon_days, "per_racer": per_racer}

    def new_racers_per_month(self) -> dict[str, int]:
        """Count brand-new racers by their first_seen month (YYYY-MM)."""
        def _norm_key(dt):
            return f"{dt.year:04d}-{dt.month:02d}"
        counts = Counter()
        for hist in self.retention.histories.values():
            counts[_norm_key(hist.first_seen)] += 1
        return dict(sorted(counts.items()))


    def add_average_elo_step(self, date):
        elocount = 0
        valid_racers = 0  # Counter for racers with numraces > 5
        averageelo = 0

        for racer in self.racers.values():
            if racer.numraces >= 5:  # Only include racers with more than 5 races
                elocount += racer.rating
                valid_racers += 1

        if valid_racers > 0:  # Ensure there are valid racers to avoid division by zero
            averageelo = elocount / valid_racers
            self.averageelorankingsovertime[date] = averageelo
        else:
            self.averageelorankingsovertime[date] = 0  # Default to 0 if no valid racers



    def loadtrackratings(self, json_file="trackratings.json"):
        # Check if the JSON file exists
        if not os.path.exists(json_file):
            logger.info(f"{json_file} does not exist. No data loaded.")
            return

        # Load the JSON data with error handling
        try:
            with open(json_file, "r") as file:
                # Ensure the file is not empty before loading
                if os.path.getsize(json_file) == 0:
                    logger.info(f"{json_file} is empty. No data loaded.")
                    return
                data = json.load(file)
        except json.JSONDecodeError:
            logger.info(f"{json_file} contains invalid JSON. No data loaded.")
            return

        # Update tracks with loaded data
        for track in self.contentdata.tracks:
            # Check if this track's ID exists in the JSON data
            if str(track.id) in data:
                # Assign the ratings and average_rating
                track.ratings = data[str(track.id)]["ratings"]
                track.average_rating = data[str(track.id)]["average_rating"]

    def get_result_by_date(self, date):
        for result in self.raceresults:
            if result.date == date:
                return result
        return None
        
    def month_report(self, guid, month, year):
        racer = self.racers[guid]
        import datetime

        # Convert month name to a month number
        month_number = datetime.datetime.strptime(month, '%B').month
        year_prefix = "20"  # Assuming the year is in the 21st century
        year = int(year_prefix + year)

        filtered_dates = [date for date in racer.progression_plot.keys() 
                  if parse(date).month == month_number and parse(date).year == year]

        filtered_entries = []
        if not filtered_dates:
            return None        
        earliest_date = min(filtered_dates)
        earliest_rating = racer.progression_plot[earliest_date]
        latest_date = max(filtered_dates)
        latest_rating = racer.progression_plot[latest_date]
        for entry in racer.entries:
            # Convert the ISO-8601 date string to a datetime object
            entry_date = datetime.datetime.fromisoformat(entry.date)
            
            # Check if the entry's month and year match the input month and year
            if entry_date.month == month_number and entry_date.year == year:
                filtered_entries.append(entry)
                
        rettuple = (earliest_rating, latest_rating, filtered_entries)
        return rettuple


    def get_overall_stats(self, recently_active: bool = False):
        elos = []
        mx5elos = []
        gt3elos = []
        safety_per_race = []
        safety_per_km = []
        safety_ratings = []
        laptime_consistency = []

        recent_threshold = datetime.now() - timedelta(days=180)
        recent_threshold = recent_threshold.replace(tzinfo=None)

        def is_recently_active(racer):
            return any(datetime.fromisoformat(result.date).replace(tzinfo=None) >= recent_threshold
                    for result in racer.entries)

        def filter_active(rankings):
            return [r for r in rankings if is_recently_active(r)]
        safety_src = getattr(self, 'safety_rankingsperkm', [])
        # Build a pool for safety calc
        safety_pool = [
            RacerSafetySnapshot(
                name=r.name,
                incidents=float(getattr(r, 'incidents', 0.0)),
                km=float(getattr(r, 'distancedriven', 0.0)),
            )
            for r in safety_src
        ]

        league_mean = league_mean_incidents_per_km(safety_pool, min_km=50.0)
        K0 = 500.0  # tune this


        # ELO
        rankings_to_use = filter_active(self.elorankings) if recently_active else self.elorankings
        for index, elem in enumerate(rankings_to_use[:10], start=1):
            elos.append({'rank': index, 'name': elem.name, 'rating': elem.rating})

        # Safety per race
        rankings_to_use = filter_active(self.safety_rankings) if recently_active else self.safety_rankings
        for index, elem in enumerate(rankings_to_use[:10], start=1):
            safety_per_race.append({'rank': index, 'name': elem.name, 'averageincidents': elem.averageincidents})
        
        # safety ratings
        rankings_to_use = filter_active(self.safety_rating_rankings) if recently_active else self.safety_rating_rankings
        for index, elem in enumerate(rankings_to_use[:10], start=1):
            safety_ratings.append({'rank': index, 'name': elem.name, 'safetyrating': elem.safety_rating})

        # Safety per km (adjusted + CI)
        # Build a list with raw, adjusted, and upper bound
        per_km_rows = []
        rankings_to_use = filter_active(safety_src) if recently_active else safety_src
        for elem in rankings_to_use:
            km = float(getattr(elem, 'distancedriven', 0.0))
            inc = float(getattr(elem, 'incidents', 0.0))
            raw = float(getattr(elem, 'incidentsperkm', 0.0)) if km > 0 else None
            adj = eb_adjusted_rate(inc, km, league_mean, K0)
            ub  = poisson_upper_rate(inc, km, 0.95)
            per_km_rows.append({
                'name': elem.name,
                'km': km,
                'incidents': inc,
                'raw': raw,
                'adjusted': adj,
                'upper95': ub if ub is not None and isfinite(ub) else float('inf')
            })

        # Sort for leaderboard by upper bound (fair to small samples)
        per_km_rows.sort(key=lambda r: (r['upper95'], r['adjusted']))

        # Keep top 10 and assign ranks
        safety_per_km = []
        for idx, r in enumerate(per_km_rows[:10], start=1):
            safety_per_km.append({
                'rank': idx,
                'name': r['name'],
                'incidentsperkm': r['adjusted'],  # display adjusted
                'km': r['km'],
                'upper95': r['upper95']
            })

        # Lap time consistency
        rankings_to_use = filter_active(self.laptimeconsistencyrankings) if recently_active else self.laptimeconsistencyrankings
        for index, elem in enumerate(rankings_to_use[:10], start=1):
            laptime_consistency.append({'rank': index, 'name': elem.name, 'laptimeconsistency': elem.laptimeconsistency})

        return {
            'elos': elos,
            'safety_per_race': safety_per_race,
            'safety_per_km': safety_per_km,
            'safetyratings': safety_ratings,
            'laptime_consistency': laptime_consistency,
            'mx5elos': mx5elos,
            'gt3elos': gt3elos,
        }



    def calculate_raw_pace_percentages_for_all_racers(self):
        fastest_lap_times = {
            "mx5": {},
            "gt3": {}
        }

        for racerguid, racer in self.racers.items():
            total_percentage_mx5 = 0
            total_percentage_gt3 = 0
            count_mx5 = 0
            count_gt3 = 0
            overall_pace = 0
            listofvisited = []

            mx5_count_pre = 0
            gt3_count_pre = 0

            for entry in racer.entries:
                precar = entry.car
                if precar.id in result.gt3ids:
                    gt3_count_pre += 1
                elif precar.id == "ks_mazda_mx5_cup":
                    mx5_count_pre += 1
            if racer.numraces < 5:
                continue
            for entry in racer.entries:

                variant = entry.track
                if variant in listofvisited:
                    continue
                listofvisited.append(variant)
                car = entry.car
                car_type = None
                if car.id in result.gt3ids:
                    car_type = 'gt3'
                    if gt3_count_pre < 5:
                        continue
                elif car.id == "ks_mazda_mx5_cup":
                    car_type = 'mx5'
                    if mx5_count_pre < 5:
                        continue
                if car_type is None:
                    continue

                if variant in fastest_lap_times[car_type]:
                    fastest = fastest_lap_times[car_type][variant]
                else:
                    if car_type == 'mx5':
                        
                        fastest = variant.get_fastest_lap_in_mx5().time
                        if fastest == None:
                            continue
                    elif car_type == 'gt3':
                        fastest = variant.get_fastest_lap_in_gt3().time
                        if fastest == None:
                            continue
                    fastest_lap_times[car_type][variant] = fastest

                if car_type == 'mx5':
                    thisracefastest_by_racer = entry.result.get_fastest_lap_of_racer(racer)
                    if thisracefastest_by_racer == None:
                        continue
                    if thisracefastest_by_racer and thisracefastest_by_racer.time != 0:
                        percentage_mx5 = (fastest / thisracefastest_by_racer.time) * 100
                        total_percentage_mx5 += percentage_mx5
                        overall_pace += percentage_mx5
                        count_mx5 += 1
                elif car_type == 'gt3':
                    thisracefastest_by_racer = entry.result.get_fastest_lap_of_racer(racer)
                    if thisracefastest_by_racer == None:
                        continue
                    if thisracefastest_by_racer and thisracefastest_by_racer.time != 0:
                        percentage_gt3 = (fastest / thisracefastest_by_racer.time) * 100
                        total_percentage_gt3 += percentage_gt3
                        overall_pace += percentage_gt3
                        count_gt3 += 1

            racer.pace_percentage_overall = round(overall_pace / (count_mx5 + count_gt3), 2) if count_mx5 + count_gt3 > 0 else None
            racer.pace_percentage_gt3 = round(total_percentage_gt3 / count_gt3, 2) if count_gt3 > 0 else None
            racer.pace_percentage_mx5 = round(total_percentage_mx5 / count_mx5, 2) if count_mx5 > 0 else None


    def get_rank(self, racer, rankings, filter):
        try:
            if filter != None:
                if rankings == self.wins_rankings:
                    if filter == "mx5":
                        rankings = self.wins_rankingsmx5
                    if filter == "gt3":
                        rankings = self.wins_rankingsgt3
                if rankings == self.podiums_rankings:
                    if filter == "mx5":
                        rankings = self.podiums_rankingsmx5
                    if filter == "gt3":
                        rankings = self.podiums_rankingsgt3
                if rankings == self.safety_rankings:
                    if filter == "mx5":
                        rankings = self.safety_rankingsmx5
                    if filter == "gt3":
                        rankings = self.safety_rankingsgt3
                if rankings == self.laptimeconsistencyrankings:
                    if filter == "mx5":
                        rankings = self.laptimeconsistencyrankingsmx5
                    if filter == "gt3":
                        rankings = self.laptimeconsistencyrankingsgt3
            return rankings.index(racer)
        except ValueError:
            return -1  # Or handle appropriately if racer not found

    def get_laptime_consistency_rank(self, racer,filter=None ):
        return self.get_rank(racer, self.laptimeconsistencyrankings, filter)
    
    def get_position_consistency_rank(self, racer, filter=None):
        return self.get_rank(racer, self.positionconsistencyrankings, filter)
    
    def get_pace_mx5_rank(self, racer, filter=None):
        return self.get_rank(racer, self.pacerankingsmx5, filter)
    
    def get_pace_gt3_rank(self, racer, filter=None):
        return self.get_rank(racer, self.pacerankingsgt3, filter)
    
    def get_elo_rank(self, racer, filter=None):
        return self.get_rank(racer, self.elorankings, filter)
    
    def get_safety_rating_rank(self, racer, filter=None):
        return self.get_rank(racer, self.safety_rating_rankings, filter)
    
    def get_qualifying_rank(self, racer, filter=None):
        return self.get_rank(racer, self.qualifyingrankings, filter)

    def get_wins_rank(self, racer, filter=None):
        return self.get_rank(racer, self.wins_rankings, filter)

    def get_podiums_rank(self, racer, filter=None):
        return self.get_rank(racer, self.podiums_rankings, filter)

    def get_safety_rank(self, racer, filter=None):
        return self.get_rank(racer, self.safety_rankings, filter)

    def get_safety_rank_per_km(self, racer, filter=None):
        return self.get_rank(racer, self.safety_rankingsperkm, filter)
    
    def get_racer_tracks_report(self, id, isreverse=False):
        racer = self.racers[id]
        track_stats = {}  # Dictionary to store total positions and counts

        for entry in racer.entries:
            parenttrackid = entry.track.parent_track.id
            parenttrackname = entry.track.parent_track.highest_priority_name
            if parenttrackid not in track_stats:
                track_stats[parenttrackid] = {'name': parenttrackname, 'total_positions': 0, 'count': 0}

            track_stats[parenttrackid]['total_positions'] += entry.finishingposition
            track_stats[parenttrackid]['count'] += 1

        # Calculate average positions and filter out tracks with fewer than 2 entries
        averagedict = {info['name']: round(info['total_positions'] / info['count'], 2) 
                       for trackid, info in track_stats.items() if info['count'] >= 4}

        # Sort the dictionary by average finishing position and limit to 10 elements
        # Reverse the sorting order
        sorted_averagedict = dict(sorted(averagedict.items(), key=lambda item: item[1], reverse=isreverse)[:10])


        return sorted_averagedict


    def plot_racers_scatter(self, focus_guid=None):
        logger.info("focus guid = " + str(focus_guid))
        recent_threshold = datetime.now() - timedelta(days=180)
        recent_threshold = recent_threshold.replace(tzinfo=None)

        def is_recently_active(racer):
            return any(
                datetime.fromisoformat(entry.date).replace(tzinfo=None) >= recent_threshold
                for entry in racer.entries
            )

        # 1) Filter recent racers and keep only those with SR
        racers_all = [r for r in self.elorankings if is_recently_active(r)]
        racers = [r for r in racers_all if hasattr(r, "safety_rating") and r.safety_rating is not None]

        if not racers:
            logger.warning("No racers with Safety Rating to plot.")
            return

        # 2) Extract arrays
        sr_values   = np.array([float(r.safety_rating) for r in racers], dtype=float)
        elo_ratings = np.array([float(r.rating)        for r in racers], dtype=float)
        names       = [r.name for r in racers]
        guids       = [getattr(r, "guid", None) for r in racers]

        # 3) License colors
        # (Colorblind-friendly-ish; tweak if you like)
        class_colors = {
            "A":      "tab:blue",
            "B":      "tab:green",
            "C":      "tab:orange",
            "D":      "tab:red",
            "Rookie": "tab:gray",
        }
        # default to Rookie if missing
        classes = [getattr(r, "licenseclass", "Rookie") or "Rookie" for r in racers]

        # 4) Plot per class to get a clean legend
        plt.figure(figsize=(18, 18))
        unique_classes = ["A","B","C","D","Rookie"]
        texts = []
        focus_idx = None

        for cls in unique_classes:
            idx = [i for i, c in enumerate(classes) if c == cls]
            if not idx:
                continue
            xs = sr_values[idx]
            ys = elo_ratings[idx]
            plt.scatter(
                xs, ys,
                s=100,
                c=class_colors.get(cls, "tab:gray"),
                alpha=0.65,
                edgecolors='w',
                linewidth=2,
                label=cls
            )

        # 5) Labels (non-focused)
        for i, r in enumerate(racers):
            if focus_guid is not None and guids[i] == focus_guid:
                focus_idx = i
                continue
            texts.append(
                plt.text(sr_values[i], elo_ratings[i], names[i],
                        fontsize=12, ha='right', va='bottom', color='gray')
            )

        adjust_text(texts, arrowprops=dict(arrowstyle='-', color='black', lw=0.5))

        # 6) Highlight focused racer
        if focus_idx is not None:
            fx, fy = sr_values[focus_idx], elo_ratings[focus_idx]
            fname  = names[focus_idx]
            plt.scatter([fx], [fy], s=400, c='gold', edgecolors='red', linewidth=3, zorder=3)
            # nudge label within [0,5] bounds
            label_x = min(max(fx + 0.15, 0.0), 5.0)
            plt.annotate(
                fname, xy=(fx, fy), xycoords='data',
                xytext=(label_x, fy + 150), textcoords='data',
                fontsize=24, fontweight='bold', color='red',
                arrowprops=dict(arrowstyle='->', color='red', lw=3),
                zorder=4
            )

        # 7) Average lines
        avg_sr  = 2.5
        avg_elo = float(np.nanmean(elo_ratings))
        plt.axhline(y=avg_elo, color='green',  linestyle='--', linewidth=2,
                    label=f'Average ELO: {avg_elo:.2f}')
        plt.axvline(x=avg_sr,  color='orange', linestyle='--', linewidth=2,
                    label=f'Starting SR: {avg_sr:.2f}')

        # 8) Axes & styling
        plt.xlabel('Safety Rating (0–4.99)', fontsize=14)
        plt.ylabel('ELO Score',              fontsize=14)
        plt.title('Scatter Plot of Racers (Safety Rating vs ELO)', fontsize=18)
        plt.grid(True)
        plt.xticks(fontsize=14)
        plt.yticks(fontsize=14)
        plt.xlim(0.0, 5.0)
        # Legend: group class labels and average lines
        handles, labels = plt.gca().get_legend_handles_labels()
        # put class legend first, then averages
        # (class labels are exactly 'A','B','C','D','Rookie')
        order = [i for i, lab in enumerate(labels) if lab in unique_classes] + \
                [i for i, lab in enumerate(labels) if lab.startswith("Average")]
        plt.legend([handles[i] for i in order], [labels[i] for i in order],
                loc='upper left', fontsize=12, ncol=1)

        # 9) Save
        plt.savefig('scatter_plot.png', bbox_inches='tight')
        plt.close()


    def get_times_track_used(self, variant):
        count = 0
        for result in self.raceresults:
            if result.track == variant:
                count += 1
        return count

    def test_output(self, id):
        racer = self.racers[id]
        for entry in racer.entries:
            logger.info("race for " + racer.name +"\n" )
            logger.info( "\n")
            logger.info( "\n")
            logger.info("race at : " + entry.track.id + " , finishing position is " + str(entry.finishingposition))
            logger.info( "\n")
            logger.info("filename is : " + entry.result.filename)

    def get_dirty_drivers_rows(self, recently_active: bool = False, top_n: int = 10):
        from math import isfinite

        # Use same source list used for per-km safety rankings
        safety_src = getattr(self, 'safety_rankingsperkm', [])

        # Optionally filter to recently active
        recent_threshold = datetime.now() - timedelta(days=180)
        recent_threshold = recent_threshold.replace(tzinfo=None)
        def is_recently_active(racer):
            return any(datetime.fromisoformat(r.date).replace(tzinfo=None) >= recent_threshold for r in racer.entries)
        rankings_to_use = [r for r in safety_src if is_recently_active(r)] if recently_active else safety_src

        # League mean + EB prior
        pool = [
            RacerSafetySnapshot(
                name=r.name,
                incidents=float(getattr(r, 'incidents', 0.0)),
                km=float(getattr(r, 'distancedriven', 0.0)),
            )
            for r in safety_src  # compute mean from full pool for stability
        ]
        league_mean = league_mean_incidents_per_km(pool, min_km=50.0)
        K0 = 500.0

        rows = []
        for r in rankings_to_use:
            km = float(getattr(r, 'distancedriven', 0.0))
            if km < 1.0:
                continue
            inc = float(getattr(r, 'incidents', 0.0))
            adj = eb_adjusted_rate(inc, km, league_mean, K0)
            ub  = poisson_upper_rate(inc, km, 0.95)
            lb  = poisson_lower_rate(inc, km, 0.95)
            rows.append({
                'name': r.name,
                'km': km,
                'incidents': inc,
                'adjusted': adj,
                'upper95': ub if ub is not None and isfinite(ub) else 0.0,
                'lower95': lb if lb is not None and isfinite(lb) else 0.0,
            })

        # Sort for “dirtiest”: highest lower bound first, then highest adjusted, then more km
        rows.sort(key=lambda x: (-x['lower95'], -x['adjusted'], -x['km']))

        # Add ranks and trim
        out = []
        for idx, row in enumerate(rows[:top_n], start=1):
            out.append({
                'rank': idx,
                'name': row['name'],
                'incidentsperkm': row['adjusted'],
                'km': row['km'],
                'lower95': row['lower95'],
                'upper95': row['upper95'],
            })
        return out

    

    # ---- Chart builder ----
    def create_progression_chart(self, racer, progression_plot, months: int = None) -> bool:
        """
        Builds a progression chart PNG for the given racer/series.
        Returns True if a chart was saved, or False if there wasn't enough usable data.
        """
        from datetime import datetime, timedelta, timezone

        dates = list(progression_plot.keys())
        finishes = list(progression_plot.values())

        # Convert ISO 8601 strings ("...Z") to timezone-aware datetimes (UTC) and sort
        try:
            dt_utc = [datetime.fromisoformat(d.replace('Z', '+00:00')) for d in dates]
        except Exception:
            # Fallback to naive parsing if any odd format appears
            dt_utc = [datetime.fromisoformat(d[:-1]) for d in dates]  # strip trailing 'Z' as original did

        # Optional months filtering (last N months)
        if months is not None and months > 0:
            approx_days = int(round(months * 30.4375))  # average month length
            cutoff = datetime.now(timezone.utc) - timedelta(days=approx_days)
            filtered_pairs = [(dt, f) for dt, f in zip(dt_utc, finishes) if dt >= cutoff]
            if len(filtered_pairs) < 2:
                logger.info(f"Not enough data points after month filter (last {months} months)")
                return False
            dt_utc, finishes = zip(*sorted(filtered_pairs, key=lambda t: t[0]))
        else:
            dt_utc, finishes = zip(*sorted(zip(dt_utc, finishes), key=lambda t: t[0]))

        # --- Outlier filtering ---
        # (run AFTER date filtering so we don't drop too much)
        if progression_plot == racer.paceplot:
            mean = np.mean(finishes); std_dev = np.std(finishes)
            z_scores = [(f - mean) / std_dev if std_dev else 0 for f in finishes]
            kept = [(dt, f) for dt, f, z in zip(dt_utc, finishes, z_scores) if abs(z) <= 2]
            if len(kept) < 2:
                logger.info("Not enough data points after filtering outliers (pace)")
                return False
            dt_utc, finishes = zip(*kept)

        if progression_plot == racer.safetyratingplot:
            mean = np.mean(finishes); std_dev = np.std(finishes)
            z_scores = [(f - mean) / std_dev if std_dev else 0 for f in finishes]
            kept = [(dt, f) for dt, f, z in zip(dt_utc, finishes, z_scores) if abs(z) <= 15]
            if len(kept) < 2:
                logger.info("Not enough data points after filtering outliers (safety)")
                return False
            dt_utc, finishes = zip(*kept)

        # Plot
        fig, ax = plt.subplots()

        # Main series label
        series_label = (
            "ELO Scores" if progression_plot == racer.progression_plot else
            "Pace (%)" if progression_plot == racer.paceplot else
            "Safety Rating"
        )
        ax.plot(dt_utc, finishes, marker='o', linestyle='-', alpha=0.3, label=series_label)

        # Linear trend
        x = np.array([dt.timestamp() for dt in dt_utc])
        y = np.array(finishes)
        if len(x) >= 2 and np.any(np.diff(x)):
            coeffs = np.polyfit(x, y, 1)
            linear_trend = np.poly1d(coeffs)
            ax.plot(dt_utc, linear_trend(x), linestyle='-', linewidth=2, label='Linear Trend')

        # Global-average reference lines
        averagepace = None
        averagesafety = None

        if progression_plot == racer.paceplot:
            avgs = [
                r.pace_percentage_overall for r in self.racers.values()
                if getattr(r, "pace_percentage_overall", None) is not None and getattr(r, "numraces", 0) >= 5
            ]
            if avgs:
                averagepace = float(np.mean(avgs))
                ax.axhline(y=averagepace, linestyle='--', label="RRR Average")

        elif progression_plot == racer.safetyratingplot:
            collect = []
            for r in self.racers.values():
                val = getattr(r, "safety_rating", None)
                if val is not None and getattr(r, "numraces", 0) >= 5:
                    collect.append(val)
            if collect:
                averagesafety = float(np.mean(collect))
                ax.axhline(y=averagesafety, linestyle='--', label="RRR Average")

        # Axes labels/titles
        if progression_plot == racer.progression_plot:
            ax.set(xlabel='Date', ylabel='ELO', title='Racer Progression Over Time')
        elif progression_plot == racer.paceplot:
            ax.set(xlabel='Date', ylabel='Pace (%)', title='Pace Progression Over Time')
        elif progression_plot == racer.safetyratingplot:
            ax.set(xlabel='Date', ylabel='Safety Rating', title='Safety Rating Over Time')

        ax.grid()
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))

        # Y-limits
        if progression_plot == racer.progression_plot:
            y_min = min(finishes) - 100
            y_max = max(finishes) + 100
        elif progression_plot == racer.paceplot:
            if averagepace is None:
                averagepace = float(np.mean(finishes))
            y_min = min(averagepace - 5.0, min(finishes) - 5.0)
            y_max = 100.0
        elif progression_plot == racer.safetyratingplot:
            y_min = 0.0
            y_max = 5.0

        ax.set_ylim(y_min, y_max)

        fig.autofmt_xdate()
        ax.legend()

        plt.savefig('progression_chart.png', bbox_inches='tight')
        plt.close()
        return True




    def moving_average(self, data, window_size):
        return np.convolve(data, np.ones(window_size)/window_size, mode='valid')

    def find_stabilization_point(self, data, window_size=10, threshold=15):
        ma = self.moving_average(data, window_size)
        for i in range(len(ma) - window_size):
            if np.var(ma[i:i+window_size]) < threshold:
                return i + window_size
        return len(data) - 1

    def create_skill_progression_chart(self, incidentplot, positionplot):
        dates = list(incidentplot.keys()) 
        finishes = list(positionplot.values())
        incidents = list(incidentplot.values())

        # Convert ISO_8601 strings to datetime objects and sort them
        dates = [datetime.fromisoformat(date[:-1]) for date in dates] # Remove the 'Z' at the end of the string
        dates, finishes, incidents = zip(*sorted(zip(dates, finishes, incidents)))  # Sort dates, finishes, and incidents together


        fig, ax1 = plt.subplots()

        # Plot average finishes
        ax1.plot(dates, finishes, marker='o', linestyle='-', color='b', alpha=0.7, label='Top % of Finish Position')
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Finish Position (%)', color='b')
        ax1.tick_params(axis='y', labelcolor='b')
        ax1.set_ylim(min(finishes) -5, 100)  # Dynamic scale for finish positions

        # Create a second y-axis for average incidents
        ax2 = ax1.twinx()
        ax2.plot(dates, incidents, marker='o', linestyle='-', color='g', alpha=0.7, label='Average % of pace')
        ax2.set_ylabel('Average pace (%)', color='g')
        ax2.tick_params(axis='y', labelcolor='g')
        ax2.set_ylim(min(incidents) -2.0, 100)  # Dynamic scale for incidents

        # Format the date on the x-axis to show month and year
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
        fig.autofmt_xdate()

        # Add grid and title
        ax1.grid()
        fig.suptitle('Racer Progression Over Time')

        # Add legend
        ax1.legend(loc='upper left')
        ax2.legend(loc='upper right')

        # Save chart as an image
        plt.savefig('progression_chart.png')
        plt.close()


    def create_average_elo_progression_chart(self):
        # Extract dates and elo rankings from the dictionary
        dates = list(self.averageelorankingsovertime.keys())
        elo_rankings = list(self.averageelorankingsovertime.values())

        # Filter out entries where Average ELO is zero
        filtered_data = [(date, elo) for date, elo in zip(dates, elo_rankings) if elo != 0]
        if not filtered_data:  # Ensure there is data to plot
            logger.info("No valid data to plot after filtering out zero ELO values.")
            return

        # Separate filtered dates and rankings
        dates, elo_rankings = zip(*filtered_data)

        # Convert ISO_8601 strings to datetime objects and sort them
        dates = [datetime.fromisoformat(date[:-1]) for date in dates]  # Remove the 'Z' at the end
        dates, elo_rankings = zip(*sorted(zip(dates, elo_rankings)))  # Sort dates and elo rankings together

        # Create the plot
        plt.figure(figsize=(10, 5))
        plt.plot(dates, elo_rankings, marker='o', linestyle='-', color='purple', alpha=0.8, label='Filtered Average ELO Progression')
        plt.xlabel('Date')
        plt.ylabel('Average ELO')
        plt.title('Filtered Average ELO Progression Over Time')
        plt.grid(True)
        plt.ylim(1200, 1800)  # Set y-axis limits

        # Format the date on the x-axis to show month and year
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
        plt.gcf().autofmt_xdate()

        # Add a legend
        plt.legend(loc='upper left')

        # Save chart as an image
        plt.savefig('average_elo_progression_chart.png')
        plt.close()


    def create_attendance_chart(self, eudict, nadict):
        # Convert dictionaries to DataFrames
        eu_df = pd.DataFrame(list(eudict.items()), columns=['Date', 'EU_Attendance'])
        na_df = pd.DataFrame(list(nadict.items()), columns=['Date', 'NA_Attendance'])

        # Convert dates and filter out zero attendance
        eu_df['Date'] = pd.to_datetime(eu_df['Date'], utc=True, format='mixed')
        na_df['Date'] = pd.to_datetime(na_df['Date'], utc=True, format='mixed')
        eu_df = eu_df[eu_df['EU_Attendance'] > 0].sort_values('Date')
        na_df = na_df[na_df['NA_Attendance'] > 0].sort_values('Date')

        # Merge and reindex by date
        df = pd.merge(eu_df, na_df, on='Date', how='outer').sort_values('Date')
        df = df.set_index('Date').fillna(0)

        # Calculate rolling averages with a window of 3 events
        df['EU_Rolling'] = df['EU_Attendance'].rolling(window=3, min_periods=1).mean()
        df['NA_Rolling'] = df['NA_Attendance'].rolling(window=3, min_periods=1).mean()

        # Plotting
        plt.figure(figsize=(10, 5))
        plt.plot(df.index, df['EU_Rolling'], color='blue', linestyle='-', label='EU Smoothed')
        plt.plot(df.index, df['NA_Rolling'], color='red', linestyle='-', label='NA Smoothed')

        plt.xlabel('Date')
        plt.ylabel('Attendance')
        plt.title('EU and NA Attendance Over Time (Smoothed)')
        plt.grid(True)
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
        plt.gcf().autofmt_xdate()
        plt.legend(loc='upper left')

        plt.savefig('attendance_chart.png')
        plt.close()


    def create_overall_attendance_chart(self, attendancedict):
        # Parse dates and extract year + month
        data = [
            (pd.to_datetime(date.replace("Z", "+00:00")), attendance)
            for date, attendance in attendancedict.items()
        ]

        df = pd.DataFrame(data, columns=["Date", "Attendance"])
        df = df[df["Attendance"] > 0]  # Filter out zero attendance
        df["Year"] = df["Date"].dt.year
        df["Month"] = df["Date"].dt.month

        # Filter only 2024 and 2025
        df = df[df["Year"].isin([2024, 2025])]

        # Group by month for each year
        grouped = df.groupby(["Year", "Month"])["Attendance"].mean().unstack(0).fillna(0)

        # Plot
        plt.figure(figsize=(10, 5))
        plt.plot(grouped.index, grouped[2024], marker='o', label="2024 Attendance", color='steelblue')
        plt.plot(grouped.index, grouped[2025], marker='o', label="2025 Attendance", color='darkorange')

        plt.xticks(ticks=range(1, 13), labels=["Jan", "Feb", "Mar", "Apr", "May", "Jun", 
                                            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
        plt.xlabel("Month")
        plt.ylabel("Average Attendance")
        plt.title("🟦 2024 vs 2025 Attendance by Month")
        plt.grid(True, linestyle="--", alpha=0.6)
        plt.legend()
        plt.tight_layout()

        plt.savefig("attendance_chart.png")
        plt.close()

