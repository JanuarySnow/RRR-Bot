import math
import statistics
from statistics import mean
from logger_config import logger
from collections import defaultdict
from math import fsum, sqrt
import re

_MILE_IN_METERS = 1609.344  

import math
import re

_MILE_IN_METERS = 1609.344

SR_DEFAULT_START          = 2.50          # new racers start here
SR_MIN, SR_MAX            = 0.00, 4.99
SR_JUMP                   = 0.40          # snap when crossing whole numbers
SR_TARGET_RATE_DEFAULT    = 0.053         # league mean incidents/km 
SR_MEMORY_KM              = 500.0         # EMA memory (bigger = slower to change)
SR_STEP_PER_100KM         = 0.20          # how fast SR moves if cleaner/dirter than target
SR_ELO_NUDGE_PER_100KM    = 0.02          # tiny SR nudge from elo (per 100 km)
ELO_REF, ELO_SPAN         = 1500.0, 500.0 # scale elo into about [-1..+1]
DRAG_START_SR     = 4.00   # start damping above this SR
DRAG_EXPONENT     = 1.7    # curvature; higher = stronger drag near 4.99
MIN_POS_GAIN_MULT = 0.15   # don't let gains vanish completely
NEG_BITE_AT_MAX   = 0.55   # up to +50% harsher losses at 4.99 (linear ramp)
SR_CAP_GAIN       = 0.20   
SR_CAP_LOSS       = 0.40

LICENSE_ELO_THRESH = {                    # elo gates (tune to put most in D/C)
    "D": 1450,
    "C": 1500,
    "B": 1600,
    "A": 1750,
}
LICENSE_SR_THRESH = {                     # SR gates
    "D": 1.00,
    "C": 2.00,
    "B": 3.00,
    "A": 4.00,
}
MIN_RACES_FOR_LICENSE = 5 

def parse_track_length_to_meters(raw: str | None, default_m: float = 2000.0) -> float:
    """
    Parse messy track length strings into meters.

    Heuristics:
      - Empty/zero-like -> default_m
      - 'mi', 'mile', 'miles' -> miles -> meters (supports fractions like '1/4 mile')
      - 'km', 'kilometer(s)/kilometre(s)' -> km -> meters
      - 'm' (meters) -> value in meters, EXCEPT if value < 50 -> treat as km (common modder quirk: '5.027m' meaning 5.027 km)
      - No unit -> value < 10 -> km, else meters
    Also handles:
      - Comma decimals (e.g., '9,6 Miles')
      - Extra words/symbols ('~4000m', '859 and less', 'about 3km', '3KM', 'M', 'KM')
      - Spaces ('4300 m', '  4300m')
      - Tolerates weird casing.

    Returns meters (float).
    """
    if raw is None:
        return float(default_m)

    s = str(raw).strip()
    if not s:
        return float(default_m)

    # Normalize: lower, comma as decimal, squish whitespace
    s_norm = s.lower()
    s_norm = s_norm.replace(',', '.')
    s_norm = re.sub(r'\s+', ' ', s_norm)

    # Quick zero-ish / blank-ish checks
    if re.fullmatch(r'(?:0+(\.0+)?)?\s*(?:km|m|mi|mile|miles)?', s_norm):
        return float(default_m)

    # Standardize unit keywords
    # (keep 'mi' before 'm' to avoid mis-detection)
    # We'll search for units later; this step is just to simplify variants.
    unit_map = {
        r'kilomet(?:er|re)s?': 'km',
        r'kilometers?': 'km',
        r'kilometres?': 'km',
        r'kms?': 'km',
        r'\bkms\b': 'km',
        r'\bkm\b': 'km',
        r'miles?': 'mi',
        r'\bmi\b': 'mi',
        r'\bmeters?\b': 'm',
        r'\bmetres?\b': 'm',
        r'\bm\b': 'm',  # careful: we'll differentiate from 'mi' via ordering
    }

    # We'll detect the unit explicitly later; for now we just want a clean numeric token.
    # Extract either fraction (e.g., 1/4) or decimal/integer number; take the first.
    # Allow optional leading ~ or ≈ etc.
    # Examples matched: '1/4', '5.027', '4023', '5.7'
    num_match = re.search(r'(?P<num>\d+/\d+|\d+(?:\.\d+)?)', s_norm)
    if not num_match:
        # No number found: give up -> default
        return float(default_m)

    num_str = num_match.group('num')

    # Fraction?
    if '/' in num_str:
        try:
            a, b = num_str.split('/', 1)
            val = float(a) / float(b)
        except Exception:
            return float(default_m)
    else:
        try:
            val = float(num_str)
        except Exception:
            return float(default_m)

    # Identify unit with priority: miles > km > m
    # (Use boundaries so 'mi' doesn't get grabbed by the 'm' rule)
    has_miles = bool(re.search(r'\bmi\b|miles?', s_norm))
    has_km    = bool(re.search(r'\bkm\b|kilomet(?:er|re)s?|kilometers?|kilometres?|kms?\b', s_norm))
    # 'm' as meters only if not miles and not km
    has_meters = (not has_miles and not has_km) and bool(re.search(r'(?<![a-z])m(?![a-z])|\bmeters?\b|\bmetres?\b', s_norm))

    # Core unit decision tree
    meters: float

    if has_miles:
        # Treat value as miles (supports fractions)
        meters = val * _MILE_IN_METERS

    elif has_km:
        # Treat value as kilometers
        meters = val * 1000.0

    elif has_meters:
        # Ambiguity: many modders write '5.027m' but mean 5.027 km.
        # Heuristic: if labeled meters but < 50, assume it's km (i.e., multiply by 1000).
        meters = val * (1000.0 if val < 50.0 else 1.0)

    else:
        # No explicit unit:
        # If < 10 -> km (typical track lengths like 3.6, 5.4, etc.)
        # Else -> meters (e.g., 2480, 4023, 6800).
        meters = val * (1000.0 if val < 10.0 else 1.0)

    # If meters is nonsense, fall back
    if not math.isfinite(meters) or meters <= 0:
        return float(default_m)

    return float(meters)


INC_SEVERITY_BREAKS = (
    (15,   0),   # anything below 15 km/h = no incident
    (40,   1),   # 15 – 39 km/h  → 1 ×
    (80,   2),   # 40 – 79 km/h  → 2 ×
    (120,  3),   # 80 – 119 km/h → 3 ×
    (float('inf'), 4),  # ≥120 km/h → 4 ×
)

CAR_BASE       = 1.0   # base weight for car-to-car
ENV_BASE       = 0.4   # base weight for walls / cones / tyres etc.
MAX_INC_POINTS = 4.0   # absolute cap per single incident

def severity_from_speed(rel_speed_kph: float) -> int:
    """Return 0,1,2,3,4 according to INC_SEVERITY_BREAKS."""
    for speed_limit, sev in INC_SEVERITY_BREAKS:
        if rel_speed_kph < speed_limit:
            return sev
    # (We never reach this line because of float('inf'))

gt3ids = ["ks_audi_r8_lms_2016","bmw_z4_gt3", "ks_ferrari_488_gt3", "ks_lamborghini_huracan_gt3",
         "ks_mclaren_650_gt3", "ks_mercedes_amg_gt3", "ks_nissan_gtr_gt3", "ks_porsche_911_gt3_r_2016", "amr_v8_vantage_gt3_sprint_acc"]

class Racerprofile():
    def __init__(self, newname, newguid) -> None:
        self.name = newname
        self.guid = newguid
        self.entries = [] #racer, car, track, date, laps, incidents, result, finishingposition, cuts, startingposition
        self.result_add_ticker = 0
        self.progression_plot = {}
        self.eucount = 0
        self.nacount = 0
        self.rating = 1500
        self.mosthitotherdriver = None
        self.mosthitotherdrivergt3 = None
        self.mosthitotherdrivermx5 = None
        self.wins = 0
        self.gt3wins = 0
        self.mx5wins = 0
        self.podiums = 0
        self.gt3podiums = 0
        self.mx5podiums = 0
        self.totallaps = 0
        self.mx5laps = 0
        self.gt3laps = 0
        self.mostsuccesfultrack = None
        self.mostsuccesfultrackgt3 = None
        self.mostsuccesfultrackmx5 = None
        self.leastsuccesfultrack = None
        self.leastsuccesfultrackgt3 = None
        self.leastsuccesfultrackmx5 = None
        self.incidents = 0.1
        self.incidentsgt3 = 0.1
        self.incidentsmx5 = 0.1
        self.incidentsperkm = 0.0
        self.averageincidents = 0.0
        self.averageincidentsgt3 = 0.0
        self.averageincidentsmx5 = 0.0
        self.numraces = 0
        self.numracesgt3 = 0
        self.numracesmx5 = 0
        self.laptimeconsistency = None
        self.laptimeconsistencymx5 = None
        self.laptimeconsistencygt3 = None
        self.raceconsistency = None
        self.pace_percentage_mx5 = 0
        self.distancedriven = 0
        self.pace_percentage_gt3 = 0
        self.pace_percentage_overall = 0
        self.historyofratingchange = {} # result, rating change
        self.gt3progression_plot = {}
        self.mx5progression_plot = {}
        self.positionplot = {}
        self.incidentplot = {}
        self.positionaverage = {}
        self.paceplot = {}
        self.paceplotaverage = {}
        self.logger = logger
        self.collisionracers = {}
        self.positionchangeperrace = {} # dictionary of date of race to position change delta for that race
        self.percentageracedone_overtakes = [] # array of percentage of race completed of instances of overtakes being done i.e [13, 25, 50, 75, 100]
        self.startingpositions = {} # dictionary of date of race to starting position
        self.qualifyingrating = 1500
        self.historyofqualifyingratingchange = {} # result, rating change
        self.ratingbeforeeachresult = -1
        self.qualyratingbeforeeachresult = -1
        self.licenseclass = "Rookie"
        self.safety_rating = 2.50
        self.safety_rate_ema = None
        self.sr_target_rate = SR_TARGET_RATE_DEFAULT
        self.sr_memory_km = SR_MEMORY_KM
    
    def to_dict(self):
        return {
            'name': self.name,
            'guid': self.guid,
            'entries': [entry.id for entry in self.entries],
            'result_add_ticker': self.result_add_ticker,
            'progression_plot': self.progression_plot,
            'safety_rating': self.safety_rating,
            'safety_rate_ema': self.safety_rate_ema,
            'sr_target_rate': self.sr_target_rate,
            'sr_memory_km': self.sr_memory_km,
            'eucount': self.eucount,
            'nacount': self.nacount,
            'rating': self.rating,
            'distancedriven': self.distancedriven,
            'incidentsperkm': self.incidentsperkm,
            'mosthitotherdriver': self.mosthitotherdriver.guid if self.mosthitotherdriver else None,
            'mosthitotherdrivergt3': self.mosthitotherdrivergt3.guid if self.mosthitotherdrivergt3 else None,
            'mosthitotherdrivermx5': self.mosthitotherdrivermx5.guid if self.mosthitotherdrivermx5 else None,
            'wins': self.wins,
            'gt3wins': self.gt3wins,
            'mx5wins': self.mx5wins,
            'podiums': self.podiums,
            'gt3podiums': self.gt3podiums,
            'mx5podiums': self.mx5podiums,
            'totallaps': self.totallaps,
            'mx5laps': self.mx5laps,
            'gt3laps': self.gt3laps,
            'licenseclass': self.licenseclass,
            'mostsuccesfultrack': (
                next(iter(self.mostsuccesfultrack)).id if isinstance(self.mostsuccesfultrack, dict)
                else self.mostsuccesfultrack.id if self.mostsuccesfultrack else None
            ),
            'mostsuccesfultrackgt3': (
                next(iter(self.mostsuccesfultrackgt3)).id if isinstance(self.mostsuccesfultrackgt3, dict)
                else self.mostsuccesfultrackgt3.id if self.mostsuccesfultrackgt3 else None
            ),
            'mostsuccesfultrackmx5': (
                next(iter(self.mostsuccesfultrackmx5)).id if isinstance(self.mostsuccesfultrackmx5, dict)
                else self.mostsuccesfultrackmx5.id if self.mostsuccesfultrackmx5 else None
            ),
            'leastsuccesfultrack': (
                next(iter(self.leastsuccesfultrack)).id if isinstance(self.leastsuccesfultrack, dict)
                else self.leastsuccesfultrack.id if self.leastsuccesfultrack else None
            ),
            'leastsuccesfultrackgt3': (
                next(iter(self.leastsuccesfultrackgt3)).id if isinstance(self.leastsuccesfultrackgt3, dict)
                else self.leastsuccesfultrackgt3.id if self.leastsuccesfultrackgt3 else None
            ),
            'leastsuccesfultrackmx5': (
                next(iter(self.leastsuccesfultrackmx5)).id if isinstance(self.leastsuccesfultrackmx5, dict)
                else self.leastsuccesfultrackmx5.id if self.leastsuccesfultrackmx5 else None
            ),
            'incidents': self.incidents,
            'incidentsgt3': self.incidentsgt3,
            'incidentsmx5': self.incidentsmx5,
            'averageincidents': self.averageincidents,
            'averageincidentsgt3': self.averageincidentsgt3,
            'averageincidentsmx5': self.averageincidentsmx5,
            'numraces': self.numraces,
            'numracesgt3': self.numracesgt3,
            'numracesmx5': self.numracesmx5,
            'laptimeconsistency': self.laptimeconsistency,
            'laptimeconsistencymx5': self.laptimeconsistencymx5,
            'laptimeconsistencygt3': self.laptimeconsistencygt3,
            'raceconsistency': self.raceconsistency,
            'pace_percentage_mx5': self.pace_percentage_mx5,
            'pace_percentage_gt3': self.pace_percentage_gt3,
            'pace_percentage_overall': self.pace_percentage_overall,
            'historyofratingchange': {result.id: change for result, change in self.historyofratingchange.items()},
            'gt3progression_plot': self.gt3progression_plot,
            'mx5progression_plot': self.mx5progression_plot,
            'positionplot': self.positionplot,
            'incidentplot': self.incidentplot,
            'positionaverage': self.positionaverage,
            'paceplot': self.paceplot,
            'paceplotaverage': self.paceplotaverage,
            'collisionracers': {racer.guid: count for racer, count in self.collisionracers.items()},
            'positionchangeperrace': {date: change for date, change in self.positionchangeperrace.items()},
            'percentageracedone_overtakes': self.percentageracedone_overtakes,
            'qualifyingrating': self.qualifyingrating,
            'startingpositions': {date: pos for date, pos in self.startingpositions.items()},
            'historyofqualifyingratingchange': {result.id: change for result, change in self.historyofqualifyingratingchange.items()},
    }

    
    def _sr_conf(self):
        """
        Allow per-racer overrides if you ever want them.
        Otherwise fall back to league defaults.
        """
        target_rate = getattr(self, "sr_target_rate", SR_TARGET_RATE_DEFAULT)
        memory_km   = getattr(self, "sr_memory_km", SR_MEMORY_KM)
        return target_rate, memory_km

    def ensure_sr_fields(self):
        # call once in __init__ or lazily before first update
        if not hasattr(self, "safety_rating"):
            self.safety_rating = SR_DEFAULT_START
        if not hasattr(self, "safety_rate_ema"):       # EMA of incidents/km
            self.safety_rate_ema = None
        if not hasattr(self, "licenseclass"):
            self.licenseclass = "Rookie"
        if not hasattr(self, "sr_target_rate"):
            self.sr_target_rate = SR_TARGET_RATE_DEFAULT
        if not hasattr(self, "sr_memory_km"):
            self.sr_memory_km = SR_MEMORY_KM

    def _apply_sr_jump(self, before: float, after: float) -> float:
        """
        If you cross an integer boundary (1/2/3/4), snap to X.40 like iRacing.
        Upward crossing -> set to N+.40; downward -> N-.40
        """
        boundaries = (1.0, 2.0, 3.0, 4.0)
        for b in boundaries:
            if before < b <= after:          # promotion crossing
                after = max(after, b + SR_JUMP)
            if after < b <= before:          # demotion crossing
                after = min(after, b - SR_JUMP)
        return after

    def update_safety_after_session(self, session_km: float, session_incidents: float):
        """
        Update Safety Rating after one race.
        session_km        : kilometers driven this session
        session_incidents : incident points this session (your weighted scheme)
        """
        self.ensure_sr_fields()
        km = max(0.0, float(session_km))
        inc = max(0.0, float(session_incidents))
        if km <= 0.0:
            return  # nothing to update

        # 1) Update an EMA of incidents/km (not required for SR, but useful to show + seed)
        target_rate, memory_km = self._sr_conf()
        r_sess = inc / km
        r_old  = self.safety_rate_ema if (self.safety_rate_ema is not None and math.isfinite(self.safety_rate_ema)) else target_rate
        alpha  = 1.0 - math.exp(-km / max(1e-6, memory_km))
        self.safety_rate_ema = (1.0 - alpha) * r_old + alpha * r_sess

        # 2) Compute SR delta vs target (cleaner than target -> positive)
        # normalize: +1 if perfect clean relative to target, -1 if 2x worse than target, etc.
        norm_diff = (target_rate - r_sess) / max(1e-9, target_rate)
        delta_from_clean = SR_STEP_PER_100KM * norm_diff * (km / 100.0)

        # 3) Tiny SR nudge from ELO (kept very small)
        elo_norm = max(-1.0, min(1.0, (self.rating - ELO_REF) / ELO_SPAN))
        delta_from_elo = SR_ELO_NUDGE_PER_100KM * elo_norm * (km / 100.0)

        delta = delta_from_clean + delta_from_elo
        sr_now = self.safety_rating
        if sr_now >= DRAG_START_SR:
            # distance from the cap, normalized to [0..1] across the 4.0→4.99 band
            width = max(1e-6, SR_MAX - DRAG_START_SR)
            t = (SR_MAX - sr_now) / width     # t=1 at 4.00, t→0 at 4.99

            # Positive gains are damped smoothly as SR→4.99
            # m_pos goes from ~1.0 at 4.00 down to MIN_POS_GAIN_MULT near 4.99
            m_pos = max(MIN_POS_GAIN_MULT, t ** DRAG_EXPONENT)

            # Losses can sting more near the cap (optional, keep =1 if you don't want this)
            # m_neg ramps from 1.0 at 4.00 up to 1.0+NEG_BITE_AT_MAX at 4.99
            m_neg = 1.0 + NEG_BITE_AT_MAX * (1.0 - t)

            if delta > 0:
                delta *= m_pos
            elif delta < 0:
                delta *= m_neg
        # Optional clamp so one race can't swing too much
        delta = max(-0.30, min(0.30, delta))

        # 4) Apply, clamp, and snap at integer boundaries
        before = self.safety_rating
        after  = max(SR_MIN, min(SR_MAX, before + delta))
        after  = self._apply_sr_jump(before, after)
        self.safety_rating = max(SR_MIN, min(SR_MAX, after))

        # 5) Recompute license
        self.recompute_license()

    def recompute_license(self):
        """
        Assign highest license the racer qualifies for based on SR, ELO, and races.
        """
        # Rookie if not enough races
        if getattr(self, "numraces", 0) < MIN_RACES_FOR_LICENSE:
            self.licenseclass = "Rookie"
            return

        sr = float(getattr(self, "safety_rating", SR_DEFAULT_START))
        elo = float(getattr(self, "rating", ELO_REF))

        # try A→B→C→D
        for cls in ("A", "B", "C", "D"):
            if sr >= LICENSE_SR_THRESH[cls] and elo >= LICENSE_ELO_THRESH[cls]:
                self.licenseclass = cls
                return

        # otherwise Rookie (or keep last known)
        self.licenseclass = "Rookie"
        
    def update_rating(self, opponent_rating, result, numracers, resultfile, otherracer, k_factor=16):
        if self.numraces < 10:
            k_factor=8
        else:
            k_factor=4
        selfratingtouse = self.ratingbeforeeachresult
        if self.ratingbeforeeachresult == -1:
            selfratingtouse = self.rating
        expected_score = 1 / (1 + 10 ** ((opponent_rating - selfratingtouse) / 400))
        change = k_factor * (result - expected_score)

        

        if resultfile.shortorlong == "long":
            change = change * 1.5
        self.rating += change
        if resultfile in self.historyofratingchange:
            self.historyofratingchange[resultfile] += change
        else:
            self.historyofratingchange[resultfile] = change
        self.historyofratingchange[resultfile] = round( self.historyofratingchange[resultfile], 2)
        self.rating = round(self.rating, 2)
        
        return round( self.historyofratingchange[resultfile], 2)
    
    def update_qualifying_rating(self, opponent_rating, result, numracers, resultfile, otherracer, k_factor=16):
        if self.numraces < 10:
            k_factor=8
        else:
            k_factor=4
        selfratingtouse = self.qualyratingbeforeeachresult
        if self.qualyratingbeforeeachresult == -1:
            selfratingtouse = self.qualifyingrating
        expected_score = 1 / (1 + 10 ** ((opponent_rating - selfratingtouse) / 400))
        change = k_factor * (result - expected_score)
        self.qualifyingrating += change
        if resultfile in self.historyofqualifyingratingchange:
            self.historyofqualifyingratingchange[resultfile] += change
        else:
            self.historyofqualifyingratingchange[resultfile] = change
        self.historyofqualifyingratingchange[resultfile] = round( self.historyofqualifyingratingchange[resultfile], 2)
        self.qualifyingrating = round(self.rating, 2)
        
        return round( self.historyofqualifyingratingchange[resultfile], 2)

    def get_num_races(self, filterstr = None):
        if filterstr != None:
            if filterstr == "mx5":
                return self.numracesmx5
            if filterstr == "gt3":
                return self.numracesgt3
        else:
            return self.numraces
    
    def get_average_incidents(self, filterstr = None):
        if filterstr != None:
            if filterstr == "mx5":
                return self.averageincidentsmx5
            if filterstr == "gt3":
                return self.averageincidentsgt3
        else:
            return self.averageincidents


    from collections import defaultdict
    from math import fsum, sqrt
    from statistics import mean           # just for paceplot average

    def calculate_averages(self):
        """
        One‑pass computation of:
        * most / least successful track   (overall, gt3, mx5)
        * most‑hit other driver           (overall, gt3, mx5)
        * lap‑time consistency            (overall, gt3, mx5)
        * race consistency
        * pace plot & pace plot average   (per race)
        """

        # ---------- running containers ----------
        track_sum   = defaultdict(float); track_cnt   = defaultdict(int)
        track_sum_gt3 = defaultdict(float); track_cnt_gt3 = defaultdict(int)
        track_sum_mx5 = defaultdict(float); track_cnt_mx5 = defaultdict(int)

        collision_all = defaultdict(int)
        collision_gt3 = defaultdict(int); collision_mx5 = defaultdict(int)

        lap_consistency_all = []
        lap_consistency_gt3 = []
        lap_consistency_mx5 = []

        race_pos_list = []

        pace_vals_gt3 = []; pace_vals_mx5 = []; pace_vals_all = []

        # ---------- single traversal ----------
        for entry in self.entries:
            cls   = entry.result.mx5orgt3        # "gt3", "mx5", or "neither"
            pos   = entry.finishingposition
            track = entry.track

            # --- success / failure track data ----
            track_sum[track]   += pos; track_cnt[track]   += 1
            if cls == "gt3":
                track_sum_gt3[track] += pos; track_cnt_gt3[track] += 1
            elif cls == "mx5":
                track_sum_mx5[track] += pos; track_cnt_mx5[track] += 1

            # --- collisions ----
            for inc in entry.incidents:
                if inc.otherracer is None or inc.speed < 15:
                    continue
                collision_all[inc.otherracer] += 1
                if cls == "gt3":
                    collision_gt3[inc.otherracer] += 1
                elif cls == "mx5":
                    collision_mx5[inc.otherracer] += 1

            # --- lap‑time consistency (per race) ----
            lap_times = [lap.time for lap in entry.laps]
            if lap_times:
                if len(lap_times) == 1:
                    c_score = 100.0
                else:
                    μ   = fsum(lap_times) / len(lap_times)
                    var = fsum((x-μ)**2 for x in lap_times) / (len(lap_times)-1)
                    c_score = (1 - sqrt(var) / μ) * 100
                lap_consistency_all.append(c_score)
                if cls == "gt3":
                    lap_consistency_gt3.append(c_score)
                elif cls == "mx5":
                    lap_consistency_mx5.append(c_score)

            # --- race consistency list ----
            race_pos_list.append(pos)

            # --- pace percentage & plot ----
            #  (same rules you had in calculatepace)
            if entry.result.get_numlaps_of_racer(self) < 5:
                continue
            fastest_me   = entry.result.get_fastest_lap_of_racer(self)
            fastest_over = entry.track.get_fastest_lap_in_car(entry.car)
            if not fastest_me or not fastest_over:
                continue
            if entry.result.get_numlaps_of_racer(self) < entry.result.numlaps - 2:
                continue
            pct = round((fastest_over.time / fastest_me.time) * 100, 2)

            self.paceplot[entry.date] = pct
            self.paceplotaverage[entry.date] = round(mean(self.paceplot.values()), 2)

            pace_vals_all.append(pct)
            if cls == "gt3":
                pace_vals_gt3.append(pct)
            elif cls == "mx5":
                pace_vals_mx5.append(pct)

        # ---------- helper for best / worst track ----------
        def _track_key(sum_d, cnt_d, best=min):
            candidates = {t: sum_d[t] / cnt_d[t] for t in sum_d if cnt_d[t] > 1}
            if candidates:
                return best(candidates, key=candidates.get)
            # fall‑back: first track encountered (single race)
            return next(iter(sum_d)) if sum_d else None

        # ---------- assign results ----------
        self.mostsuccesfultrack     = _track_key(track_sum, track_cnt, best=min)
        self.leastsuccesfultrack    = _track_key(track_sum, track_cnt, best=max)

        self.mostsuccesfultrackgt3  = _track_key(track_sum_gt3, track_cnt_gt3, best=min)
        self.leastsuccesfultrackgt3 = _track_key(track_sum_gt3, track_cnt_gt3, best=max)

        self.mostsuccesfultrackmx5  = _track_key(track_sum_mx5, track_cnt_mx5, best=min)
        self.leastsuccesfultrackmx5 = _track_key(track_sum_mx5, track_cnt_mx5, best=max)

        # collisions (convert to object or stay None)
        def _max_or_none(d): return max(d, key=d.get) if d else None
        self.mosthitotherdriver     = _max_or_none(collision_all)
        self.mosthitotherdrivergt3  = _max_or_none(collision_gt3)
        self.mosthitotherdrivermx5  = _max_or_none(collision_mx5)
        self.collisionracers        = dict(collision_all)

        # lap‑consistency averages
        def _avg(lst): return round(fsum(lst) / len(lst), 2) if lst else None
        self.laptimeconsistency      = _avg(lap_consistency_all)
        self.laptimeconsistencygt3   = _avg(lap_consistency_gt3)
        self.laptimeconsistencymx5   = _avg(lap_consistency_mx5)

        # race consistency
        if len(race_pos_list) > 1:
            μ = fsum(race_pos_list) / len(race_pos_list)
            var = fsum((p-μ)**2 for p in race_pos_list) / (len(race_pos_list)-1)
            self.raceconsistency = round((1 - sqrt(var) / μ) * 100, 2)
        else:
            self.raceconsistency = 100.0

        # overall pace percentage fields (if you use them elsewhere)
        self.pace_percentage_overall = _avg(pace_vals_all) or 0
        self.pace_percentage_gt3     = _avg(pace_vals_gt3) or 0
        self.pace_percentage_mx5     = _avg(pace_vals_mx5) or 0


    def add_result(self, entry):
        """
        racer
        car
        track
        laps
        incidents
        result # parent result
        cuts
        finishingposition"""
        self.numraces += 1
        if entry.finishingposition == 1:
            self.wins += 1
            if entry.result.mx5orgt3 == "gt3":
                self.gt3wins += 1
            if entry.result.mx5orgt3 == "mx5":
                self.mx5wins += 1
        elif entry.finishingposition < 4:
            self.podiums += 1
            if entry.result.mx5orgt3 == "gt3":
                self.gt3podiums += 1
            if entry.result.mx5orgt3 == "mx5":
                self.mx5podiums += 1
        self.totallaps += len(entry.laps)
        if entry.result.mx5orgt3 == "gt3":
            self.gt3laps += len(entry.laps)
            self.numracesgt3 += 1
        if entry.result.mx5orgt3 == "mx5":
            self.mx5laps += len(entry.laps)
            self.numracesmx5 += 1
        self.entries.append(entry)
        self.result_add_ticker += 1
        self.startingpositions[entry.date] = entry.startingposition
        currentinchidents = 0
        for inchident in entry.incidents:
            sev = severity_from_speed(inchident.speed)          # 0-to-4
            if not sev:                                   # sev==0 → below 15 km/h
                continue

            base = CAR_BASE if inchident.otherracer else ENV_BASE
            pts  = min(base * sev, MAX_INC_POINTS)        # clamp at 4.0

            self.incidents      += pts
            currentinchidents    += pts
            if entry.result.mx5orgt3 == "mx5":
                self.incidentsmx5 += pts
            if entry.result.mx5orgt3 == "gt3":
                self.incidentsgt3 += pts
        length = parse_track_length_to_meters(entry.track.length, default_m=2000.0)
        session_km = (length * len(entry.laps)) / 1000.0
        self.distancedriven += (length * len(entry.laps)) / 1000.0 # in km
        self.averageincidents = (self.incidents / self.numraces) if self.numraces > 0 else 0.0
        km = float(self.distancedriven)
        inc = float(self.incidents)
        self.incidentsperkm = (inc / km) if km > 0 else 0.0
        if self.numracesgt3 > 0:
            self.averageincidentsgt3 = round(self.incidentsgt3 / self.numracesgt3, 2)
        if self.numracesmx5 > 0:
            self.averageincidentsmx5 = round(self.incidentsmx5 / self.numracesmx5, 4)
        self.update_safety_after_session(session_km, currentinchidents)
        
        finishingposition = entry.finishingposition
        if entry.startingposition != 0:
            self.positionchangeperrace[entry.date] = finishingposition - entry.startingposition
        numracers = len(entry.result.entries)
        percent = (finishingposition / numracers) * 100
        top = 100 - percent
        top = round(top, 2)
        self.positionplot[entry.date] = top
        self.positionaverage[entry.date] = round(mean(self.positionplot.values()), 2)
        self.incidentplot[entry.date] = currentinchidents
        if entry.result.region == "EU":
            self.eucount += 1
        if entry.result.region == "NA":
            self.nacount += 1

    def geteuorna(self):
        if self.eucount > self.nacount:
            return "EU"
        if self.nacount > self.eucount:
            return "NA"
        if self.eucount == self.nacount:
            return "EU"

    def update_chart(self, result, entry):
        if self.result_add_ticker >= 5:
            self.progression_plot[entry.date] = self.rating

        