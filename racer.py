import math
import statistics
from statistics import mean
from logger_config import logger
from collections import defaultdict
from math import fsum, sqrt  

gt3ids = ["ks_audi_r8_lms_2016","bmw_z4_gt3", "ks_ferrari_488_gt3", "ks_lamborghini_huracan_gt3",
         "ks_mclaren_650_gt3", "ks_mercedes_amg_gt3", "ks_nissan_gtr_gt3", "ks_porsche_911_gt3_r_2016"]

class Racerprofile():
    def __init__(self, newname, newguid) -> None:
        self.name = newname
        self.guid = newguid
        self.entries = [] #racer, car, track, date, laps, incidents, result, finishingposition, cuts
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
    
    def to_dict(self):
        return {
            'name': self.name,
            'guid': self.guid,
            'entries': [entry.id for entry in self.entries],
            'result_add_ticker': self.result_add_ticker,
            'progression_plot': self.progression_plot,
            'eucount': self.eucount,
            'nacount': self.nacount,
            'rating': self.rating,
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
    }

    

        
    def update_rating(self, opponent_rating, result, numracers, resultfile, otherracer, k_factor=16):
        if self.numraces < 10:
            k_factor=8
        else:
            k_factor=4
        if numracers < 5:
            k_factor = k_factor / 4
        expected_score = 1 / (1 + 10 ** ((opponent_rating - self.rating) / 400))
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
        self.retroactive_rating = self.rating
        return round( self.historyofratingchange[resultfile], 2)

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
                if inc.otherracer is None or inc.speed < 7:
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
        currentinchidents = 0
        for inchident in entry.incidents:
            if inchident.speed < 7:
                continue
            if inchident.otherracer != None:
                self.incidents += 1.0
                currentinchidents += 1.0
                if entry.result.mx5orgt3 == "mx5":
                    self.incidentsmx5 += 1.0
                if entry.result.mx5orgt3 == "gt3":
                    self.incidentsgt3 += 1.0
            else:
                self.incidents += 0.4
                currentinchidents += 0.4
                if entry.result.mx5orgt3 == "mx5":
                    self.incidentsmx5 += 0.4
                if entry.result.mx5orgt3 == "gt3":
                    self.incidentsgt3 += 0.4
        self.averageincidents = round(self.incidents / self.numraces, 2)
        if self.numracesgt3 > 0:
            self.averageincidentsgt3 = round(self.incidentsgt3 / self.numracesgt3, 2)
        if self.numracesmx5 > 0:
            self.averageincidentsmx5 = round(self.incidentsmx5 / self.numracesmx5, 2)
        finishingposition = entry.finishingposition
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

        