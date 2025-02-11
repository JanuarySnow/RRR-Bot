import math
import statistics
from statistics import mean

gt3ids = ["ks_audi_r8_lms_2016","bmw_z4_gt3", "ks_ferrari_488_gt3", "ks_lamborghini_huracan_gt3",
         "ks_mclaren_650_gt3", "ks_mercedes_amg_gt3", "ks_nissan_gtr_gt3", "ks_porsche_911_gt3_r_2016"]

class Racerprofile():
    def __init__(self, newname, newguid) -> None:
        self.name = newname
        self.guid = newguid
        self.entries = [] #racer, car, track, date, laps, incidents, result, finishingposition, cuts
        self.result_add_ticker = 0
        self.progression_plot = {}
        self.rating = 1500
        self.mx5rating = 1500
        self.gt3rating = 1500
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
        self.historyofratingchange = {} # result, rating change
        self.historyofratingchangemx5 = {} # result, rating change
        self.historyofratingchangegt3 = {} # result, rating change
        self.gt3progression_plot = {}
        self.mx5progression_plot = {}


    def update_rating(self, opponent_rating, result, numracers, resultfile, k_factor=16):
        if self.numraces < 10:
            k_factor=8
        else:
            k_factor=4
        if numracers < 5:
            k_factor = k_factor / 4
        expected_score = 1 / (1 + 10 ** ((opponent_rating - self.rating) / 400))
        
        change = k_factor * (result - expected_score)
        if resultfile.mx5orgt3 == "gt3":
            self.gt3rating += change
            if resultfile in self.historyofratingchangegt3:
                self.historyofratingchangegt3[resultfile] += change
            else:
                self.historyofratingchangegt3[resultfile] = change
            self.historyofratingchangegt3[resultfile] = round( self.historyofratingchangegt3[resultfile], 2)
            self.gt3rating = round(self.gt3rating, 2)
        elif resultfile.mx5orgt3 == "mx5":
            self.mx5rating += change
            if resultfile in self.historyofratingchangemx5:
                self.historyofratingchangemx5[resultfile] += change
            else:
                self.historyofratingchangemx5[resultfile] = change
            self.historyofratingchangemx5[resultfile] = round( self.historyofratingchangemx5[resultfile], 2)
            self.mx5rating = round(self.mx5rating, 2)
        self.rating += change
        if resultfile in self.historyofratingchange:
            self.historyofratingchange[resultfile] += change
        else:
            self.historyofratingchange[resultfile] = change
        self.historyofratingchange[resultfile] = round( self.historyofratingchange[resultfile], 2)
        self.rating = round(self.rating, 2)
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
    
    def calculate_most_hit_other_driver(self):
        racertointdict = {}
        racertointdictgt3 = {}
        racertointdictmx5 = {}
        #racer, car, track, date, laps, incidents, result, finishingposition, cuts
        for entry in self.entries:
            for inchident in entry.incidents:
                #speed, racer, otherracer
                if inchident.otherracer != None:
                    if inchident.speed < 7:
                        continue
                    if not inchident.otherracer in racertointdict:
                        racertointdict[inchident.otherracer] = 1
                    else:
                        racertointdict[inchident.otherracer] += 1
                    if entry.result.mx5orgt3 == "mx5":
                        if not inchident.otherracer in racertointdictmx5:
                            racertointdictmx5[inchident.otherracer] = 1
                        else:
                            racertointdictmx5[inchident.otherracer] += 1
                    if entry.result.mx5orgt3 == "gt3":
                        if not inchident.otherracer in racertointdictgt3:
                            racertointdictgt3[inchident.otherracer] = 1
                        else:
                            racertointdictgt3[inchident.otherracer] += 1

        if racertointdict:
            mostoften = max(racertointdict, key = racertointdict.get)
            self.mosthitotherdriver = mostoften
        if racertointdictgt3:
            mostoften = max(racertointdictgt3, key = racertointdictgt3.get)
            self.mosthitotherdrivergt3 = mostoften
        if racertointdictmx5:
            mostoften = max(racertointdictmx5, key = racertointdictmx5.get)
            self.mosthitotherdrivermx5 = mostoften

    from statistics import mean

    def calculate_most_successful_track(self):
        trackresults = {}
        trackresultsgt3 = {}
        trackresultsmx5 = {}
        for entry in self.entries:
            position = entry.finishingposition
            if entry.track not in trackresults:
                trackresults[entry.track] = []
            if entry.result.mx5orgt3 == "mx5" and entry.track not in trackresultsmx5:
                trackresultsmx5[entry.track] = []
            if entry.result.mx5orgt3 == "gt3" and entry.track not in trackresultsgt3:
                trackresultsgt3[entry.track] = []
            trackresults[entry.track].append(position)
            if entry.result.mx5orgt3 == "gt3":
                trackresultsgt3[entry.track].append(position)
            if entry.result.mx5orgt3 == "mx5":
                trackresultsmx5[entry.track].append(position)

        averages = {}
        averagesgt3 = {}
        averagesmx5 = {}
        found_any_over_one = False

        for track, positions in trackresults.items():
            if len(positions) < 2:
                continue
            found_any_over_one = True
            averages[track] = mean(positions)

        if not found_any_over_one:
            # Return the first track with a single race position
            for track, positions in trackresults.items():
                return {track: positions[0]}  # Return a dict with track and first position entry

        # Return the track with the lowest average finishing position
        self.mostsuccesfultrack = min(averages, key=averages.get)


        found_any_over_one = False
        for track, positions in trackresultsmx5.items():
            if len(positions) < 2:
                continue
            found_any_over_one = True
            averagesmx5[track] = mean(positions)

        if not found_any_over_one:
            # Return the first track with a single race position
            for track, positions in trackresultsmx5.items():
                self.mostsuccesfultrackmx5 = {track: positions[0]}  # Return a dict with track and first position entry

        # Return the track with the lowest average finishing position
        if len(averagesmx5) > 0:
            self.mostsuccesfultrackmx5 = min(averagesmx5, key=averagesmx5.get)

        found_any_over_one = False
        for track, positions in trackresultsgt3.items():
            if len(positions) < 2:
                continue
            found_any_over_one = True
            averagesgt3[track] = mean(positions)

        if not found_any_over_one:
            # Return the first track with a single race position
            for track, positions in trackresultsgt3.items():
                self.mosthitotherdrivergt3 = {track: positions[0]}  # Return a dict with track and first position entry

        # Return the track with the lowest average finishing position
        if len(averagesgt3) > 0:
            self.mostsuccesfultrackgt3 = min(averagesgt3, key=averagesgt3.get)
    
    def calculate_least_successful_track(self):
        trackresults = {}
        trackresultsgt3 = {}
        trackresultsmx5 = {}

        for entry in self.entries:
            position = entry.finishingposition
            if entry.track not in trackresults:
                trackresults[entry.track] = []
            if entry.result.mx5orgt3 == "mx5" and entry.track not in trackresultsmx5:
                trackresultsmx5[entry.track] = []
            if entry.result.mx5orgt3 == "gt3" and entry.track not in trackresultsgt3:
                trackresultsgt3[entry.track] = []
            trackresults[entry.track].append(position)
            if entry.result.mx5orgt3 == "gt3":
                trackresultsgt3[entry.track].append(position)
            if entry.result.mx5orgt3 == "mx5":
                trackresultsmx5[entry.track].append(position)

        averages = {}
        averagesgt3 = {}
        averagesmx5 = {}
        found_any_over_one = False

        for track, positions in trackresults.items():
            if len(positions) < 2:
                continue
            found_any_over_one = True
            averages[track] = mean(positions)

        if not found_any_over_one:
            # Return the first track with a single race position
            for track, positions in trackresults.items():
                return {track: positions[0]}  # Return a dict with track and first position entry

        # Return the track with the highest average finishing position
        self.leastsuccesfultrack = max(averages, key=averages.get)
    
        found_any_over_one = False
        for track, positions in trackresultsgt3.items():
            if len(positions) < 2:
                continue
            found_any_over_one = True
            averagesgt3[track] = mean(positions)

        if not found_any_over_one:
            # Return the first track with a single race position
            for track, positions in trackresultsgt3.items():
                self.leastsuccesfultrackgt3 = {track: positions[0]}  # Return a dict with track and first position entry

        # Return the track with the highest average finishing position
        if len(averagesgt3) > 0:
            self.leastsuccesfultrackgt3 = max(averagesgt3, key=averagesgt3.get)
    
        found_any_over_one = False

        for track, positions in trackresultsmx5.items():
            if len(positions) < 2:
                continue
            found_any_over_one = True
            averagesmx5[track] = mean(positions)

        if not found_any_over_one:
            # Return the first track with a single race position
            for track, positions in trackresultsmx5.items():
                self.leastsuccesfultrackmx5 = {track: positions[0]}  # Return a dict with track and first position entry

        # Return the track with the highest average finishing position
        if len(averagesmx5) > 0:
            self.leastsuccesfultrackmx5 = max(averagesmx5, key=averagesmx5.get)


    def calculate_laptime_consistency(self):
        consistency_scores = []
        consistency_scoresgt3 = []
        consistency_scoresmx5 = []
        for entry in self.entries:
            laptimes = [lap.time for lap in entry.laps]

            if len(laptimes) > 1:
                mean_laptime = sum(laptimes) / len(laptimes)
                consistency_score = (1 - (statistics.stdev(laptimes) / mean_laptime)) * 100
            else:
                consistency_score = 100  # If there's only one lap, consistency is perfect
            if entry.result.mx5orgt3 == "mx5":
                consistency_scoresmx5.append(consistency_score)
            if entry.result.mx5orgt3 == "gt3":
                consistency_scoresgt3.append(consistency_score)
            consistency_scores.append(consistency_score)

        if consistency_scores:
            self.laptimeconsistency = round(sum(consistency_scores) / len(consistency_scores), 2)
        else:
            self.laptimeconsistency = None  # Indicate no lap data available
        
        if consistency_scoresmx5:
            self.laptimeconsistencymx5 = round(sum(consistency_scoresmx5) / len(consistency_scoresmx5), 2)
        else:
            self.laptimeconsistencymx5 = None  # Indicate no lap data available

        if consistency_scoresgt3:
            self.laptimeconsistencygt3 = round(sum(consistency_scoresgt3) / len(consistency_scoresgt3), 2)
        else:
            self.laptimeconsistencygt3 = None  # Indicate no lap data available



    def calculate_race_consistency(self):
        consistency_scores = []

        for entry in self.entries:
            position = entry.finishingposition
            consistency_scores.append(position)

        if len(consistency_scores) > 1:
            mean_position = sum(consistency_scores) / len(consistency_scores)
            consistency_score = (1 - (statistics.stdev(consistency_scores) / mean_position)) * 100
            self.raceconsistency = round(consistency_score, 2)
        else:
            self.raceconsistency = 100  # Indicate perfect consistency if only one race


    def calculate_averages(self):
        self.calculate_most_successful_track()
        self.calculate_least_successful_track()
        self.calculate_most_hit_other_driver()
        self.calculate_laptime_consistency()
        self.calculate_race_consistency()

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
        for inchident in entry.incidents:
            if inchident.speed < 7:
                continue
            if inchident.otherracer != None:
                self.incidents += 1.0
                if entry.result.mx5orgt3 == "mx5":
                    self.incidentsmx5 += 1.0
                if entry.result.mx5orgt3 == "gt3":
                    self.incidentsgt3 += 1.0
            else:
                self.incidents += 0.4
                if entry.result.mx5orgt3 == "mx5":
                    self.incidentsmx5 += 0.4
                if entry.result.mx5orgt3 == "gt3":
                    self.incidentsgt3 += 0.4
        self.averageincidents = round(self.incidents / self.numraces, 2)
        if self.numracesgt3 > 0:
            self.averageincidentsgt3 = round(self.incidentsgt3 / self.numracesgt3, 2)
        if self.numracesmx5 > 0:
            self.averageincidentsmx5 = round(self.incidentsmx5 / self.numracesmx5, 2)
            

    def update_chart(self, result, entry):
        if self.result_add_ticker >= 5:
            self.progression_plot[entry.date] = self.rating
        if entry.result.mx5orgt3 == "gt3":
            self.gt3progression_plot[entry.date] = self.gt3rating
        if entry.result.mx5orgt3 == "mx5":
            self.mx5progression_plot[entry.date] = self.mx5rating

        