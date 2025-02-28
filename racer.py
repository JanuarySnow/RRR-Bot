import math
import statistics
from statistics import mean
from logger_config import logger


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
        self.wins = 0
        self.podiums = 0
        self.totallaps = 0
        self.mostsuccesfultrack = None
        self.leastsuccesfultrack = None
        self.incidents = 0.1
        self.averageincidents = 0.0
        self.numraces = 0
        self.laptimeconsistency = None
        self.raceconsistency = None
        self.pace_percentage = 0
        self.historyofratingchange = {} # result, rating change
        self.positionplot = {}
        self.incidentplot = {}
        self.positionaverage = {}
        self.paceplot = {}
        self.paceplotaverage = {}
        self.logger = logger

        
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

    def get_num_races(self):
        return self.numraces
    
    def get_average_incidents(self, filterstr = None):
        return self.averageincidents
    
    def calculate_most_hit_other_driver(self):
        racertointdict = {}
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

        if racertointdict:
            mostoften = max(racertointdict, key = racertointdict.get)
            self.mosthitotherdriver = mostoften

    from statistics import mean

    def calculate_most_successful_track(self):
        trackresults = {}
        for entry in self.entries:
            position = entry.finishingposition
            if entry.track not in trackresults:
                trackresults[entry.track] = []
            trackresults[entry.track].append(position)


        averages = {}
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

    
    def calculate_least_successful_track(self):
        trackresults = {}


        for entry in self.entries:
            position = entry.finishingposition
            if entry.track not in trackresults:
                trackresults[entry.track] = []

            trackresults[entry.track].append(position)


        averages = {}
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


    def calculate_laptime_consistency(self):
        consistency_scores = []

        for entry in self.entries:
            laptimes = [lap.time for lap in entry.laps]

            if len(laptimes) > 1:
                mean_laptime = sum(laptimes) / len(laptimes)
                consistency_score = (1 - (statistics.stdev(laptimes) / mean_laptime)) * 100
            else:
                consistency_score = 100  # If there's only one lap, consistency is perfect

            consistency_scores.append(consistency_score)

        if consistency_scores:
            self.laptimeconsistency = round(sum(consistency_scores) / len(consistency_scores), 2)
        else:
            self.laptimeconsistency = None  # Indicate no lap data available
        



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
        elif entry.finishingposition < 4:
            self.podiums += 1
        self.totallaps += len(entry.laps)

        self.entries.append(entry)
        self.result_add_ticker += 1
        currentinchidents = 0
        for inchident in entry.incidents:
            if inchident.speed < 7:
                continue
            if inchident.otherracer != None:
                self.incidents += 1.0
                currentinchidents += 1.0
            else:
                self.incidents += 0.4
                currentinchidents += 0.4

        self.averageincidents = round(self.incidents / self.numraces, 2)

        finishingposition = entry.finishingposition
        numracers = len(entry.result.entries)
        percent = (finishingposition / numracers) * 100
        top = 100 - percent
        top = round(top, 2)
        self.positionplot[entry.date] = top
        self.positionaverage[entry.date] = round(mean(self.positionplot.values()), 2)
        self.incidentplot[entry.date] = self.averageincidents


    def calculatepace(self):
        for entry in self.entries:
            fastestlapthere_thisrace_byracer = entry.result.get_fastest_lap_of_racer(self)
            fastestlapthere = entry.track.get_fastest_lap_in_car(entry.car)
            if fastestlapthere_thisrace_byracer == None or fastestlapthere == None:
                continue
            fastestlapthere_thisrace_byracer = fastestlapthere_thisrace_byracer.time
            fastestlapthere = fastestlapthere.time
            if entry.result.get_numlaps_of_racer(self) < 5:
                continue
            percentage = round((fastestlapthere / fastestlapthere_thisrace_byracer) * 100, 2)
            if entry.result.get_numlaps_of_racer(self) < (entry.result.numlaps -2 ):
                continue
            self.paceplot[entry.date] = percentage
            self.paceplotaverage[entry.date] = round(mean(self.paceplot.values()), 2)

    def update_chart(self, result, entry):
        if self.result_add_ticker >= 5:
            self.progression_plot[entry.date] = self.rating

        