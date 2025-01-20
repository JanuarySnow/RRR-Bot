import math
import statistics

class Racerprofile():
    def __init__(self, newname, newguid) -> None:
        self.name = newname
        self.guid = newguid
        self.entries = [] #racer, car, track, date, laps, incidents, result, finishingposition, cuts
        self.result_add_ticker = 0
        self.progression_plot = {}
        self.rating = 1500
        self.mosthitotherdriver = None
        self.wins = 0
        self.podiums = 0
        self.totallaps = 0
        self.mostsuccesfultrack = None
        self.averageincidents = -1.0
        self.numraces = 0
        self.laptimeconsistency = None
        self.raceconsistency = None
        self.pace_percentage_mx5 = 0
        self.pace_percentage_gt3 = 0


    def update_rating(self, opponent_rating, result, numracers, k_factor=32):
        if self.numraces < 10:
            k_factor=16
        else:
            k_factor=8
        if numracers < 5:
            k_factor = k_factor / 4
        expected_score = 1 / (1 + 10 ** ((opponent_rating - self.rating) / 400))
        self.rating += k_factor * (result - expected_score)
        self.rating = round(self.rating, 2)

    def get_num_races(self):
        return len(self.entries)
    
    def get_num_wins(self):
        numwins = 0
        for entry in self.entries:
            if entry.finishingposition == 1:
                numwins += 1
        return numwins
    
    def get_num_podiums(self):
        numpodiums = 0
        for entry in self.entries:
            if entry.finishingposition < 4 and entry.finishingposition != 1:
                numpodiums += 1
        return numpodiums
    
    def get_average_incidents(self):
        inchidents = 0
        for entry in self.entries:
            for inchident in entry.incidents:
                if inchident.speed < 7:
                    continue
                if inchident.otherracer != None:
                    inchidents += 1.0
                else:
                    inchidents += 0.5
            for lap in entry.laps:
                if lap.cuts > 0:
                    inchidents += (lap.cuts * 0.1)
        return round(inchidents / len(self.entries), 2)
    
    def calculate_most_hit_other_driver(self):
        racertointdict = {}
        if self.name == "Josh":
            print("Josh entries size = " + str(len(self.entries)))
        #racer, car, track, date, laps, incidents, result, finishingposition, cuts
        for entry in self.entries:
            for inchident in entry.incidents:
                #speed, racer, otherracer
                if inchident.otherracer != None:
                    if self.name == "Josh":
                        print("found other incident " + inchident.otherracer.name)
                    if inchident.speed < 7:
                        continue
                    if not inchident.otherracer in racertointdict:
                        racertointdict[inchident.otherracer] = 1
                    else:
                        racertointdict[inchident.otherracer] += 1
        if racertointdict:
            mostoften = max(racertointdict, key = racertointdict.get)
            if self.name == "Josh":
                print(" MOST OFTEN + " + mostoften.name)
            return mostoften
        return None

    def calculate_most_succesful_track(self):
        trackresults = {}
        averages = {}
        onlyonetrack = None
        for entry in self.entries:
            position = entry.finishingposition
            if entry.track in trackresults:
                trackresults[entry.track].append(position)
            else:
                trackresults[entry.track] = []
                trackresults[entry.track].append(position)
        foundanyoverone = False
        for trackresult in trackresults.keys():
            array = trackresults[trackresult]
            numtimes = len(array)
            if numtimes < 2:
                continue
            else:
                foundanyoverone = True
            runningtotal = 0
            for pos in array:
                runningtotal += pos
            average = round(runningtotal / numtimes, 2)
            averages[trackresult] = average
        if not foundanyoverone:
            for trackresult in trackresults.keys():
                return trackresults[trackresult]
            # first track entry, first position entry at that track ( as there should only be one)
        else:
            return min(averages, key = averages.get)

    def calculate_total_laps(self):
        total = 0
        for entry in self.entries:
            total += len(entry.laps)
        return total

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
        self.wins = self.get_num_wins()
        self.podiums = self.get_num_podiums()
        self.averageincidents = self.get_average_incidents()
        self.mostsuccesfultrack = self.calculate_most_succesful_track()
        self.totallaps = self.calculate_total_laps()
        self.mosthitotherdriver = self.calculate_most_hit_other_driver()
        self.numraces = self.get_num_races()
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
        if self.result_add_ticker >= 5:
            self.progression_plot[entry.date] = self.rating

        