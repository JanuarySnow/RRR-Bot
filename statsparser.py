import math
import pandas as pd
from discord.ext import commands, tasks
from discord.ext.commands import Context
from dateutil.parser import parse
import json
from operator import itemgetter
import os
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

class parser():
    def __init__(self):
        self.raceresults = []

        self.racers = {} # guid to racer object
        self.usedtracks = {} # guid to trackvariant object
        self.usedcars = {}
        self.safety_rankings = []
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
        self.elorankingsgt3 = []
        self.elorankingsmx5 = []
        self.laptimeconsistencyrankings = []
        self.laptimeconsistencyrankingsmx5 = []
        self.laptimeconsistencyrankingsgt3 = []
        self.positionconsistencyrankings = []
        self.pacerankingsmx5 = []
        self.pacerankingsgt3 = []

        self.contentdata = None

    def get_summary_last_races(self, racer, num):
        retdict = {}
        sorted_entries = racer.entries.sort(key=lambda entry: entry.date, reverse=True)
        sorted_results = sorted(racer.historyofratingchange.items(), key=lambda item: item[0].date, reverse=True)
        x = num  # number of recent results you want to retrieve
        most_recent_results = sorted_results[:x]
        # Output the most recent results
        for result, rating_change in most_recent_results:
            retdict[result] = (result.get_position_of_racer(racer), rating_change)
        return retdict

    
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
    
    def get_track_name(self, id:str):
        for track in content_data.tracks:
            if id == track.id:
                return track.highest_priority_name
        return None
    
    def get_track_variants(self, id:str):
        for track in content_data.tracks:
            if id == track.id:
                return track.variants
        return None

    def calculate_rankings(self):
        safetydict = {}
        safetydictgt3 = {}
        safetydictmx5 = {}
        winsdict = {}
        podiumsdict = {}
        elodict = {}

        winsdictgt3 = {}
        podiumsdictgt3 = {}
        elodictgt3 = {}

        winsdictmx5 = {}
        podiumsdictmx5 = {}
        elodictmx5 = {}

        laptimeconsistencydict = {}
        laptimeconsistencydictgt3 = {}
        laptimeconsistencydictmx5 = {}
        raceconsistencydict = {}
        pacedictgt3 = {}
        pacedictmx5 = {}
        for racerid in self.racers.keys():
            racer = self.racers[racerid]
            if racer.numraces > 10:
                safetydict[racer] = racer.averageincidents
                if racer.averageincidentsgt3:
                    safetydictgt3[racer] = racer.averageincidentsgt3
                if racer.averageincidentsmx5:
                    safetydictmx5[racer] = racer.averageincidentsmx5
                winsdict[racer] = racer.wins
                podiumsdict[racer] = racer.podiums
                elodict[racer] = racer.rating

                winsdictgt3[racer] = racer.gt3wins
                podiumsdictgt3[racer] = racer.gt3podiums
                elodictgt3[racer] = racer.gt3rating

                winsdictmx5[racer] = racer.mx5wins
                podiumsdictmx5[racer] = racer.mx5podiums
                elodictmx5[racer] = racer.mx5rating
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
        self.wins_rankings = [racer for racer in sorted(winsdict, key=winsdict.get, reverse=True)]
        self.podiums_rankings = [racer for racer in sorted(podiumsdict, key=podiumsdict.get, reverse=True)]
        self.elorankings = [racer for racer in sorted(elodict, key=elodict.get, reverse=True)]
        self.laptimeconsistencyrankings = [racer for racer in sorted(laptimeconsistencydict, key=laptimeconsistencydict.get, reverse=True)]
        self.laptimeconsistencyrankingsgt3 = [racer for racer in sorted(laptimeconsistencydictgt3, key=laptimeconsistencydictgt3.get, reverse=True)]
        self.laptimeconsistencyrankingsmx5 = [racer for racer in sorted(laptimeconsistencydictmx5, key=laptimeconsistencydictmx5.get, reverse=True)]
        self.positionconsistencyrankings = [racer for racer in sorted(raceconsistencydict, key=raceconsistencydict.get, reverse=True)]
        self.pacerankingsgt3 = [racer for racer in sorted(pacedictgt3, key=pacedictgt3.get, reverse=True)]
        self.pacerankingsmx5 = [racer for racer in sorted(pacedictmx5, key=pacedictmx5.get, reverse=True)]
        
        self.wins_rankingsgt3 = [racer for racer in sorted(winsdictgt3, key=winsdictgt3.get, reverse=True)]
        self.podiums_rankingsgt3 = [racer for racer in sorted(podiumsdictgt3, key=podiumsdictgt3.get, reverse=True)]
        self.elorankingsgt3 = [racer for racer in sorted(elodictgt3, key=elodictgt3.get, reverse=True)]

        self.wins_rankingsmx5 = [racer for racer in sorted(winsdictmx5, key=winsdictmx5.get, reverse=True)]
        self.podiums_rankingsmx5 = [racer for racer in sorted(podiumsdictmx5, key=podiumsdictmx5.get, reverse=True)]
        self.elorankingsmx5 = [racer for racer in sorted(elodictmx5, key=elodictmx5.get, reverse=True)]
    
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
            if racer.numraces > 5:
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
                        matchesfound = 3
                    matchesfound += 1
                    break
            if matchesfound >= 3:
                break
        return result

    def get_all_result_files(self):
        datalist = []

    # Traverse the entire directory tree
        for root, dirs, files in os.walk("results/"):
            for filename in files:
                if filename.endswith(".json"):
                    filepath = os.path.join(root, filename)
                    with open(filepath, encoding="utf8") as f:
                        data = json.load(f)
                        if data.get("Type") == "RACE":
                            data["Filename"] = filename
                            # Parse the date string to a datetime object
                            race_time = datetime.fromisoformat(data["Date"].replace("Z", "+00:00"))
                            # Determine the region based on the race time
                            if race_time.hour < 24 and race_time.hour >= 12:
                                data["Region"] = "EU"
                            else:
                                data["Region"] = "NA"
                            datalist.append(data)
        return datalist
    
    def get_eu_racers(self):
        euracers = []
        for racer in self.racers.values():
            if racer.geteuorna() == "EU" and racer.numraces > 5:
                euracers.append(racer)
        return euracers
    
    def get_na_racers(self):
        euracers = []
        for racer in self.racers.values():
            if racer.geteuorna() == "NA" and racer.numraces > 5:
                euracers.append(racer)
        return euracers
    
    async def add_one_result(self, filepath, filename):
        print("adding one result " + filename)
        with open(filepath, encoding="utf8") as f:
            data = json.load(f)
            if data.get("Type") == "RACE":
                data["Filename"] = filename
                resultobj = result.Result()
                resultobj.filename = data["Filename"]

                # Parse the date string to a datetime object
                race_time = datetime.fromisoformat(data["Date"].replace("Z", "+00:00"))
                # Determine the region based on the race time
                if race_time.hour < 24 and race_time.hour >= 12:
                    data["Region"] = "EU"
                else:
                    data["Region"] = "NA"
                self.parse_one_result(resultobj, data)

                for car in data["Result"]:
                    if not car["DriverGuid"] or car["DriverGuid"] == "" :
                        continue
                    guid = car["DriverGuid"]
                    racer = self.racers[guid]
                    racer.calculate_averages()
                    racer.calculatepace()
        self.calculate_raw_pace_percentages_for_all_racers()
        self.calculate_rankings()
        print("done adding one result")

    
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
        else:
            #this is a track without a variant id
            combinedid = baseid + ";" + baseid
            if combinedid in self.usedtracks:
                result.track = self.usedtracks[combinedid]
            else:
                # not in used tracks
                #does it exist in content library?
                trackvariant = self.contentdata.get_track(combinedid)
                if trackvariant == None:
                    trackvariant = self.contentdata.create_basic_track(baseid, "")
                    self.usedtracks[combinedid] = trackvariant
                    result.track = trackvariant
                else:
                    self.usedtracks[combinedid] = trackvariant
                    result.track = trackvariant
        if result.track == None:
            print("found a NONE track")   
            print("track id in result file = " + data["TrackName"])
            if "TrackConfig" in data and data["TrackConfig"] != "":
                print("and config = " + data["TrackConfig"])
            else:
                print("no config")

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

    
    def parse_one_result(self, result, data):
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
        self.elorankingsgt3.clear()
        self.elorankingsmx5.clear()
        self.laptimeconsistencyrankings.clear()
        self.laptimeconsistencyrankingsmx5.clear()
        self.laptimeconsistencyrankingsgt3.clear()
        self.positionconsistencyrankings.clear()
        self.pacerankingsmx5.clear()
        self.pacerankingsgt3.clear()

    def refresh_all_data(self):
        self.clear_old_data()
        self.contentdata = content_data.Contentdata()
        print("loaded content data")
        self.contentdata.load_cars()
        print("loaded cars")
        self.contentdata.load_tracks()
        print("loaded tracks")
        print("loaded parser")
        datalist = self.get_all_result_files()
        datalist = sorted(datalist, key=lambda d: d["Date"])
        for data in datalist:
            resultobj = result.Result()
            resultobj.filename = data["Filename"]
            self.parse_one_result(resultobj, data)
        for elem in self.racers.keys():
            racer = self.racers[elem]
            racer.calculate_averages()
            racer.calculatepace()
        self.calculate_raw_pace_percentages_for_all_racers()
        self.calculate_rankings()
        
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


    def get_overall_stats(self, recently_active=False):
        elos = []
        mx5elos = []
        gt3elos = []
        safety = []
        laptime_consistency = []

        recent_threshold = datetime.now() - timedelta(days=180)
        recent_threshold = recent_threshold.replace(tzinfo=None)  

        def is_recently_active(racer):
            return any(datetime.fromisoformat(result.date).replace(tzinfo=None) >= recent_threshold for result in racer.entries)

        def filter_active(rankings):
            return [racer for racer in rankings if is_recently_active(racer)]

        # Get top 10 ELO rankings
        rankings_to_use = filter_active(self.elorankings) if recently_active else self.elorankings
        for index, elem in enumerate(rankings_to_use):
            if index >= 10:
                break
            elos.append({
                'rank': index + 1,
                'name': elem.name,
                'rating': elem.rating
            })

        # Get top 10 GT3 rankings
        rankings_to_use = filter_active(self.elorankingsgt3) if recently_active else self.elorankingsgt3
        for index, elem in enumerate(rankings_to_use):
            if index >= 10:
                break
            gt3elos.append({
                'rank': index + 1,
                'name': elem.name,
                'rating': elem.gt3rating
            })

        # Get top 10 MX5 rankings
        rankings_to_use = filter_active(self.elorankingsmx5) if recently_active else self.elorankingsmx5
        for index, elem in enumerate(rankings_to_use):
            if index >= 10:
                break
            mx5elos.append({
                'rank': index + 1,
                'name': elem.name,
                'rating': elem.mx5rating
            })

        # Get top 10 clean racers
        rankings_to_use = filter_active(self.safety_rankings) if recently_active else self.safety_rankings
        for index, elem in enumerate(rankings_to_use):
            if index >= 10:
                break
            safety.append({
                'rank': index + 1,
                'name': elem.name,
                'averageincidents': elem.averageincidents
            })

        # Get top 10 lap time consistency
        rankings_to_use = filter_active(self.laptimeconsistencyrankings) if recently_active else self.laptimeconsistencyrankings
        for index, elem in enumerate(rankings_to_use):
            if index >= 10:
                break
            laptime_consistency.append({
                'rank': index + 1,
                'name': elem.name,
                'laptimeconsistency': elem.laptimeconsistency
            })

        return {
            'elos': elos,
            'safety': safety,
            'laptime_consistency': laptime_consistency,
            'mx5elos': mx5elos,
            'gt3elos': gt3elos
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

            listofvisited = []

            for entry in racer.entries:
                variant = entry.track
                if variant in listofvisited:
                    continue
                listofvisited.append(variant)
                car = entry.car
                car_type = None
                if car.id in result.gt3ids:
                    car_type = 'gt3'
                elif car.id == "ks_mazda_mx5_cup":
                    car_type = 'mx5'

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
                    thisracerfastest = variant.get_fastest_lap_in_mx5(racerguid)
                    if thisracerfastest == None:
                        continue
                    if thisracerfastest and thisracerfastest.time != 0:
                        percentage_mx5 = (fastest / thisracerfastest.time) * 100
                        total_percentage_mx5 += percentage_mx5
                        count_mx5 += 1
                elif car_type == 'gt3':
                    thisracerfastest = variant.get_fastest_lap_in_gt3(racerguid)
                    if thisracerfastest == None:
                        continue
                    if thisracerfastest and thisracerfastest.time != 0:
                        percentage_gt3 = (fastest / thisracerfastest.time) * 100
                        total_percentage_gt3 += percentage_gt3
                        count_gt3 += 1
    
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
                if rankings == self.elorankings:
                    if filter == "mx5":
                        rankings = self.elorankingsmx5
                    if filter == "gt3":
                        rankings = self.elorankingsgt3
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

    def get_wins_rank(self, racer, filter=None):
        return self.get_rank(racer, self.wins_rankings, filter)

    def get_podiums_rank(self, racer, filter=None):
        return self.get_rank(racer, self.podiums_rankings, filter)

    def get_safety_rank(self, racer, filter=None):
        return self.get_rank(racer, self.safety_rankings, filter)

    
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

    def plot_racers_scatter(self):
        recent_threshold = datetime.now() - timedelta(days=180)
        recent_threshold = recent_threshold.replace(tzinfo=None) 

        def is_recently_active(racer):
            return any(datetime.fromisoformat(result.date).replace(tzinfo=None) >= recent_threshold for result in racer.entries)

        racers = self.elorankings  # Assuming self.elorankings has all racers
        racers = [racer for racer in self.elorankings if is_recently_active(racer)]

        elo_ratings = [racer.rating for racer in racers]
        cleanliness_ratings = [racer.averageincidents for racer in racers]
        names = [racer.name for racer in racers]

        plt.figure(figsize=(18, 18))  # Set the figure size
        plt.scatter(cleanliness_ratings, elo_ratings, s=100, c='blue', alpha=0.6, edgecolors='w', linewidth=2)
        texts = [plt.text(cleanliness_ratings[i], elo_ratings[i], name, fontsize=14, ha='right', va='bottom', color='red')
            for i, name in enumerate(names)]
        adjust_text(texts, arrowprops=dict(arrowstyle='-', color='black'))
        # Calculate averages
        avg_cleanliness = sum(cleanliness_ratings) / len(cleanliness_ratings)
        avg_elo = sum(elo_ratings) / len(elo_ratings)

        # Add average lines
        plt.axhline(y=avg_elo, color='green', linestyle='--', linewidth=2, label=f'Average ELO: {avg_elo:.2f}')
        plt.axvline(x=avg_cleanliness, color='orange', linestyle='--', linewidth=2, label=f'Average Incidents per Race: {avg_cleanliness:.2f}')
        plt.xlabel('Clean Racer', fontsize=14)  # Larger font size for X axis
        plt.ylabel('ELO Score', fontsize=14)  # Larger font size for Y axis
        plt.title('Scatter Plot of Racers', fontsize=18)  # Larger font size for the title
        plt.grid(True)
        plt.xticks(fontsize=14)  # Larger font size for X axis ticks
        plt.yticks(fontsize=14)  # Larger font size for Y axis ticks
        plt.gca().invert_xaxis()
        plt.savefig('scatter_plot.png')  # Save the figure as an image file
        plt.close()  # Close the plot to free up memory



    def test_output(self, id):
        racer = self.racers[id]
        for entry in racer.entries:
            print("race for " + racer.name +"\n" )
            print( "\n")
            print( "\n")
            print("race at : " + entry.track.id + " , finishing position is " + str(entry.finishingposition))
            print( "\n")
            print("filename is : " + entry.result.filename)

    def get_dirty_drivers(self):
        revlist = list(reversed(self.safety_rankings))
        index = 1
        retstring = "Top 10 Dirtiest drivers:" + "\n" + "\n"
        for elem in revlist:
            if index > 10:
                break
            retstring += str(index) + ": " + elem.name + " : " + str(elem.averageincidents) + "\n"
            index += 1
        return retstring
    

    def create_progression_chart(self, progression_plot):
        dates = list(progression_plot.keys()) 
        finishes = list(progression_plot.values())
        
        # Convert ISO_8601 strings to datetime objects and sort them
        dates = [datetime.fromisoformat(date[:-1]) for date in dates] # Remove the 'Z' at the end of the string
        dates, finishes = zip(*sorted(zip(dates, finishes)))  # Sort dates and finishes together
        
        fig, ax = plt.subplots()
        
        # Plot main data points with reduced opacity
        ax.plot(dates, finishes, marker='o', linestyle='-', color='b', alpha=0.3, label='ELO Scores')
        
        # Fit and plot linear trend line
        x = np.array([date.timestamp() for date in dates])
        y = np.array(finishes)
        coeffs = np.polyfit(x, y, 1)
        linear_trend = np.poly1d(coeffs)
        
        ax.plot(dates, linear_trend(x), linestyle='-', color='r', label='Linear Trend', linewidth=2)

        ax.set(xlabel='Date', ylabel='ELO', title='Racer Progression Over Time')
        ax.grid()
        
        # Format the date on the x-axis to show month and year
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
        
        # Set y-axis limits based on the actual ELO values
        y_min = min(finishes) - 50  # Adjust as needed
        y_max = max(finishes) + 50  # Adjust as needed
        ax.set_ylim(y_min, y_max)
        
        fig.autofmt_xdate()
        
        # Add legend
        ax.legend()
        
        # Save chart as an image
        plt.savefig('progression_chart.png')
        plt.close()

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
