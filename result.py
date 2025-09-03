import math
import content_data
import racer
import statsparser
from logger_config import logger
import uuid


gt3ids = ["ks_audi_r8_lms_2016","bmw_z4_gt3", "ks_ferrari_488_gt3", "ks_lamborghini_huracan_gt3",
         "ks_mclaren_650_gt3", "ks_mercedes_amg_gt3", "ks_nissan_gtr_gt3", "ks_porsche_911_gt3_r_2016", "amr_v8_vantage_gt3_sprint_acc"]

class Incident():
    def __init__(self, speed, racer, otherracer) -> None:
        self.id = str(uuid.uuid4()) 
        self.speed = speed
        self.racer = racer
        self.otherracer = otherracer
        self.logger = logger
    
    def to_dict(self):
        return {
            "id": self.id,
            "speed": self.speed,
            "racer": self.racer.guid if self.racer else None,
            "otherracer": self.otherracer.guid if self.otherracer else None
        }

class Lap():
    def __init__(self, time, car, racerguid, result, valid, cuts, timestamp) -> None:
        self.id = str(uuid.uuid4()) 
        self.time = time
        self.car = car
        self.racerguid = racerguid
        self.result = result # "parent" result
        self.valid = valid
        self.cuts = cuts
        self.logger = logger
        self.position = 0
        self.timestamp = timestamp

    def to_dict(self):
        return {
            "id": self.id,
            "time": self.time,
            "car": self.car,
            "racerguid": self.racerguid,
            "result": self.result.id,
            "valid": self.valid,
            "cuts": self.cuts,
            "position": self.position,
            "timestamp": self.timestamp
        }

# this is the driver-specific result
class Entry():
    def __init__(self, racer, car, track, date):
        self.id = str(uuid.uuid4()) 
        self.racer = racer
        self.car = car
        self.track = track
        self.date = date
        self.laps = []
        self.incidents = []
        self.result = None
        self.cuts = 0
        self.finishingposition = 0
        self.ratingchange = 0
        self.logger = logger
        self.startingposition = 0

    def to_dict(self):
        return {
            "id": self.id,
            "racer": self.racer.guid,
            "car": self.car.id,
            "track": self.track.id,
            "date": self.date,
            "laps": [lap.id for lap in self.laps],
            "incidents": [incident.id for incident in self.incidents],
            "result": self.result.id if self.result else None,
            "cuts": self.cuts,
            "finishingposition": self.finishingposition,
            "ratingchange": self.ratingchange,
            "startingposition": self.startingposition
        }

class Result():
    def __init__(self):
        self.id = str(uuid.uuid4()) 
        #racers is a dictionary of dictionarys
        #outer key is racer guid
        #inner key is finishing position, fastest lap ( for that racer ), starting position, incidents
        self.track = None
        self.laps = []
        self.entries = [] #list of Entry objects, listed in order of finishing result
        self.endurance = False
        self.incidents = []
        self.mx5orgt3 = "neither"
        self.filename = ""
        self.date = ""
        self.region = "EU"
        self.championshipid = ""
        self.numlaps = 0
        self.driverlaps = {}
        self.shortorlong = "short"
        self.logger = logger
        self.issecond = False
        self.server = ""
        self.url = ""
        self.directory = ""

    def to_dict(self):
        return {
            "id": self.id,
            "track": self.track.id if self.track else None,
            "laps": [lap.to_dict() for lap in self.laps],
            "entries": [entry.to_dict() for entry in self.entries],
            "endurance": self.endurance,
            "incidents": [incident.to_dict() for incident in self.incidents],
            "mx5orgt3": self.mx5orgt3,
            "filename": self.filename,
            "date": self.date,
            "region": self.region,
            "championshipid": self.championshipid,
            "numlaps": self.numlaps,
            "driverlaps": self.driverlaps,
            "shortorlong": self.shortorlong,
            "server": self.server,
            "url": self.url,
            "directory": self.directory,
            "issecond": self.issecond
        }

    def set_region(self, data):
        if data["Region"] == "EU":
            self.region = "EU"
        elif data["Region"] == "NA":
            self.region = "NA"
        

    def update_ratings(self):
        # results is a list of tuples (racer_name, position)
        n = len(self.entries)
        for i in range(n):
            racer_i = self.entries[i].racer
            
            position_i = self.entries[i].finishingposition
            for j in range(i + 1, n):
                racer_j = self.entries[j].racer
                position_j = self.entries[j].finishingposition
                ratingtousei = racer_i.ratingbeforeeachresult
                if racer_i.ratingbeforeeachresult == -1:
                    ratingtousei = racer_i.rating
                ratingtousej = racer_j.ratingbeforeeachresult
                if racer_j.ratingbeforeeachresult == -1:
                    ratingtousej = racer_j.rating
                if position_i < position_j:
                    self.entries[i].ratingchange = racer_i.update_rating(ratingtousej, 1, len(self.entries), self, self.entries[j].racer)
                    self.entries[j].ratingchange = racer_j.update_rating(ratingtousei, 0, len(self.entries), self, self.entries[i].racer)
                elif position_i > position_j:
                    self.entries[i].ratingchange = racer_i.update_rating(ratingtousej, 0, len(self.entries), self, self.entries[j].racer)
                    self.entries[j].ratingchange = racer_j.update_rating(ratingtousei, 1, len(self.entries), self, self.entries[i].racer)
        n = len(self.entries)
        for i in range(n):
            racer_i = self.entries[i].racer
            racer_i.ratingbeforeeachresult = racer_i.rating
        self.update_qualifying_ratings()

    def update_qualifying_ratings(self):
        if self.issecond:
            return
        n = len(self.entries)
        for i in range(n):
            racer_i = self.entries[i].racer
            
            position_i = self.entries[i].startingposition
            if position_i == 0:
                continue
            for j in range(i + 1, n):
                racer_j = self.entries[j].racer
                position_j = self.entries[j].startingposition
                ratingtousei = racer_i.qualyratingbeforeeachresult
                if racer_i.qualyratingbeforeeachresult == -1:
                    ratingtousei = racer_i.qualifyingrating
                ratingtousej = racer_j.qualyratingbeforeeachresult
                if racer_j.qualyratingbeforeeachresult == -1:
                    ratingtousej = racer_j.qualifyingrating
                if position_j == 0:
                    continue
                if position_i < position_j:
                    racer_i.update_qualifying_rating(ratingtousej, 1, len(self.entries), self, self.entries[j].racer)
                    racer_j.update_qualifying_rating(ratingtousei, 0, len(self.entries), self, self.entries[i].racer)
                elif position_i > position_j:
                    racer_i.update_qualifying_rating(ratingtousej, 0, len(self.entries), self, self.entries[j].racer)
                    racer_j.update_qualifying_rating(ratingtousei, 1, len(self.entries), self, self.entries[i].racer)

    def get_position_of_racer(self, racer):
        index = 1
        for elem in self.entries:
            if elem.racer == racer:
                return elem.finishingposition
            index += 1
        return -1

    def finalize_entries(self):
        for entry in self.entries:
            cuts = 0
            racerlaps = []
            racerincidents = []
            for lap in self.laps:
                racerguid = entry.racer.guid
                if racerguid == lap.racerguid:
                    racerlaps.append(lap)
                cuts += lap.cuts
            for incident in self.incidents:
                if entry.racer == incident.racer:
                    racerincidents.append(incident)
            entry.laps = racerlaps
            entry.incidents = racerincidents
            entry.cuts = cuts
            entry.result = self
            entry.racer.add_result(entry)
            self.calculate_positions_at_laps(entry)
        self.numlaps = self.get_numlaps_total()
        self.entries.sort(key=lambda x: x.finishingposition, reverse=False)
        self.update_ratings()
        self.update_charts()

    def calculate_positions_at_laps(self, entry):
        all_laps = []
        lap_objects_by_guid = {}

        # 1. Build a dict of laps per driver and a flat list of all laps
        for e in self.entries:
            lap_objects_by_guid[e.racer.guid] = sorted(e.laps, key=lambda x: x.timestamp)
            for lap in e.laps:
                all_laps.append(lap)

        # 2. Sort all laps chronologically (this is our "event stream")
        all_laps.sort(key=lambda x: x.timestamp)

        # 3. Initialize cumulative time per driver and lap index
        cumulative_time = {e.racer.guid: 0 for e in self.entries}
        completed_laps = {e.racer.guid: 0 for e in self.entries}
        latest_lap_obj = {e.racer.guid: None for e in self.entries}

        # 4. Race duration reference: first timestamp and last timestamp (for % calculation)
        if not all_laps:
            return

        first_timestamp = all_laps[0].timestamp
        last_timestamp = max(lap.timestamp for lap in all_laps)
        total_race_ms = last_timestamp - first_timestamp if last_timestamp > first_timestamp else 1

        # 5. For the driver we're analyzing, store their position per lap
        previous_position = entry.startingposition
        entry.percentageracedone_overtakes = []

        for lap in all_laps:
            guid = lap.racerguid
            cumulative_time[guid] += lap.time
            completed_laps[guid] += 1
            latest_lap_obj[guid] = lap

            # Build current running order (only include drivers who have completed a lap)
            current_racers = [
                (g, cumulative_time[g])
                for g in cumulative_time
                if completed_laps[g] > 0
            ]

            # Sort by race time ascending = lower time is ahead
            sorted_guids = [g for g, _ in sorted(current_racers, key=lambda x: x[1])]

            # Assign positions based on this ordering
            for pos, g in enumerate(sorted_guids, 1):
                obj = latest_lap_obj[g]
                if obj:
                    obj.position = pos  # Update the lap object itself

            # For the target entry, if this is their lap, check if position improved
            if lap.racerguid == entry.racer.guid:
                current_pos = lap.position
                if current_pos < previous_position:
                    # Overtake detected
                    percentage = int(((lap.timestamp - first_timestamp) / total_race_ms) * 100)
                    entry.racer.percentageracedone_overtakes.append(percentage)
                previous_position = current_pos


    def update_charts(self):
        for entry in self.entries:
            entry.racer.update_chart(self, entry)
        
    def get_race_duration(self, data):
        longesttime = 0
        for elem in data["Result"]:
            if "TotalTime" in elem:
                if elem["TotalTime"] > longesttime:
                    longesttime = elem["TotalTime"]
        longesttime = (longesttime / 1000) / 60
        if longesttime > 10 and longesttime < 30:
            self.shortorlong = "short"
        elif longesttime > 30:
            self.shortorlong = "long"

    def get_fastest_lap_of_race(self)->dict:
        fastest = None
        for lap in self.laps:
            if lap.valid:
                if fastest == None:
                    fastest = lap
                else:
                    if lap.time < fastest.time:
                        fastest = lap
        return fastest

    def get_fastest_lap_of_racer(self, racer:racer.Racerprofile):
        fastest = None
        for lap in self.laps:
            if lap.racerguid == racer.guid:
                if lap.valid:
                    if fastest == None:
                        fastest = lap
                    else:
                        if lap.time < fastest.time:
                            fastest = lap
        return fastest
    
    def get_numlaps_of_racer(self, racer:racer.Racerprofile):
        laps = 0
        if racer.guid in self.driverlaps:
            laps = self.driverlaps[racer.guid]
        return laps
    
    def get_numlaps_total(self):
        for lap in self.laps:
            lapguid = lap.racerguid
            if lapguid in self.driverlaps:
                self.driverlaps[lapguid] += 1
            else:
                self.driverlaps[lapguid] = 1

        if not self.driverlaps:
            self.logger.warning("No driver laps recorded in get_numlaps_total")
            return 0  # fallback value when no laps are present

        return max(self.driverlaps.values())

    
    def calculate_collisions(self, data):
        if not data["Events"]:
            return
        for event in data["Events"]:
            if event["AfterSessionEnd"]:
                continue
            raceralpha = event["Driver"]["Guid"]
            racerbeta = None
            if event["Type"] == "COLLISION_WITH_CAR":
                racerbeta = event["OtherDriver"]["Guid"]
            racer = None
            otherracer = None
            for entry in self.entries:
                if entry.racer.guid == raceralpha:
                    racer = entry.racer
                elif entry.racer.guid == racerbeta:
                    otherracer = entry.racer
            collision = Incident(event["ImpactSpeed"], racer, otherracer)
            self.incidents.append(collision)

    
    def calculate_positions(self, data):
        for result in data["Result"]:
            racerguid = result["DriverGuid"]
            for entry in self.entries:
                if entry.racer.guid == racerguid:
                    entry.startingposition = result["GridPosition"] if "GridPosition" in result else 0
                    break
        highest_num_laps_done = 0
        driverlaps = {}
        for lap in self.laps:
            lapguid = lap.racerguid
            if lapguid in driverlaps:
                driverlaps[lapguid] += 1
            else:
                driverlaps[lapguid] = 1
        if len(driverlaps) == 0:
            self.logger.error("No driver laps found in result data, cannot calculate positions")
            return
        highest_num_laps_done = max(driverlaps.values())

        list_of_drivers_who_did_all_laps = []
        list_of_drivers_who_did_not_do_all_laps = []
        for result_entry in data["Result"]:
            result_field_guid = result_entry["DriverGuid"]
            if "TotalTime" in result_entry:
                if not result_field_guid in driverlaps:
                    driverlaps[result_field_guid] = 0
                totaltime = result_entry["TotalTime"]
                totaltimedict = {}
                totaltimedict["guid"] = result_field_guid
                totaltimedict["totaltime"] = totaltime
                totaltimedict["lapsdone"] = driverlaps[result_field_guid]

                if driverlaps[result_field_guid] == highest_num_laps_done:
                    list_of_drivers_who_did_all_laps.append(totaltimedict)
                else:
                    list_of_drivers_who_did_not_do_all_laps.append(totaltimedict)
            else:
                print("no totaltime found in result entry")
        sorted(list_of_drivers_who_did_all_laps, key=lambda e: (-e['lapsdone'], e['totaltime']))
        sorted(list_of_drivers_who_did_not_do_all_laps, key=lambda e: (-e['lapsdone'], e['totaltime']))
        merged_results_list = list_of_drivers_who_did_all_laps + list_of_drivers_who_did_not_do_all_laps
        index = 1
        for elem in merged_results_list:
            for entry in self.entries:
                if elem["guid"] == entry.racer.guid:
                    entry.finishingposition = index
            index += 1

    def get_driver_laps(self, guid:str):
        retlist = []
        for lap in self.laps:
            if lap.racer == guid:
                retlist.append(lap)
        return retlist

    def calculate_laps(self, data):
        # this assumes self.entries have already been filled
        for lap in data["Laps"]:
            racerguid = lap["DriverGuid"]
            laptime = lap["LapTime"]
            carid = lap["CarModel"]
            lap = Lap(lap["LapTime"], carid, racerguid, self, lap["Cuts"] == 0, lap["Cuts"], lap["Timestamp"])
            self.laps.append(lap)
            self.track.laps.append(lap)
    
    def calculate_is_mx5_or_gt3(self, data):
        self.championshipid = data["ChampionshipID"]
        gt3race = False
        mx5race = False
        for elem in data["Cars"]:
            if elem["Model"] != "":
                firstcar = elem["Model"]
                if firstcar in gt3ids:
                    gt3race = True
                    break
                if firstcar == "ks_mazda_mx5_cup":
                    mx5race = True
                    break
        if gt3race:
            self.mx5orgt3 = "gt3"
            return "gt3"
        if mx5race:
            self.mx5orgt3 = "mx5"
            return "mx5"

    def is_endurance_race(self, data):
        for driverresult in data["Result"]:
            driverguid = driverresult["DriverGuid"]
            if driverguid.find(';') != -1:
                endurance = True
