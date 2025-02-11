import math
import content_data
import racer
import statsparser

gt3ids = ["ks_audi_r8_lms_2016","bmw_z4_gt3", "ks_ferrari_488_gt3", "ks_lamborghini_huracan_gt3",
         "ks_mclaren_650_gt3", "ks_mercedes_amg_gt3", "ks_nissan_gtr_gt3", "ks_porsche_911_gt3_r_2016"]

class Incident():
    def __init__(self, speed, racer, otherracer) -> None:
        self.speed = speed
        self.racer = racer
        self.otherracer = otherracer

class Lap():
    def __init__(self, time, car, racerguid, result, valid, cuts) -> None:
        self.time = time
        self.car = car
        self.racerguid = racerguid
        self.result = result # "parent" result
        self.valid = valid
        self.cuts = cuts

# this is the driver-specific result
class Entry():
    def __init__(self, racer, car, track, date):
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

class Result():
    def __init__(self):
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
        self.championshipid = ""

    def update_ratings(self):
        # results is a list of tuples (racer_name, position)
        n = len(self.entries)
        for i in range(n):
            racer_i = self.entries[i].racer
            position_i = self.entries[i].finishingposition
            for j in range(i + 1, n):
                racer_j = self.entries[j].racer
                position_j = self.entries[j].finishingposition
                if position_i < position_j:
                    self.entries[i].ratingchange = racer_i.update_rating(racer_j.rating, 1, len(self.entries), self)
                    self.entries[j].ratingchange = racer_j.update_rating(racer_i.rating, 0, len(self.entries), self)
                elif position_i > position_j:
                    self.entries[i].ratingchange = racer_i.update_rating(racer_j.rating, 0, len(self.entries), self)
                    self.entries[j].ratingchange = racer_j.update_rating(racer_i.rating, 1, len(self.entries), self)

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
        self.entries.sort(key=lambda x: x.finishingposition, reverse=False)
        self.update_ratings()
        self.update_charts()

    def update_charts(self):
        for entry in self.entries:
            entry.racer.update_chart(self, entry)
        

    def get_fastest_lap_of_race(self)->dict:
        fastest = None
        for lap in self.laps:
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
                if fastest == None:
                    fastest = lap
                else:
                    if lap.time < fastest.time:
                        fastest = lap
        return fastest
    
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
        highest_num_laps_done = 0
        driverlaps = {}
        for lap in self.laps:
            lapguid = lap.racerguid
            if lapguid in driverlaps:
                driverlaps[lapguid] += 1
            else:
                driverlaps[lapguid] = 1

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
            lap = Lap(lap["LapTime"], carid, racerguid, self, lap["Cuts"] == 0, lap["Cuts"])
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