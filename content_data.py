import json
import racer

gt3ids = ["ks_audi_r8_lms_2016","bmw_z4_gt3", "ks_ferrari_488_gt3", "ks_lamborghini_huracan_gt3",
         "ks_mclaren_650_gt3", "ks_mercedes_amg_gt3", "ks_nissan_gtr_gt3", "ks_porsche_911_gt3_r_2016"]


class Car:
    def __init__(self, car_id, name):
        self.id = car_id
        self.name = name
        self.rrrusages = 0
        self.trackusages = {} # trackid, int
        self.brand = ""
        self.carclass = ""
        self.country = ""
        self.description = ""
        self.tags = []
        self.torquecurve = []
        self.powercurve = []
        self.specs = {}
        self.year = ""
        self.author = ""
        self.url = ""
        self.version = ""

class Track:
    def __init__(self, track_id, highest_priority_id, highest_priority_name):
        self.id = track_id
        self.highest_priority_id = highest_priority_id
        self.highest_priority_name = highest_priority_name
        self.variants = []

class TrackVariant:
    def __init__(self, variant_id, name, priority, parent_track):
        self.id = variant_id
        self.name = name
        self.priority = priority
        self.parent_track = parent_track  # Reference to the parent track
        self.description = ""
        self.tags = []
        self.geotags = []
        self.country = ""
        self.city = ""
        self.length = ""
        self.width = ""
        self.pitboxes = ""
        self.run = ""
        self.author = ""
        self.version = ""
        self.url = ""
        self.year = 0
        self.laps = [] # list of lap objects

    def add_lap(self, lap):
        self.laps.append(lap)

    def get_fastest_lap_in_f4(self):
        fastest = None
        for lap in self.laps:
            if not lap.valid:
                continue
            if lap.car == "rss_formula_rss_4_2024":
                if fastest == None:
                    fastest = lap
                else:
                    if lap.time < fastest.time:
                        fastest = lap
        return fastest

    def get_fastest_lap_in_car(self, car:Car):
        fastest = None
        for lap in self.laps:
            if not lap.valid:
                continue
            if lap.car == car.id:
                if fastest == None:
                    fastest = lap
                else:
                    if lap.time < fastest.time:
                        fastest = lap
        return fastest
    
    def get_racer_fastest_lap_in_car(self, car:Car, racerguid:str):
        fastest = None
        for lap in self.laps:
            if not lap.valid:
                continue
            if lap.car == car.id and lap.racerguid == racerguid:
                if fastest == None:
                    fastest = lap
                else:
                    if lap.time < fastest.time:
                        fastest = lap
        return fastest
    
    def get_racer_fastest_lap_in_car_id(self, carid:str, racerguid:str):
        fastest = None
        for lap in self.laps:
            if not lap.valid:
                continue
            if lap.car == carid and lap.racerguid == racerguid:
                if fastest == None:
                    fastest = lap
                else:
                    if lap.time < fastest.time:
                        fastest = lap
        return fastest
    
    def get_fastest_lap_in_gt3(self, racerguid:str = None):
        fastest = None
        for lap in self.laps:
            if not lap.valid:
                continue
            if lap.car in gt3ids:
                if racerguid != None and lap.racerguid != racerguid:
                    continue
                if fastest == None:
                    fastest = lap
                else:
                    if lap.time < fastest.time:
                        fastest = lap
        return fastest
    
    def get_fastest_lap_in_mx5(self, racerguid:str = None):
        fastest = None
        for lap in self.laps:
            if not lap.valid:
                continue
            if lap.car == "ks_mazda_mx5_cup":
                if racerguid != None and lap.racerguid != racerguid:
                    continue
                if fastest == None:
                    fastest = lap
                else:
                    if lap.time < fastest.time:
                        fastest = lap
        return fastest


class Contentdata:
    def __init__(self):
        self.cars = []
        self.tracks = []
        #these bits of content are in the results but not scanned on my machine as I dont have them
        self.missingcars = []
        self.missingtracks = []

    def get_car(self, id:str)->Car:
        for car in self.cars:
            if car.id == id:
                return car
        return None
    
    def get_base_track(self, id:str)->Track:
        for track in self.tracks:
            if track.id.lower() == id.lower():
                return track
    
    def get_track(self, id:str)->TrackVariant:
        splitstr = id.split(";")
        baseid = splitstr[0]
        for track in self.tracks:
            if track.id == baseid:
                for variant in track.variants:
                    if variant.id == id:
                        return variant
        return None
    
    #for when the prototype data for this car dosnt exist, but its in the results file
    # so we fall back to jsut basic id
    def create_basic_car(self, id:str):
        basiccar = Car(id, id)
        self.cars.append(basiccar)
        return basiccar

    def create_basic_track(self, id:str, variantid:str=""):
        combinedid = ""
        if variantid == "":
            combinedid = id + ";" + id
        else:
            combinedid = id+";"+variantid
        basictrack = Track( id, 0, id)
        basicvariant = TrackVariant(combinedid, variantid, 0, basictrack)
        if variantid != "":
            basicvariant.name = variantid
        else:
            basicvariant.name = id
        basictrack.highest_priority_id = basicvariant.id
        basictrack.variants.append(basicvariant)
        self.tracks.append(basictrack)
        return basicvariant

    def load_cars(self):
        with open('merged_car_data.json', 'r') as car_file:
            car_json = json.load(car_file)
        for car_id, car_data in car_json.items():
            car = Car(car_id, car_data["name"])
            car.brand = car_data.get("brand", "")
            car.carclass = car_data.get("class", "")
            car.country = car_data.get("country", "")
            car.description = car_data.get("description", "")
            car.tags = car_data.get("tags", [])
            car.torquecurve = car_data.get("torquecurve", [])
            car.powercurve = car_data.get("powercurve", [])
            car.year = car_data.get("year", "")
            car.author = car_data.get("author", "")
            car.url = car_data.get("url", "")
            car.version = car_data.get("version", "")
            self.cars.append(car)

    def load_tracks(self):
        with open('merged_track_data.json', 'r') as track_file:
            track_json = json.load(track_file)
        for track_id, track_data in track_json.items():
            track = Track(track_id, track_data["highestpriorityid"], track_data["highestpriorityname"])
            for variant in track_data["variants"]:
                for variant_id, variant_data in variant.items():
                    track_variant = TrackVariant(
                        variant_id,
                        variant_data["name"],
                        variant_data["priority"],
                        track  # Reference to the parent track
                    )
                    track_variant.description = variant_data.get("description", "")
                    track_variant.tags = variant_data.get("tags", [])
                    track_variant.geotags = variant_data.get("geotags", [])
                    track_variant.country = variant_data.get("country", "")
                    track_variant.city = variant_data.get("city", "")
                    track_variant.length = variant_data.get("length", "")
                    track_variant.width = variant_data.get("width", "")
                    track_variant.pitboxes = variant_data.get("pitboxes", "")
                    track_variant.run = variant_data.get("run", "")
                    track_variant.author = variant_data.get("author", "")
                    track_variant.version = variant_data.get("version", "")
                    track_variant.url = variant_data.get("url", "")
                    track_variant.year = variant_data.get("year", 0)
                    track.variants.append(track_variant)
            self.tracks.append(track)
