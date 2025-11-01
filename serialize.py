import json, os
from statsparser import parser
from content_data import Track, TrackVariant, Car, Contentdata
from racer import Racerprofile
from result import Result, Entry, Lap, Incident
import shutil
import time
from datetime import datetime
from logger_config import logger
from championship import Championship, Event
from statsparser import RetentionTracker


RACER_SCALAR_FIELDS = [
    "result_add_ticker",
    "eucount", "nacount",
    "rating",
    "safety_rating",
    "safety_rate_ema",
    "sr_target_rate",
    "sr_memory_km",
    "incidentsperkm",
    "wins", "gt3wins", "mx5wins",
    "podiums", "gt3podiums", "mx5podiums",
    "totallaps", "mx5laps", "gt3laps",
    "incidents", "incidentsgt3", "incidentsmx5",
    "averageincidents", "averageincidentsgt3", "averageincidentsmx5",
    "numraces", "numracesgt3", "numracesmx5",
    "laptimeconsistency", "laptimeconsistencymx5", "laptimeconsistencygt3",
    "raceconsistency",
    "distancedriven",
    "pace_percentage_mx5", "pace_percentage_gt3", "pace_percentage_overall",
]

def serialize_all_data(parser_obj, clean: bool = True):
    # start fresh if requested
    if clean and os.path.exists("output"):
        shutil.rmtree("output")
    os.makedirs("output", exist_ok=True)
    os.makedirs("output/racers", exist_ok=True)
    os.makedirs("output/cars", exist_ok=True)
    os.makedirs("output/tracks", exist_ok=True)
    os.makedirs("output/results", exist_ok=True)
    os.makedirs("output/championships", exist_ok=True)
    os.makedirs("output/retention", exist_ok=True)
    with open("output/retention/retention.json", "w", encoding="utf-8") as f:
        json.dump(parser_obj.retention.to_jsonable(), f, indent=4)
    # 1. Serialize racers
    racers_data = {guid: racer.to_dict()
               for guid, racer in parser_obj.racers.items()}
    with open("output/racers/racers.json", "w", encoding="utf-8") as f:
        json.dump(racers_data, f, indent=4)
    
    # 2. Serialize cars
    cars_data = [c.to_dict() for c in parser_obj.contentdata.cars]
    with open("output/cars/cars.json", "w") as f:
        json.dump(cars_data, f, indent=4)
    
    # 3. Serialize tracks (with variants nested)
    tracks_data = [t.to_dict() for t in parser_obj.contentdata.tracks]
    with open("output/tracks/tracks.json", "w") as f:
        json.dump(tracks_data, f, indent=4)

    # championships
    championships_data = [c.to_dict() for c in parser_obj.championships.values()]
    with open("output/championships/championships.json", "w") as f:
        json.dump(championships_data, f, indent=4)

    completed_championships_data = [c.to_dict() for c in parser_obj.completedchampionships]
    with open("output/championships/completedchampionships.json", "w") as f:
        json.dump(completed_championships_data, f, indent=4)
    
    results_root = os.path.join("output", "results")


    for result in parser_obj.raceresults:
        # ------------------------------
        # Decide which sub‑folder to use
        # ------------------------------
        # Priority: 1) explicit directory stored on the Result
        #           2) fall back to region  (EU / NA) so we always have *something*
        subfolder = result.directory or result.region
        if not subfolder:
            subfolder = "misc"                            # ultra‑safe fallback

        dest_dir = os.path.join(results_root, subfolder)
        os.makedirs(dest_dir, exist_ok=True)

        # ------------------------------
        # File name – keep whatever was on disk, ensure .json
        # ------------------------------
        filename = result.filename
        if not filename.lower().endswith(".json"):
            filename += ".json"

        dest_path = os.path.join(dest_dir, filename)

        # ------------------------------
        # Dump the Result ( incl. laps / entries / incidents )
        # ------------------------------
        with open(dest_path, "w", encoding="utf-8") as f:
            json.dump(result.to_dict(), f, indent=4)

def deserialize_all_data():
    def log(msg):
        # timestamped print helper
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] {msg}")
    parser_obj = parser()                      # 1) make a fresh parser
    parser_obj.contentdata = Contentdata()     # <‑‑‑‑ add this line

    # ─────────────────────── racers ───────────────────────
    t0 = time.perf_counter()
    log("⏳  Deserialising RACERS …")
    with open("output/racers/racers.json", "r", encoding="utf-8") as f:
        racers_json = json.load(f)             # dict keyed by guid

    racers_by_guid = {}
    for guid, rd in racers_json.items():
        racer = Racerprofile(rd["name"], guid)
        _populate_racer_from_json(racer, rd)      # <── copies all stats
        racers_by_guid[guid] = racer
        parser_obj.racers[guid] = racer   # parser keeps dict → Racerprofile

    t1 = time.perf_counter()
    log(f"✅  Deserialised RACERS in {t1 - t0:0.3f}s")
   
    
    log("⏳  Deserialising CARS …")
    # ─────────────────────── cars ─────────────────────────
    cars_by_id = {}
    with open("output/cars/cars.json", "r", encoding="utf-8") as f:
        cars_list = json.load(f)

    for cd in cars_list:                       # (now outside the racer loop!)
        car = Car(cd["id"], cd["name"])
        car.brand = cd.get("brand", "")
        car.year  = cd.get("year", "")
        car.trackusages = cd.get("trackusages", {})
        car.rrrusages = cd.get("rrrusages", 0)
        car.carclass = cd.get("carclass","")
        car.country = cd.get("country","")
        car.description = cd.get("description","")
        car.tags = cd.get("tags",[])
        car.torquecurve = cd.get("torquecurve",[])
        car.powercurve = cd.get("powercurve",[])
        car.specs = cd.get("specs",{})
        car.author = cd.get("author","")
        car.url = cd.get("url","")
        car.version = cd.get("version","")
        car.download_url = cd.get("download_url","")
        car.imagepath = cd.get("imagepath","")
        cars_by_id[car.id] = car
        parser_obj.contentdata.cars.append(car)

    t2 = time.perf_counter()
    log(f"✅  Deserialized cars calc done in {t2 - t1:0.3f}s")



    log("⏳  Deserialising TRACKS …")
    # ─────────────────────── tracks & variants ────────────
    track_variants_by_id = {}
    with open("output/tracks/tracks.json", "r", encoding="utf-8") as f:
        tracks_list = json.load(f)

    for td in tracks_list:
        track = Track(td["id"],
                      td["highestpriorityid"],
                      td["highestpriorityname"])
        track.average_rating = td["average_rating"]
        track.timesused      = td["timesused"]
        parser_obj.contentdata.tracks.append(track)

        for vd in td["variants"]:
            variant = TrackVariant(vd["id"], vd["name"], vd["priority"], track)
            variant.country = vd.get("country", "")
            variant.length  = vd.get("length", "")
            variant.parent_track = track  # Reference to the parent track
            variant.description = vd.get("description", "")
            variant.tags = vd.get("tags", [])
            variant.geotags = vd.get("geotags", [])
            variant.country = vd.get("country", "")
            variant.city = vd.get("city", "")
            variant.length = vd.get("length", "")
            variant.width = vd.get("width", "")
            variant.pitboxes = vd.get("pitboxes", "")
            variant.run = vd.get("run", "")
            variant.author = vd.get("author", "")
            variant.version = vd.get("version", "")
            variant.url = vd.get("url", "")
            variant.year = vd.get("year", "")
            variant.laps = []
            track.variants.append(variant)
            track_variants_by_id[variant.id] = variant

    t3 = time.perf_counter()
    log(f"✅  Deserializing tracks done in {t3 - t2:0.3f}s")


    log("⏳  Deserialising RESULTS …")
    # ─────────────────────── results ──────────────────────
    results_by_id = {}
    for root, _, files in os.walk("output/results"):
        for fname in files:
            if not fname.lower().endswith(".json"):
                continue
            with open(os.path.join(root, fname), "r", encoding="utf-8") as f:
                result_data = json.load(f)

            result = Result()
            result.id        = result_data["id"]
            result.region    = result_data["region"]
            result.date      = result_data["date"]
            result.filename  = result_data["filename"]
            result.numlaps   = result_data["numlaps"]
            result.directory = result_data["directory"]
            result.track     = track_variants_by_id.get(result_data["track"])
            result.endurance = result_data["endurance"]
            result.mx5orgt3 = result_data["mx5orgt3"]
            result.championshipid = result_data["championshipid"]
            result.driverlaps = result_data["driverlaps"]
            result.shortorlong = result_data["shortorlong"]
            result.server = result_data["server"]
            result.url = result_data["url"]
            result.issecond = result_data.get("issecond", False)  # New field

            # ----- laps -----
            laps_by_id = {}

            for lap_dict in result_data["laps"]:
                timestamp = lap_dict.get("timestamp")
                lap = Lap(lap_dict["time"],
                          lap_dict["car"],
                          lap_dict["racerguid"],
                          result,
                          lap_dict["valid"],
                          lap_dict["cuts"],
                          timestamp or 0)
                lap.id = lap_dict["id"]
                lap.position = lap_dict.get("position", 0)  # New field
                result.laps.append(lap)
                laps_by_id[lap.id] = lap
                if result.track:
                    result.track.laps.append(lap)

            # ----- entries -----
            incidents_by_id = {}
            for entry_dict in result_data["entries"]:
                racer = racers_by_guid[entry_dict["racer"]]
                car   = cars_by_id[entry_dict["car"]]
                track_var = track_variants_by_id[entry_dict["track"]]

                entry = Entry(racer, car, track_var, entry_dict["date"])
                entry.id = entry_dict["id"]
                entry.finishingposition = entry_dict["finishingposition"]
                entry.cuts          = entry_dict["cuts"]
                entry.ratingchange  = entry_dict["ratingchange"]
                entry.result        = result
                entry.startingposition = entry_dict.get("startingposition", 0)  # New field

                result.entries.append(entry)
                racer.entries.append(entry)

            # ----- incidents -----
            for inc_dict in result_data["incidents"]:
                racerA = racers_by_guid.get(inc_dict["racer"])
                racerB = racers_by_guid.get(inc_dict["otherracer"])
                incident = Incident(inc_dict["speed"], racerA, racerB)
                incident.id = inc_dict["id"]
                result.incidents.append(incident)
                incidents_by_id[incident.id] = incident

            # ----- back‑link laps & incidents to each entry -----
            for entry, entry_dict in zip(result.entries, result_data["entries"]):
                entry.laps      = [laps_by_id[lid]       for lid in entry_dict["laps"]]
                entry.incidents = [incidents_by_id[iid]  for iid in entry_dict["incidents"]]

            parser_obj.raceresults.append(result)
            results_by_id[result.id] = result

    t4 = time.perf_counter()
    log(f"✅  Deserialized results loaded in {t4 - t3:0.3f}s")
    log("⏳  Deserialising RETENTION …")
    parser_obj.retention = RetentionTracker()

    retention_path = "output/retention/retention.json"
    if os.path.isfile(retention_path):
        with open(retention_path, "r", encoding="utf-8") as f:
            retention_json = json.load(f)
        parser_obj.retention = RetentionTracker.from_jsonable(retention_json)
        log("✅  Retention loaded from snapshot")
    else:
        # Fallback: rebuild from results (slower, but robust)
        rebuilt = 0
        for res in parser_obj.raceresults:
            # res.entries have (racer, car, track, date)
            for e in res.entries:
                guid = e.racer.guid  # or however you expose guid on Racerprofile
                parser_obj.retention.register_race(guid, e.date)
                rebuilt += 1
        log(f"✅  Retention rebuilt from {rebuilt} entry dates")

    log("⏳  Deserialising CHAMPIONSHIPS …")
    champs_file = "output/championships/championships.json"
    if os.path.isfile(champs_file):
        with open(champs_file, "r", encoding="utf-8") as f:
            championships_json = json.load(f)      # list[dict]

        for chd in championships_json:
            # 1. shell ----------------------------------------------------
            racers_objs = [racers_by_guid[g] for g in chd.get("racers", [])
                           if g in racers_by_guid]

            champ = Championship(
                name      = chd["name"],
                racers    = racers_objs,
                schedule  = [],                 # we fill it below
                open      = chd.get("open", False),
                type      = chd.get("type", ""),
            )
            champ.id        = chd["id"]
            champ.completed = chd.get("completed", False)
            champ.car_download_links = chd.get("car_download_links", {}) # Dictionary to store car download links
            champ.standingsmessage =  chd.get("standingsmessage", None)
            champ.infomessage = chd.get("infomessage", None)
            for car in chd.get("available_cars", []):
                car_obj = cars_by_id.get(car)
                if car_obj:
                    champ.available_cars.append(car_obj)
            champ.baseurl = chd.get("baseurl", None)

            # standings  (name  → position)
            for racer, pos in chd.get("standings", {}).items():
                champ.standings[racer] = pos

            # 2. events ---------------------------------------------------
            for evd in chd.get("schedule", []):
                tvar = track_variants_by_id.get(evd["track"])
                if not tvar:                     # should not happen, skip
                    logger.warning(f"[CHAMP-LOAD] Missing track variant '{evd['track']}' "
                   f"for event '{evd.get('name','')}'. Creating placeholder.")
                    # Reconstruct from "base;layout"
                    base_id, _, layout_id = evd["track"].partition(";")
                    if not layout_id:
                        layout_id = base_id

                    # Try contentdata, then create a stub variant
                    tvar = parser_obj.contentdata.get_track(evd["track"])
                    if not tvar:
                        tvar = parser_obj.contentdata.create_basic_track(base_id, layout_id)

                    # register in the local index so subsequent events can find it
                    track_variants_by_id[tvar.id] = tvar

                ev = Event(
                    name               = evd["name"],
                    date               = evd["date"],
                    track              = tvar,
                    doublerace         = evd["doublerace"],
                    practicelength     = evd["practicelength"],
                    qualifyinglength   = evd["qualifyinglength"],
                    raceonelength      = evd["raceonelength"],
                    racetwolength      = evd["racetwolength"],
                    location           = evd.get("location", ""),
                    sessionstarttime   = evd.get("sessionstarttime", ""),
                    track_download_link= evd.get("track_download_link"),

                )
                ev.id        = evd["id"]
                ev.fuelrate  = evd.get("fuelrate", 100)
                ev.damage    = evd.get("damage",   100)
                ev.tirewear  = evd.get("tirewear", 100)
                ev.resultmessage = evd.get("resultmessage", None)
                ev.schedulemessage = evd.get("schedulemessage", None)
                ev.racelaps = evd.get("racelaps", 0)

                # back‑link to Result object (if present)
                res_id = evd.get("result")
                if res_id and res_id in results_by_id:
                    ev.result = results_by_id[res_id]

                champ.schedule.append(ev)
            print("loaded championship ", champ.name)
            # 3. dump into contentdata ------------------------------------
            parser_obj.championships[champ.type] = champ

    completed_champs_file = "output/championships/completedchampionships.json"
    if os.path.isfile(completed_champs_file):
        with open(completed_champs_file, "r", encoding="utf-8") as f:
            championships_json = json.load(f)      # list[dict]

        for chd in championships_json:
            # 1. shell ----------------------------------------------------
            racers_objs = [racers_by_guid[g] for g in chd.get("racers", [])
                           if g in racers_by_guid]

            champ = Championship(
                name      = chd["name"],
                racers    = racers_objs,
                schedule  = [],                 # we fill it below
                open      = chd.get("open", False),
                type      = chd.get("type", ""),
            )
            champ.id        = chd["id"]
            champ.completed = chd.get("completed", False)
            champ.car_download_links = chd.get("car_download_links", {}) # Dictionary to store car download links
            champ.standingsmessage =  chd.get("standingsmessage", None)
            champ.infomessage = chd.get("infomessage", None)
            for car in chd.get("available_cars", []):
                car_obj = cars_by_id.get(car)
                if car_obj:
                    champ.available_cars.append(car_obj)
            champ.baseurl = chd.get("baseurl", None)

            # standings  (name  → position)
            for racer, pos in chd.get("standings", {}).items():
                champ.standings[racer] = pos

            # 2. events ---------------------------------------------------
            for evd in chd.get("schedule", []):
                tvar = track_variants_by_id.get(evd["track"])
                if not tvar:                     # should not happen, skip
                    continue

                ev = Event(
                    name               = evd["name"],
                    date               = evd["date"],
                    track              = tvar,
                    doublerace         = evd["doublerace"],
                    practicelength     = evd["practicelength"],
                    qualifyinglength   = evd["qualifyinglength"],
                    raceonelength      = evd["raceonelength"],
                    racetwolength      = evd["racetwolength"],
                    location           = evd.get("location", ""),
                    sessionstarttime   = evd.get("sessionstarttime", ""),
                    track_download_link= evd.get("track_download_link"),

                )
                ev.id        = evd["id"]
                ev.fuelrate  = evd.get("fuelrate", 100)
                ev.damage    = evd.get("damage",   100)
                ev.tirewear  = evd.get("tirewear", 100)
                ev.resultmessage = evd.get("resultmessage", None)
                ev.schedulemessage = evd.get("schedulemessage", None)
                ev.racelaps = evd.get("racelaps", 0)

                # back‑link to Result object (if present)
                res_id = evd.get("result")
                if res_id and res_id in results_by_id:
                    ev.result = results_by_id[res_id]

                champ.schedule.append(ev)
            print("loaded championship ", champ.name)
            # 3. dump into contentdata ------------------------------------
            parser_obj.completedchampionships.append(champ)

    log("✅  Deserialised CHAMPIONSHIPS")

    log("⏳  Fixing cross-references racers and RESULTS …")

    # ────────────────── fix cross‑references in racers ──────────────────
    for racer in parser_obj.racers.values():           # iterate objects, not keys
        if isinstance(racer.mosthitotherdriver, str):
            racer.mosthitotherdriver = racers_by_guid.get(racer.mosthitotherdriver)

        # collisionracers  (may be empty)
        if racer.collisionracers:
            racer.collisionracers = {racers_by_guid[g]: n
                                     for g, n in racer.collisionracers.items()
                                     if g in racers_by_guid}

        # historyofratingchange
        if racer.historyofratingchange:
            racer.historyofratingchange = {results_by_id[rid]: delta
                                           for rid, delta in racer.historyofratingchange.items()
                                           if rid in results_by_id}
        racer.calculate_averages()

    t5 = time.perf_counter()
    log(f"✅  fixed cross-references loaded in {t5 - t4:0.3f}s")

    return parser_obj


def _populate_racer_from_json(racer: Racerprofile, rd: dict):
    """Copy every simple field straight from the JSON dict onto the racer."""
    for field in RACER_SCALAR_FIELDS:
        if field in rd:
            setattr(racer, field, rd[field])

    # big dict / plot fields — copy wholesale
    racer.progression_plot    = rd.get("progression_plot", {})
    racer.gt3progression_plot = rd.get("gt3progression_plot", {})
    racer.mx5progression_plot = rd.get("mx5progression_plot", {})
    racer.positionplot        = rd.get("positionplot", {})
    racer.incidentplot        = rd.get("incidentplot", {})
    racer.safetyratingplot    = rd.get("safetyratingplot", {})
    racer.positionaverage     = rd.get("positionaverage", {})
    racer.paceplot            = rd.get("paceplot", {})
    racer.paceplotaverage     = rd.get("paceplotaverage", {})
    racer.licenseclass       = rd.get("licenseclass", "Rookie")

    # fields that hold GUIDs / IDs -> leave strings for now, we resolve later
    racer.mosthitotherdriver     = rd.get("mosthitotherdriver")
    racer.mosthitotherdrivergt3  = rd.get("mosthitotherdrivergt3")
    racer.mosthitotherdrivermx5  = rd.get("mosthitotherdrivermx5")
    racer.mostsuccesfultrack     = rd.get("mostsuccesfultrack")
    racer.mostsuccesfultrackgt3  = rd.get("mostsuccesfultrackgt3")
    racer.mostsuccesfultrackmx5  = rd.get("mostsuccesfultrackmx5")
    racer.leastsuccesfultrack    = rd.get("leastsuccesfultrack")
    racer.leastsuccesfultrackgt3 = rd.get("leastsuccesfultrackgt3")
    racer.leastsuccesfultrackmx5 = rd.get("leastsuccesfultrackmx5")

    racer.historyofratingchange = rd.get("historyofratingchange", {})
    racer.collisionracers       = rd.get("collisionracers", {})