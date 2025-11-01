import os
import re
from html import unescape
import json

def clean_html_tags(text):
    text = re.sub(r'<br\s*/?>', ' ', text)
    text = re.sub(r'</?[^>]+>', '', text)
    text = re.sub(r'[\x00-\x1F\x7F]', '', text)
    return text

def replace_html_entities(text):
    return unescape(text)

def escape_problematic_characters(text):
    text = re.sub(r'&quot;', '"', text)
    text = re.sub(r'\u2018|\u2019', "'", text)
    text = re.sub(r'\u201c|\u201d', '"', text)
    text = re.sub(r'\\', '', text)
    return text

def sanitize_text(text):
    text = clean_html_tags(text)
    text = replace_html_entities(text)
    text = escape_problematic_characters(text)
    return text

def extract_key_value_pairs(text):
    pattern = r'"([^"]+)"\s*:\s*"([^"]+)"'
    return dict(re.findall(pattern, text))

def parse_json_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            raw_data = f.read()
    except (UnicodeDecodeError):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                raw_data = f.read()
        except (UnicodeDecodeError):
            try:
                with open(file_path, 'r', encoding='latin-1') as f:
                    raw_data = f.read()
            except (UnicodeDecodeError) as e:
                print(f"Error reading {file_path}: {e}")
                return None

    sanitized_data = sanitize_text(raw_data)
    data = extract_key_value_pairs(sanitized_data)
    return data

def extract_track_data(base_path):
    track_data = {}

    for root, dirs, files in os.walk(base_path):
        for file in files:
            if file == "ui_track.json":
                file_path = os.path.join(root, file)
                data = parse_json_file(file_path)

                if data is None:
                    continue

                track_name = data.get("name", "Unknown")
                priority = float(data.get("priority", 0))
                description = data.get("description", "")
                tags = data.get("tags", [])
                geotags = data.get("geotags", [])
                country = data.get("country", "")
                city = data.get("city", "")
                length = data.get("length", "")
                width = data.get("width", "")
                pitboxes = data.get("pitboxes", "")
                run = data.get("run", "")
                author = data.get("author", "")
                version = data.get("version", "")
                url = data.get("url", "")
                year = int(data.get("year", 0))

                relative_path = os.path.relpath(file_path, base_path)
                parts = relative_path.split(os.sep)

                if len(parts) >= 4:
                    track_id = parts[-4]
                    variant = parts[-2]
                    unique_id = f"{track_id};{variant}"
                else:
                    track_id = parts[-3]
                    ui_folder_path = os.path.join(base_path, track_id, 'ui')
                    variant_folders = [d for d in os.listdir(ui_folder_path) if os.path.isdir(os.path.join(ui_folder_path, d))]

                    if variant_folders:
                        unique_id = f"{track_id};{track_id}"
                    else:
                        unique_id = f"{track_id};{track_id}"

                variant_data = {
                    "name": track_name,
                    "description": description,
                    "tags": tags,
                    "geotags": geotags,
                    "country": country,
                    "city": city,
                    "length": length,
                    "width": width,
                    "pitboxes": pitboxes,
                    "run": run,
                    "author": author,
                    "version": version,
                    "url": url,
                    "year": year,
                    "priority": priority
                }

                if track_id not in track_data:
                    track_data[track_id] = {
                        "highestpriorityid": unique_id,
                        "highestpriorityname": track_name,
                        "highestpriority": priority,
                        "variants": []
                    }

                if priority > track_data[track_id]["highestpriority"]:
                    track_data[track_id]["highestpriorityid"] = unique_id
                    track_data[track_id]["highestpriorityname"] = track_name
                    track_data[track_id]["highestpriority"] = priority

                track_data[track_id]["variants"].append({unique_id: variant_data})

                print(f"Processed track: {track_name}")

    return track_data

# Example usage
base_path = "D://SteamLibrary/steamapps/common/assettocorsa/content/tracks/"
track_data = extract_track_data(base_path)

# Save the merged data to a JSON file
output_file = 'merged_track_data.json'
with open(output_file, 'w') as out_file:
    json.dump(track_data, out_file, indent=4)

# Print the results
print("\nExtracted Track Data:")
for track_id, data in track_data.items():
    print(track_id, data)
