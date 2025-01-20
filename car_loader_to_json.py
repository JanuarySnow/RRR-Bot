import os
import json
import re

def clean_html_tags(text):
    text = re.sub(r'</?[^>]+>', '', text)  # Remove HTML tags
    text = re.sub(r'[\x00-\x1F\x7F]', '', text)  # Remove control characters
    return text

def extract_number(value):
    if isinstance(value, str):
        match = re.search(r'[\d.]+', value)
        if match:
            number_str = match.group()
            try:
                return float(number_str)
            except ValueError:
                return None
    return None

def extract_car_data(base_path):
    car_data = {}

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
        
        cleaned_data = clean_html_tags(raw_data)

        try:
            data = json.loads(cleaned_data)
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON data in {file_path}: {e}")
            return None
        
        return data

    # Walk through the directory
    for root, dirs, files in os.walk(base_path):
        for file in files:
            if file == "ui_car.json":
                file_path = os.path.join(root, file)
                data = parse_json_file(file_path)

                if data is None:
                    continue

                car_name = data.get("name", "Unknown")
                brand = data.get("brand", "")
                car_class = data.get("class", "")
                country = data.get("country", "")
                description = data.get("description", "")
                tags = data.get("tags", [])
                torque_curve = data.get("torqueCurve", [])
                power_curve = data.get("powerCurve", [])
                specs = data.get("specs", {})
                year = data.get("year", 0)
                author = data.get("author", "")
                url = data.get("url", "")
                version = data.get("version", "")

                bhp = extract_number(specs.get("bhp"))
                torque = extract_number(specs.get("torque"))
                weight = extract_number(specs.get("weight"))
                topspeed = extract_number(specs.get("topspeed"))
                acceleration = extract_number(specs.get("acceleration"))
                pwratio = extract_number(specs.get("pwratio"))

                # Determine the car ID
                relative_path = os.path.relpath(file_path, base_path)
                parts = relative_path.split(os.sep)
                car_id = parts[-3]

                car_data[car_id] = {
                    "name": car_name,
                    "brand": brand,
                    "class": car_class,
                    "country": country,
                    "description": description,
                    "tags": tags,
                    "torqueCurve": torque_curve,
                    "powerCurve": power_curve,
                    "specs": {
                        "bhp": bhp,
                        "torque": torque,
                        "weight": weight,
                        "topspeed": topspeed,
                        "acceleration": acceleration,
                        "pwratio": pwratio
                    },
                    "year": year,
                    "author": author,
                    "url": url,
                    "version": version
                }

    return car_data

# Example usage
base_path = '/path/to/car/files'
car_data = extract_car_data(base_path)

# Save the extracted data to a JSON file
output_file = 'merged_car_data.json'
with open(output_file, 'w') as out_file:
    json.dump(car_data, out_file, indent=4)


# Example usage
base_path = '/path/to/car/files'
car_data = extract_car_data(base_path)

# Save the extracted data to a JSON file
output_file = 'merged_car_data.json'
with open(output_file, 'w') as out_file:
    json.dump(car_data, out_file, indent=4)


# Example usage
base_path = '/path/to/car/files'
car_data = extract_car_data(base_path)

# Save the extracted data to a JSON file
output_file = 'merged_car_data.json'
with open(output_file, 'w') as out_file:
    json.dump(car_data, out_file, indent=4)


# Example usage
base_path = 'D://SteamLibrary/steamapps/common/assettocorsa/content/cars'
car_data = extract_car_data(base_path)

# Save the extracted data to a JSON file
output_file = 'merged_car_data.json'
with open(output_file, 'w') as out_file:
    json.dump(car_data, out_file, indent=4)
