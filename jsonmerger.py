import json

def merge_json_files(file1, file2, output_file):
    with open(file1, 'r') as f1, open(file2, 'r') as f2:
        data1 = json.load(f1)
        data2 = json.load(f2)

    merged_data = data1.copy()

    for key in data2:
        if key not in merged_data:
            merged_data[key] = data2[key]

    with open(output_file, 'w') as out_file:
        json.dump(merged_data, out_file, indent=4)

# Example usage
file1 = 'track_names.json'
file2 = 'track_names (1).json'
output_file = 'merged_output.json'

merge_json_files(file1, file2, output_file)
