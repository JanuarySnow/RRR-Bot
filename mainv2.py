import tkinter as tk
from tkinter import filedialog
from PIL import Image, ImageDraw, ImageFont, ImageOps, ImageTk
import json
import os
import difflib
from datetime import datetime

# Ensure consistent working directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

settings = {
    "x_name": 80,
    "y_start": 150,
    "line_spacing": 35,
    "logo_offset_x": 10,
    "logo_offset_y": -5,
    "logo_fixed_x": 500,
    "logo_size": 30,
    "font_size": 32,
    "font_path": "fonts/BaijamJuree-Medium.ttf",
    "template_dir": "templates",
    "json_dir": "results",
    "preset_dir": "presets"
}

os.makedirs(settings["preset_dir"], exist_ok=True)

root = tk.Tk()
root.title("Race Results Processor")
root.geometry("700x700")
root.configure(bg="#2e2e2e")

button_style = {"bg": "#3a3a3a", "fg": "white", "activebackground": "#505050", "activeforeground": "white", "relief": "flat", "bd": 1, "highlightthickness": 0, "padx": 10, "pady": 5}
label_style = {"bg": "#2e2e2e", "fg": "white", "wraplength": 400}

template_label = tk.Label(root, text="No file selected", **label_style)
template_label.grid(row=0, column=1, columnspan=3, sticky="w")
template_button = tk.Button(root, text="Browse Template Image", command=lambda: template_label.config(text=filedialog.askopenfilename(initialdir=settings["template_dir"], filetypes=[("Image Files", "*.png;*.jpg;*.jpeg")])), **button_style)
template_button.grid(row=0, column=0, padx=10, pady=5, sticky="w")

json_label = tk.Label(root, text="No file selected", **label_style)
json_label.grid(row=1, column=1, columnspan=3, sticky="w")
json_button = tk.Button(root, text="Browse Race Results JSON", command=lambda: json_label.config(text=filedialog.askopenfilename(initialdir=settings["json_dir"], filetypes=[("JSON Files", "*.json")])), **button_style)
json_button.grid(row=1, column=0, padx=10, pady=5, sticky="w")

frame_left = tk.Frame(root, bg="#2e2e2e")
frame_left.grid(row=2, column=0, rowspan=6, sticky="nw", padx=10)
frame_right = tk.Frame(root, bg="#2e2e2e")
frame_right.grid(row=2, column=1, rowspan=6, sticky="nw")

def make_spinbox(master, label, default, row):
    tk.Label(master, text=label, bg="#2e2e2e", fg="white").grid(row=row, column=0, sticky="w", padx=5, pady=2)
    entry = tk.Spinbox(master, from_=-1000, to=2000, width=10)
    entry.delete(0, tk.END)
    entry.insert(0, default)
    entry.grid(row=row, column=1, sticky="w", padx=5, pady=2)
    return entry

x_entry = make_spinbox(frame_left, "Text X Position", settings["x_name"], row=0)
y_entry = make_spinbox(frame_left, "Text Y Start", settings["y_start"], row=1)
spacing_entry = make_spinbox(frame_left, "Line Spacing", settings["line_spacing"], row=2)

logo_x_entry = make_spinbox(frame_right, "Logo X Offset", settings["logo_offset_x"], row=0)
logo_y_entry = make_spinbox(frame_right, "Logo Y Offset", settings["logo_offset_y"], row=1)
logo_fixed_x_entry = make_spinbox(frame_right, "Logo Fixed X Pos", settings["logo_fixed_x"], row=2)
logo_size_entry = make_spinbox(frame_right, "Logo Size", settings["logo_size"], row=3)

custom_x_entry = make_spinbox(frame_left, "Custom X Pos", 30, row=3)
custom_y_entry = make_spinbox(frame_left, "Custom Y Pos", 30, row=4)
custom_size_entry = make_spinbox(frame_left, "Custom Text Size", 24, row=5)

track_x_entry = make_spinbox(frame_right, "Track X Pos", 30, row=4)
track_y_entry = make_spinbox(frame_right, "Track Y Pos", 70, row=5)
track_size_entry = make_spinbox(frame_right, "Track Font Size", 28, row=6)

user_text_label = tk.Label(root, text="Custom Text:", bg="#2e2e2e", fg="white")
user_text_label.grid(row=9, column=0, sticky="w", padx=5, pady=2)
user_text_entry = tk.Entry(root, width=30)
user_text_entry.grid(row=9, column=1, padx=5, pady=2, sticky="w")

track_text_label = tk.Label(root, text="Track Name Text:", bg="#2e2e2e", fg="white")
track_text_label.grid(row=10, column=0, sticky="w", padx=5, pady=2)
track_text_entry = tk.Entry(root, width=30)
track_text_entry.grid(row=10, column=1, padx=5, pady=2, sticky="w")

status_label = tk.Label(root, text="", **label_style)
status_label.grid(row=11, column=0, columnspan=2, pady=10, sticky="w")
preset_var = tk.StringVar()
preset_menu = tk.OptionMenu(root, preset_var, "")
preset_menu.config(**button_style)
preset_menu.grid(row=12, column=1, sticky="w", padx=5, pady=5)

def save_preset():
    preset = {
        "x_name": int(x_entry.get()),
        "y_start": int(y_entry.get()),
        "line_spacing": int(spacing_entry.get()),
        "logo_offset_x": int(logo_x_entry.get()),
        "logo_offset_y": int(logo_y_entry.get()),
        "logo_fixed_x": int(logo_fixed_x_entry.get()),
        "logo_size": int(logo_size_entry.get()),
        "custom_x": int(custom_x_entry.get()),
        "custom_y": int(custom_y_entry.get()),
        "custom_size": int(custom_size_entry.get()),
        "track_x": int(track_x_entry.get()),
        "track_y": int(track_y_entry.get()),
        "track_size": int(track_size_entry.get())
    }
    filepath = filedialog.asksaveasfilename(defaultextension=".json", initialdir=settings["preset_dir"], filetypes=[("JSON Files", "*.json")])
    if filepath:
        with open(filepath, "w") as f:
            json.dump(preset, f)
        status_label.config(text=f"Preset saved: {filepath}")
    refresh_presets()

def load_preset():
    filename = preset_var.get()
    filepath = os.path.join(settings["preset_dir"], filename)
    if not os.path.isfile(filepath):
        status_label.config(text="No valid preset selected.")
        return
    try:
        with open(filepath, "r") as f:
            preset = json.load(f)
            x_entry.delete(0, tk.END); x_entry.insert(0, preset["x_name"])
            y_entry.delete(0, tk.END); y_entry.insert(0, preset["y_start"])
            spacing_entry.delete(0, tk.END); spacing_entry.insert(0, preset["line_spacing"])
            logo_x_entry.delete(0, tk.END); logo_x_entry.insert(0, preset["logo_offset_x"])
            logo_y_entry.delete(0, tk.END); logo_y_entry.insert(0, preset["logo_offset_y"])
            logo_fixed_x_entry.delete(0, tk.END); logo_fixed_x_entry.insert(0, preset["logo_fixed_x"])
            logo_size_entry.delete(0, tk.END); logo_size_entry.insert(0, preset["logo_size"])
            custom_x_entry.delete(0, tk.END); custom_x_entry.insert(0, preset["custom_x"])
            custom_y_entry.delete(0, tk.END); custom_y_entry.insert(0, preset["custom_y"])
            custom_size_entry.delete(0, tk.END); custom_size_entry.insert(0, preset["custom_size"])
            track_x_entry.delete(0, tk.END); track_x_entry.insert(0, preset["track_x"])
            track_y_entry.delete(0, tk.END); track_y_entry.insert(0, preset["track_y"])
            track_size_entry.delete(0, tk.END); track_size_entry.insert(0, preset["track_size"])
        status_label.config(text=f"Preset loaded: {filepath}")
    except Exception as e:
        status_label.config(text=f"Failed to load preset: {str(e)}")

def refresh_presets():
    preset_menu["menu"].delete(0, "end")
    preset_files = [f for f in os.listdir(settings["preset_dir"]) if f.endswith(".json")]
    for preset in preset_files:
        preset_menu["menu"].add_command(label=preset, command=lambda p=preset: preset_var.set(p))
    if preset_files:
        preset_var.set(preset_files[0])

refresh_presets()

save_button = tk.Button(root, text="Save Preset", command=save_preset, **button_style)
save_button.grid(row=12, column=0, padx=5, pady=5, sticky="e")

load_button = tk.Button(root, text="Load Preset", command=load_preset, **button_style)
load_button.grid(row=12, column=1, padx=5, pady=5, sticky="e")

# ---- CUT TO FIXED DRAWING LOGIC ----

def update_preview(path):
    pass  # Placeholder to prevent crash

def process_and_merge():
    template_path = template_label.cget("text")
    json_path = json_label.cget("text")

    if not os.path.isfile(template_path) or not os.path.isfile(json_path):
        status_label.config(text="Please select both files before proceeding.")
        return

    settings["x_name"] = int(x_entry.get())
    settings["y_start"] = int(y_entry.get())
    settings["line_spacing"] = int(spacing_entry.get())
    settings["logo_offset_x"] = int(logo_x_entry.get())
    settings["logo_offset_y"] = int(logo_y_entry.get())
    settings["logo_fixed_x"] = int(logo_fixed_x_entry.get())
    settings["logo_size"] = int(logo_size_entry.get())

    with open(json_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    # Track name cleanup
    track_candidates = [k for k in data.keys() if 'track' in k.lower() or 'name' in k.lower()]
    best_match = difflib.get_close_matches("TrackName", track_candidates, n=1, cutoff=0.5)
    track_key = best_match[0] if best_match else "TrackName"
    raw_track = data.get(track_key, "Unknown Track")
    track_name = raw_track.replace("rt_", "").replace("_", " ").title()

    # Sort drivers
    results = data.get("Result", [])
    sorted_results = sorted(
        results,
        key=lambda x: (
            x.get("Disqualified", False),
            -x.get("NumLaps", 0),
            float(x.get("TotalTime", float("inf")))
        )
    )

    # Nation mapping by GUID
    guid_to_nation = {}
    for car in data.get("Cars", []):
        guid = car.get("Driver", {}).get("Guid", "")
        nation = car.get("Driver", {}).get("Nation", "")
        if guid:
            guid_to_nation[guid] = nation

    # Build driver display data
    driver_data = [{
        "DriverName": r["DriverName"],
        "GridPosition": r.get("GridPosition", 0),
        "CarModel": r["CarModel"],
        "Nation": guid_to_nation.get(r.get("DriverGuid", ""), "")
    } for r in sorted_results]

    image = Image.open(template_path).convert("RGBA")
    draw = ImageDraw.Draw(image)

    try:
        time_font = ImageFont.truetype("fonts/BaijamJuree-Regular.ttf", size=26)
    except:
        time_font = ImageFont.load_default()

    try:
        font = ImageFont.truetype(settings["font_path"], size=settings["font_size"])
        track_font = ImageFont.truetype("fonts/Microgramma D Extended Bold.ttf", size=int(track_size_entry.get()))
    except:
        font = ImageFont.load_default()
        track_font = ImageFont.load_default()

    draw.text((int(track_x_entry.get()), int(track_y_entry.get())), track_text_entry.get().strip() or track_name, font=track_font, fill="white")

    y = settings["y_start"]
    for index, item in enumerate(driver_data):
        draw.text((settings["x_name"], y), item["DriverName"], font=font, fill="white")

        # Total time formatting
        total_time_ms = sorted_results[index].get("TotalTime", 0)
        num_laps = sorted_results[index].get("NumLaps", 0)
        max_laps = sorted_results[0].get("NumLaps", 0)
        dnf = (num_laps < max_laps - 2)

        if dnf:
            time_text = "DNF"
        elif index == 0:
            leader_time = total_time_ms
            if total_time_ms >= 60000:
                time_text = datetime.utcfromtimestamp(total_time_ms / 1000).strftime("%M:%S.%f")[:-3]
            else:
                time_text = f"{(total_time_ms / 1000):.3f}"
        else:
            gap_seconds = (total_time_ms - leader_time) / 1000
            time_text = f"+ {gap_seconds:.3f}"

        time_x = 775
        draw.text((time_x, y + 3), time_text, font=time_font, fill="white")

        if not dnf:
            position_change = (index + 1) - item.get("GridPosition", 0)
            arrow = "-"; color = "white"
            if position_change > 0:
                arrow = f"▼ {abs(position_change)}"; color = "red"
            elif position_change < 0:
                arrow = f"▲ {abs(position_change)}"; color = "lime"
            else:
                arrow = "-"; color = "white"

            try:
                arrow_font = ImageFont.truetype("fonts/BaijamJuree-Medium.ttf", size=24)
            except:
                arrow_font = ImageFont.load_default()

            draw.text((settings["x_name"] - -500, y + 5), arrow[0], font=arrow_font, fill=color)
            draw.text((settings["x_name"] - -515, y + 1), arrow[1:], font=font, fill=color)

        # Flags
        nation_code_3 = item.get("Nation", "").upper()
        nation_code = nation_code_3 or "TS"
        flag_file = next((f for f in os.listdir("flags") if os.path.splitext(f)[0].upper() == nation_code), "TS.png")
        print(f"Driver: {item['DriverName']} → Nation: {nation_code_3} ↳ Flag match: 3-letter={nation_code_3} → File={flag_file}")

        flag = Image.open(os.path.join("flags", flag_file)).convert("RGBA")
        flag = ImageOps.contain(flag, (40, 40))
        flag_x = 90         
        flag_y = y + (font.size - flag.height) // 2 + 6
        image.paste(flag, (int(flag_x), int(flag_y)), flag)

        # Logos
        model_name = item["CarModel"].lower()
        logo_files = [os.path.splitext(f)[0].lower() for f in os.listdir("logos") if f.lower().endswith((".png", ".jpg", ".jpeg"))]
        model_name_parts = model_name.split('_')
        brand_guess = next(
            (match for part in model_name_parts for match in difflib.get_close_matches(part, logo_files, n=1, cutoff=0.7)),
            model_name_parts[0]
        )

        logo_path = next((os.path.join("logos", f) for f in os.listdir("logos") if os.path.splitext(f)[0].lower() == brand_guess.lower()), None)
        if logo_path:
            try:
                logo = Image.open(logo_path).convert("RGBA")
                logo = ImageOps.contain(logo, (settings["logo_size"], settings["logo_size"]))
                image.paste(logo, (settings["logo_fixed_x"] + settings["logo_offset_x"], y + settings["logo_offset_y"]), logo)
            except:
                pass
        else:
            draw.text((settings["logo_fixed_x"] + settings["logo_offset_x"], y + settings["logo_offset_y"]), brand_guess.upper(), font=font, fill="white")

        y += settings["line_spacing"]

    # Custom Text
    user_text = user_text_entry.get().strip()
    if user_text:
        try:
            user_font = ImageFont.truetype("fonts/Microgramma D Extended Bold.ttf", size=int(custom_size_entry.get() or 24))
        except:
            user_font = ImageFont.load_default()
        x = int(custom_x_entry.get() or 0)
        y = int(custom_y_entry.get() or 0)
        draw.text((x, y), user_text, font=user_font, fill="white")

    # Date
    timestamp_str = data.get("Date") or next((data.get(k) for k in data.keys() if 'date' in k.lower() or 'time' in k.lower()), None)
    try:
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        date_text = dt.strftime("%B %d, %Y")
    except:
        date_text = timestamp_str or "Unknown Date"

    try:
        date_font = ImageFont.truetype("fonts/BaijamJuree-Bold.ttf", size=24)
    except:
        date_font = ImageFont.load_default()
    draw.text((60, image.height - 36), date_text, font=date_font, fill="white")

    # Save image
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    filename_safe = f"{track_name}_{date_text}".replace(" ", "_").replace(",", "").lower()
    preview_path = os.path.join(output_dir, f"{filename_safe}.png")
    image.save(preview_path)

    try:
        image.show()
    except:
        pass

    update_preview(preview_path)

process_button = tk.Button(root, text="Generate Image", command=process_and_merge, **button_style)
process_button.grid(row=30, column=0, columnspan=2, pady=10)

root.mainloop()
