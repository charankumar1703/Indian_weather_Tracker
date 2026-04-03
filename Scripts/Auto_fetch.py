import requests
import sqlite3
from datetime import datetime, timedelta
import os
import time

# -----------------------------
# CONFIGURATION
# -----------------------------

API_KEY = "acad7625e6e2010595e5704ba2a5d77e"  # Replace with your actual OpenWeatherMap API key

CITIES = [
    "Kolkata", "Patna", "Lucknow", "Mumbai", "Bengaluru", "Hyderabad",
    "Chennai", "Bhopal", "Gandhinagar", "Raipur", "Shimla", "Chandigarh",
    "Panaji", "Dehradun", "Ranchi", "Dispur", "Itanagar", "Aizawl", 
    "Shillong", "Kohima", "Agartala", "Gangtok", "Jaipur", 
    "Thiruvananthapuram", "Amaravati", "Imphal", "Leh", "Puducherry","Delhi"
]

DB_PATH = r"C:/Indian_Weather_Tracker/data/weather_live.db"
LOG_PATH = r"C:/Indian_Weather_Tracker/logs/fetch_log.txt"
IST_OFFSET = timedelta(hours=5, minutes=30)

# -----------------------------
# LOGGING FUNCTION
# -----------------------------

def log(msg):
    try:
        os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
        time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_msg = f"[{time_now}] {msg}"
        print(full_msg)
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(full_msg + "\n")
    except Exception as e:
        print(f"Failed to write to log: {e}")

# -----------------------------
# CREATE TABLE IF NOT EXISTS
# -----------------------------

def create_table():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()

    # Enable Write-Ahead Logging to prevent locking
    cur.execute("PRAGMA journal_mode=WAL;")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_live (
            Timestamp TEXT,
            City TEXT,
            Temperature REAL,
            FeelsLike REAL,
            Humidity INTEGER,
            Pressure INTEGER,
            Cloudiness INTEGER,
            Weather TEXT,
            Description TEXT,
            WindSpeed REAL,
            WindDirection INTEGER,
            RainLabel TEXT,
            Sunrise TEXT,
            Sunset TEXT,
            AQI INTEGER,
            PRIMARY KEY (Timestamp, City)
        )
    """)
    conn.commit()
    conn.close()

# -----------------------------
# FETCH WEATHER DATA
# -----------------------------

def fetch_weather(city):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city},IN&appid={API_KEY}&units=metric"
    response = requests.get(url)
    return response.json()

# -----------------------------
# FETCH AQI DATA
# -----------------------------

def fetch_aqi(lat, lon):
    url = f"https://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_KEY}"
    try:
        response = requests.get(url)
        data = response.json()
        return data["list"][0]["main"]["aqi"]
    except:
        return None

# -----------------------------
# PARSE AND INSERT INTO DB
# -----------------------------

def parse_and_insert(city, data):
    timestamp = (datetime.utcnow() + IST_OFFSET).strftime("%Y-%m-%d %H:%M:%S")
    temp = data['main']['temp']
    feels_like = data['main']['feels_like']
    humidity = data['main']['humidity']
    pressure = data['main']['pressure']
    cloudiness = data['clouds']['all']
    weather = data['weather'][0]['main']
    description = data['weather'][0]['description']
    wind_speed = data['wind'].get('speed', 0)
    wind_dir = data['wind'].get('deg', 0)
    rain_label = "Rain" if "rain" in weather.lower() else "No Rain"

    # Sunrise & Sunset in IST
    sunrise_ts = data['sys'].get('sunrise')
    sunset_ts = data['sys'].get('sunset')
    sunrise = (datetime.utcfromtimestamp(sunrise_ts) + IST_OFFSET).strftime('%H:%M:%S') if sunrise_ts else None
    sunset = (datetime.utcfromtimestamp(sunset_ts) + IST_OFFSET).strftime('%H:%M:%S') if sunset_ts else None

    # Coordinates for AQI
    lat = data['coord']['lat']
    lon = data['coord']['lon']
    aqi = fetch_aqi(lat, lon)

    row = (
        timestamp, city, temp, feels_like, humidity, pressure,
        cloudiness, weather, description, wind_speed, wind_dir,
        rain_label, sunrise, sunset, aqi
    )

    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        cur = conn.cursor()
        cur.execute("""
            INSERT OR IGNORE INTO weather_live (
                Timestamp, City, Temperature, FeelsLike, Humidity, Pressure,
                Cloudiness, Weather, Description, WindSpeed, WindDirection,
                RainLabel, Sunrise, Sunset, AQI
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row)
        conn.commit()
        conn.close()
    except Exception as e:
        log(f"Database insert error for {city}: {e}")

# -----------------------------
# MAIN FUNCTION
# -----------------------------

def main():
    start_time = time.time()
    log("Script started.")
    create_table()

    for city in CITIES:
        try:
            log(f"Fetching weather for {city}")
            data = fetch_weather(city)

            if data.get("cod") != 200:
                log(f"[API ERROR] {city}: {data.get('message', 'Unknown error')}")
                continue

            parse_and_insert(city, data)
            log(f"Inserted data for {city}")
            time.sleep(1.5)  # Avoids DB locks

        except Exception as e:
            log(f"[ERROR] {city}: {e}")

    duration = round(time.time() - start_time, 2)
    log(f"Script completed in {duration} seconds.\n")

# -----------------------------
# RUN SCRIPT
# -----------------------------

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        with open("C:\\Indian_Weather_Tracker\\scripts\\crash_log.txt", "w") as f:
            f.write(f"Script crashed: {e}")
