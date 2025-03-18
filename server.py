from fastapi import FastAPI, Query, HTTPException
import psycopg2
import requests
from typing import Dict, Any

app = FastAPI()


# Database connection
def get_db_connection():
    return psycopg2.connect(
        dbname="railways",
        user="fuhadsanin",
        password="123",
        host="localhost",
        port="5432",
    )


# Get coordinates of a place using Nominatim API
def get_coordinates(place: str):
    url = f"https://nominatim.openstreetmap.org/search?q={place}&format=json&limit=1"
    response = requests.get(url, headers={"User-Agent": "FastAPI-GeoSearch"})

    if response.status_code == 200 and response.json():
        data = response.json()[0]
        return float(data["lat"]), float(data["lon"])

    raise HTTPException(status_code=404, detail="Location not found")


# Find the nearest station for given lat/lon
def get_nearest_station(lat: float, lon: float):
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
        SELECT station_code, station_name, state, zone, address, latitude, longitude,
               earth_distance(ll_to_earth(%s, %s), ll_to_earth(latitude, longitude)) / 1000 AS distance_km
        FROM stations
        ORDER BY distance_km ASC
        LIMIT 1;
    """

    cursor.execute(query, (lat, lon))
    row = cursor.fetchone()

    cursor.close()
    conn.close()

    if row:
        return {
            "station_code": row[0],
            "station_name": row[1],
            "state": row[2],
            "zone": row[3],
            "address": row[4],
            "latitude": row[5],
            "longitude": row[6],
            "distance_km": row[7],
        }

    raise HTTPException(status_code=404, detail="No stations found")


# Find an intermediate station between two stations
def get_transfer_station(start_code: str, end_code: str):
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
        SELECT s.station_code, s.station_name
        FROM stations s
        WHERE s.station_code != %s AND s.station_code != %s
        ORDER BY (earth_distance(ll_to_earth(
                    (SELECT latitude FROM stations WHERE station_code = %s),
                    (SELECT longitude FROM stations WHERE station_code = %s)
                ), ll_to_earth(s.latitude, s.longitude))
              +
              earth_distance(ll_to_earth(
                    (SELECT latitude FROM stations WHERE station_code = %s),
                    (SELECT longitude FROM stations WHERE station_code = %s)
                ), ll_to_earth(s.latitude, s.longitude))) ASC
        LIMIT 1;
    """

    cursor.execute(
        query, (start_code, end_code, start_code, start_code, end_code, end_code)
    )
    row = cursor.fetchone()

    cursor.close()
    conn.close()

    if row:
        return {"station_code": row[0], "station_name": row[1]}

    return None

# Helper function to extract city name (before the comma)
def extract_city(place: str) -> str:
    return place.split(",")[0].strip() if "," in place else place.strip()

# Fetch private bus details from API
def get_private_bus_details(departure: str, destination: str) -> bool:
    print("Fetching private bus details")
    url = f"https://busapi.amithv.xyz/api/v1/schedules?departure={departure}&destination={destination}"
    response = requests.get(url)

    if response.status_code == 200 and response.json():
        return True

    return False


# Fetch KSRTC bus details from local server
def get_ksrtc_bus_details(departure: str, destination: str) -> bool:
    print("Fetching KSRTC bus details")
    url = f"http://127.0.0.1:9000/api/v1/ksrtc/?source={departure}&destination={destination}"
    response = requests.get(url)
    print("url:", url)
    print(response.json())
    if response.status_code == 200 and response.json():
        return True  
    return False  


# Determine if any bus service (private or KSRTC) is available
def is_bus_available(departure: str, destination: str) -> str:
    if get_private_bus_details(departure, destination):
        return "private_bus"
    elif get_ksrtc_bus_details(departure, destination):
        return "ksrtc_bus"
    return None  # No bus available


@app.get("/best-route")
def get_best_route(start: str = Query(...), end: str = Query(...)):
    # Extract city names
    start_city = extract_city(start)
    end_city = extract_city(end)

    bus_type = is_bus_available(start_city, end_city)
    if bus_type:
        return {"route_type": [bus_type]}  # ✅ Direct bus available

    # Convert place names to coordinates
    start_lat, start_lon = get_coordinates(start)
    end_lat, end_lon = get_coordinates(end)

    # Find nearest railway stations
    start_station = get_nearest_station(start_lat, start_lon)
    end_station = get_nearest_station(end_lat, end_lon)

    if start_station["station_name"] == end_station["station_name"]:
        return {"route_type": ["taxi"]}

    # Check if a direct train is possible
    if (
        start_city.lower() == start_station["station_name"].lower()
        and end_city.lower() == end_station["station_name"].lower()
        and start_station["distance_km"] < 100
        and end_station["distance_km"] < 100
    ):
        return {
            "route_type": ["train", "train"],
            "start_station": start_station,
            "end_station": end_station,
        }

    # Check if bus (private or KSRTC) is needed for start and end locations
    bus_needed_start = is_bus_available(start_city, start_station["station_name"].capitalize())
    bus_needed_end = is_bus_available(end_station["station_name"].capitalize(), end_city)

    route_type = []
    if (
        start_city.lower() not in start_station["station_name"].lower()
        and end_city.lower() not in end_station["station_name"].lower()
    ):
        if bus_needed_start and bus_needed_end:
            route_type = [bus_needed_start, "train", "train", bus_needed_end]
        elif bus_needed_start:
            route_type = [bus_needed_start, "train", "train", "taxi"]
        elif bus_needed_end:
            route_type = ["taxi", "train", "train", bus_needed_end]
        else:
            route_type = ["taxi", "train", "train", "taxi"]
    elif start_city.lower() == start_station["station_name"].lower():
        if bus_needed_end:
            route_type = ["train", "train", bus_needed_end]
        else:
            route_type = ["train", "train", "taxi"]
    elif end_city.lower() == end_station["station_name"].lower():
        if bus_needed_start:
            route_type = [bus_needed_start, "train", "train"]
        else:
            route_type = ["taxi", "train", "train"]

    return {
        "route_type": route_type,
        "start_station": start_station,
        "end_station": end_station,
    }