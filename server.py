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
        port="5432"
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
    
    cursor.execute(query, (start_code, end_code, start_code, start_code, end_code, end_code))
    row = cursor.fetchone()
    
    cursor.close()
    conn.close()

    if row:
        return {"station_code": row[0], "station_name": row[1]}
    
    return None

# Fetch private bus details from API
def get_bus_details(departure: str, destination: str) -> Any:
    url = f"https://busapi.amithv.xyz/api/v1/schedules?departure={departure}&destination={destination}"
    print(url)
    response = requests.get(url)

    if response.status_code == 200 and response.json():
        return response.json()
    
    return None  # No buses available

# Helper function to extract city name (before the comma)
def extract_city(place: str) -> str:
    return place.split(",")[0].strip() if "," in place else place.strip()

@app.get("/best-route")
def get_best_route(start: str = Query(...), end: str = Query(...)):
    # Extract only the city name
    start_city = extract_city(start)
    end_city = extract_city(end)

    # Convert place names to coordinates
    start_lat, start_lon = get_coordinates(start)
    end_lat, end_lon = get_coordinates(end)

    # Find nearest railway stations
    start_station = get_nearest_station(start_lat, start_lon)
    end_station = get_nearest_station(end_lat, end_lon)

    # Determine if a bus is needed
    bus_needed_start = start_city.lower() not in start_station["station_name"].lower()
    bus_needed_end = end_city.lower() not in end_station["station_name"].lower()

    # Fetch bus details using extracted city names
    bus_details_start = get_bus_details(start_city, start_station["station_name"]) if bus_needed_start else None
    bus_details_end = get_bus_details(end_station["station_name"], end_city) if bus_needed_end else None

    # Ensure bus details are included only if data exists
    if not bus_details_start:
        bus_needed_start = False
    if not bus_details_end:
        bus_needed_end = False

    # Assume direct if distance between stations is < 100 km
    if start_station["distance_km"] < 100 and end_station["distance_km"] < 100:
        return {
            "route_type": "Direct Train",
            "start_station": start_station,
            "bus_needed_start": bus_needed_start,
            "bus_details_start": bus_details_start,
            "end_station": end_station,
            "bus_needed_end": bus_needed_end,
            "bus_details_end": bus_details_end,
        }

    # Find an intermediate station if no direct connection
    transfer_station = get_transfer_station(start_station["station_code"], end_station["station_code"])
    
    if transfer_station:
        return {
            "route_type": "Indirect Train (One Transfer)",
            "start_station": start_station,
            "bus_needed_start": bus_needed_start,
            "bus_details_start": bus_details_start,
            "transfer_station": transfer_station,
            "end_station": end_station,
            "bus_needed_end": bus_needed_end,
            "bus_details_end": bus_details_end,
        }

    # If no train connection found, suggest Train + Bus
    return {
        "route_type": "Train + Bus",
        "start_station": start_station,
        "bus_needed_start": bus_needed_start,
        "bus_details_start": bus_details_start,
        "end_station": end_station,
        "bus_needed_end": bus_needed_end,
        "bus_details_end": bus_details_end,
    }