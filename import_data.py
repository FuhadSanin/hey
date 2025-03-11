import json
import psycopg2

# Load the GeoJSON file
with open("stations.json", "r") as file:
    data = json.load(file)

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="railways",
    user="fuhadsanin",
    password="123",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Insert stations into the table
for feature in data["features"]:
    properties = feature["properties"]
    geometry = feature["geometry"]

    station_code = properties["code"]  # Matches "station_code" in DB
    station_name = properties["name"]  # Matches "station_name" in DB
    state = properties.get("state")
    zone = properties.get("zone")
    address = properties.get("address")
    
    # Extract latitude & longitude if geometry exists
    latitude, longitude = None, None
    if geometry and "coordinates" in geometry:
        longitude, latitude = geometry["coordinates"]

    cursor.execute(
        """
        INSERT INTO stations (station_code, station_name, state, zone, address, latitude, longitude)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (station_code) DO NOTHING;
        """,
        (station_code, station_name, state, zone, address, latitude, longitude)
    )

# Commit & close
conn.commit()
cursor.close()
conn.close()

print("✅ Data inserted successfully!")