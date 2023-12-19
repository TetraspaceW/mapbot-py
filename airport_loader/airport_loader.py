import geopy
import time
import csv
import os
from supabase import create_client

supabase_url = os.environ.get("SUPABASE_URL")
supabase_key = os.environ.get("SUPABASE_KEY")
supabase = create_client(supabase_url, supabase_key)


def update_airport_names(airports: list[str] = None):
    with open("./airport_loader/frontier_airports.csv") as file:
        airports_reader = csv.reader(file, quotechar="'", delimiter=",")
        for row in airports_reader:
            [code, name] = row
            if (not airports) or (code in airports):
                airports_table = supabase.table("airport")
                airports_table.update({"name": name}).eq("code", code).execute()
                print(f"Updated name of {code} to {name}.")


def populate_geocoded_airports(airports: list[str]):
    with open("./airport_loader/frontier_airports.csv") as file:
        airports_reader = csv.reader(file, quotechar="'", delimiter=",")
        for row in airports_reader:
            [code, name] = row
            if (not airports) or code in airports:
                airports_table = supabase.table("airport")
                airport_location: geopy.Location = geopy.Nominatim(
                    user_agent="ampmap-airport-loader"
                ).geocode(f"{code} airport", exactly_one=True)
                print(airport_location)

                # Check if airport already exists in the table
                existing_airport = (
                    airports_table.select().eq("code", code).execute().first()
                )

                new_airport_data = {
                    "location": {
                        "lat": airport_location.latitude,
                        "lng": airport_location.longitude,
                    },
                    "name": name,
                }

                if existing_airport:
                    airports_table.update(new_airport_data).eq("code", code).execute()
                    print(f"Updated {code} airport.")
                else:
                    new_airport_data.update({"code": code, "frontier": True})
                    airports_table.insert(new_airport_data).execute()
                    print(f"Inserted {code} airport.")

                time.sleep(2)  # let's not get rate limited


def populate_routes():
    routes_table = supabase.table("route")
    # add some web scraping from the frontier website

    routes_table.insert(
        {"start": None, "end": None, "frontier": None, "length": None}
    ).execute()
