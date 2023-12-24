from dataclasses import dataclass
import geopy
import time
import csv
import os
from supabase import create_client
import requests
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import json

load_dotenv()

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


def populate_geocoded_airports(airports: list[str], frontier: bool = True):
    with open(
        f"./airport_loader/{'frontier_' if frontier else ''}airports.csv"
    ) as file:
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
                    airports_table.select().eq("code", code).execute().count > 0
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
                    new_airport_data.update({"code": code, "frontier": frontier})
                    airports_table.insert(new_airport_data).execute()
                    print(f"Inserted {code} airport.")

                time.sleep(2)  # let's not get rate limited


@dataclass
class AirlineRoute:
    start: str
    end: str
    length: float

    def to_json(self):
        return self.__dict__


def populate_routes_and_save(ratelimit: float = 6):
    """
    Retrieves routes from the flightaware api and saves them to a file.

    Parameters
    ----------
    ratelimit : float
        The rate limit in number of seconds to wait between pages (pages will be fetched in batches of nine).
    """

    scheduled_flights = []

    # retrieve from the flightaware api
    nextPageURL = (
        "https://aeroapi.flightaware.com/aeroapi/operators/FFT/flights?max_pages=9"
    )
    headers = {"x-apikey": os.environ.get("FLIGHTAWARE_API_KEY")}

    for _ in range(20):
        response = requests.get(nextPageURL, headers=headers)

        print(response.json())

        if (
            (response.status_code != 200)
            or (not response.json()["scheduled"])
            or (len(response.json()["scheduled"]) == 0)
        ):
            break

        for flight in response.json()["scheduled"]:
            try:
                flightStart = flight["origin"]["code_iata"]
                flightEnd = flight["destination"]["code_iata"]
                scheduledOut: datetime = datetime.strptime(
                    flight["scheduled_out"], "%Y-%m-%dT%H:%M:%SZ"
                )
                scheduledIn: datetime = datetime.strptime(
                    flight["scheduled_in"], "%Y-%m-%dT%H:%M:%SZ"
                )
                duration = (scheduledIn - scheduledOut).total_seconds() / 60

                scheduled_flights.append(
                    AirlineRoute(start=flightStart, end=flightEnd, length=duration)
                )
            except Exception as e:
                print(
                    {
                        "flightStart": flight["origin"],
                        "flightEnd": flight["destination"],
                        "scheduledOut": flight["scheduled_out"],
                        "scheduledIn": flight["scheduled_in"],
                    }
                )
                print(e)

        if response.json()["links"] is None:
            break

        nextPagePath = response.json()["links"]["next"]
        nextPageURL = f"https://aeroapi.flightaware.com/aeroapi{nextPagePath}"

        time.sleep(ratelimit * 10)  # 10 pages/min rate limit

    parsed_routes = [
        {**route.to_json(), "frontier": True}
        for route in parse_retrieved_routes(scheduled_flights)
    ]

    print(parsed_routes)

    input("Press enter to continue...")

    with open("./airport_loader/frontier_routes.json", "w") as file:
        json.dump(parsed_routes, file)


def parse_retrieved_routes(routes: list[AirlineRoute]):
    routes_df = pd.DataFrame([route.to_json() for route in routes])
    print(routes_df)
    routes_df = routes_df.groupby(["start", "end"]).mean()
    routes = [
        AirlineRoute(index[0], index[1], row["length"])
        for index, row in routes_df.iterrows()
    ]

    return routes


def populate_routes_from_file(filename: str = "./airport_loader/frontier_routes.json"):
    with open(filename) as file:
        parsed_routes = json.load(file)

    routes_table = supabase.table("route")
    routes_table.insert(parsed_routes).execute()
