import os
from typing import TypedDict

import discord
import geopy
from discord.ext import commands
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
from supabase import create_client

import dotenv

intents = discord.Intents.default()
intents.message_content = True
client = commands.Bot(command_prefix="!ampmap ", intents=intents)

dotenv.load_dotenv()

supabase_url = os.environ.get("SUPABASE_URL")
supabase_key = os.environ.get("SUPABASE_KEY")
supabase = create_client(supabase_url, supabase_key)


@client.event
async def on_ready():
    print(f"Logged in as {client.user}")


@client.event
async def on_command_error(ctx: commands.Context, _):
    await ctx.message.add_reaction("⚠️")


@client.command()
async def location(ctx: commands.Context, *args):
    location_input = " ".join(args)
    geolocator = Nominatim(user_agent="ampmap")
    geocoded_location: geopy.Location = geolocator.geocode(
        location_input, exactly_one=True
    )
    coordinates: Location = {
        "lat": geocoded_location.latitude,
        "lng": geocoded_location.longitude,
    }

    locations_table = supabase.table("location")

    _, id_exists = (
        locations_table.select("user_id").eq("user_id", ctx.author.id).execute()
    )

    airport_data = nearest_airport(coordinates)
    if airport_data["distance"] <= 100:
        airport_code = airport_data["code"]
    else:
        airport_code = None

    supabase_request = {
        "location": coordinates,
        "user_name": ctx.author.name,
        "nearest_airport": airport_code,
    }

    if id_exists:
        locations_table.update(supabase_request).eq("user_id", ctx.author.id).execute()
    else:
        supabase_request["user_id"] = ctx.author.id
        locations_table.insert(supabase_request).execute()

    await ctx.send(f"Your nearest airport is {nearest_airport(coordinates)}")


@client.command()
async def obscure(ctx: commands.Context):
    locations_table = supabase.table("location")
    locations_table.delete().eq("user_id", ctx.author.id).execute()
    await ctx.send(f"Removed your location from the database.")


class Location(TypedDict):
    lat: float
    lng: float


def nearest_airport(location: Location, frontier: bool = True):
    airports = (
        supabase.table("airport")
        .select("code,location,name")
        .eq("frontier", frontier)
        .execute()
    )
    distances = [
        {
            "name": airport["name"],
            "code": airport["code"],
            "distance": geodesic(
                (location["lat"], location["lng"]),
                (airport["location"]["lat"], airport["location"]["lng"]),
            ).miles,
        }
        for airport in airports.data
    ]
    distances.sort(key=lambda d: d["distance"])
    return distances[0]


client.run(os.environ.get("DISCORD_TOKEN"))
