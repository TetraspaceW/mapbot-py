import os

import discord
from discord.ext import commands
from geopy.geocoders import Nominatim
from supabase import create_client, Client

intents = discord.Intents.default()
intents.message_content = True
client = commands.Bot(command_prefix="!ampmap ", intents=intents)

supabase_url = os.environ.get('SUPABASE_URL')
supabase_key = os.environ.get('SUPABASE_KEY')
supabase = create_client(supabase_url, supabase_key)


@client.event
async def on_ready():
    print(f"Logged in as {client.user}")


@client.event
async def on_command_error(ctx: commands.Context):
    await ctx.message.add_reaction("⚠️")


@client.command()
async def location(ctx: commands.Context, *args):
    location_input = " ".join(args)
    geolocator = Nominatim(user_agent='ampmap')
    geocoded_location = geolocator.geocode(location_input)

    locations_table = supabase.table('location')

    _, id_exists = locations_table.select('user_id').eq('user_id', ctx.author.id).execute()

    supabase_request = {'location': {'lat': geocoded_location.latitude, 'lng': geocoded_location.longitude},
                        'user_name': ctx.author.name}

    if id_exists:
        locations_table.update(supabase_request).eq('user_id', ctx.author.id).execute()
    else:
        supabase_request["user_id"] = ctx.author.id
        locations_table.insert(supabase_request).execute()

    await ctx.send(
        f"Revealed your location {(geocoded_location.latitude, geocoded_location.longitude)} to amp's sight.")


@client.command()
async def obscure(ctx: commands.Context):
    locations_table = supabase.table("location")
    locations_table.delete().eq('user_id', ctx.author.id).execute()
    await ctx.send(f"Obscured your location from amp's sight.")


client.run(os.environ.get("DISCORD_TOKEN"))
