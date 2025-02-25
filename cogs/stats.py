import aiohttp
import discord
import math
import pandas as pd
from discord.ext import commands, tasks
from discord.ext.commands import Context
from discord.ext.commands import Bot
from discord.ui import Button, View
from discord import Embed
import json
import logging
from operator import itemgetter
import os
import platform
import random
import sys
import matplotlib.pyplot as plt
import matplotlib.dates as mdates 
from datetime import datetime
import matplotlib.ticker as mtick
import numpy as np
from fuzzywuzzy import process
import content_data
import statsparser
import racer
import result
import asyncio
import shutil
from logger_config import logger

ON_READY_FIRST_RUN_DOWNLOAD = True
ON_READY_FIRST_TIME_SCAN = True

# Here we name the cog and create a new class for the cog.
class Stats(commands.Cog, name="stats"):
    def __init__(self, bot) -> None:
        self.bot = bot
        print("loading stats cog")
        self.parsed = statsparser.parser()
        self.user_data = self.load_user_data()
        self.first_load()
        self.fetch_results_list.start()
        self.justadded = []
        self.logger = logger
        self.timetrialserver = 'https://timetrial.ac.tekly.racing'
        self.mx5euserver = "https://eu.mx5.ac.tekly.racing"
        self.mx5naserver = "https://us.mx5.ac.tekly.racing"
        self.gt3euserver = "https://eu.gt3.ac.tekly.racing"
        self.gt3naserver = "https://us.gt3.ac.tekly.racing"
        self.worldtourserver = "https://worldtour.ac.tekly.racing"
        self.mx5naproserver = "https://us.gpk.ac.tekly.racing"
        self.servers = (self.mx5euserver, self.mx5naserver, self.gt3euserver, self.gt3naserver, self.worldtourserver, self.mx5naproserver)
        self.blacklist = ["2025_1_4_21_37_RACE.json", "2025_1_4_22_2_RACE.json",
                          "2024_12_21_21_58_RACE.json", "2024_12_21_21_32_RACE.json",
                          "2025_2_17_20_30_RACE.json", "2025_2_17_20_57_RACE.json",
                          "2025_2_22_22_0_RACE.json", "2025_2_22_21_35_RACE.json"]

        self.servertodirectory = {
            self.mx5euserver: "eumx5",
            self.mx5naserver: "namx5",
            self.mx5naproserver: "namx5pro",
            self.gt3euserver: "eugt3",
            self.gt3naserver: "nagt3",
            self.worldtourserver: "worldtour",
        }
        self.download_queue = []
        logger.info("Stats cog loaded")

    def first_load(self):
        self.parsed.refresh_all_data()
    
    def load_user_data(self):
        try:
            with open('user_data.json', 'r') as file:
                data = json.load(file)
                if isinstance(data, dict):
                    return data
                else:
                    return {}
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def save_user_data(self):
        with open('user_data.json', 'w') as file:
            json.dump(self.user_data, file, indent=4)


    def get_steam_guid(self, ctx, query: str = None):
        user_id = str(ctx.author.id)

        # Scenario One: No additional string
        if query is None:
            print("no steamid provided, looking up Discord ID")
            if user_id in self.user_data:
                return self.user_data[user_id]
            else:
                return None
        # Scenario Two: Steam GUID provided
        else:
            print("steamid provided, it is " + query)
            if query in self.parsed.racers.keys():
                return query
        return None

    @commands.hybrid_command(name="register", description="register steamid")
    async def register(self, ctx, steam_guid: str):
        user_id = str(ctx.author.id)
        self.user_data[user_id] = steam_guid
        self.save_user_data()
        await ctx.send(f'Registered Steam GUID {steam_guid} for Discord user {ctx.author.name}')

    @commands.hybrid_command(name="unregister", description="unregister steamid")
    async def unregister(self, ctx): 
        user_id = str(ctx.author.id) 
        if user_id in self.user_data: 
            del self.user_data[user_id] 
            self.save_user_data() 
            await ctx.send(f'Removed registration for Discord user {ctx.author.name}') 
        else: 
            await ctx.send(f'No registration found for Discord user {ctx.author.name}')

    @commands.hybrid_command(name="showlink", description="show linked steamid for user")
    async def showlink(self, ctx): 
        user_id = str(ctx.author.id) 
        if user_id in self.user_data: 
            steam_guid = self.user_data[user_id] 
            await ctx.send(f'Steam GUID linked to {ctx.author.name} is {steam_guid}') 
        else: 
            await ctx.send(f'No Steam GUID linked to Discord user {ctx.author.name}')


    @commands.hybrid_command(name="testoutput", description="show linked steamid for user")
    @commands.is_owner()
    async def testoutput(self, ctx, guid:str):
        self.parsed.test_output(guid)

    @commands.hybrid_command(name="mostimproved", description="show most improved racers, 3 or 6 months")
    async def mostimproved(self, ctx, time:int=3):
        improvedlist = self.parsed.most_improved(time)
        embed = discord.Embed(title="Most improved racers over " + str(time) + " months", description="Most improved racers", color=discord.Color.blue()) 
        for racer,improvement in improvedlist.items():
            embed.add_field(name=racer.name, value=str(round(improvement, 2)), inline=False)
        await ctx.send(embed=embed)


    @commands.hybrid_command(name="successfulgt3", description="show which gt3 is the most successful")
    @commands.is_owner()
    async def successfulgt3(self, ctx):
        sorted_cars = self.parsed.successfulgt3()
        embed = discord.Embed(title="Average finishing position of each GT3 car", description="GT3 car avg. position ( normalized for racer rating )", color=discord.Color.blue()) 
        for data in sorted_cars: 
            embed.add_field(name=data[0].name, value=str(round(data[1], 2)), inline=False) 
            embed.set_footer(text="GT3 Performance report") 
        await ctx.send(embed=embed)

    @commands.hybrid_command(name="mytracks", description="show each tracks average positions")
    async def mytracks(self, ctx, query: str = None):
        steam_guid = self.get_steam_guid(ctx, query)
        if steam_guid:
            tracks_report = self.parsed.get_racer_tracks_report(steam_guid)
            embed = discord.Embed(title="Track Average Positions", description=f"Average finishing positions for racer `{self.parsed.racers[steam_guid].name}`", color=discord.Color.blue()) 
            for track, avg_position in tracks_report.items(): 
                embed.add_field(name=track, value=f"{avg_position}", inline=False) 
                embed.set_footer(text="Track Performance Report") 
            await ctx.send(embed=embed)

    @commands.hybrid_command(name="worsttracks", description="show each tracks average positions")
    async def worsttracks(self, ctx, query: str = None):
        steam_guid = self.get_steam_guid(ctx, query)
        if steam_guid:
            tracks_report = self.parsed.get_racer_tracks_report(steam_guid, True)
            embed = discord.Embed(title="Worst tracks Average Positions", description=f"Average finishing positions for racer `{self.parsed.racers[steam_guid].name}`", color=discord.Color.blue()) 
            for track, avg_position in tracks_report.items(): 
                embed.add_field(name=track, value=f"{avg_position}", inline=False) 
                embed.set_footer(text="Track Performance Report") 
            await ctx.send(embed=embed)
        else:
            await ctx.send('Invalid query. Provide a valid Steam GUID or /register your steam guid to your Discord name.')

    @commands.hybrid_command(name="randomcombo", description="get my stats")
    async def randomcombo(self, ctx):
        tracklist = self.parsed.contentdata.tracks
        carlist = self.parsed.contentdata.cars
        chosentrack = random.choice(tracklist)
        chosencar = random.choice(carlist)

        await ctx.send("Car: " + chosencar.name + " ( id : " + chosencar.id  + " )"  + "\n" + 
                       "Track : " + chosentrack.highest_priority_name + " ( id: " + chosentrack.highest_priority_id + " ) ")


    @commands.hybrid_command(name="lastraces", description="get my stats for last x races")
    async def lastraces(self, ctx, num:int = 1, query: str = None):
        steam_guid = self.get_steam_guid(ctx, query)
        if num > 5:
            await ctx.send('Invalid query. please select a number smaller than 6')
            return
        if steam_guid:
            racer = self.parsed.racers[steam_guid]
            mostrecentdict = self.parsed.get_summary_last_races(racer, num)
            # Create an embed
            embed = discord.Embed(title="Last Races Summary", description=f"Summary for the last {num} races")

            # Loop through mostrecentdict and add fields to the embed
            for result, (position, rating_change) in mostrecentdict.items():
                result_date = datetime.fromisoformat(result.date)
                race_date = result_date.strftime("%d %B %Y")
                track_name = result.track.parent_track.highest_priority_name
                embed.add_field(name="Race Summary", value=f"Race on {race_date} at {track_name}, finished in position: {position}, and gained/lost rating: {rating_change}", inline=False)
            await ctx.send(embed=embed)
        else:
            await ctx.send('Invalid query. Provide a valid Steam GUID or /register your steam guid to your Discord name.')

    @commands.hybrid_command(name="mystatsmx5", description="get my stats for mx5")
    async def mystatsmx5(self, ctx, query: str = None):
        steam_guid = self.get_steam_guid(ctx, query)
        if steam_guid:
            racer = self.parsed.racers[steam_guid]
            user = ctx.author

            embed = discord.Embed(title="MX5 Racer Stats", description="User Stats for " + racer.name, color=discord.Color.blue())
            embed.set_thumbnail(url=user.display_avatar.url)
            embed.add_field(name="🏁 **Total MX5 races**", value=racer.get_num_races("mx5"), inline=True)
            embed.add_field(name="🥈 **MX5 ELO**", value=f"{racer.mx5rating} (Rank: {self.parsed.get_elo_rank(racer, "mx5") + 1}/{len(self.parsed.elorankingsmx5)})", inline=True)
            embed.add_field(name="🏆 **Total MX5 wins**", value=f"{racer.mx5wins} (Rank: {self.parsed.get_wins_rank(racer, "mx5") + 1}/{len(self.parsed.wins_rankingsmx5)})"if racer.mx5wins is not None else "No data", inline=True)
            embed.add_field(name="🥉 **Total MX5 podiums**", value=f"{racer.mx5podiums} (Rank: {self.parsed.get_podiums_rank(racer, "mx5") + 1}/{len(self.parsed.podiums_rankingsmx5)})"if racer.mx5podiums is not None else "No data", inline=True)
            embed.add_field(name="⚠️ **Average MX5 incidents/race**", value=f"{racer.averageincidentsmx5} (Rank: {self.parsed.get_safety_rank(racer, "mx5") + 1}/{len(self.parsed.safety_rankingsmx5)})"if racer.averageincidentsmx5 is not None else "No data", inline=True)
            embed.add_field(name="🛣️ **Most successful MX5 track**", value=racer.mostsuccesfultrackmx5.name, inline=True)
            embed.add_field(name="🔄 **Total MX5 race laps**", value=racer.mx5laps, inline=True)
            embed.add_field(name="💥 **Most collided with other MX5 racer**", value=racer.mosthitotherdrivermx5.name, inline=True)
            embed.add_field(name="⏱️ **MX5 Lap Time Consistency**", value=f"{racer.laptimeconsistencymx5:.2f}% (Rank: {self.parsed.get_laptime_consistency_rank(racer, "mx5") + 1}/{len(self.parsed.laptimeconsistencyrankingsmx5)})" if racer.laptimeconsistencygt3 is not None else "No data", inline=True)
            embed.add_field(name="🚗 **Average Pace % Compared to Top Lap Times in MX5**", value=f"{racer.pace_percentage_mx5:.2f}% (Rank: {self.parsed.get_pace_mx5_rank(racer) + 1}/{len(self.parsed.pacerankingsmx5)})" if racer.pace_percentage_mx5 is not None else "No data", inline=True)

            await ctx.send(embed=embed)
        else:
            await ctx.send('Invalid query. Provide a valid Steam GUID or /register your steam guid to your Discord name.')

    @commands.hybrid_command(name="mystatsgt3", description="get my stats for gt3")
    async def mystatsgt3(self, ctx, query: str = None):
        steam_guid = self.get_steam_guid(ctx, query)
        if steam_guid:
            racer = self.parsed.racers[steam_guid]
            user = ctx.author

            embed = discord.Embed(title="GT3 Racer Stats", description="User Stats for " + racer.name, color=discord.Color.blue())
            embed.set_thumbnail(url=user.display_avatar.url)
            embed.add_field(name="🏁 **Total GT3 races**", value=racer.get_num_races("gt3"), inline=True)
            embed.add_field(name="🥈 **GT3 ELO**", value=f"{racer.gt3rating} (Rank: {self.parsed.get_elo_rank(racer, "gt3") + 1}/{len(self.parsed.elorankingsgt3)})", inline=True)
            embed.add_field(name="🏆 **Total GT3 wins**", value=f"{racer.gt3wins} (Rank: {self.parsed.get_wins_rank(racer, "gt3") + 1}/{len(self.parsed.wins_rankingsgt3)})"if racer.gt3wins is not None else "No data", inline=True)
            embed.add_field(name="🥉 **Total GT3 podiums**", value=f"{racer.gt3podiums} (Rank: {self.parsed.get_podiums_rank(racer, "gt3") + 1}/{len(self.parsed.podiums_rankingsgt3)})"if racer.gt3podiums is not None else "No data", inline=True)
            embed.add_field(name="⚠️ **Average GT3 incidents/race**", value=f"{racer.averageincidentsgt3} (Rank: {self.parsed.get_safety_rank(racer, "gt3") + 1}/{len(self.parsed.safety_rankingsgt3)})"if racer.averageincidentsgt3 is not None else "No data", inline=True)
            embed.add_field(name="🛣️ **Most successful GT3 track**", value=racer.mostsuccesfultrackgt3.name, inline=True)
            embed.add_field(name="🔄 **Total GT3 race laps**", value=racer.gt3laps, inline=True)
            embed.add_field(name="💥 **Most collided with other GT3 racer**", value=racer.mosthitotherdrivergt3.name, inline=True)
            embed.add_field(name="⏱️ **GT3 Lap Time Consistency**", value=f"{racer.laptimeconsistencygt3:.2f}% (Rank: {self.parsed.get_laptime_consistency_rank(racer, "gt3") + 1}/{len(self.parsed.laptimeconsistencyrankingsgt3)})" if racer.laptimeconsistencygt3 is not None else "No data", inline=True)
            embed.add_field(name="🚗 **Average Pace % Compared to Top Lap Times in GT3**", value=f"{racer.pace_percentage_gt3:.2f}% (Rank: {self.parsed.get_pace_gt3_rank(racer) + 1}/{len(self.parsed.pacerankingsgt3)})" if racer.pace_percentage_gt3 is not None else "No data", inline=True)

            await ctx.send(embed=embed)
        else:
            await ctx.send('Invalid query. Provide a valid Steam GUID or /register your steam guid to your Discord name.')
    
    @commands.hybrid_command(name="mystats", description="get my stats")
    async def mystats(self, ctx, query: str = None):
        steam_guid = self.get_steam_guid(ctx, query)
        if steam_guid:
            racer = self.parsed.racers[steam_guid]
            user = ctx.author

            embed = discord.Embed(title="Racer Stats", description="User Stats for " + racer.name, color=discord.Color.blue())
            embed.set_thumbnail(url=user.display_avatar.url)
            embed.add_field(name="🏁 **Total races**", value=racer.get_num_races(), inline=True)
            embed.add_field(name="🥈 **ELO**", value=f"{racer.rating} (Rank: {self.parsed.get_elo_rank(racer) + 1}/{len(self.parsed.elorankings)})", inline=True)
            embed.add_field(name="🏆 **Total wins**", value=f"{racer.wins} (Rank: {self.parsed.get_wins_rank(racer) + 1}/{len(self.parsed.wins_rankings)})", inline=True)
            embed.add_field(name="🥉 **Total podiums**", value=f"{racer.podiums} (Rank: {self.parsed.get_podiums_rank(racer) + 1}/{len(self.parsed.podiums_rankings)})", inline=True)
            embed.add_field(name="⚠️ **Average incidents/race**", value=f"{racer.averageincidents} (Rank: {self.parsed.get_safety_rank(racer) + 1}/{len(self.parsed.safety_rankings)})", inline=True)
            embed.add_field(name="🛣️ **Most successful track**", value=racer.mostsuccesfultrack.name, inline=True)
            embed.add_field(name="🔄 **Total race laps**", value=racer.totallaps, inline=True)
            embed.add_field(name="💥 **Most collided with other racer**", value=racer.mosthitotherdriver.name, inline=True)
            embed.add_field(name="⏱️ **Lap Time Consistency**", value=f"{racer.laptimeconsistency:.2f}% (Rank: {self.parsed.get_laptime_consistency_rank(racer) + 1}/{len(self.parsed.laptimeconsistencyrankings)})" if racer.laptimeconsistency is not None else "No data", inline=True)
            embed.add_field(name="🏎️ **Average Pace % Compared to Top Lap Times in MX-5**", value=f"{racer.pace_percentage_mx5:.2f}% (Rank: {self.parsed.get_pace_mx5_rank(racer) + 1}/{len(self.parsed.pacerankingsmx5)})" if racer.pace_percentage_mx5 is not None else "No data", inline=True)
            embed.add_field(name="🚗 **Average Pace % Compared to Top Lap Times in GT3**", value=f"{racer.pace_percentage_gt3:.2f}% (Rank: {self.parsed.get_pace_gt3_rank(racer) + 1}/{len(self.parsed.pacerankingsgt3)})" if racer.pace_percentage_gt3 is not None else "No data", inline=True)

            await ctx.send(embed=embed)
        else:
            await ctx.send('Invalid query. Provide a valid Steam GUID or /register your steam guid to your Discord name.')

    @commands.hybrid_command(name="livetiming", description="check for latest results")
    async def livetiming(self, ctx, server, raw:str=None):
        if server == "" or server is None:
            await ctx.send("Please provide a server name from one of the following: mx5eu, mx5na, mx5napro, gt3eu, gt3na, worldtour, timetrial")
            return
        servertouse = None
        if server == "mx5eu":
            servertouse = self.mx5euserver
        elif server == "mx5na":
            servertouse = self.mx5naserver
        elif server == "mx5napro":
            servertouse = self.mx5naproserver
        elif server == "gt3eu":
            servertouse = self.gt3euserver
        elif server == "gt3na":
            servertouse = self.gt3naserver
        elif server == "worldtour":
            servertouse = self.worldtourserver
        elif server == "timetrial":
            servertouse = self.timetrialserver
        else:
            await ctx.send("Please provide a server name from one of the following: mx5eu, mx5na, mx5napro, gt3eu, gt3na, worldtour, timetrial")
            return
        
        user_agent = "https://github.com/JanuarySnow/RRR-Bot"
        headers  = {"User-Agent":user_agent}
        try:
        # This will prevent your bot from stopping everything when doing a web request - see: https://discordpy.readthedocs.io/en/stable/faq.html#how-do-i-make-a-web-request
            print("fetching live timings list from " + server)
            livetimingsurl = servertouse + "/api/live-timings/leaderboard.json"
            async with aiohttp.ClientSession() as session:
                async with session.get(
                        livetimingsurl, 
                        headers=headers) as request:
                    if request.status == 200:
                        data = await request.json(content_type='application/json')
                        await self.print_live_timings(ctx, data, raw!="raw")
                    else:
                        print("error fetching from " + server)
        except Exception as e:
            import traceback
            traceback.print_exception(e)
            1/0



    async def print_live_timings(self, ctx, data, pretty=False):

        # Prepare a list to store driver data tuples
        driver_data_list = []

        # Extract the DisconnectedDrivers list
        drivers = data.get('DisconnectedDrivers', [])

        for driver in drivers:
            car_info = driver.get('CarInfo', {})
            driver_name = car_info.get('DriverName', 'Unknown')
            num_laps = driver.get('TotalNumLaps', 0)

            # Access the Cars dictionary to get the best lap times for each car
            cars = driver.get('Cars', {})

            for car_model, car_data in cars.items():
                best_lap_ns = car_data.get('BestLap', None)
                if best_lap_ns and best_lap_ns > 0:
                    # Convert nanoseconds to minutes:seconds:milliseconds
                    total_seconds = best_lap_ns / 1e9
                    minutes = int(total_seconds // 60)
                    seconds = int(total_seconds % 60)
                    milliseconds = int((total_seconds * 1000) % 1000)
                    best_lap_formatted = f"{minutes:02d}:{seconds:02d}.{milliseconds:03d}"
                else:
                    # Assign a very high value for sorting purposes
                    best_lap_ns = float('inf')
                    best_lap_formatted = "N/A"
                num_laps = car_data.get('NumLaps')

                car_name = car_data.get('CarName', car_model)
                # Append a tuple to the list: (best lap time in ns for sorting, formatted time, driver name, car name, number of laps)
                driver_data_list.append((best_lap_ns, best_lap_formatted, driver_name, car_name, num_laps))

        # Sort the list by best lap time (fastest to slowest)
        driver_data_list.sort(key=lambda x: x[0])

        output_lines = []
        for idx, (lap_time_ns, lap_time_str, driver_name, car_name, num_laps) in enumerate(driver_data_list, start=1):
            line = f"{driver_name},{car_name},{lap_time_str},{num_laps}"
            output_lines.append(line)

        # Discord's message character limit
        max_message_length = 2000

        # Combine lines into messages within the character limit
        messages = []
        current_message = ''
        for line in output_lines:
            if len(current_message) + len(line) + 1 > max_message_length:
                messages.append(current_message)
                current_message = line
            else:
                current_message = f"{current_message}\n{line}" if current_message else line
        if current_message:
            messages.append(current_message)

        # Prepare the embed pages
        embed_pages = []
        total_pages = len(messages)

        for page_number, message_content in enumerate(messages):
            embed = discord.Embed(
                title=f"Race Timing Results (Page {page_number + 1}/{total_pages})",
                color=0x00ff00,
                description=message_content
            )
            embed_pages.append(embed)

        current_page_number = 0
        message = await ctx.send(embed=embed_pages[current_page_number])

        # Add reactions if there's more than one page
        if total_pages > 1:
            await message.add_reaction("◀️")
            await message.add_reaction("▶️")

            def check(reaction, user):
                return (
                    user == ctx.author and
                    str(reaction.emoji) in ["◀️", "▶️"] and
                    reaction.message.id == message.id
                )

            while True:
                try:
                    reaction, user = await self.bot.wait_for(
                        "reaction_add", timeout=60.0, check=check
                    )

                    if str(reaction.emoji) == "▶️":
                        current_page_number = (current_page_number + 1) % total_pages
                        await message.edit(embed=embed_pages[current_page_number])
                        await message.remove_reaction(reaction, user)

                    elif str(reaction.emoji) == "◀️":
                        current_page_number = (current_page_number - 1) % total_pages
                        await message.edit(embed=embed_pages[current_page_number])
                        await message.remove_reaction(reaction, user)

                except asyncio.TimeoutError:
                    await message.clear_reactions()
                    break


    @commands.hybrid_command(name="forcerefreshalldata", description="forcescanresults")
    @commands.is_owner()
    async def forcerefreshalldata(self, ctx):
        print("force refreshing all data")
        await ctx.defer()
        self.parsed.refresh_all_data()
        await ctx.send("Finished processing results")


    @commands.hybrid_command(name="allwinners", description="allwinners")
    async def allwinners(self, ctx):
        retstring = self.parsed.getallwinners()
        for i in range(math.ceil(len(retstring) / 4096)):
            embed = discord.Embed(title='Winners:')
            embed.description = (retstring[(4096*i):(4096*(i+1))])
            await ctx.send(embed=embed)


    async def check_one_server_for_results(self, server, query):
        user_agent = "https://github.com/JanuarySnow/RRR-Bot"
        headers = {"User-Agent": user_agent}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(query, headers=headers) as request:
                    if request.status == 200:
                        data = await request.json(content_type='application/json')
                        data["results"].sort(key=lambda elem: datetime.fromisoformat(elem["date"]), reverse=True)
                        print(f"Results size from results list = {len(data['results'])}")

                        for result in data["results"]:
                            download_url = result["results_json_url"]
                            filename = os.path.basename(download_url)
                            directory = self.servertodirectory[server]
                            filepath = os.path.join("results", directory, filename)

                            # Only add to the download queue if the file doesn't already exist
                            if filename in self.blacklist:
                                print("skipping + " + filename + " due to blacklist")
                                continue
                            if not os.path.exists(filepath):
                                print("adding to download queue " + download_url)
                                self.download_queue.append((server, download_url))
                            else:
                                print(f"File {filepath} already exists, skipping download")
                        return data
                    else:
                        print(f"Error fetching from {server}")
                        return None
        except Exception as e:
            import traceback
            traceback.print_exception(e)
            return None
            1/0

    async def download_files_from_queue(self):
        user_agent = "https://github.com/JanuarySnow/RRR-Bot"
        headers = {"User-Agent": user_agent}

        while self.download_queue:
            print("size of download queue = " + str(len(self.download_queue)))
            server, download_url = self.download_queue.pop(0)
            download_url = server + download_url
            print("downloading from " + download_url)
            directory = self.servertodirectory[server]
            filename = os.path.basename(download_url)
            filepath = os.path.join("results", directory, filename)

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(download_url, headers=headers) as request:
                        if request.status == 200:
                            data = await request.json(content_type='application/json')

                            # Ensure the directory exists
                            os.makedirs(os.path.join("results", directory), exist_ok=True)

                            # Save the JSON data to a file
                            with open(filepath, 'w') as json_file:
                                json.dump(data, json_file, indent=4)
                                print(f"JSON data saved to {filepath}")
                                self.justadded.append(filepath)
                        else:
                            print(f"Error fetching from {download_url}")
            except Exception as e:
                import traceback
                traceback.print_exception(e)
            if len(self.download_queue) >= 1:
                print("waiting for next download")
                await asyncio.sleep(20)
            else:
                print("no more files to download, exiting")
                break

    @commands.hybrid_command(name='euvsna', description="compare EU vs NA")
    async def euvsna(self, ctx):
        euracers = self.parsed.get_eu_racers()
        naracers = self.parsed.get_na_racers()
        print("size of euracers = " + str(len(euracers)))
        print("size of naracers = " + str(len(naracers)))

        average_eu_elo = sum(racer.rating for racer in euracers) / len(euracers)
        average_na_elo = sum(racer.rating for racer in naracers) / len(naracers)
        print("average eu elo = " + str(average_eu_elo))
        print("average na elo = " + str(average_na_elo))

        average_eu_clean = sum(racer.averageincidents for racer in euracers) / len(euracers)
        average_na_clean = sum(racer.averageincidents for racer in naracers) / len(naracers)
        print("average eu clean = " + str(average_eu_clean))
        print("average na clean = " + str(average_na_clean))

        average_pace_percentage_gt3_eu = sum(racer.pace_percentage_gt3 for racer in euracers if racer.pace_percentage_gt3 is not None) / len(euracers)
        average_pace_percentage_gt3_na = sum(racer.pace_percentage_gt3 for racer in naracers if racer.pace_percentage_gt3 is not None) / len(naracers)
        average_pace_percentage_mx5_eu = sum(racer.pace_percentage_mx5 for racer in euracers if racer.pace_percentage_mx5 is not None) / len(euracers)
        average_pace_percentage_mx5_na = sum(racer.pace_percentage_mx5 for racer in naracers if racer.pace_percentage_mx5 is not None) / len(naracers)
        print("average pace percentage gt3 eu = " + str(average_pace_percentage_gt3_eu))
        print("average pace percentage gt3 na = " + str(average_pace_percentage_gt3_na))
        print("average pace percentage mx5 eu = " + str(average_pace_percentage_mx5_eu))
        print("average pace percentage mx5 na = " + str(average_pace_percentage_mx5_na))


        embed = discord.Embed(
                title="EU VS NA",
                color=discord.Color.blue()
            )

        
        embed.add_field(name="🏆 Average EU racer ELO 🏆", value=(f"🔴 {round(average_eu_elo, 2)}" if average_na_elo > average_eu_elo else f"🟢 {round(average_eu_elo, 2)}") or "\u200b", inline=False)
        embed.add_field(name="🏆 Average NA racer ELO 🏆", value=(f"🟢 {round(average_na_elo, 2)}" if average_na_elo > average_eu_elo else f"🔴 {round(average_na_elo, 2)}") or "\u200b", inline=False)

        embed.add_field(name="🚗 Average EU racer Incidents per race 🚗", value=(f"🔴 {round(average_eu_clean, 2)}" if average_na_clean < average_eu_clean else f"🟢 {round(average_eu_clean, 2)}") or "\u200b", inline=False)
        embed.add_field(name="🚗 Average NA racer Incidents per race 🚗", value=(f"🟢 {round(average_na_clean, 2)}" if average_na_clean < average_eu_clean else f"🔴 {round(average_na_clean, 2)}") or "\u200b", inline=False)

        embed.add_field(name="⏱️ Average EU Racer pace percentage GT3 ⏱️", value=(f"🔴 {round(average_pace_percentage_gt3_eu, 2)}" if average_pace_percentage_gt3_na > average_pace_percentage_gt3_eu else f"🟢 {round(average_pace_percentage_gt3_eu, 2)}") or "\u200b", inline=False)
        embed.add_field(name="⏱️ Average NA Racer pace percentage GT3 ⏱️", value=(f"🟢 {round(average_pace_percentage_gt3_na, 2)}" if average_pace_percentage_gt3_na > average_pace_percentage_gt3_eu else f"🔴 {round(average_pace_percentage_gt3_na, 2)}") or "\u200b", inline=False)

        embed.add_field(name="⏱️ Average EU Racer pace percentage MX5 ⏱️", value=(f"🔴 {round(average_pace_percentage_mx5_eu, 2)}" if average_pace_percentage_mx5_na > average_pace_percentage_mx5_eu else f"🟢 {round(average_pace_percentage_mx5_eu, 2)}") or "\u200b", inline=False)
        embed.add_field(name="⏱️ Average NA Racer pace percentage MX5 ⏱️", value=(f"🟢 {round(average_pace_percentage_mx5_na, 2)}" if average_pace_percentage_mx5_na > average_pace_percentage_mx5_eu else f"🔴 {round(average_pace_percentage_mx5_na, 2)}") or "\u200b", inline=False)
        await ctx.send(embed=embed)


    @commands.hybrid_command(name='forcetimedtask', description="force timed task")
    async def forcetimedtask(self, ctx):
        await self.fetch_results_list()

    @tasks.loop(seconds=6000.0)
    async def fetch_results_list(self):
        global ON_READY_FIRST_TIME_SCAN
        if ON_READY_FIRST_TIME_SCAN:
            ON_READY_FIRST_TIME_SCAN = False
            return
        channel = discord.utils.get(self.bot.get_all_channels(), name='bot-testing')
        print("starting fetch")
        async with channel.typing():
            for server in self.servers:
                query = server + "/api/results/list.json?q=Type:\"RACE\"&sort=date&page=0"
                print("query for server = " + query)
                await self.check_one_server_for_results(server,query)
            await self.download_files_from_queue()
        if len(self.justadded) == 0:
            pass
        else:
            for elem in self.justadded:
                await channel.send("Added " + elem)
            async with channel.typing():
                for elem in self.justadded:
                    await self.parsed.add_one_result(elem, os.path.basename(elem) )
                    await asyncio.sleep(3)
            await channel.send("All results have been processed and data has been refreshed")
            self.justadded.clear()
        

    @commands.hybrid_command(name='carlookup', description="get car info")
    async def carlookup(self, ctx, *, input_string: str, guid:str = None):
        
        # Try to match the input string as a track ID
        matched_car = None
        for elem in self.parsed.contentdata.cars:
            base_id = elem.id
            if input_string == base_id:
                matched_car = elem
                break
            carname = elem.name
            if input_string == carname:
                matched_car = elem

        # If no direct matches are found, fall back to fuzzy matching
        if not matched_car:
            matches = self.parsed.find_and_list_cars(input_string)
            if not matches:
                await ctx.send('No matching cars found.')
                return

            # Check if there's only one match with 100% confidence
            if len(matches) == 1:
                match = matches[0]
                matched_car = self.parsed.contentdata.get_car(match["id"])
                if matched_car:
                    embed = self.create_car_embed(matched_car, guid)
                    await ctx.send(embed=embed)
                else:
                    await ctx.send('No car found')
                    return

            # Create buttons for the top 3 matches
            view = View()

            async def button_callback(interaction: discord.Interaction, match):
                matched_car = self.parsed.contentdata.get_car(match["id"])

                if matched_car:
                        embed = self.create_car_embed(matched_car, guid)
                        await interaction.response.send_message(embed=embed)
                else:
                    await interaction.response.send_message('No matching car found.')

            # Add buttons to the view with their respective callbacks
            for match in matches[:3]:
                button = Button(label=match["input_match"], style=discord.ButtonStyle.primary)
                button.callback = lambda interaction, m=match: button_callback(interaction, m)
                view.add_item(button)

            await ctx.send('Select what car you want to see:', view=view)
        else:
            embed = self.create_car_embed(matched_car, guid)
            await ctx.send(embed=embed)

    @commands.hybrid_command(name='myrecords', description="See if I hold any track records")
    async def myrecords(self, ctx, guid: str = None):
        steam_guid = self.get_steam_guid(ctx, guid)
        if steam_guid:
            racer = self.parsed.racers[steam_guid]
            fastests = self.parsed.get_fastest_laps_for_racer(racer)
            if len(fastests) > 0:
                embed = discord.Embed(
                    title=f"{racer.name}'s Track Records",
                    description="Here are the fastest laps recorded at each track:",
                    color=discord.Color.purple()
                )

                for track_variant, records in fastests.items():
                    record_texts = []
                    for car_class, record in records.items():
                        if not record["time"] or not record["car"]:
                            continue

                        # Convert laptime from milliseconds to a readable format
                        total_seconds = float(record["time"]) / 1000.0
                        minutes = int(total_seconds // 60)
                        seconds = total_seconds % 60

                        record_text = f"**{car_class}**: ⏱️ {minutes}:{seconds:06.3f} - Set with {record['car'].name}"
                        record_texts.append(record_text)

                    if record_texts:
                        embed.add_field(
                            name=f"{track_variant.name}",
                            value="\n".join(record_texts),
                            inline=False
                        )

                embed.set_footer(text="Track Records Report")
                await ctx.send(embed=embed)
            else:
                await ctx.send(f'No track records found for racer `{racer.name}`.')
        else:
            await ctx.send('Invalid query. Provide a valid Steam GUID or /register your steam GUID to your Discord name.')


    @commands.hybrid_command(name='top10mx5pace', description="get MX5 pace top 10 rankings")
    async def top10mx5pace(self, ctx):
        rankings = self.parsed.pacerankingsmx5[:10]  # Get top 10 rankings
        embed = discord.Embed(
            title="Top 10 MX5 Pace Rankings",
            color=discord.Color.blue()
        )

        for index, racer in enumerate(rankings):
            embed.add_field(
                name=f"{index + 1}. {racer.name}",
                value=f"**Pace Percentage**: {racer.pace_percentage_mx5}%",
                inline=False
            )

        await ctx.send(embed=embed)


    @commands.hybrid_command(name='top10gt3pace', description="get GT3 pace top 10 rankings")
    async def top10gt3pace(self, ctx):
        rankings = self.parsed.pacerankingsgt3[:10]  # Get top 10 rankings
        embed = discord.Embed(
            title="Top 10 GT3 Pace Rankings",
            color=discord.Color.green()
        )

        for index, racer in enumerate(rankings):
            embed.add_field(
                name=f"{index + 1}. {racer.name}",
                value=f"**Pace Percentage**: {racer.pace_percentage_gt3}%",
                inline=False
            )

        await ctx.send(embed=embed)



    @commands.hybrid_command(name='tracklookup', description="get track info")
    async def tracklookup(self, ctx, *, input_string: str, guid:str = None):
        
        # Try to match the input string as a track ID
        matched_track = None
        for elem in self.parsed.contentdata.tracks:
            for variant in elem.variants:
                base_id = elem.id

                if input_string == base_id:
                    matched_track = variant.parent_track
                    break
                if input_string + ";" + input_string == base_id:
                    matched_track = variant.parent_track
                    break
                variantname = variant.name
                if input_string == variantname:
                    matched_track = variant.parent_track

        # If no direct matches are found, fall back to fuzzy matching
        if not matched_track:
            matches = self.parsed.find_and_list_variants(input_string)
            if not matches:
                await ctx.send('No matching track variants found.')
                return

            # Check if there's only one match with 100% confidence
            if len(matches) == 1:
                match = matches[0]
                matched_track = self.parsed.contentdata.get_base_track(match["id"])
                if matched_track:
                    highest_priority_variant = None
                    for variant in matched_track.variants:
                        if variant.name == matched_track.highest_priority_name or variant.id == matched_track.highest_priority_id:
                            highest_priority_variant = variant
                            break  
                    if highest_priority_variant:
                        embed = self.create_variant_embed(highest_priority_variant, guid)
                        await ctx.send(embed=embed)
                    else:
                        await ctx.send('No highest priority variant found for the matching track.')
                else:
                    await ctx.send('No matching track variants found.')
                return

            # Create buttons for the top 3 matches
            view = View()

            async def button_callback(interaction: discord.Interaction, match):
                matched_track = self.parsed.contentdata.get_base_track(match["id"])

                if matched_track:
                    highest_priority_variant = matched_track.highest_priority_name
                    for variant in matched_track.variants:
                        if variant.name == matched_track.highest_priority_name or variant.variant_id == matched_track.highest_priority_id:
                            highest_priority_variant = variant
                            break

                    if highest_priority_variant:
                        embed = self.create_variant_embed(highest_priority_variant, guid)
                        await interaction.response.send_message(embed=embed)
                    else:
                        await interaction.response.send_message('No highest priority variant found for the matching track.')
                else:
                    await interaction.response.send_message('No matching track variants found.')

            # Add buttons to the view with their respective callbacks
            for match in matches[:3]:
                button = Button(label=match["input_match"], style=discord.ButtonStyle.primary)
                button.callback = lambda interaction, m=match: button_callback(interaction, m)
                view.add_item(button)

            await ctx.send('Select what track you want to see:', view=view)
        else:
            # Select the highest priority variant if direct match is found
            highest_priority_variant = None
            for variant in matched_track.variants:
                if variant.name == matched_track.highest_priority_name or variant.id == matched_track.highest_priority_id:
                    highest_priority_variant = variant
                    break

            if highest_priority_variant:
                embed = self.create_variant_embed(highest_priority_variant, guid)
                await ctx.send(embed=embed)
            else:
                await ctx.send('No highest priority variant found for the matching track.')

    def create_car_embed(self, car, guid:str=None):
        embed = discord.Embed(
            title=f"Car: {car.name}",
            description=car.description or "N/A",
            color=discord.Color.green()
        )
        embed.add_field(name="🏷️ Tags", value=", ".join(car.tags) if car.tags else "N/A", inline=True)
        embed.add_field(name="🚗 Brand", value=car.brand or "N/A", inline=True)
        embed.add_field(name="🏎️ Class", value=car.carclass or "N/A", inline=True)
        embed.add_field(name="🇺🇳 Country", value=car.country or "N/A", inline=True)
        #embed.add_field(name="📏 Torque Curve", value=car.torquecurve or "N/A", inline=True)
        #embed.add_field(name="📏 Power Curve", value=car.powercurve or "N/A", inline=True)
        #embed.add_field(name="⚙️ Specs", value=car.specs or "N/A", inline=True)
        embed.add_field(name="🖋️ Author", value=car.author or "N/A", inline=True)
        embed.add_field(name="🔢 Version", value=car.version or "N/A", inline=True)
        embed.add_field(name="🌐 URL", value=car.url or "N/A", inline=True)
        embed.add_field(name="📅 Year", value=str(car.year) if car.year else "N/A", inline=True)

        embed.set_footer(text="Car Information Report")
        return embed



    def create_variant_embed(self, variant, guid:str=None):
        embed = discord.Embed(
            title=f"Track Variant: {variant.name}",
            description=variant.description,
            color=discord.Color.blue()
        )
        embed.add_field(name="🏷️ Tags", value=", ".join(variant.tags) if variant.tags else "N/A", inline=True)
        embed.add_field(name="🌍 GeoTags", value=", ".join(variant.geotags) if variant.geotags else "N/A", inline=True)
        embed.add_field(name="🇺🇳 Country", value=variant.country or "N/A", inline=True)
        embed.add_field(name="🏙️ City", value=variant.city or "N/A", inline=True)
        embed.add_field(name="📏 Length", value=variant.length or "N/A", inline=True)
        embed.add_field(name="📏 Width", value=variant.width or "N/A", inline=True)
        embed.add_field(name="🚗 Pitboxes", value=variant.pitboxes or "N/A", inline=True)
        embed.add_field(name="🏃 Run", value=variant.run or "N/A", inline=True)
        embed.add_field(name="🖋️ Author", value=variant.author or "N/A", inline=True)
        embed.add_field(name="🔢 Version", value=variant.version or "N/A", inline=True)
        embed.add_field(name="🌐 URL", value=variant.url or "N/A", inline=True)
        embed.add_field(name="📅 Year", value=str(variant.year) if variant.year else "N/A", inline=True)

        for elem in variant.parent_track.variants:
            fastest_mx5_lap = elem.get_fastest_lap_in_mx5(guid)
            fastest_gt3_lap = elem.get_fastest_lap_in_gt3(guid)

            if fastest_mx5_lap and fastest_mx5_lap.racerguid:
                total_seconds = float(fastest_mx5_lap.time / 1000.0)
                minutes = int(total_seconds // 60)
                seconds = total_seconds % 60
                if guid:
                    embed.add_field(
                    name=f"{self.parsed.racers[fastest_mx5_lap.racerguid].name}'s fastest ever MX5 lap at: {elem.name}",
                    value=f"⏱️ {minutes}:{seconds:06.3f}",
                    inline=False
                    )
                else:
                    embed.add_field(
                    name=f"fastest ever MX5 lap at: {elem.name} by : {self.parsed.racers[fastest_mx5_lap.racerguid].name}",
                    value=f"⏱️ {minutes}:{seconds:06.3f}",
                    inline=False
                    )
            if fastest_gt3_lap and fastest_gt3_lap.racerguid:
                total_seconds = float(fastest_gt3_lap.time / 1000.0)
                minutes = int(total_seconds // 60)
                seconds = total_seconds % 60
                if guid:
                    embed.add_field(
                        name=f"{self.parsed.racers[fastest_gt3_lap.racerguid].name}'s fastest ever GT3 lap at: {elem.name}",
                        value=f"⏱️ {minutes}:{seconds:06.3f}",
                        inline=False
                    )
                else:
                    embed.add_field(
                        name=f"fastest ever GT3 lap at: {elem.name} by {self.parsed.racers[fastest_gt3_lap.racerguid].name}",
                        value=f"⏱️ {minutes}:{seconds:06.3f}",
                        inline=False
                    )

        return embed
    
    @commands.hybrid_command(name="mytrackrecord", description="get users fastest lap at track")
    async def mytrackrecord(self, ctx: commands.Context, input_string: str, guid: str = None) -> None:
        steam_guid = self.get_steam_guid(ctx, guid)
        if steam_guid:
            racer = self.parsed.racers[steam_guid]
            await self.tracklookup(ctx, input_string=input_string, guid=steam_guid)
        else:
            await ctx.send('Invalid query. Provide a valid Steam GUID or /register your steam guid to your Discord name.')

    @commands.hybrid_command(name="myskillprogression", description="show improvement over time")
    async def myskillprogression(self, ctx: Context, guid:str=None) -> None:
        steam_guid = self.get_steam_guid(ctx, guid)
        if steam_guid:
            racer = self.parsed.racers[steam_guid]
            if not racer.progression_plot:
                await ctx.send("Racer hasnt done enough races yet")
                return
            self.parsed.create_skill_progression_chart(racer.paceplotaverage,racer.positionaverage)
            file = discord.File("progression_chart.png", filename="progression_chart.png") 
            embed = discord.Embed( title="Racer Progression", description=f"Progression Over Time for {racer.name}", color=discord.Color.green() ) 
            embed.set_image(url="attachment://progression_chart.png") 
            await ctx.send(embed=embed, file=file)
        else:
            await ctx.send('Invalid query. Provide a valid Steam GUID or /register your steam guid to your Discord name.')
    
    @commands.hybrid_command(name="myprogression", description="show improvement over time")
    async def myprogression(self, ctx: Context, guid:str=None) -> None:
        steam_guid = self.get_steam_guid(ctx, guid)
        if steam_guid:
            racer = self.parsed.racers[steam_guid]
            if not racer.progression_plot:
                await ctx.send("Racer hasnt done enough races yet")
                return
            self.parsed.create_progression_chart(racer.progression_plot)
            file = discord.File("progression_chart.png", filename="progression_chart.png") 
            embed = discord.Embed( title="Racer Progression", description=f"Progression Over Time for {racer.name}", color=discord.Color.green() ) 
            embed.set_image(url="attachment://progression_chart.png") 
            await ctx.send(embed=embed, file=file)
        else:
            await ctx.send('Invalid query. Provide a valid Steam GUID or /register your steam guid to your Discord name.')

    @commands.hybrid_command(name="gt3rankings", description="gt3 rankings")
    async def gt3rankings(self, ctx: Context) -> None:
        stats = self.parsed.get_overall_stats()
        embed = discord.Embed(
            title="GT3 Rankings",
            color=discord.Color.blue()
        )

        def format_rankings(rankings, value_formatter):
                        formatted_lines = [value_formatter(entry) for entry in rankings]
                        return "\n".join(formatted_lines)

        def elo_formatter(entry):
            return f"{entry['rank']}. {entry['name']} - **Rating**: {entry['rating']}"

        def safety_formatter(entry):
            return f"{entry['rank']}. {entry['name']} - **Average Incidents**: {entry['averageincidents']:.2f}"

        def consistency_formatter(entry):
            return f"{entry['rank']}. {entry['name']} - **Consistency**: {entry['laptimeconsistency']:.2f}%"

        # Add top 10 ELO rankings
        elo_rankings = format_rankings(stats['gt3elos'], elo_formatter)
        embed.add_field(name="🏆 Top 10 GT3 ELO Rankings 🏆", value=elo_rankings or "\u200b", inline=False)


        await ctx.send(embed=embed)

    @commands.hybrid_command(name="mx5rankings", description="mx5 rankings")
    async def mx5rankings(self, ctx: Context) -> None:
        stats = self.parsed.get_overall_stats()
        embed = discord.Embed(
            title="MX5 Rankings",
            color=discord.Color.blue()
        )

        def format_rankings(rankings, value_formatter):
                        formatted_lines = [value_formatter(entry) for entry in rankings]
                        return "\n".join(formatted_lines)

        def elo_formatter(entry):
            return f"{entry['rank']}. {entry['name']} - **Rating**: {entry['rating']}"

        def safety_formatter(entry):
            return f"{entry['rank']}. {entry['name']} - **Average Incidents**: {entry['averageincidents']:.2f}"

        def consistency_formatter(entry):
            return f"{entry['rank']}. {entry['name']} - **Consistency**: {entry['laptimeconsistency']:.2f}%"

        # Add top 10 ELO rankings
        elo_rankings = format_rankings(stats['mx5elos'], elo_formatter)
        embed.add_field(name="🏆 Top 10 MX5 ELO Rankings 🏆", value=elo_rankings or "\u200b", inline=False)


        await ctx.send(embed=embed)

    
    @commands.hybrid_command(name="myprogressiongt3", description="show improvement over time in GT3")
    async def myprogressiongt3(self, ctx: Context, guid:str=None) -> None:
        steam_guid = self.get_steam_guid(ctx, guid)
        if steam_guid:
            racer = self.parsed.racers[steam_guid]
            if not racer.gt3progression_plot:
                await ctx.send("Racer hasnt done enough GT3 races yet")
                return
            self.parsed.create_progression_chart(racer.gt3progression_plot)
            file = discord.File("progression_chart.png", filename="progression_chart.png") 
            embed = discord.Embed( title="Racer GT3 Progression", description=f"Progression Over Time for {racer.name}", color=discord.Color.green() ) 
            embed.set_image(url="attachment://progression_chart.png") 
            await ctx.send(embed=embed, file=file)
        else:
            await ctx.send('Invalid query. Provide a valid Steam GUID or /register your steam guid to your Discord name.')
    
    @commands.hybrid_command(name="myprogressionmx5", description="show improvement over time in mx5")
    async def myprogressionmx5(self, ctx: Context, guid:str=None) -> None:
        steam_guid = self.get_steam_guid(ctx, guid)
        if steam_guid:
            racer = self.parsed.racers[steam_guid]
            if not racer.mx5progression_plot:
                await ctx.send("Racer hasnt done enough MX5 races yet")
                return
            self.parsed.create_progression_chart(racer.mx5progression_plot)
            file = discord.File("progression_chart.png", filename="progression_chart.png") 
            embed = discord.Embed( title="Racer MX5 Progression", description=f"Progression Over Time for {racer.name}", color=discord.Color.green() ) 
            embed.set_image(url="attachment://progression_chart.png") 
            await ctx.send(embed=embed, file=file)
        else:
            await ctx.send('Invalid query. Provide a valid Steam GUID or /register your steam guid to your Discord name.')
    
    @commands.hybrid_command(name="dickpic", description="get dickpic")
    async def dickpic(self, ctx: commands.Context) -> None:

        # Load the local image
        with open("iosh.png", "rb") as file:
            image = discord.File(file)
        
        # Create an embed
        embed = discord.Embed(
            title="Here is a dick!",
            description="",
            color=0x00ff00
        )
        embed.set_image(url="attachment://iosh.png")
        
        # Send the embed with the image
        await ctx.send(file=image, embed=embed)


    @commands.hybrid_command(name="top10wins", description="get top 10 wins")
    async def top10wins(self, ctx: commands.Context) -> None:
        if self.parsed:
            retstring = "Top 10 winners :" + "\n"
            winrankings = self.parsed.wins_rankings
            index = 1
            for elem in winrankings:
                retstring += str(index) + " : " + elem.name + "\n"
                index += 1
                if index == 10:
                    break
            await ctx.send(retstring)

    @commands.hybrid_command(name="monthreport", description="get monthlyreport")
    async def monthreport(self, ctx: commands.Context, datemonth:str, guid:str = None) -> None:
        steam_guid = self.get_steam_guid(ctx, guid)
        if steam_guid:
            year = datemonth[-2:]
            month = datemonth[:-2]
            report = self.parsed.month_report(steam_guid, month, year)
            retstr = "Month report for: " + self.parsed.racers[steam_guid].name + " in : " + month + " , " + "20" + year
            retstr += "\n"
            retstr += "**rating at start of month :** " + str(report[0])
            retstr += "\n"
            for elem in report[2]:
                retstr += elem.track.parent_track.highest_priority_name + " : " + elem.car.name + " : finished : " + str(elem.finishingposition) + " rating change: " + str(elem.ratingchange)
                retstr += "\n"
            retstr += "**rating at end of month :** " + str(report[1])
            retstr += "\n"
            retstr += "**total change in december :** " + str(round(report[1] - report[0], 2) )
            await ctx.send(retstr)
        else:
            await ctx.send('Invalid query. Provide a valid Steam GUID or /register your steam guid to your Discord name.')

    @commands.hybrid_command(name="top10podiums", description="get top 10 podums")
    async def top10podiums(self, ctx: commands.Context) -> None:
        if self.parsed:
            retstring = "Top 10 podium finishes:" + "\n"
            winrankings = self.parsed.podiums_rankings
            index = 1
            for elem in winrankings:
                retstring += str(index) + " : " + elem.name + "\n"
                index += 1
                if index == 10:
                    break
            await ctx.send(retstring)

    @commands.hybrid_command(name="scatterplot", description="scatter plot of racers")
    async def scatterplot(self, ctx: commands.Context, only_recent = False) -> None: 
        self.parsed.plot_racers_scatter()
        file = discord.File("scatter_plot.png", filename="scatter_plot.png") 
        embed = discord.Embed( title="Scatter Plot", description=f"Cleanliness vs ELO scatter", color=discord.Color.green() ) 
        embed.set_image(url="attachment://scatter_plot.png") 
        await ctx.send(embed=embed, file=file)

    @commands.hybrid_command(name="rrrstats", description="get overall top 10s")
    async def rrrstats(self, ctx: commands.Context, only_recent = False) -> None:
        if self.parsed:
            stats = self.parsed.get_overall_stats(only_recent)
            embed = discord.Embed(
                title="Overall Stats " + ("(Recently Active Racers)" if only_recent else ""),
                color=discord.Color.blue()
            )

            def format_rankings(rankings, value_formatter):
                formatted_lines = [value_formatter(entry) for entry in rankings]
                return "\n".join(formatted_lines)

            def elo_formatter(entry):
                return f"{entry['rank']}. {entry['name']} - **Rating**: {entry['rating']}"

            def safety_formatter(entry):
                return f"{entry['rank']}. {entry['name']} - **Average Incidents**: {entry['averageincidents']:.2f}"

            def consistency_formatter(entry):
                return f"{entry['rank']}. {entry['name']} - **Consistency**: {entry['laptimeconsistency']:.2f}%"

            # Add top 10 ELO rankings
            elo_rankings = format_rankings(stats['elos'], elo_formatter)
            embed.add_field(name="🏆 Top 10 ELO Rankings 🏆", value=elo_rankings or "\u200b", inline=False)

            # Add top 10 clean racers
            safety_rankings = format_rankings(stats['safety'], safety_formatter)
            embed.add_field(name="🚗 Top 10 Clean Racers 🚗", value=safety_rankings or "\u200b", inline=False)

            # Add top 10 lap time consistency rankings
            laptime_consistency_rankings = format_rankings(stats['laptime_consistency'], consistency_formatter)
            embed.add_field(name="⏱️ Top 10 Lap Time Consistency ⏱️", value=laptime_consistency_rankings or "\u200b", inline=False)

            await ctx.send(embed=embed)
        else:
            await ctx.send("ERROR: Overall results have not been parsed yet")

    @commands.hybrid_command(name="dumptracks", description="dump all tracks")
    @commands.is_owner()
    async def dumptracks(self, context: Context) -> None:
        for track in self.parsed.contentdata.tracks:
            print(track.highest_priority_name)


    @commands.hybrid_command(name="rrrdirty", description="get dirtiest drivers")
    @commands.is_owner()
    async def rrrdirty(self, context: Context) -> None:
        if self.parsed:
            retstr = self.parsed.get_dirty_drivers()
            await context.send(retstr)
        else:
            await context.send("ERROR:Overall results have not been parsed yet")

# And then we finally add the cog to the bot so that it can load, unload, reload and use it's content.
async def setup(bot) -> None:
    await bot.add_cog(Stats(bot))