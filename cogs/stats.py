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
        self.venusserver = 'https://usa.assettohosting.com:10724'
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
            embed.add_field(name="🏎️ **Average Pace % Compared to Top Lap Times**", value=f"{racer.pace_percentage:.2f}% (Rank: {self.parsed.get_pace_rank(racer) + 1}/{len(self.parsed.pacerankings)})" if racer.pace_percentage is not None else "No data", inline=True)

            await ctx.send(embed=embed)
        else:
            await ctx.send('Invalid query. Provide a valid Steam GUID or /register your steam guid to your Discord name.')


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
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
                async with session.get(query, headers=headers) as request:
                    if request.status == 200:
                        data = await request.json(content_type='application/json')
                        data["results"].sort(key=lambda elem: datetime.fromisoformat(elem["date"]), reverse=True)
                        print(f"Results size from results list = {len(data['results'])}")

                        for result in data["results"]:
                            download_url = result["results_json_url"]
                            filename = os.path.basename(download_url)
                            filepath = os.path.join("results", filename)

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
            filename = os.path.basename(download_url)
            filepath = os.path.join("results", filename)

            try:
                async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:

                    async with session.get(download_url, headers=headers) as request:
                        if request.status == 200:
                            data = await request.json(content_type='application/json')

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


    @commands.hybrid_command(name='forcetimedtask', description="force timed task")
    async def forcetimedtask(self, ctx):
        await self.fetch_results_list()

    @tasks.loop(seconds=6000.0)
    async def fetch_results_list(self):
        global ON_READY_FIRST_TIME_SCAN
        if ON_READY_FIRST_TIME_SCAN:
            ON_READY_FIRST_TIME_SCAN = False
            return
        channel = discord.utils.get(self.bot.get_all_channels(), name='general')
        print("starting fetch")
        async with channel.typing():
            query = self.venusserver + "/api/results/list.json?q=Type:\"RACE\"&sort=date&page=0"
            print("query for server = " + query)
            await self.check_one_server_for_results(self.venusserver,query)
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


    @commands.hybrid_command(name='top10pace', description="get pace top 10 rankings")
    async def top10pace(self, ctx):
        rankings = self.parsed.pacerankings[:10]  # Get top 10 rankings
        embed = discord.Embed(
            title="Top 10 Pace Rankings",
            color=discord.Color.blue()
        )

        for index, racer in enumerate(rankings):
            embed.add_field(
                name=f"{index + 1}. {racer.name}",
                value=f"**Pace Percentage**: {racer.pace_percentage}%",
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
            fastest_f4_lap = elem.get_racer_fastest_lap_in_car_id( "rss_formula_rss_4_2024", guid)

            if fastest_f4_lap and fastest_f4_lap.racerguid:
                total_seconds = float(fastest_f4_lap.time / 1000.0)
                minutes = int(total_seconds // 60)
                seconds = total_seconds % 60
                if guid:
                    embed.add_field(
                    name=f"{self.parsed.racers[fastest_f4_lap.racerguid].name}'s fastest ever F4 lap at: {elem.name}",
                    value=f"⏱️ {minutes}:{seconds:06.3f}",
                    inline=False
                    )
                else:
                    embed.add_field(
                    name=f"fastest ever F4 lap at: {elem.name} by : {self.parsed.racers[fastest_f4_lap.racerguid].name}",
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

    @commands.hybrid_command(name="venusstats", description="get overall top 10s")
    async def venusstats(self, ctx: commands.Context, only_recent = False) -> None:
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


    @commands.hybrid_command(name="venusdirty", description="get dirtiest drivers")
    @commands.is_owner()
    async def venusdirty(self, context: Context) -> None:
        if self.parsed:
            retstr = self.parsed.get_dirty_drivers()
            await context.send(retstr)
        else:
            await context.send("ERROR:Overall results have not been parsed yet")

# And then we finally add the cog to the bot so that it can load, unload, reload and use it's content.
async def setup(bot) -> None:
    await bot.add_cog(Stats(bot))