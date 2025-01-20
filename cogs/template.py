"""
Copyright © Krypton 2019-Present - https://github.com/kkrypt0nn (https://krypton.ninja)
Description:
🐍 A simple template to start to code your own and personalized Discord bot in Python

Version: 6.2.0
"""
import aiohttp
import discord
from discord.ext import commands, tasks
from discord.ext.commands import Context


# Here we name the cog and create a new class for the cog.
class Template(commands.Cog, name="potato"):
    def __init__(self, bot) -> None:
        self.bot = bot
    # Here you can just add your own commands, you'll always need to provide "self" as first parameter.

    @commands.hybrid_command(name="goodbot", description="Tell BOTato he is a good bot.")
    async def goodbot(self, context: Context) -> None:
        """
        Get a random insult back.
        
        :param context: The hybrid command context.
        """
        # This will prevent your bot from stopping everything when doing a web request - see: https://discordpy.readthedocs.io/en/stable/faq.html#how-do-i-make-a-web-request
        async with aiohttp.ClientSession() as session:
            async with session.get(
                    "https://evilinsult.com/generate_insult.php?lang=en&type=json") as request:
                if request.status == 200:
                    data = await request.json(content_type='application/json')
                    embed = discord.Embed(description=data["insult"], color=0xD75BF4)
                else:
                    embed = discord.Embed(
                        title="Error!",
                        description="There is something wrong with the API, please try again later",
                        color=0xE02B2B,
                    )
                await context.send(embed=embed)

    @commands.hybrid_command(name="dmsay", description="say in channel.")
    @discord.app_commands.allowed_contexts(dms=True)
    @commands.is_owner()
    async def dmsay(self, context: Context, tosay:str , channelid="1328800009189195828") -> None:
        if isinstance(context.channel, discord.channel.DMChannel):
            channeltosend = self.bot.get_channel(int(channelid))
            await channeltosend.send(tosay)

    @commands.hybrid_command(name="dad_joke", description="Get a random dadjoke.")
    async def dad_joke(self, context: Context) -> None:
        """
        Get a random dadjoke
        
        :param context: The hybrid command context.
        """
        user_agent = "https://github.com/JanuarySnow/RRR-Bot"
        headers  = {"Accept": "application/json", "User-Agent":user_agent}
        # This will prevent your bot from stopping everything when doing a web request - see: https://discordpy.readthedocs.io/en/stable/faq.html#how-do-i-make-a-web-request
        async with aiohttp.ClientSession() as session:
            async with session.get(
                    "https://icanhazdadjoke.com/", headers=headers) as request:
                if request.status == 200:
                    data = await request.json(content_type='application/json')
                    embed = discord.Embed(description=data["joke"], color=0xD75BF4)
                else:
                    embed = discord.Embed(
                        title="Error!",
                        description="There is something wrong with the API, please try again later",
                        color=0xE02B2B,
                    )
                await context.send(embed=embed)

    @commands.hybrid_command(name="dogpic", description="Get a random dog.")
    async def dogpic(self, context: Context) -> None:
        """
        Get a random dog
        
        :param context: The hybrid command context.
        """
        user_agent = "https://github.com/JanuarySnow/RRR-Bot"
        headers  = {"Accept": "application/json", "User-Agent":user_agent}
        # This will prevent your bot from stopping everything when doing a web request - see: https://discordpy.readthedocs.io/en/stable/faq.html#how-do-i-make-a-web-request
        async with aiohttp.ClientSession() as session:
            async with session.get(
                    "https://random.dog/woof.json", headers=headers) as request:
                if request.status == 200:
                    data = await request.json(content_type='application/json')
                else:
                    embed = discord.Embed(
                        title="Error!",
                        description="There is something wrong with the API, please try again later",
                        color=0xE02B2B,
                    )
                await context.send(data["url"])

    
    async def announce_sale(self, discount_amount, channelid):
        remind_channel = self.bot.get_channel(channelid)
        allowed_mentions = discord.AllowedMentions(everyone = True)

        await remind_channel.send("@everyone . Assetto Corsa: Utimate Edition is on sale right now!"+"\n"+
                                      "It is currently "+ str(discount_amount) + "% " + "off!"+"\n"+
                                       "get it here: " + "https://store.steampowered.com/bundle/6998/Assetto_Corsa_Ultimate_Edition/" )
    
    
    @commands.hybrid_command(name="test_sale_announce", description="Test the announcement.")
    async def test_sale(self, context: Context) -> None:
        """
        Test announcement
        
        :param context: The hybrid command context.
        """
        await self.announce_sale( 30, 1328800009189195828)
    
    async def getacprice(self, country: str='us'):
        user_agent = "https://github.com/JanuarySnow/RRR-Bot"
        headers  = {"User-Agent":user_agent}
        retdict = {}
        # This will prevent your bot from stopping everything when doing a web request - see: https://discordpy.readthedocs.io/en/stable/faq.html#how-do-i-make-a-web-request
        async with aiohttp.ClientSession() as session:
            async with session.get(
                    "https://store.steampowered.com/actions/ajaxresolvebundles?bundleids=6998&cc=%s&l=english" % (country), 
                    headers=headers) as request:
                if request.status == 200:
                    data = await request.json(content_type='application/json')
                    retdict["formatted_orig_price"] = data[0]["formatted_orig_price"]
                    retdict["formatted_final_price"] = data[0]["formatted_final_price"]
                    retdict["discount_percent"] = data[0]["discount_percent"]
                    retdict["bundle_base_discount"] = data[0]["bundle_base_discount"]
                else:
                    print("error")
                    return retdict
        return retdict
    
    @tasks.loop(minutes=1440.0)
    async def check_ac_sale(self) -> None:
        """
        Setup the game status task of the bot.
        """
        retdict = await self.getacprice('us')
        if retdict["discount_percent"] > 0:
            await self.announce_sale( retdict["discount_percent"], 1102816381348626462)
            
    @commands.hybrid_command(name="acprice", description="Get the price of AC:Ultimate Edition.")
    async def acprice(self, context: Context, country :str= 'us') -> None:
        """
        Get price of AC
        
        :param context: The hybrid command context.
        """
        retdict = await self.getacprice('us')
        if not retdict:
            embed = discord.Embed(
                        title="Error!",
                        description="There is something wrong with the API, please try again later",
                        color=0xE02B2B,
                    )
            await context.send(embed=embed)
            return
        embedstring = "Current Price of Assetto Corsa: Ultimate Edition: " + retdict["formatted_final_price"] + "\n"
        embedstring += "This is a sale of : " + str(retdict["discount_percent"]) + "%"
        embed = discord.Embed(description=embedstring, color=0xD75BF4)
        await context.send(embed=embed)

# And then we finally add the cog to the bot so that it can load, unload, reload and use it's content.
async def setup(bot) -> None:
    await bot.add_cog(Template(bot))
