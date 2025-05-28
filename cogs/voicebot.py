import discord
from discord.ext import commands, voice_recv
import asyncio
import tempfile
import soundfile as sf
import numpy as np
import aiohttp
import subprocess
import yaml
import queue
from collections import defaultdict
import threading

def get_gpt_config(filename="config.yaml"):
    with open(filename, "r") as file:
        return yaml.safe_load(file)
    
@commands.Cog.listener()
async def on_speaking(self, member, speaking):
    print(f"🔊 {member.display_name} is speaking: {speaking}")

class voicebot(commands.Cog, name="voicebot"):
    def __init__(self, bot) -> None:
        self.bot = bot
        self.cfg = get_gpt_config()
        self.provider, self.model = self.cfg["model"].split("/", 1)
        self.base_url = self.cfg["providers"][self.provider]["base_url"]
        self.api_key = self.cfg["providers"][self.provider].get("api_key", "sk-no-key-required")
        self.audio_queues = defaultdict(lambda: queue.Queue(maxsize=100))
        self.system_prompt = "You are a friendly, sarcastic and acerbic simracer who likes racing with The Tekly Racing community for Assetto Corsa, and Real Rookie Racing, you respect Buggy, " \
        "our dear leader, and your creator Potato, you like to offer simracing tips and setup help with Assetto Corsa, you love the Mazda MX5, you will make jokes and banter with people, your name is Botato"

    @commands.hybrid_command(name="testvoice", description="Joins VC, listens for 'bot', replies with voice")
    async def testvoice(self, ctx):
        def callback(user, data: voice_recv.VoiceData):
            if user is None:
                return

            uid = user.id
            try:
                self.audio_queues[uid].put_nowait(data.pcm)
            except queue.Full:
                pass

        vc = await ctx.author.voice.channel.connect(cls=voice_recv.VoiceRecvClient)

        # Wait a second to allow speaking event subscriptions to complete
        await asyncio.sleep(1)

        vc.listen(voice_recv.BasicSink(callback))
        await ctx.send("🎤 Listening for 'bot'...")

        while True:
            await asyncio.sleep(5)

            for uid, q in list(self.audio_queues.items()):
                buffered_bytes = bytearray()
                while not q.empty():
                    try:
                        buffered_bytes.extend(q.get_nowait())
                    except queue.Empty:
                        break

                if len(buffered_bytes) < 48000 * 2 * 2 * 5:  # 5 sec of stereo, 16-bit PCM
                    continue

                with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as f:
                    pcm_array = np.frombuffer(buffered_bytes, dtype=np.int16).reshape(-1, 2)
                    sf.write(f.name, pcm_array, samplerate=48000, format='WAV')
                    file_path = f.name

                transcript = await self.transcribe_audio(file_path)
                print(f"Transcript from {uid}: {transcript}")

                if "bot" in transcript.lower():
                    speaker = ctx.guild.get_member(uid)
                    username = speaker.display_name if speaker else "someone"
                    reply = await self.generate_chat_response(username, transcript)
                    print(reply)
                    await self.respond_in_voice(vc, reply)
                    self.audio_queues[uid].queue.clear()


    async def generate_chat_response(self, username, transcript):
        url = "https://api.openai.com/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": f"{username} said: \"{transcript}\""}
            ],
            "max_tokens": 100,
            "temperature": 0.8,
            "stream": False
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                result = await resp.json()
                print("result = " + result["choices"][0]["message"]["content"])
                return result["choices"][0]["message"]["content"]

    async def transcribe_audio(self, file_path):
        url = "https://api.openai.com/v1/audio/transcriptions"
        headers = {
            "Authorization": f"Bearer {self.api_key}"
        }

        data = aiohttp.FormData()
        data.add_field("file", open(file_path, "rb"), filename="audio.wav", content_type="audio/wav")
        data.add_field("model", "whisper-1")

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=data) as resp:
                result = await resp.json()
                return result.get("text", "")

    async def respond_in_voice(self, vc, text):
        print("🔊 Generating voice...")

        url = "https://api.openai.com/v1/audio/speech"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": "tts-1",
            "voice": "echo",
            "input": text,
            "response_format": "mp3",
            "stream": True
        }

        process = subprocess.Popen(
            ['ffmpeg', '-i', 'pipe:0', '-f', 's16le', '-ar', '48000', '-ac', '2', 'pipe:1'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE
        )

        def feed_audio():
            async def run():
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, json=payload, headers=headers) as resp:
                        async for chunk in resp.content.iter_chunked(1024):
                            if chunk:
                                process.stdin.write(chunk)
                        process.stdin.close()

            asyncio.run(run())

        threading.Thread(target=feed_audio).start()
        audio_source = discord.PCMAudio(process.stdout)
        vc.play(audio_source)

    @commands.command()
    async def stop(self, ctx):
        await ctx.voice_client.disconnect()

    @commands.command()
    async def die(self, ctx):
        ctx.voice_client.stop()
        await ctx.bot.close()

async def setup(bot) -> None:
    await bot.add_cog(voicebot(bot))
