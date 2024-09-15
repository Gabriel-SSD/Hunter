import discord
import os
from discord.ext import commands
from dotenv import load_dotenv
from tasks import af_tickets, ah_tickets, af_tickets_missed, ah_tickets_missed

# Carregar variáveis de ambiente
load_dotenv()

# Configurações do bot
intents = discord.Intents.all()
bot = commands.Bot(command_prefix=".", intents=intents)

@bot.event
async def on_ready():
    # Iniciar as tasks
    af_tickets.start(bot)
    ah_tickets.start(bot)
    af_tickets_missed.start(bot)
    ah_tickets_missed.start(bot)

    await bot.change_presence(activity=discord.Game(name="Star Wars: Galaxy of Heroes"))

def run_bot():
    token = os.getenv("BOT_TOKEN")
    if not token:
        exit(1)
    bot.run(token)

if __name__ == "__main__":
    run_bot()
