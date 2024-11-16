import discord
import os
from discord.ext import commands
from dotenv import load_dotenv
from bot_tasks import af_tickets, ah_tickets, af_tickets_missed, ah_tickets_missed
from logger import setup_logging

load_dotenv()
logger = setup_logging()
intents = discord.Intents.all()
bot = commands.Bot(command_prefix=".", intents=intents)


@bot.event
async def on_ready():
    logger.info("Bot is now connected and ready to go!")
    af_tickets.start(bot)
    ah_tickets.start(bot)
    af_tickets_missed.start(bot)
    ah_tickets_missed.start(bot)

    await bot.change_presence(activity=discord.Game(name="Star Wars: Galaxy of Heroes"))

def run_bot():
    logger.info("Attempting to start the bot...")
    token = os.getenv("BOT_TOKEN")
    if not token:
        logger.error("Bot token not found. Exiting.")
        exit(1)
    logger.info("Bot started successfully.")
    bot.run(token)

if __name__ == "__main__":
    run_bot()
