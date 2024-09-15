import os
import discord
from discord.ext import tasks
from datetime import datetime
from bot_utils import plot_ticket_report, get_tickets_missed, format_embed

CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
FILE = "./data/tickets.png"

@tasks.loop(minutes=1)
async def af_tickets(bot):
    """Task that sends the ticket report for 'Awakening Fear' guild at 17:31 daily."""
    now = datetime.now()
    if now.hour == 17 and now.minute == 31:
        channel = bot.get_channel(CHANNEL_ID)
        plot_ticket_report("1HE3bh3LRcWVOto5KuGvzQ")
        await channel.send(file=discord.File(fp=FILE))

@tasks.loop(minutes=1)
async def ah_tickets(bot):
    """Task that sends the ticket report for 'Awakening Hope' guild at 22:31 daily."""
    now = datetime.now()
    if now.hour == 22 and now.minute == 31:
        channel = bot.get_channel(CHANNEL_ID)
        plot_ticket_report("iO-khl_0TVu64OussT1Y7g")
        await channel.send(file=discord.File(fp=FILE))

@tasks.loop(minutes=1)
async def af_tickets_missed(bot):
    """Task that sends the missed tickets report for 'Awakening Fear' guild at 17:32 every Sunday."""
    now = datetime.now()
    if now.weekday() == 6 and now.hour == 17 and now.minute == 32:
        channel = bot.get_channel(CHANNEL_ID)
        df = get_tickets_missed("1HE3bh3LRcWVOto5KuGvzQ")
        embed = format_embed(df, "Awakening Fear")
        await channel.send(embed=embed)

@tasks.loop(minutes=1)
async def ah_tickets_missed(bot):
    """Task that sends the missed tickets report for 'Awakening Hope' guild at 22:32 every Sunday."""
    now = datetime.now()
    if now.weekday() == 6 and now.hour == 22 and now.minute == 32:
        channel = bot.get_channel(CHANNEL_ID)
        df = get_tickets_missed("iO-khl_0TVu64OussT1Y7g")
        embed = format_embed(df, "Awakening Hope")
        await channel.send(embed=embed)

