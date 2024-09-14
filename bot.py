import discord
import os
from discord.ext import commands, tasks
from dotenv import load_dotenv
from random import choice
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from PIL import Image
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime

load_dotenv()
bot = commands.Bot(command_prefix=".", intents=discord.Intents.all())
CHANNEL_ID = int(os.environ.get("CHANNEL_ID"))
FILE = "./data/tickets.png"
# RAID_PATH = ["./data/rancor.png", "./data/rancor2.png", "./data/triumvirate.png", "./data/endor.png", "./data/aat.png", "./data/krayt.png", "./data/naboo.png"]
RAID_PATH = ["./data/naboo.png"]

def plot_ticket_report(guild_id: str):
    DATABASE_URL = os.getenv('DATABASE_URL')
    engine = create_engine(DATABASE_URL)

    query = f"""
        select
            cast(dt."date" as date) as date,
            sum(tickets) as tickets,
            dg."name" 
        from f_tickets ft
        join d_time dt on ft.sk_time = dt.id
        join d_guild dg on ft.sk_guild = dg.id
        where 1=1
            and dt."date" > current_date - 7
            and dg.guild_id = '{guild_id}' 
        group by dt."date", dg."name" 
    """
    
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)

    conn.close()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by='date', ascending=True).reset_index(drop=True)

    plt.rcParams['figure.facecolor'] = '#333333'
    plt.rcParams['text.color'] = 'white'
    plt.rcParams['font.size'] = 18

    fig, ax = plt.subplots(figsize=(10, 8))

    img_path = choice(RAID_PATH) 
    if os.path.exists(img_path):
        # Open and resize the image
        img = np.array(Image.open(img_path).resize((int(fig.get_size_inches()[0] * fig.dpi), int(fig.get_size_inches()[1] * fig.dpi)), Image.LANCZOS))
        # Add the image as background to the figure
        fig.figimage(img, 0, 0, zorder=0, alpha=0.2)

    # Plot the ticket data (garanta que os dados estão ordenados)
    ax.plot(df['date'], df['tickets'], marker='o', color='darkorange', linestyle='-', linewidth=2, markersize=8, zorder=1)

    # Customize the plot appearance
    ax.patch.set_facecolor((0, 0, 0, 0))
    ax.set_title(f"{df['name'].iloc[0]} Tickets (Last 7 days)", size=32, color='white', pad=20)
    ax.tick_params(axis='x', colors='white', labelsize=18)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_color('white')
    ax.spines['left'].set_visible(False)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))

    # Annotate data points on the plot
    for i, txt in enumerate(df['tickets']):
        ax.annotate(txt, (df['date'][i], df['tickets'][i]), textcoords="offset points", xytext=(0,10), ha='center', color='white', zorder=2)

    # Set dynamic y-axis limits based on the data
    y_min = df['tickets'].min() - df['tickets'].min() * 0.1
    y_max = df['tickets'].max() + df['tickets'].max() * 0.1
    ax.set_aspect('auto')  # Adjust the aspect ratio of the plot
    ax.set_ylim([y_min, y_max])
    ax.get_yaxis().set_visible(False)

    # Configure grid lines for the x-axis
    ax.xaxis.grid(True, linestyle=":", alpha=0.4)

    # Adjust layout and save the plot
    plt.subplots_adjust(left=0, right=1)
    plt.tight_layout()
    plt.savefig(FILE)

def get_tickets_missed(guild_id: str):
    DATABASE_URL = os.getenv('DATABASE_URL')
    engine = create_engine(DATABASE_URL)
    query = f"""
            select
                dp.name as player,
                sum(600 - tickets) as tickets_missed
            from f_tickets ft
            join d_time dt on dt.id = ft.sk_time
            join d_player dp on dp.id = ft.sk_player
            join d_guild dg on dg.id = ft.sk_guild
            where 1=1
                and ft.tickets < 600
                and dg.guild_id = '{guild_id}'
                and dt.date >= CURRENT_DATE - interval '7 days'
            group by dp.name
            """
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
    return df

def format_embed(df, guild_name):
    if df.empty:
        embed = discord.Embed(
                title=f"{guild_name} Missed Tickets Report",
                description="Perfect week!",
                colour=discord.Color(0x7F8C8D)
            )
    else:
        sum_missed = 0
        member_list = "```\n"
        for index, row in df.iterrows():
            member_list += f"{row['tickets_missed']:>4d} : {row['player']}\n"
            sum_missed += row['tickets_missed']
        member_list += "```"
        body = member_list
        header = (f"**{sum_missed}** tickets missed in the last **7** days\n"
                      f"Members that missed tickets:")
        embed = discord.Embed(
                title=f"{guild_name} Missed Tickets Report",
                description=header + body,
                colour=discord.Color(0x7F8C8D)
            )
    return embed

@bot.event
async def on_ready():

    af_tickets.start()
    ah_tickets.start()
    ah_tickets_missed.start()
    af_tickets_missed.start()

    # synced = await bot.tree.sync()
    await bot.change_presence(activity=discord.Game(name="Star Wars: Galaxy of Heroes"))

@tasks.loop(minutes=1)
async def af_tickets():
    now = datetime.now()
    if now.hour == 17 and now.minute == 31:
        channel = bot.get_channel(CHANNEL_ID)
        plot_ticket_report("1HE3bh3LRcWVOto5KuGvzQ")
        if channel:
            await channel.send(file=discord.File(fp=FILE))


@tasks.loop(minutes=1)
async def ah_tickets():
    now = datetime.now()
    if now.hour == 22 and now.minute == 31:
        channel = bot.get_channel(CHANNEL_ID)
        plot_ticket_report("iO-khl_0TVu64OussT1Y7g")
        if channel:
            await channel.send(file=discord.File(fp=FILE))


@tasks.loop(minutes=1)
async def ah_tickets_missed():
    now = datetime.now()
    if now.weekday() == 6 and now.hour == 22 and now.minute == 32:
        channel = bot.get_channel(CHANNEL_ID)
        df = get_tickets_missed("iO-khl_0TVu64OussT1Y7g")
        embed = format_embed(df, "Awakening Hope")
        if channel:
            await channel.send(embed=embed)

@tasks.loop(minutes=1)
async def af_tickets_missed():
    now = datetime.now()
    if now.weekday() == 6 and now.hour == 17 and now.minute == 32:
        channel = bot.get_channel(CHANNEL_ID)
        df = get_tickets_missed("1HE3bh3LRcWVOto5KuGvzQ")
        embed = format_embed(df, "Awakening Fear")
        if channel:
            await channel.send(embed=embed)

@af_tickets.before_loop
@ah_tickets.before_loop
@ah_tickets_missed.before_loop
@af_tickets_missed.before_loop
async def before_tasks():
    await bot.wait_until_ready()

def run_bot():
    token = os.environ.get("BOT_TOKEN")
    if not token:
        exit(1)
    bot.run(token)

if __name__ == "__main__":
    run_bot()
