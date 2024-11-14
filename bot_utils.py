import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
from random import choice
import numpy as np
from PIL import Image
from dotenv import load_dotenv
from sqlalchemy import create_engine
import discord
from logger import setup_logging

load_dotenv()
logger = setup_logging()

CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
FILE = "./data/tickets.png"
RAID_PATH = ["./data/naboo.png"]


def plot_ticket_report(guild_id: str):
    DATABASE_URL = os.getenv('DATABASE_URL')
    logger.info(f"Attempting to connect to the database at {DATABASE_URL}")
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

    try:
        logger.info(f"Running query to fetch ticket data for guild {guild_id}")
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn)

        logger.info(f"Data fetched successfully for guild {guild_id}")
    except Exception as e:
        logger.error(f"Error occurred while fetching data: {e}")
        return

    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by='date', ascending=True).reset_index(drop=True)

    # Plot setup
    logger.info(f"Creating ticket report plot for guild {guild_id}")
    plt.rcParams['figure.facecolor'] = '#333333'
    plt.rcParams['text.color'] = 'white'
    plt.rcParams['font.size'] = 18

    fig, ax = plt.subplots(figsize=(10, 8))

    img_path = choice(RAID_PATH)
    if os.path.exists(img_path):
        logger.info(f"Using background image: {img_path}")
        img = np.array(Image.open(img_path).resize(
            (int(fig.get_size_inches()[0] * fig.dpi), int(fig.get_size_inches()[1] * fig.dpi)), Image.LANCZOS))
        fig.figimage(img, 0, 0, zorder=0, alpha=0.2)

    # Plot the ticket data (ensure it's sorted)
    ax.plot(df['date'], df['tickets'], marker='o', color='darkorange', linestyle='-', linewidth=2, markersize=8,
            zorder=1)

    # Customize plot appearance
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
        ax.annotate(txt, (df['date'][i], df['tickets'][i]), textcoords="offset points", xytext=(0, 10), ha='center',
                    color='white', zorder=2)

    # Set dynamic y-axis limits based on data
    y_min = df['tickets'].min() - df['tickets'].min() * 0.1
    y_max = df['tickets'].max() + df['tickets'].max() * 0.1
    ax.set_aspect('auto')  # Adjust aspect ratio
    ax.set_ylim([y_min, y_max])
    ax.get_yaxis().set_visible(False)

    # Configure grid lines for the x-axis
    ax.xaxis.grid(True, linestyle=":", alpha=0.4)

    # Save plot to file
    logger.info(f"Saving plot to {FILE}")
    plt.subplots_adjust(left=0, right=1)
    plt.tight_layout()
    plt.savefig(FILE)
    logger.info(f"Plot saved successfully to {FILE}")


def get_tickets_missed(guild_id: str, days: str):
    DATABASE_URL = os.getenv('DATABASE_URL')
    logger.info(f"Attempting to connect to the database at {DATABASE_URL}")
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
                and dt.date >= CURRENT_DATE - interval '{days} days'
            group by dp.name
            order by 2 desc
            """

    try:
        logger.info(f"Running query to fetch missed tickets data for guild {guild_id} over the last {days} days")
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn)

        logger.info(f"Missed tickets data fetched successfully for guild {guild_id}")
    except Exception as e:
        logger.error(f"Error occurred while fetching missed tickets data: {e}")
        return pd.DataFrame()

    return df


def format_embed(df, guild_name, days):
    logger.info(f"Formatting embed for {guild_name} missed tickets report")

    if df.empty:
        embed = discord.Embed(
            title=f"{guild_name} Missed Tickets Report",
            description="Perfect!",
            colour=discord.Color(0x7F8C8D)
        )
        logger.info(f"No missed tickets data found for {guild_name}")
    else:
        sum_missed = 0
        member_list = "```\n"
        for index, row in df.iterrows():
            member_list += f"{row['tickets_missed']:>4d} : {row['player']}\n"
            sum_missed += row['tickets_missed']
        member_list += "```"
        body = member_list
        header = (f"**{sum_missed}** tickets missed in the last **{days}** days\n"
                  f"Members that missed tickets:")
        embed = discord.Embed(
            title=f"{guild_name} Missed Tickets Report",
            description=header + body,
            colour=discord.Color(0x7F8C8D)
        )
        logger.info(f"Embed formatted successfully for {guild_name} with {sum_missed} tickets missed")

    return embed
