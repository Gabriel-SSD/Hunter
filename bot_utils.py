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
FILE = "./data/tickets.png"  # Path for saving the plot
RAID_PATH = ["./data/naboo.png"]  # Path for the background image used in the plot

def plot_ticket_report(guild_id: str):
    """
    Function that generates a ticket report plot for the specified guild_id.
    Connects to the database, fetches ticket data, plots it, and saves the plot as an image.
    """
    DATABASE_URL = os.getenv('DATABASE_URL')
    logger.info(f"Attempting to connect to the database at {DATABASE_URL}")
    engine = create_engine(DATABASE_URL)

    # SQL query to retrieve ticket data for the past 7 days for the given guild
    query = f"""
        select
            cast(dt."date" as date) as date,
            sum(tickets) as tickets,
            dg."name" 
        from f_swgoh_tickets ft
        join d_time dt on ft.sk_time = dt.id
        join d_swgoh_guild dg on ft.sk_guild = dg.id
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

    # Convert 'date' column to datetime and sort DataFrame by date
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by='date', ascending=True).reset_index(drop=True)

    # Setup for the plot
    logger.info(f"Creating ticket report plot for guild {guild_id}")
    plt.rcParams['figure.facecolor'] = '#333333'  # Set figure background color
    plt.rcParams['text.color'] = 'white'  # Set text color
    plt.rcParams['font.size'] = 18  # Set font size

    fig, ax = plt.subplots(figsize=(10, 8))  # Create figure and axis

    # Add a background image to the plot if the path exists
    img_path = choice(RAID_PATH)
    if os.path.exists(img_path):
        logger.info(f"Using background image: {img_path}")
        img = np.array(Image.open(img_path).resize(
            (int(fig.get_size_inches()[0] * fig.dpi), int(fig.get_size_inches()[1] * fig.dpi)), Image.LANCZOS))
        fig.figimage(img, 0, 0, zorder=0, alpha=0.2)  # Overlay image with transparency

    # Plot ticket data with custom styling
    ax.plot(df['date'], df['tickets'], marker='o', color='darkorange', linestyle='-', linewidth=2, markersize=8,
            zorder=1)

    # Customize the appearance of the plot
    ax.patch.set_facecolor((0, 0, 0, 0))  # Transparent background for axis
    ax.set_title(f"{df['name'].iloc[0]} Tickets (Last 7 days)", size=32, color='white', pad=20)
    ax.tick_params(axis='x', colors='white', labelsize=18)
    ax.spines['top'].set_visible(False)  # Hide top spine
    ax.spines['right'].set_visible(False)  # Hide right spine
    ax.spines['bottom'].set_color('white')  # Set bottom spine color
    ax.spines['left'].set_visible(False)  # Hide left spine
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))  # Format x-axis labels as month/day
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))  # Set x-axis ticks at daily intervals

    # Annotate data points with ticket numbers
    for i, txt in enumerate(df['tickets']):
        ax.annotate(txt, (df['date'][i], df['tickets'][i]), textcoords="offset points", xytext=(0, 10), ha='center',
                    color='white', zorder=2)

    # Set dynamic y-axis limits based on data
    y_min = df['tickets'].min() - df['tickets'].min() * 0.1
    y_max = df['tickets'].max() + df['tickets'].max() * 0.1
    ax.set_aspect('auto')  # Adjust aspect ratio
    ax.set_ylim([y_min, y_max])
    ax.get_yaxis().set_visible(False)  # Hide y-axis labels

    # Configure grid lines for the x-axis
    ax.xaxis.grid(True, linestyle=":", alpha=0.4)

    # Save the plot to a file
    logger.info(f"Saving plot to {FILE}")
    plt.subplots_adjust(left=0, right=1)  # Adjust subplot margins
    plt.tight_layout()
    plt.savefig(FILE)  # Save the plot
    logger.info(f"Plot saved successfully to {FILE}")

def get_tickets_missed(guild_id: str, days: str):
    """
    Function that retrieves data on missed tickets for the given guild and time period.
    """
    DATABASE_URL = os.getenv('DATABASE_URL')  # Database connection URL
    logger.info(f"Attempting to connect to the database at {DATABASE_URL}")
    engine = create_engine(DATABASE_URL)  # Create a connection to the database

    # SQL query to fetch missed tickets for players in the guild over the specified number of days
    query = f"""
        select
            case 
                when dp.current_flag = false then 'Member who left the guild'
                else dp.name
            end as player,
            sum(600 - tickets) as tickets_missed
        from f_swgoh_tickets ft
        join d_time dt on dt.id = ft.sk_time
        join d_swgoh_player dp on dp.id = ft.sk_player
        join d_swgoh_guild dg on dg.id = ft.sk_guild
        where 1=1
            and ft.tickets < 600
            and dg.guild_id = '{guild_id}'
            and dt.date >= CURRENT_DATE - interval '{days} days'
        group by 
            case 
                when dp.current_flag = false then 'Member who left the guild'
                else dp.name
            end
        order by 2 desc
    """

    try:
        logger.info(f"Running query to fetch missed tickets data for guild {guild_id} over the last {days} days")
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn)

        logger.info(f"Missed tickets data fetched successfully for guild {guild_id}")
    except Exception as e:
        logger.error(f"Error occurred while fetching missed tickets data: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error

    return df  # Return the DataFrame with the missed tickets data

def format_embed(df, guild_name, days):
    """
    Function that formats a Discord embed message displaying missed ticket information.
    """
    logger.info(f"Formatting embed for {guild_name} missed tickets report")

    if df.empty:
        # Create an embed indicating no missed tickets
        embed = discord.Embed(
            title=f"{guild_name} Missed Tickets Report",
            description="Perfect!",
            colour=discord.Color(0x7F8C8D)
        )
        logger.info(f"No missed tickets data found for {guild_name}")
    else:
        sum_missed = 0  # Initialize a counter for total missed tickets
        member_list = "```\n"  # Start a code block for the member list
        for index, row in df.iterrows():
            # Format each row of missed tickets and add to the member list
            member_list += f"{row['tickets_missed']:>4d} : {row['player']}\n"
            sum_missed += row['tickets_missed']  # Accumulate the total missed tickets
        member_list += "```"  # End the code block

        # Create the message body
        body = member_list
        header = (f"**{sum_missed}** tickets missed in the last **{days}** days\n"
                  f"Members that missed tickets:")

        # Create an embed with the summary and member details
        embed = discord.Embed(
            title=f"{guild_name} Missed Tickets Report",
            description=header + body,
            colour=discord.Color(0x7F8C8D)
        )
        logger.info(f"Embed formatted successfully for {guild_name} with {sum_missed} tickets missed")

    return embed
