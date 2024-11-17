import pandas as pd
from sqlalchemy import create_engine, text
from swgoh_comlink import SwgohComlink
from dotenv import load_dotenv
from logger import setup_logging
from datetime import datetime
import os

# List of ally codes representing specific players in the game
ALLYCODES = ['798472764', '684751897', '755251518', '347738465', '637187182', '144447734']

logger = setup_logging()
load_dotenv()
comlink = SwgohComlink()


def process_units_for_tracking(rosters):
    """Process player units and prepare them for tracking in the database."""
    logger.info("Processing units for tracking.")
    records = []  # List to hold processed unit data

    try:
        # Iterate over each row in the DataFrame containing player rosters
        for _, row in rosters.iterrows():
            # Iterate over each unit in the player's roster
            for unit in row['roster']:
                records.append({
                    'player_id': row['player_id'],
                    'base_id': unit['definitionId'].split(":")[0],
                    'gear': unit.get('currentTier', None),
                    'relic': int(unit['relic'].get('currentTier', 0)) - 2 if unit.get('relic') else -3,
                    'level': unit.get('currentLevel', None),
                    'stars': unit.get('currentRarity', None),
                    'gp': unit.get('gp', None),
                    'date': datetime.now().strftime('%Y%m%d')
                })
        logger.info(f"Successfully processed {len(records)} units for tracking.")
        # Convert the list of records into a Pandas DataFrame
        return pd.DataFrame(records)
    except Exception as e:
        logger.error(f"Error processing units for tracking: {e}")
        raise


def get_player_roster(players: list):
    """Fetch the roster data for each player based on their ally codes."""
    logger.info(f"Fetching player rosters for {len(players)} players.")
    try:
        player_data = []  # List to hold player data

        for allycode in players:
            logger.info(f"Fetching data for player with ally code: {allycode}")
            # Retrieve player data using the SWGOH API client
            player = comlink.get_player(allycode=allycode)
            # Append the player data, including the roster and stats, to the list
            player_data.append(
                {
                    "player_id": player["playerId"],
                    "roster": comlink.get_unit_stats(player["rosterUnit"], flags=['onlyGP']),
                    # "stat": player["profileStat"],
                }
            )
        logger.info("Successfully fetched player rosters.")
        # Convert the list of player data into a Pandas DataFrame
        return pd.DataFrame(player_data)
    except Exception as e:
        logger.error(f"Error fetching player roster: {e}")
        raise


if __name__ == "__main__":
    try:
        logger.info("Script execution started.")
        engine = create_engine(os.getenv('DATABASE_URL'))
        logger.info("Database engine created successfully.")

        with engine.begin() as connection:
            logger.info("Database connection established. Processing roster data.")
            # Fetch player rosters and process them for tracking
            df = process_units_for_tracking(get_player_roster(ALLYCODES))
            logger.info("Uploading roster data to the database.")
            # Upload the processed DataFrame to the database table `stg_snapshot_roster`
            df.to_sql("stg_snapshot_roster", con=connection, if_exists="replace", index=False)
            # Execute a stored procedure to insert data into the snapshot table
            connection.execute(text("CALL insert_snapshot_roster()"))
            logger.info("Data successfully inserted into the database.")
    except Exception as e:
        logger.error(f"An error occurred during script execution: {e}")
