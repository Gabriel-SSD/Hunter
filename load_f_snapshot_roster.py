import pandas as pd
from sqlalchemy import create_engine, text
from swgoh_comlink import SwgohComlink
from dotenv import load_dotenv
from logger import setup_logging
from datetime import datetime
import os

ALLYCODES = ['637187182', '994672379', '144447734', '653624496', '448398595']

logger = setup_logging()
load_dotenv()
comlink = SwgohComlink()


def process_units_for_tracking(rosters):
    logger.info("Processing units for tracking.")
    records = []
    try:
        for _, row in rosters.iterrows():
            for unit in row['roster']:
                if unit.get('relic', None) is not None:
                    records.append({
                        'player_id': row['player_id'],
                        'base_id': unit['definitionId'].split(":")[0],
                        'gear': unit.get('currentTier', None),
                        'relic': int(unit['relic'].get('currentTier', 0)) - 2 if unit.get('relic') else None,
                        'level': unit.get('currentLevel', None),
                        'stars': unit.get('currentRarity', None),
                        'gp': unit.get('gp', None),
                        'date': datetime.now().strftime('%Y%m%d')
                    })
        logger.info(f"Successfully processed {len(records)} units for tracking.")
        return pd.DataFrame(records)
    except Exception as e:
        logger.error(f"Error processing units for tracking: {e}")
        raise


def get_player_roster(players: list):
    logger.info(f"Fetching player rosters for {len(players)} players.")
    try:
        player_data = []
        for allycode in players:
            logger.info(f"Fetching data for player with ally code: {allycode}")
            player = comlink.get_player(allycode=allycode)
            player_data.append(
                {
                    "player_id": player["playerId"],
                    "roster": comlink.get_unit_stats(player["rosterUnit"], flags=['onlyGP']),
                    "stat": player["profileStat"],
                }
            )
        logger.info("Successfully fetched player rosters.")
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
            df = process_units_for_tracking(get_player_roster(ALLYCODES))
            logger.info("Uploading roster data to the database.")
            df.to_sql("stg_snapshot_roster", con=connection, if_exists="replace", index=False)
            connection.execute(text("CALL insert_snapshot_roster()"))
            logger.info("Data successfully inserted into the database.")
    except Exception as e:
        logger.error(f"An error occurred during script execution: {e}")
