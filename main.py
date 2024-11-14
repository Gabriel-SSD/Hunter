import pandas as pd
from sqlalchemy import create_engine, text
from swgoh_comlink import SwgohComlink
from dotenv import load_dotenv
from datetime import datetime, timezone
import logging
import os
import argparse

# Logging setup
logging.basicConfig(filename='main.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
comlink = SwgohComlink()


def get_player_meta(players: list):
    try:
        logging.info("Starting player data collection.")
        player_data = []
        for player_id in players:
            player = comlink.get_player_arena_profile(player_id=player_id)
            player_data.append(
                {
                    "player_id": player["playerId"],
                    "name": player["name"],
                    "allycode": player["allyCode"]
                }
            )
        logging.info(f"Successfully collected data for {len(player_data)} players.")
        return pd.DataFrame(player_data)
    except Exception as e:
        logging.error(f"Error collecting player data: {e}")
        raise


def get_guild_meta(guild_id: str):
    try:
        logging.info(f"Starting guild data collection for guild {guild_id}.")
        guild = comlink.get_guild(guild_id=guild_id, include_recent_guild_activity_info=False)
        logging.info(f"Successfully collected data for guild {guild_id}.")
        return pd.DataFrame([
            {
                "guild_id": guild_id,
                "name": guild["profile"]["name"],
                "guild_reset": datetime.fromtimestamp(int(guild["nextChallengesRefresh"]), tz=timezone.utc).strftime(
                    '%H:%M:%S')
            }
        ])
    except Exception as e:
        logging.error(f"Error collecting guild data for guild {guild_id}: {e}")
        raise


def get_tickets(guild_id: str):
    try:
        logging.info(f"Starting ticket data collection for guild {guild_id}.")
        guild = comlink.get_guild(guild_id=guild_id, include_recent_guild_activity_info=True)
        guild_tickets = []

        for member in guild["member"]:
            player_id = member["playerId"]
            for contribution in member["memberContribution"]:
                if contribution["type"] == 2:
                    current_value = contribution["currentValue"]
                    guild_tickets.append(
                        {
                            "guild_id": guild_id,
                            "player_id": player_id,
                            "tickets": current_value,
                            "date": datetime.now().strftime('%Y%m%d')
                        }
                    )
        logging.info(f"Successfully collected ticket data for {len(guild_tickets)} members.")
        return pd.DataFrame(guild_tickets)
    except Exception as e:
        logging.error(f"Error collecting ticket data for guild {guild_id}: {e}")
        raise


def get_raid_result(guild_id: str):
    try:
        logging.info(f"Starting raid result data collection for guild {guild_id}.")
        guild = comlink.get_guild(guild_id=guild_id, include_recent_guild_activity_info=True)
        guild_raid_points = []

        for raid_result in guild["recentRaidResult"]:
            for member in raid_result["raidMember"]:
                guild_raid_points.append(
                    {
                        "guild_id": guild_id,
                        "raid_id": raid_result["raidId"],
                        "player_id": member["playerId"],
                        "points": member["memberProgress"],
                        "date": datetime.fromtimestamp(int(raid_result["endTime"])).strftime('%Y%m%d')
                    }
                )
        logging.info(f"Successfully collected raid result data for {len(guild_raid_points)} members.")
        return pd.DataFrame(guild_raid_points)
    except Exception as e:
        logging.error(f"Error collecting raid result data for guild {guild_id}: {e}")
        raise


def main(guild_id: str):
    try:
        logging.info(f"Starting main execution for guild {guild_id}.")

        df_tickets = get_tickets(guild_id=guild_id)
        df_player = get_player_meta(players=df_tickets["player_id"].to_list())
        df_guild = get_guild_meta(guild_id=guild_id)
        df_raid_result = get_raid_result(guild_id=guild_id)

        logging.info("Connecting to the database.")
        engine = create_engine(os.getenv('DATABASE_URL'))

        with engine.begin() as connection:
            logging.info("Inserting data into the database.")
            df_guild.to_sql("stg_guild", con=connection, if_exists="replace", index=False)
            df_player.to_sql("stg_player", con=connection, if_exists="replace", index=False)
            df_tickets.to_sql("stg_tickets", con=connection, if_exists="replace", index=False)
            df_raid_result.to_sql("stg_raid_result", con=connection, if_exists='replace', index=False)

            connection.execute(text("CALL insert_guilds()"))
            connection.execute(text("CALL upsert_players()"))
            connection.execute(text("CALL insert_tickets()"))
            connection.execute(text("CALL insert_raid_result()"))

        logging.info("Data successfully inserted into the database.")
    except Exception as e:
        logging.error(f"Error during main execution for guild {guild_id}: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SWGOH Guild Data Collection")
    parser.add_argument('guild_id', type=str)
    args = parser.parse_args()

    logging.info(f"Starting process for guild {args.guild_id}.")
    main(args.guild_id)
    logging.info(f"Process for guild {args.guild_id} completed.")
