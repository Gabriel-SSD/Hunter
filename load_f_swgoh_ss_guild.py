from sqlalchemy import create_engine
from swgoh_comlink import SwgohComlink
from dotenv import load_dotenv
from logger import setup_logging
from datetime import datetime, timezone
import os
import pandas as pd

logger = setup_logging()
load_dotenv()
comlink = SwgohComlink()


def get_guild_meta(guild_ids: list):
    try:
        all_guild_data = []
        current_date = int(datetime.now(timezone.utc).strftime('%Y%m%d'))

        for guild_id in guild_ids:
            try:
                logger.info(f"Starting guild data collection for guild {guild_id}.")
                guild = comlink.get_guild(guild_id=guild_id, include_recent_guild_activity_info=True)

                total_character_gp = 0
                total_ship_gp = 0

                for member in guild.get("member", []):
                    try:
                        total_character_gp += int(member.get("characterGalacticPower", "0"))
                        total_ship_gp += int(member.get("shipGalacticPower", "0"))
                    except ValueError as ve:
                        logger.warning(f"Error parsing GP for a member in guild {guild_id}: {ve}")

                logger.info(f"Successfully collected data for guild {guild_id}.")

                all_guild_data.append({
                    "guild_id": guild_id,
                    "name": guild["profile"]["name"],
                    "description": guild["profile"]["externalMessageKey"],
                    "member_count": guild["profile"]["memberCount"],
                    "banner_color": guild["profile"]["bannerColorId"],
                    "banner_logo": guild["profile"]["bannerLogoId"],
                    "guild_reset": datetime.fromtimestamp(int(guild["nextChallengesRefresh"]), tz=timezone.utc).strftime(
                        '%H:%M:%S'),
                    "char_gp": total_character_gp,
                    "ship_gp": total_ship_gp,
                    "gp": total_character_gp + total_ship_gp,
                    "date": current_date
                })
            except Exception as e:
                logger.error(f"Error collecting data for guild {guild_id}: {e}")

        return pd.DataFrame(all_guild_data)

    except Exception as e:
        logger.error(f"Error during guild data collection: {e}")
        raise


if __name__ == "__main__":
    try:
        logger.info("Script execution started.")
        engine = create_engine(os.getenv('DATABASE_URL'))
        logger.info("Database engine created successfully.")

        with engine.begin() as connection:
            logger.info("Database connection established. Processing guild data.")
            # Fetch guilds and process them for tracking
            df = get_guild_meta(guild_ids=["iO-khl_0TVu64OussT1Y7g", "1HE3bh3LRcWVOto5KuGvzQ"])
            logger.info("Uploading guild data to the database.")
            # Upload the processed DataFrame to the database table `stg_swgoh_ss_guild`
            df.to_sql("f_swgoh_ss_guild", con=connection, if_exists="append", index=False)
            logger.info("Data successfully inserted into the database.")
    except Exception as e:
        logger.error(f"An error occurred during script execution: {e}")
