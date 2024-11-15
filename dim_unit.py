import requests
import pandas as pd
import os
from sqlalchemy import create_engine
from logger import setup_logging
from dotenv import load_dotenv

logger = setup_logging()
load_dotenv()

url = "https://swgoh.gg/api/units/"

if __name__ == "__main__":
    try:
        logger.info("Script execution started.")

        engine = create_engine(os.getenv('DATABASE_URL'))
        logger.info("Database engine created successfully.")

        with engine.begin() as connection:
            logger.info("Database connection established. Fetching data from API.")

            response = requests.get(url)
            if response.status_code == 200:
                logger.info("Data retrieved successfully from API.")
                data = response.json()

                units = [
                    {
                        "base_id": unit.get("base_id"),
                        "name": unit.get("name"),
                        "url": unit.get("url"),
                        "image": unit.get("image"),
                        "description": unit.get("description"),
                        "combat_type": "char" if unit.get("combat_type") == 1 else "ship" if unit.get(
                            "combat_type") == 2 else unit.get("combat_type")
                    }
                    for unit in data.get("data", [])
                ]

                df = pd.DataFrame(units)
                logger.info(f"Data processed successfully. Total units processed: {len(df)}")
            else:
                logger.error(f"Failed to retrieve data from API. Status code: {response.status_code}")
                raise Exception(f"API request failed with status code {response.status_code}")

            logger.info("Uploading data to the database table 'd_unit'.")
            df.to_sql("d_unit", con=connection, if_exists="replace", index=False)
            logger.info("Data uploaded successfully to 'd_unit'.")

    except Exception as e:
        logger.error(f"An error occurred during script execution: {e}")
