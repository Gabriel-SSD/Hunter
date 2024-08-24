import pandas as pd
from sqlalchemy import create_engine, text
from swgoh_comlink import SwgohComlink
from dotenv import load_dotenv
from datetime import datetime, timezone
import os

load_dotenv()
comlink = SwgohComlink()

def get_player_meta(players:list):
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
    return pd.DataFrame(player_data)


def get_guild_meta(guild_id:str):
    guild = comlink.get_guild(guild_id=guild_id, include_recent_guild_activity_info=False)
    return pd.DataFrame([
        {
            "guild_id": guild_id,
            "name": guild["profile"]["name"],
            "guild_reset": datetime.fromtimestamp(int(guild["nextChallengesRefresh"]), tz=timezone.utc).strftime('%H:%M:%S')
        }
    ])

def get_tickets(guild_id:str):
    
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
    
    return pd.DataFrame(guild_tickets)

guild_id = "iO-khl_0TVu64OussT1Y7g" if datetime.now().hour > 22 else "1HE3bh3LRcWVOto5KuGvzQ"

df_tickets = get_tickets(guild_id=guild_id)
df_player = get_player_meta(players=df_tickets["player_id"].to_list())
df_guild = get_guild_meta(guild_id=guild_id)

engine = create_engine(os.getenv('DATABASE_URL'))

with engine.begin() as connection:
    df_guild.to_sql("stg_guild", con=connection, if_exists="replace", index=False)
    df_player.to_sql("stg_player", con=connection, if_exists="replace", index=False)
    df_tickets.to_sql("stg_tickets", con=connection, if_exists="replace", index=False)

    connection.execute(text("CALL insert_guilds()"))
    connection.execute(text("CALL upsert_players()"))
    connection.execute(text("CALL insert_tickets()"))