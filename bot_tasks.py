import os
import discord
import time
import pandas as pd
from logger import setup_logging
from dotenv import load_dotenv
from discord.ext import tasks
from datetime import datetime, timedelta
from bot_utils import plot_ticket_report, get_tickets_missed, format_embed
from sqlalchemy import create_engine, text

logger = setup_logging()
load_dotenv()
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
FILE = "./data/tickets.png"


@tasks.loop(minutes=1)
async def af_tickets(bot):
    """Task that sends the ticket report for 'Awakening Fear' guild at 17:31 daily."""
    now = datetime.now()
    if now.hour == 17 and now.minute == 31:
        try:
            logger.info(f"Sending ticket report for 'Awakening Fear' guild at {now}.")
            channel = bot.get_channel(CHANNEL_ID)
            plot_ticket_report("1HE3bh3LRcWVOto5KuGvzQ")
            await channel.send(file=discord.File(fp=FILE))
            logger.info("Ticket report for 'Awakening Fear' sent successfully.")
        except Exception as e:
            logger.error(f"Error sending ticket report for 'Awakening Fear' at {now}: {e}")


@tasks.loop(minutes=1)
async def ah_tickets(bot):
    """Task that sends the ticket report for 'Awakening Hope' guild at 22:31 daily."""
    now = datetime.now()
    if now.hour == 22 and now.minute == 31:
        try:
            logger.info(f"Sending ticket report for 'Awakening Hope' guild at {now}.")
            channel = bot.get_channel(CHANNEL_ID)
            plot_ticket_report("iO-khl_0TVu64OussT1Y7g")
            await channel.send(file=discord.File(fp=FILE))
            df = get_tickets_missed("iO-khl_0TVu64OussT1Y7g", '0')
            embed = format_embed(df, "Awakening Hope", '0')
            await channel.send(embed=embed)
            logger.info("Ticket report and missed tickets report for 'Awakening Hope' sent successfully.")
        except Exception as e:
            logger.error(f"Error sending ticket report for 'Awakening Hope' at {now}: {e}")


@tasks.loop(minutes=1)
async def af_tickets_missed(bot):
    """Task that sends the missed tickets report for 'Awakening Fear' guild at 17:32 every Sunday."""
    now = datetime.now()
    if now.weekday() == 6 and now.hour == 17 and now.minute == 32:
        try:
            logger.info(f"Sending missed tickets report for 'Awakening Fear' guild at {now}.")
            channel = bot.get_channel(CHANNEL_ID)
            df = get_tickets_missed("1HE3bh3LRcWVOto5KuGvzQ", '7')
            embed = format_embed(df, "Awakening Fear", '7')
            await channel.send(embed=embed)
            logger.info("Missed tickets report for 'Awakening Fear' sent successfully.")
        except Exception as e:
            logger.error(f"Error sending missed tickets report for 'Awakening Fear' at {now}: {e}")


@tasks.loop(minutes=1)
async def ah_tickets_missed(bot):
    """Task that sends the missed tickets report for 'Awakening Hope' guild at 22:32 every Sunday."""
    now = datetime.now()
    if now.weekday() == 6 and now.hour == 22 and now.minute == 32:
        try:
            logger.info(f"Sending missed tickets report for 'Awakening Hope' guild at {now}.")
            channel = bot.get_channel(CHANNEL_ID)
            df = get_tickets_missed("iO-khl_0TVu64OussT1Y7g", '7')
            embed = format_embed(df, "Awakening Hope", '7')
            await channel.send(embed=embed)
            logger.info("Missed tickets report for 'Awakening Hope' sent successfully.")
        except Exception as e:
            logger.error(f"Error sending missed tickets report for 'Awakening Hope' at {now}: {e}")



@tasks.loop(minutes=1)
async def load_messages(bot):
    """Task that collects and logs messages from the past day across all text channels."""
    now = datetime.now()


    if now.hour == 23 and now.minute == 56:
        logger.info(f"Starting the message collection task. Current time: {now.strftime('%Y-%m-%d %H:%M:%S')}")
        engine = create_engine(os.getenv('DATABASE_URL'))

        guild = bot.get_guild(1193323906015187044)
        if guild is None:
            logger.error("Guild not found!")
            return
        logger.info(f"Connected to server: {guild.name} (ID: {guild.id})")

        logger.info("Collecting channel data...")
        channels_data = []
        for channel in guild.channels:
            channel_info = {
                'channel_id': channel.id,
                'channel_name': channel.name,
                'channel_type': str(channel.type),
                'topic': channel.topic if isinstance(channel, discord.TextChannel) else None,
                'nsfw': channel.nsfw if isinstance(channel, discord.TextChannel) else None,
                'user_limit': channel.user_limit if isinstance(channel, discord.VoiceChannel) else None,
                'bitrate': channel.bitrate if isinstance(channel, discord.VoiceChannel) else None,
                'category': channel.name if isinstance(channel, discord.CategoryChannel) else None
            }
            channels_data.append(channel_info)

        if channels_data:
            logger.info(f"Found {len(channels_data)} channels. Inserting into database.")
            df_channels = pd.DataFrame(channels_data)
            df_channels.to_sql('stg_disc_channels', con=engine, if_exists='replace', index=False)
            logger.info("Channel data inserted into 'stg_disc_channels' successfully.")
        else:
            logger.warning("No channel data found to insert.")

        logger.info("Collecting member data...")
        members_data = []
        for member in guild.members:
            member_info = {
                'member_id': member.id,
                'username': member.name,
                'display_name': member.display_name,
                'joined_at': member.joined_at,
            }
            members_data.append(member_info)

        if members_data:
            logger.info(f"Found {len(members_data)} members. Inserting into database.")
            df_members = pd.DataFrame(members_data)
            df_members.to_sql('stg_disc_members', con=engine, if_exists='replace', index=False)
            logger.info("Member data inserted into 'stg_disc_members' successfully.")
        else:
            logger.warning("No member data found to insert.")

        with engine.connect() as connection:
            try:
                logger.info("Calling database function 'insert_discord_channels' to insert channel data.")
                connection.execute(text("CALL insert_discord_channels()"))
                logger.info("Database function 'insert_discord_channels' called successfully.")
            except Exception as e:
                logger.error(f"Error calling database function 'insert_discord_channels': {e}")

            try:
                logger.info("Calling database function 'insert_discord_members' to insert member data.")
                connection.execute(text("CALL insert_discord_members()"))
                logger.info("Database function 'insert_discord_members' called successfully.")
            except Exception as e:
                logger.error(f"Error calling database function 'insert_discord_members': {e}")

        logger.info("Starting message collection from text channels...")
        d_minus_1 = datetime.now() - timedelta(days=1)
        start_time2 = datetime(d_minus_1.year, d_minus_1.month, d_minus_1.day, 0, 0, 0)
        end_time = datetime(d_minus_1.year, d_minus_1.month, d_minus_1.day, 23, 59, 59)

        start_time = time.time()
        total_messages_collected = 0

        if guild is None:
            logger.error("Guild not found!")
        else:
            logger.info(f"Collecting messages for server: {guild.name} ({guild.id})")
            messages_to_insert = []
            for channel in guild.text_channels:
                try:
                    logger.info(f"Collecting messages from channel: {channel.name}")
                    async for msg in channel.history(after=start_time2, before=end_time, limit=None):
                        member = guild.get_member(msg.author.id)
                        nickname = member.nick if member and member.nick else msg.author.name
                        messages_to_insert.append({
                            "channel": channel.name,
                            "user_id": msg.author.id,
                            "user_name": msg.author.name,
                            "nickname": nickname,
                            "message_content": msg.content,
                            "timestamp": msg.created_at
                        })
                        total_messages_collected += 1
                except Exception as e:
                    logger.error(f"Error reading messages from channel {channel.name}: {e}")

            if messages_to_insert:
                logger.info(f"Inserting {total_messages_collected} messages into 'stg_disc_messages'.")
                df = pd.DataFrame(messages_to_insert)
                df.to_sql('stg_disc_messages', engine, if_exists='replace', index=False)

                with engine.connect() as connection:
                    try:
                        logger.info("Calling database function 'insert_discord_messages' to insert message data.")
                        connection.execute(text("CALL insert_discord_messages()"))
                        logger.info("Database function 'insert_discord_messages' called successfully.")
                    except Exception as e:
                        logger.error(f"Error calling database function 'insert_discord_messages': {e}")
            else:
                logger.warning("No messages found to insert.")

        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Message collection task completed. Total messages collected: {total_messages_collected}")
        logger.info(f"Duration of the process: {duration:.2f} seconds")