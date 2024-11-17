import os
import discord
import time
from logger import setup_logging
from dotenv import load_dotenv
from discord.ext import tasks
from datetime import datetime, timedelta
from bot_utils import plot_ticket_report, get_tickets_missed, format_embed
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

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


DATABASE_URL = os.getenv('DATABASE_URL')  # Obtenha a URL do banco de dados do .env
engine = create_engine(DATABASE_URL)

# Definir a base para o modelo ORM
Base = declarative_base()

# Definindo o modelo de dados para mensagens
class Message(Base):
    __tablename__ = 'discord_messages'

    id = Column(Integer, primary_key=True)
    channel = Column(String(255), nullable=False)
    user_id = Column(String(255), nullable=False)
    user_name = Column(String(255), nullable=False)
    nickname = Column(String(255), nullable=True)
    message_content = Column(Text, nullable=False)
    timestamp = Column(DateTime, nullable=False)

Base.metadata.create_all(engine)

# Criar uma sessão para interagir com o banco de dados
Session = sessionmaker(bind=engine)
session = Session()

# Função para adicionar uma mensagem ao banco de dados
def add_message_to_db(channel, user_id, user_name, nickname, message, timestamp):
    new_message = Message(
        channel=channel,
        user_id=user_id,
        user_name=user_name,
        nickname=nickname,
        message_content=message,
        timestamp=timestamp
    )
    session.add(new_message)
    session.commit()

@tasks.loop(minutes=1)
async def load_messages(bot):
    """Task that collects and logs messages from the past day across all text channels."""
    now = datetime.now()
    if now.hour == 21 and now.minute == 10:
        logger.info("Starting message collection task.")
        d_minus_1 = datetime.now() - timedelta(days=1)
        start_time2 = datetime(d_minus_1.year, d_minus_1.month, d_minus_1.day, 0, 0, 0)
        end_time = datetime(d_minus_1.year, d_minus_1.month, d_minus_1.day, 23, 59, 59)

        start_time = time.time()
        total_messages_collected = 0

        for guild in bot.guilds:
            logger.info(f"Collecting messages for guild: {guild.name} ({guild.id})")
            for channel in guild.text_channels:
                try:
                    async for msg in channel.history(after=start_time2, before=end_time, limit=None):
                        member = guild.get_member(msg.author.id)
                        nickname = member.nick if member and member.nick else msg.author.name
                        add_message_to_db(channel.name, msg.author.id, msg.author.name, nickname, msg.content, msg.created_at)
                        total_messages_collected += 1
                except Exception as e:
                    logger.error(f"Error reading messages from channel {channel.name}: {e}")

        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Message collection task completed. Total messages collected: {total_messages_collected}")
        logger.info(f"Duration of the process: {duration:.2f} seconds")