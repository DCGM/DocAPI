import argparse
import logging
import sys
import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.exc import ProgrammingError
from metakat.app.config import config

logger = logging.getLogger(__name__)

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--logging-level', default=logging.INFO)
    args = parser.parse_args()
    return args

def main():
    """Creates the database asynchronously if it doesn't exist."""
    args = parse_arguments()

    logging.basicConfig(
        level=args.logging_level,
        format="DB CREATE - %(asctime)s - %(filename)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    logger.info(' '.join(sys.argv))

    asyncio.run(create_database_async())

async def create_database_async():

    # Extract database name and create an engine for the default database
    db_url = config.DATABASE_URL
    db_name = db_url.rsplit("/", 1)[-1]  # Extracts the database name
    db_url_root = db_url.rsplit("/", 1)[0] + "/postgres"  # Connect to default DB (PostgreSQL)

    engine = create_async_engine(db_url_root)

    async with engine.begin() as conn:
        try:
            await conn.execute(text("COMMIT"))
            await conn.execute(text(f"CREATE DATABASE {db_name}"))
            logger.info(f"Database '{db_name}' created successfully!")
        except ProgrammingError:
            logger.info(f"Database '{db_name}' already exists or cannot be created")

    await engine.dispose()

if __name__ == "__main__":
    main()
