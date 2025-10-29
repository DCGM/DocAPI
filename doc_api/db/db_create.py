import argparse
import logging
import sys
import asyncio

from sqlalchemy import text, NullPool, make_url
from sqlalchemy.ext.asyncio import create_async_engine
from doc_api.config import config

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

    asyncio.run(create_database_if_does_not_exist())


async def create_database_if_does_not_exist():
    url = make_url(config.DATABASE_URL)
    db_name = url.database or "postgres"
    root_url = url.set(database="postgres").render_as_string(hide_password=False)

    # Use a short-lived engine with NO pooling
    engine = create_async_engine(root_url, poolclass=NullPool)

    try:
        async with engine.connect() as conn:
            # CREATE DATABASE must be outside of any transaction
            await conn.execution_options(isolation_level="AUTOCOMMIT")

            exists = await conn.scalar(
                text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": db_name},
            )
            if not exists:
                # Quote the identifier safely (double any double-quotes)
                safe = db_name.replace('"', '""')
                await conn.execute(text(f'CREATE DATABASE "{safe}"'))
                logger.info(f"Database '{db_name}' created.")
            else:
                logger.info(f"Database '{db_name}' exists.")
    finally:
        # Ensure everything is torn down before the loop ends
        await engine.dispose()


if __name__ == "__main__":
    main()
