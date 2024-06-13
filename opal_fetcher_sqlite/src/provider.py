# Imports
from typing import Optional, List

import sqlite3

from pydantic import BaseModel, Field
from opal_common.fetcher.fetch_provider import BaseFetchProvider
from opal_common.fetcher.events import FetcherConfig, FetchEvent
from opal_common.logger import logger


# Configuration for SQLite connection
class SQLiteConnectionParams(BaseModel):
    """
    Parameters for connecting to an SQLite database.
    """

    database: str = Field(..., description="The path to the SQLite database file")


# Configuration for the SQLite Fetcher
class SQLiteFetcherConfig(FetcherConfig):
    """
    Config for SQLiteFetchProvider, instance of `FetcherConfig`.

    Contains parameters for fetching data from an SQLite database.
    """

    fetcher: str = "SQLiteFetchProvider"
    connection_params: Optional[SQLiteConnectionParams] = Field(
        None, description="Parameters for connecting to the SQLite database"
    )
    query: str = Field(..., description="The SQL query to fetch data from the database")


# Event shape for the SQLite Fetch Provider
class SQLiteFetchEvent(FetchEvent):
    """
    A FetchEvent shape for the SQLite Fetch Provider.

    Contains configuration specific to fetching data from an SQLite database.
    """

    fetcher: str = "SQLiteFetchProvider"
    config: SQLiteFetcherConfig = None


# SQLite Fetch Provider implementation
class SQLiteFetchProvider(BaseFetchProvider):
    """
    An OPAL fetch provider for SQLite databases.

    Fetches data from an SQLite database using a provided SQL query.
    """

    def __init__(self, event: SQLiteFetchEvent) -> None:
        if event.config is None:
            event.config = SQLiteFetcherConfig()
        super().__init__(event)
        self._connection: Optional[sqlite3.Connection] = None

    def parse_event(self, event: FetchEvent) -> SQLiteFetchEvent:
        return SQLiteFetchEvent(**event.dict(exclude={"config"}), config=event.config)

    async def __aenter__(self):
        self._event: SQLiteFetchEvent

        database_path = self._event.config.connection_params.database
        if database_path is None:
            raise ValueError(
                "SQLite database path is required in connection parameters"
            )

        self._connection = sqlite3.connect(database_path)
        return self

    async def __aexit__(self, exc_type=None, exc_val=None, tb=None) -> None:
        if self._connection is not None:
            self._connection.close()

    async def _fetch_(self) -> List[tuple]:
        self._event: SQLiteFetchEvent  # type casting

        if self._event.config is None:
            logger.warning(
                "Incomplete fetcher config: SQLite data entries require a query to specify what data to fetch!"
            )
            return []

        logger.debug(f"{self.__class__.__name__} fetching from SQLite database")

        cursor = await self._connection.cursor()
        await cursor.execute(self._event.config.query)
        results = await cursor.fetchall()
        return results
