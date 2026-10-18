"""Real PostgreSQL integration tests, isolated in disposable schemas."""

import importlib.util
import os
import unittest
from uuid import uuid4

HAS_POSTGRES = bool(
    importlib.util.find_spec("psycopg") and importlib.util.find_spec("psycopg_pool")
)
if HAS_POSTGRES:
    from psycopg import AsyncConnection, sql
    from psycopg.conninfo import make_conninfo

    from agent_runtime.storage import DatabaseConfig
    from agent_runtime.storage.postgres import PostgresStore


class PostgresCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        dsn = os.getenv("AGENT_TEST_DB_URL")
        if not HAS_POSTGRES or not dsn:
            self.skipTest("Install the postgres extra and set AGENT_TEST_DB_URL")
        self.schema = "runtime_test_" + uuid4().hex
        self.admin = await AsyncConnection.connect(dsn, autocommit=True)
        await self.admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.schema)))
        self.dsn = make_conninfo(dsn, options=f"-c search_path={self.schema}")
        self.stores = []
        self.addAsyncCleanup(self.cleanup_database)
        self.store = await self.open_store()
        await self.store.install_schema()

    async def open_store(self):
        store = await PostgresStore.connect(DatabaseConfig(self.dsn, pool_max_size=3))
        self.stores.append(store)
        return store

    async def cleanup_database(self):
        for store in self.stores:
            await store.close()
        await self.admin.execute(
            sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema))
        )
        await self.admin.close()
