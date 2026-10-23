import os
import logging
from typing import Optional, Any
# pyrefly: ignore[missing-import]
from sqlalchemy import create_engine
# pyrefly: ignore[missing-import]
from sqlalchemy.engine import Engine

logger = logging.getLogger("airflow.task")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class DatabaseConnector:
    """Reusable Database Connector for creating and managing SQLAlchemy engines."""

    def __init__(
        self,
        db_type: str,
        user: str,
        password: str,
        host: str,
        port: int | str,
        database: str,
        driver: Optional[str] = None,
    ):
        self.db_type = db_type.lower()
        self.user = user
        self.password = password
        self.host = host
        self.port = str(port)
        self.database = database
        self.driver = driver

    def get_connection_uri(self) -> str:
        """Construct database connection URI based on database type and driver."""
        if self.db_type in ("postgres", "postgresql"):
            dialect = f"postgresql+{self.driver}" if self.driver else "postgresql+psycopg2"
            return f"{dialect}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif self.db_type == "mysql":
            dialect = f"mysql+{self.driver}" if self.driver else "mysql+pymysql"
            return f"{dialect}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif self.db_type == "clickhouse":
            dialect = f"clickhouse+{self.driver}" if self.driver else "clickhouse+connect"
            return f"{dialect}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        else:
            raise ValueError(f"Unsupported database type: '{self.db_type}'")

    def build_engine(
        self,
        pool_size: int = 5,
        pool_pre_ping: bool = True,
        max_overflow: int = 10,
        **engine_kwargs: Any,
    ) -> Engine:
        """Instantiate SQLAlchemy Engine and verify connection liveness."""
        try:
            logger.info(
                "Configuring SQLAlchemy engine for %s at host: %s, port: %s, database: %s",
                self.db_type,
                self.host,
                self.port,
                self.database,
            )

            uri = self.get_connection_uri()
            engine = create_engine(
                uri,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_pre_ping=pool_pre_ping,
                **engine_kwargs,
            )

            # Test connection health
            with engine.connect() as connection:
                logger.info(
                    "Database connection test succeeded for %s (%s:%s/%s).",
                    self.db_type,
                    self.host,
                    self.port,
                    self.database,
                )

            return engine

        except Exception as e:
            logger.error(
                "Failed to establish database connection for %s at %s:%s/%s. Error: %s",
                self.db_type,
                self.host,
                self.port,
                self.database,
                str(e),
                exc_info=True,
            )
            raise


def db_warehouse_engine(**kwargs: Any) -> Engine:
    """Factory function to build and return PostgreSQL Data Warehouse SQLAlchemy Engine."""
    try:
        user = os.getenv("POSTGRES_DW_USER", "de_admin")
        password = os.getenv("POSTGRES_DW_PASSWORD", "secure_postgres_password_2026")
        host = os.getenv("POSTGRES_DW_HOST", "dw_postgres")
        port = os.getenv("POSTGRES_DW_PORT_INTERNAL", "5432")
        database = os.getenv("POSTGRES_DW_DB", "toll_learning_dw")

        connector = DatabaseConnector(
            db_type="postgresql",
            user=user,
            password=password,
            host=host,
            port=port,
            database=database,
            driver="psycopg2",
        )
        return connector.build_engine(**kwargs)

    except Exception as e:
        logger.error("Failed to initialize Data Warehouse engine: %s", str(e), exc_info=True)
        raise
