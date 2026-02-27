import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus
from typing import Optional, Dict, Any
from contextlib import contextmanager
import logging

from database.models import (
    Base, 
)

logger = logging.getLogger(__name__)


class DatabaseConnector:
    """Enhanced database connector with CDC support"""
    
    def __init__(self, username: str, password: str, host: str, port: int, database: str = None):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.engine = None
        self.SessionLocal = None

    def get_db_string(self, with_db: bool = True) -> str:
        """Generate database connection string"""
        encoded_username = quote_plus(self.username)
        encoded_password = quote_plus(self.password)

        if with_db and self.database:
            return f"mysql+pymysql://{encoded_username}:{encoded_password}@{self.host}:{self.port}/{self.database}"
        else:
            return f"mysql+pymysql://{encoded_username}:{encoded_password}@{self.host}:{self.port}/"

    def create_database_if_not_exists(self):
        """Create database if it doesn't exist"""
        try:
            engine = create_engine(self.get_db_string(with_db=False))
            with engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{self.database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))
                conn.commit()
            engine.dispose()
            logger.info(f"Database '{self.database}' ensured to exist")
        except Exception as e:
            logger.error(f"Error creating database: {e}")
            raise

    def get_engine(self):
        """Get or create SQLAlchemy engine"""
        if not self.engine:
            self.engine = create_engine(
                self.get_db_string(),
                pool_pre_ping=True,  # Enable connection health checks
                pool_recycle=3600,   # Recycle connections after 1 hour
                pool_size=10,        # Connection pool size
                max_overflow=20,     # Max overflow connections
                echo=False,          # Set to True for SQL debugging
                isolation_level="READ COMMITTED"
            )
            logger.info(f"Database engine created for '{self.database}'")
        return self.engine

    def create_tables(self):
        """Create all tables defined in models"""
        try:
            engine = self.get_engine()
            Base.metadata.create_all(engine)
            logger.info("All tables created successfully")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise

    def drop_all_tables(self):
        """Drop all tables (use with caution!)"""
        try:
            engine = self.get_engine()
            Base.metadata.drop_all(engine)
            logger.warning("All tables dropped")
        except Exception as e:
            logger.error(f"Error dropping tables: {e}")
            raise

    def get_session(self) -> Session:
        """Get a new database session"""
        if not self.SessionLocal:
            engine = self.get_engine()
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=engine
            )
        return self.SessionLocal()

    @contextmanager
    def session_scope(self):
        """
        Provide a transactional scope with automatic commit/rollback
        
        Usage:
            with db_connector.session_scope() as session:
                session.add(object)
                # Automatically commits on success, rolls back on exception
        """
        session = self.get_session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Session rollback due to error: {e}")
            raise
        finally:
            session.close()

    def execute_raw_sql(self, sql: str, params: Dict[str, Any] = None) -> Any:
        """Execute raw SQL query"""
        try:
            with self.session_scope() as session:
                result = session.execute(text(sql), params or {})
                return result
        except Exception as e:
            logger.error(f"Error executing raw SQL: {e}")
            raise

    def test_connection(self) -> bool:
        """Test database connectivity"""
        try:
            with self.session_scope() as session:
                session.execute(text("SELECT 1"))
            logger.info("Database connection test successful")
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False

    def get_table_row_count(self, table_name: str) -> int:
        """Get row count for a specific table"""
        try:
            with self.session_scope() as session:
                result = session.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.scalar()
                return count
        except Exception as e:
            logger.error(f"Error getting row count for {table_name}: {e}")
            return 0

    def close(self):
        """Close database connections"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database engine disposed")