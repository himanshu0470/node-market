from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from sqlalchemy.pool import QueuePool, NullPool

DATABASE_URL = "postgresql://postgres:Score12345@34.172.208.183:5432/UATDB"
#DATABASE_URL = "postgresql+psycopg2://score23dec:score54321@35.176.50.206:5432/panel"
engine = create_engine(
    DATABASE_URL,
    echo=True,
    poolclass=NullPool,  # Change the pool class to NullPool
    connect_args={
        "sslmode": "require",  # Ensuring SSL is still required
    },
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@contextmanager
def get_db_session():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Session rollback due to: {e}")
        raise
    finally:
        session.close()

DB_CONNECTION=None