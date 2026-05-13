from datetime import UTC, datetime, timedelta
from random import Random

from sqlalchemy.exc import OperationalError
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import settings
from app.core.security import hash_password
from app.db.base import Base
from app.models import User


connect_args = {"check_same_thread": False} if settings.database_url.startswith("sqlite") else {}
engine = create_async_engine(settings.database_url, echo=False, future=True, connect_args=connect_args)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


def db_timestamp(value: datetime) -> datetime | str:
    normalized = value.replace(tzinfo=None)
    if settings.database_url.startswith("sqlite"):
        return normalized.isoformat(sep=" ", timespec="seconds")
    return normalized


async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session


async def init_db() -> None:
    if settings.auto_create_tables:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS orders (
                        id INTEGER PRIMARY KEY,
                        created_at TIMESTAMP NOT NULL,
                        user_id INTEGER NOT NULL,
                        total_amount NUMERIC NOT NULL,
                        status VARCHAR(32) NOT NULL
                    )
                    """
                )
            )
            await ensure_sqlite_artifact_columns(conn)
            await conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS events (
                        id INTEGER PRIMARY KEY,
                        created_at TIMESTAMP NOT NULL,
                        user_id INTEGER NOT NULL,
                        event_name VARCHAR(64) NOT NULL,
                        payload TEXT
                    )
                    """
                )
            )
            await conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS customers (
                        id INTEGER PRIMARY KEY,
                        email VARCHAR(255) NOT NULL,
                        segment VARCHAR(64) NOT NULL,
                        created_at TIMESTAMP NOT NULL
                    )
                    """
                )
            )

    async with AsyncSessionLocal() as session:
        await seed_admin(session)
        await seed_demo_data(session)


async def close_db() -> None:
    await engine.dispose()


async def ensure_sqlite_artifact_columns(conn) -> None:
    if not settings.database_url.startswith("sqlite"):
        return

    statements = [
        "ALTER TABLE artifact_versions ADD COLUMN git_status VARCHAR(32) DEFAULT 'unknown'",
        "ALTER TABLE artifact_versions ADD COLUMN git_repository TEXT",
        "ALTER TABLE artifact_versions ADD COLUMN git_commit_sha VARCHAR(64)",
        "ALTER TABLE artifact_versions ADD COLUMN git_commit_short_sha VARCHAR(16)",
        "ALTER TABLE artifact_versions ADD COLUMN git_error TEXT",
    ]
    for statement in statements:
        try:
            await conn.execute(text(statement))
        except OperationalError:
            continue


async def seed_admin(session: AsyncSession) -> None:
    existing = await session.scalar(select(User).where(User.email == "admin@local.dev"))
    if existing:
        return
    session.add(
        User(
            email="admin@local.dev",
            full_name="Pavel Ivanov",
            role="admin",
            status="active",
            password_hash=hash_password("admin"),
        )
    )
    await session.commit()


async def seed_demo_data(session: AsyncSession) -> None:
    order_count = await session.scalar(text("SELECT COUNT(*) FROM orders"))
    if order_count and int(order_count) > 0:
        return

    rnd = Random(42)
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    rows: list[dict] = []
    row_id = 1
    for hour in range(24 * 30):
        ts = now - timedelta(hours=hour)
        base = 120 + (ts.hour * 3) + rnd.randint(-25, 25)
        if hour == 17:
            base = 980
        if hour in (84, 85):
            base = 0
        for _ in range(max(base, 0)):
            rows.append(
                {
                    "id": row_id,
                    "created_at": db_timestamp(ts),
                    "user_id": rnd.randint(1, 180),
                    "total_amount": round(rnd.uniform(8, 450), 2),
                    "status": "paid" if rnd.random() > 0.08 else "cancelled",
                }
            )
            row_id += 1

    await session.execute(
        text(
            """
            INSERT INTO orders (id, created_at, user_id, total_amount, status)
            VALUES (:id, :created_at, :user_id, :total_amount, :status)
            """
        ),
        rows[:5000],
    )
    await session.execute(
        text(
            """
            INSERT INTO customers (id, email, segment, created_at)
            VALUES (:id, :email, :segment, :created_at)
            """
        ),
        [
            {
                "id": idx,
                "email": f"user{idx}@example.com",
                "segment": ["retail", "b2b", "vip"][idx % 3],
                "created_at": db_timestamp(now - timedelta(days=idx % 120)),
            }
            for idx in range(1, 181)
        ],
    )
    await session.execute(
        text(
            """
            INSERT INTO events (id, created_at, user_id, event_name, payload)
            VALUES (:id, :created_at, :user_id, :event_name, :payload)
            """
        ),
        [
            {
                "id": idx,
                "created_at": db_timestamp(now - timedelta(minutes=idx * 7)),
                "user_id": rnd.randint(1, 180),
                "event_name": ["page_view", "checkout", "search", "add_to_cart"][idx % 4],
                "payload": "{}",
            }
            for idx in range(1, 600)
        ],
    )
    await session.commit()
