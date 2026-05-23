from celery import Celery

from app.core.config import settings


celery_app = Celery(
    "ai_data_engineer",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=["app.tasks.connections"],
)

celery_app.conf.update(
    broker_connection_retry_on_startup=True,
    task_acks_late=True,
    task_default_queue="default",
    worker_prefetch_multiplier=1,
    timezone="UTC",
    task_routes={
        "connections.refresh_statuses": {"queue": "connections"},
    },
    beat_schedule={
        "refresh-database-connections-every-minute": {
            "task": "connections.refresh_statuses",
            "schedule": settings.connection_health_check_interval_seconds,
        },
    },
)
