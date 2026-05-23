from app.tasks.connections import refresh_connection_statuses
from app.worker import celery_app


def test_celery_routes_connection_health_checks_to_connections_queue():
    assert celery_app.conf.task_routes["connections.refresh_statuses"]["queue"] == "connections"
    assert (
        celery_app.conf.beat_schedule["refresh-database-connections-every-minute"]["task"]
        == "connections.refresh_statuses"
    )


def test_connection_health_check_task_handles_empty_selection():
    result = refresh_connection_statuses.run([])

    assert result == {"checked": 0, "connections": []}
