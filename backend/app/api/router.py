from fastapi import APIRouter

from app.api.routes import agent, airflow, artifacts, auth, catalog, connections, health, pipelines, sessions, spark, sql, users

api_router = APIRouter()
api_router.include_router(health.router)
api_router.include_router(auth.router)
api_router.include_router(users.router)
api_router.include_router(agent.router)
api_router.include_router(sessions.router)
api_router.include_router(catalog.router)
api_router.include_router(connections.router)
api_router.include_router(sql.router)
api_router.include_router(pipelines.router)
api_router.include_router(airflow.router)
api_router.include_router(spark.router)
api_router.include_router(artifacts.router)
