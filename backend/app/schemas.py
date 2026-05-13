from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, EmailStr, Field


class TokenPair(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


class UserRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    email: EmailStr
    full_name: str
    role: Literal["admin", "engineer", "analyst"]
    status: Literal["active", "invited", "disabled"]
    created_at: datetime


class UserCreate(BaseModel):
    email: EmailStr
    full_name: str
    role: Literal["admin", "engineer", "analyst"] = "engineer"
    password: str = Field(min_length=4)


class UserUpdate(BaseModel):
    full_name: str | None = None
    role: Literal["admin", "engineer", "analyst"] | None = None
    status: Literal["active", "invited", "disabled"] | None = None


class AgentQueryRequest(BaseModel):
    query: str = Field(min_length=1)
    session_id: str | None = None
    app_state: dict[str, Any] | None = None


class ToolCallRead(BaseModel):
    tool_name: str
    status: str
    input: dict[str, Any]
    output: dict[str, Any]
    latency_ms: int


class AgentQueryResponse(BaseModel):
    session_id: str
    message_id: str
    intent: str
    answer: str
    tool_calls: list[ToolCallRead]
    ui_actions: list[dict[str, Any]] = Field(default_factory=list)


class SessionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    title: str
    created_at: datetime
    updated_at: datetime


class MessageRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    role: str
    content: str
    metadata_json: dict[str, Any] | None
    created_at: datetime


class SqlExecuteRequest(BaseModel):
    query: str
    limit: int = Field(default=100, ge=1, le=500)


class SqlExecuteResponse(BaseModel):
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    latency_ms: int


class TableColumn(BaseModel):
    name: str
    type: str
    nullable: bool = True


class CatalogTable(BaseModel):
    schema_name: str = "public"
    name: str
    type: str = "table"
    columns: list[TableColumn]


class PipelineRead(BaseModel):
    dag_id: str
    name: str
    schedule: str
    status: str
    owner: str
    description: str | None = None
    last_run: str | None = None
    next_run: str | None = None
    tasks: list[dict[str, Any]] = Field(default_factory=list)


class AirflowRunRequest(BaseModel):
    conf: dict[str, Any] = Field(default_factory=dict)


class AirflowRunRead(BaseModel):
    dag_id: str
    run_id: str
    status: str
    external_url: str | None = None
    created_at: datetime | None = None


class AirflowTaskInstanceRead(BaseModel):
    dag_id: str
    run_id: str
    task_id: str
    state: str
    try_number: int = 1
    operator: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    duration: float | None = None


class AirflowTaskLogRead(BaseModel):
    dag_id: str
    run_id: str
    task_id: str
    try_number: int
    content: str
    source: str = "airflow"


class SparkJobRequest(BaseModel):
    name: str
    app_resource: str
    params: dict[str, Any] = Field(default_factory=dict)


class SparkJobRead(BaseModel):
    job_id: str
    name: str
    status: str
    app_resource: str
    params: dict[str, Any]
    result_sample: list[dict[str, Any]] | None = None
    driver_log: str | None = None
    created_at: datetime | None = None
