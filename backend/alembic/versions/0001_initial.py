"""initial app tables

Revision ID: 0001_initial
Revises:
Create Date: 2026-05-03
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "app_users",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("full_name", sa.String(length=255), nullable=False),
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("password_hash", sa.Text(), nullable=False),
        sa.Column("invite_token", sa.String(length=128)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_app_users_email", "app_users", ["email"], unique=True)

    op.create_table(
        "agent_sessions",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("user_id", sa.String(length=36), sa.ForeignKey("app_users.id", ondelete="CASCADE")),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_table(
        "messages",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("session_id", sa.String(length=36), sa.ForeignKey("agent_sessions.id", ondelete="CASCADE")),
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("metadata_json", sa.JSON()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_table(
        "tool_runs",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("session_id", sa.String(length=36), sa.ForeignKey("agent_sessions.id", ondelete="CASCADE")),
        sa.Column("message_id", sa.String(length=36), sa.ForeignKey("messages.id", ondelete="SET NULL")),
        sa.Column("tool_name", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("input_json", sa.JSON(), nullable=False),
        sa.Column("output_json", sa.JSON(), nullable=False),
        sa.Column("latency_ms", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_tool_runs_tool_name", "tool_runs", ["tool_name"])
    op.create_table(
        "pipeline_runs",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("dag_id", sa.String(length=255), nullable=False),
        sa.Column("run_id", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("conf_json", sa.JSON()),
        sa.Column("external_url", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_pipeline_runs_dag_id", "pipeline_runs", ["dag_id"])
    op.create_index("ix_pipeline_runs_run_id", "pipeline_runs", ["run_id"])
    op.create_table(
        "pipeline_states",
        sa.Column("dag_id", sa.String(length=255), primary_key=True),
        sa.Column("is_paused", sa.Boolean(), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_table(
        "spark_jobs",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("job_id", sa.String(length=255), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("app_resource", sa.Text(), nullable=False),
        sa.Column("params_json", sa.JSON()),
        sa.Column("result_sample_json", sa.JSON()),
        sa.Column("driver_log", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_spark_jobs_job_id", "spark_jobs", ["job_id"])
    op.create_table(
        "artifact_versions",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("artifact_type", sa.String(length=32), nullable=False),
        sa.Column("artifact_name", sa.String(length=255), nullable=False),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("author_user_id", sa.String(length=36)),
        sa.Column("validation_status", sa.String(length=32), nullable=False),
        sa.Column("validation_output", sa.Text()),
        sa.Column("git_status", sa.String(length=32), nullable=False),
        sa.Column("git_repository", sa.Text()),
        sa.Column("git_commit_sha", sa.String(length=64)),
        sa.Column("git_commit_short_sha", sa.String(length=16)),
        sa.Column("git_error", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_artifact_versions_artifact_type", "artifact_versions", ["artifact_type"])
    op.create_index("ix_artifact_versions_artifact_name", "artifact_versions", ["artifact_name"])
    op.create_index("ix_artifact_versions_content_hash", "artifact_versions", ["content_hash"])


def downgrade():
    op.drop_table("artifact_versions")
    op.drop_table("spark_jobs")
    op.drop_table("pipeline_states")
    op.drop_table("pipeline_runs")
    op.drop_table("tool_runs")
    op.drop_table("messages")
    op.drop_table("agent_sessions")
    op.drop_table("app_users")
