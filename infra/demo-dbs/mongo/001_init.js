db = db.getSiblingDB("analytics");

db.createUser({
  user: "demo",
  pwd: "demo",
  roles: [{ role: "readWrite", db: "analytics" }],
});

db.interview_sessions.insertMany([
  {
    session_id: "it-001",
    candidate: "Anna",
    track: "backend",
    score: 82,
    created_at: ISODate("2026-05-10T10:00:00Z"),
  },
  {
    session_id: "it-002",
    candidate: "Ivan",
    track: "data-engineering",
    score: 91,
    created_at: ISODate("2026-05-11T11:30:00Z"),
  },
  {
    session_id: "it-003",
    candidate: "Maria",
    track: "frontend",
    score: 76,
    created_at: ISODate("2026-05-12T13:15:00Z"),
  },
]);

db.feedback_events.insertMany([
  { session_id: "it-001", event_name: "hint_requested", payload: { topic: "sql" } },
  { session_id: "it-002", event_name: "answer_reviewed", payload: { topic: "spark" } },
  { session_id: "it-003", event_name: "mock_finished", payload: { topic: "react" } },
]);
