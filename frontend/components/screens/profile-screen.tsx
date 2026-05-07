"use client";

import { useAppStore } from "@/lib/store";
import { Badge, Card } from "@/components/ui";

export function ProfileScreen() {
  const user = useAppStore((state) => state.user);

  return (
    <div className="content">
      <Card title="Profile" sub="Current JWT identity">
        {user && (
          <div style={{ display: "flex", alignItems: "center", gap: 14 }}>
            <div className="avatar ai">{user.full_name.slice(0, 2).toUpperCase()}</div>
            <div style={{ flex: 1 }}>
              <div style={{ fontWeight: 700 }}>{user.full_name}</div>
              <div className="card-sub">{user.email}</div>
            </div>
            <span className="tag">{user.role}</span>
            <Badge status={user.status} />
          </div>
        )}
      </Card>
    </div>
  );
}
