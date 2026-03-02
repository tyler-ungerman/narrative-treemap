"use client";

import { SourceHealth } from "@/lib/types";

interface SourceHealthBarProps {
  health: SourceHealth[];
}

export function SourceHealthBar({ health }: SourceHealthBarProps) {
  const failing = health.filter((row) => Boolean(row.last_error));
  const healthyCount = health.length - failing.length;

  return (
    <section className="health-strip" aria-label="Source health">
      <div>
        <strong>Sources:</strong> {health.length}
      </div>
      <div>
        <strong>Healthy:</strong> {healthyCount}
      </div>
      <div>
        <strong>Failing:</strong> {failing.length}
      </div>
      {failing.slice(0, 2).map((row) => (
        <div key={row.source_name} className="health-error" title={row.last_error ?? ""}>
          {row.source_name}: error
        </div>
      ))}
    </section>
  );
}
