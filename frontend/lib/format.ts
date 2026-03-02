export function formatPercent(value: number): string {
  const sign = value > 0 ? "+" : "";
  return `${sign}${(value * 100).toFixed(0)}%`;
}

export function formatRelativeHeat(value: number): string {
  if (value > 0.5) return "Hot";
  if (value > 0.1) return "Warming";
  if (value < -0.5) return "Cold";
  if (value < -0.1) return "Cooling";
  return "Stable";
}

export function formatTime(isoTimestamp: string | null): string {
  if (!isoTimestamp) return "n/a";
  const date = new Date(isoTimestamp);
  return date.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit"
  });
}

export function formatGeneratedTime(isoTimestamp: string | null): string {
  if (!isoTimestamp) return "n/a";
  const date = new Date(isoTimestamp);
  const datePart = date.toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric"
  });
  const timePart = date.toLocaleTimeString(undefined, {
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short"
  });
  return `${datePart} · ${timePart}`;
}
