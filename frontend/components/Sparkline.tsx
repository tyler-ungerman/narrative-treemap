"use client";

interface SparklineProps {
  values: number[];
  className?: string;
}

export function Sparkline({ values, className }: SparklineProps) {
  if (!values.length) {
    return <svg className={className} viewBox="0 0 100 24" aria-hidden="true" />;
  }

  const max = Math.max(...values, 1);
  const min = Math.min(...values, 0);
  const range = Math.max(max - min, 1);
  const step = values.length > 1 ? 100 / (values.length - 1) : 100;

  const path = values
    .map((value, index) => {
      const x = index * step;
      const normalized = (value - min) / range;
      const y = 22 - normalized * 20;
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(" ");

  return (
    <svg className={className} viewBox="0 0 100 24" preserveAspectRatio="none" aria-hidden="true">
      <polyline points={path} fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
    </svg>
  );
}
