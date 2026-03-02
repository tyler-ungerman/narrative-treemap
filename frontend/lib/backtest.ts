import { BacktestHorizon } from "@/lib/types";

export type WarmupFallbackHorizon = BacktestHorizon & { is_fallback: true };

export function isWarmupFallbackHorizon(horizon: BacktestHorizon): horizon is WarmupFallbackHorizon {
  return horizon.is_fallback === true;
}
