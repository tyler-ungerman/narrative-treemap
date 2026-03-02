"use client";

interface AboutModalProps {
  open: boolean;
  onClose: () => void;
}

export function AboutModal({ open, onClose }: AboutModalProps) {
  if (!open) return null;

  return (
    <div className="about-overlay" role="dialog" aria-modal="true">
      <div className="about-modal">
        <header>
          <h2>Methodology</h2>
          <button type="button" onClick={onClose}>
            Close
          </button>
        </header>
        <p>
          Narrative Treemap ingests public RSS/HTML sources across multiple verticals, normalizes items,
          computes local embeddings, clusters stories into topic groups, and scores each group by volume,
          momentum, novelty, and source diversity.
        </p>
        <ul>
          <li>Tile area represents current volume in the selected window.</li>
          <li>Tile color indicates momentum from the previous equivalent window.</li>
          <li>Sparklines show topic activity over twelve buckets in-window.</li>
          <li>Topic summaries and entities are generated with deterministic heuristics.</li>
          <li>No API keys are used and cached results are served instantly before background refresh.</li>
        </ul>
      </div>
    </div>
  );
}
