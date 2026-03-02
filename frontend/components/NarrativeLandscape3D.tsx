"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import * as THREE from "three";
import { OrbitControls } from "three/examples/jsm/controls/OrbitControls.js";

import { formatPercent } from "@/lib/format";
import { Topic } from "@/lib/types";

interface NarrativeLandscape3DProps {
  topics: Topic[];
  selectedTopicId: string | null;
  onSelectTopic: (topic: Topic) => void;
}

interface LandscapeNode {
  topic: Topic;
  position: [number, number, number];
  radius: number;
  color: string;
}

interface LandscapeEdge {
  sourceTopicId: string;
  targetTopicId: string;
  strength: number;
}

interface ConnectionEvidence {
  topic: Topic;
  reason: string;
  sharedKeywords: string[];
  sharedEntities: string[];
}

const MAX_NODES = 180;
const MAX_CONNECTIONS = 8;

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(value, max));
}

function blendHex(start: string, end: string, ratio: number): string {
  const toRgb = (hex: string) => {
    const normalized = hex.replace("#", "");
    return {
      r: Number.parseInt(normalized.slice(0, 2), 16),
      g: Number.parseInt(normalized.slice(2, 4), 16),
      b: Number.parseInt(normalized.slice(4, 6), 16)
    };
  };
  const startRgb = toRgb(start);
  const endRgb = toRgb(end);
  const mix = (a: number, b: number) => Math.round(a + (b - a) * ratio);
  const r = mix(startRgb.r, endRgb.r).toString(16).padStart(2, "0");
  const g = mix(startRgb.g, endRgb.g).toString(16).padStart(2, "0");
  const b = mix(startRgb.b, endRgb.b).toString(16).padStart(2, "0");
  return `#${r}${g}${b}`;
}

function momentumColor(momentum: number): string {
  const normalized = clamp((momentum + 1) / 2, 0, 1);
  if (normalized < 0.5) {
    return blendHex("#1d4ed8", "#94a3b8", normalized / 0.5);
  }
  return blendHex("#94a3b8", "#ea580c", (normalized - 0.5) / 0.5);
}

function nodeRank(topic: Topic): number {
  const weightedVolume = topic.weighted_volume_now ?? topic.volume_now;
  return Math.log(weightedVolume + 1) * 0.9 + topic.momentum * 1.2 + topic.novelty * 0.55;
}

function normalizeTerm(term: string): string {
  return term.trim().toLowerCase();
}

function intersectTerms(source: string[], target: string[], limit: number): string[] {
  const sourceMap = new Map<string, string>();
  source.forEach((term) => {
    const normalized = normalizeTerm(term);
    if (!normalized) return;
    if (!sourceMap.has(normalized)) {
      sourceMap.set(normalized, term);
    }
  });

  const hits: string[] = [];
  const seen = new Set<string>();
  target.forEach((term) => {
    const normalized = normalizeTerm(term);
    if (!normalized || seen.has(normalized)) return;
    if (sourceMap.has(normalized)) {
      hits.push(sourceMap.get(normalized) ?? term);
      seen.add(normalized);
    }
  });

  return hits.slice(0, limit);
}

function summarizeConnectionReason(
  origin: Topic,
  target: Topic,
  sharedKeywords: string[],
  sharedEntities: string[]
): string {
  if (sharedKeywords.length > 0) {
    return `Shared keywords: ${sharedKeywords.join(", ")}`;
  }
  if (sharedEntities.length > 0) {
    return `Shared entities: ${sharedEntities.join(", ")}`;
  }
  if (origin.vertical === target.vertical) {
    return `Same vertical: ${origin.vertical}`;
  }

  const momentumDelta = Math.abs(origin.momentum - target.momentum);
  if (momentumDelta <= 0.25) {
    return "Momentum is moving in a similar range";
  }

  return "Linked by embedding similarity in recent coverage";
}

function edgeColor(strength: number): string {
  return blendHex("#27344a", "#7dd3fc", clamp(strength, 0, 1));
}

function computeEdgeStrength(source: Topic, target: Topic): number {
  const sharedKeywords = intersectTerms(source.keywords, target.keywords, 4).length;
  const sharedEntities = intersectTerms(source.entities, target.entities, 3).length;
  const keywordScore = clamp(sharedKeywords / 3, 0, 1);
  const entityScore = clamp(sharedEntities / 2, 0, 1);
  const verticalScore = source.vertical === target.vertical ? 1 : 0;
  const momentumScore = 1 - clamp(Math.abs(source.momentum - target.momentum), 0, 1);
  const averageQuality = ((source.source_quality_score ?? 1) + (target.source_quality_score ?? 1)) / 2;
  const qualityScore = clamp(averageQuality / 1.35, 0, 1);

  const rawStrength =
    keywordScore * 0.36 +
    entityScore * 0.22 +
    verticalScore * 0.18 +
    momentumScore * 0.16 +
    qualityScore * 0.08;

  return clamp(rawStrength, 0.12, 1);
}

function buildConnectionEvidence(
  focusNode: LandscapeNode | null,
  nodeByTopicId: Map<string, LandscapeNode>
): ConnectionEvidence[] {
  if (!focusNode) {
    return [];
  }

  const focusTopic = focusNode.topic;
  const evidence: ConnectionEvidence[] = [];

  focusTopic.related_topic_ids.forEach((relatedTopicId) => {
    if (evidence.length >= MAX_CONNECTIONS) {
      return;
    }
    const related = nodeByTopicId.get(relatedTopicId);
    if (!related) {
      return;
    }

    const sharedKeywords = intersectTerms(focusTopic.keywords, related.topic.keywords, 3);
    const sharedEntities = intersectTerms(focusTopic.entities, related.topic.entities, 2);

    evidence.push({
      topic: related.topic,
      reason: summarizeConnectionReason(focusTopic, related.topic, sharedKeywords, sharedEntities),
      sharedKeywords,
      sharedEntities
    });
  });

  return evidence;
}

function buildLandscape(topics: Topic[]): { nodes: LandscapeNode[]; edges: LandscapeEdge[] } {
  const limitedTopics = [...topics].sort((left, right) => nodeRank(right) - nodeRank(left)).slice(0, MAX_NODES);

  const byVertical = new Map<string, Topic[]>();
  limitedTopics.forEach((topic) => {
    const bucket = byVertical.get(topic.vertical) ?? [];
    bucket.push(topic);
    byVertical.set(topic.vertical, bucket);
  });

  const verticalBuckets = [...byVertical.entries()].sort((left, right) => right[1].length - left[1].length);
  const verticalRadius = Math.max(90, 64 + verticalBuckets.length * 9);

  const nodeByTopicId = new Map<string, LandscapeNode>();
  verticalBuckets.forEach(([_vertical, bucket], verticalIndex) => {
    const theta = (verticalIndex / Math.max(verticalBuckets.length, 1)) * Math.PI * 2;
    const centerX = Math.cos(theta) * verticalRadius;
    const centerZ = Math.sin(theta) * verticalRadius;

    bucket.forEach((topic, topicIndex) => {
      const spiralAngle = topicIndex * 2.399963;
      const spiralRadius = 12 + Math.sqrt(topicIndex + 1) * 10;
      const x = centerX + Math.cos(spiralAngle) * spiralRadius;
      const z = centerZ + Math.sin(spiralAngle) * spiralRadius;
      const y = clamp(topic.momentum * 36 + topic.novelty * 18, -24, 54);
      const weightedVolume = topic.weighted_volume_now ?? topic.volume_now;
      const radius = clamp(1.7 + Math.log(weightedVolume + 1) * 0.9, 1.7, 8);

      nodeByTopicId.set(topic.topic_id, {
        topic,
        position: [x, y, z],
        radius,
        color: momentumColor(clamp(topic.momentum, -1, 1))
      });
    });
  });

  const edgeByKey = new Map<string, LandscapeEdge>();
  nodeByTopicId.forEach((node) => {
    node.topic.related_topic_ids.slice(0, 6).forEach((relatedTopicId) => {
      const relatedNode = nodeByTopicId.get(relatedTopicId);
      if (!relatedNode || relatedTopicId === node.topic.topic_id) {
        return;
      }
      const pair = [node.topic.topic_id, relatedTopicId].sort();
      const key = `${pair[0]}::${pair[1]}`;
      const strength = computeEdgeStrength(node.topic, relatedNode.topic);
      const existing = edgeByKey.get(key);
      if (!existing || strength > existing.strength) {
        edgeByKey.set(key, {
          sourceTopicId: pair[0],
          targetTopicId: pair[1],
          strength
        });
      }
    });
  });

  return {
    nodes: [...nodeByTopicId.values()],
    edges: [...edgeByKey.values()]
  };
}

function createLabelSprite(text: string): THREE.Sprite {
  const canvas = document.createElement("canvas");
  const context = canvas.getContext("2d");
  if (!context) {
    const material = new THREE.SpriteMaterial({ color: "#cbd5e1" });
    return new THREE.Sprite(material);
  }

  const safeText = text.length > 32 ? `${text.slice(0, 29)}...` : text;
  const font = "600 22px ui-monospace, Menlo, Monaco, monospace";
  context.font = font;
  const metrics = context.measureText(safeText);
  const paddingX = 20;
  const paddingY = 12;
  const width = Math.ceil(metrics.width + paddingX * 2);
  const height = 54;

  canvas.width = width;
  canvas.height = height;

  context.font = font;
  context.textBaseline = "middle";
  context.fillStyle = "rgba(8, 18, 31, 0.86)";
  context.strokeStyle = "rgba(125, 211, 252, 0.55)";
  context.lineWidth = 2;

  const radius = 12;
  context.beginPath();
  context.moveTo(radius, 0);
  context.lineTo(width - radius, 0);
  context.quadraticCurveTo(width, 0, width, radius);
  context.lineTo(width, height - radius);
  context.quadraticCurveTo(width, height, width - radius, height);
  context.lineTo(radius, height);
  context.quadraticCurveTo(0, height, 0, height - radius);
  context.lineTo(0, radius);
  context.quadraticCurveTo(0, 0, radius, 0);
  context.closePath();
  context.fill();
  context.stroke();

  context.fillStyle = "#dbeafe";
  context.fillText(safeText, paddingX, height / 2 + 1);

  const texture = new THREE.CanvasTexture(canvas);
  texture.colorSpace = THREE.SRGBColorSpace;
  texture.needsUpdate = true;

  const material = new THREE.SpriteMaterial({
    map: texture,
    transparent: true,
    depthTest: false,
    depthWrite: false,
    opacity: 0.9
  });

  const sprite = new THREE.Sprite(material);
  const scale = 0.075;
  sprite.scale.set(width * scale, height * scale, 1);
  return sprite;
}

export function NarrativeLandscape3D({ topics, selectedTopicId, onSelectTopic }: NarrativeLandscape3DProps) {
  const shellRef = useRef<HTMLDivElement | null>(null);
  const mountRef = useRef<HTMLDivElement | null>(null);
  const controlsRef = useRef<OrbitControls | null>(null);
  const cameraRef = useRef<THREE.PerspectiveCamera | null>(null);
  const defaultViewRef = useRef<{ position: THREE.Vector3; target: THREE.Vector3 } | null>(null);
  const meshByTopicIdRef = useRef<Map<string, THREE.Mesh> | null>(null);
  const lineSegmentsRef = useRef<Array<{ edge: LandscapeEdge; line: THREE.Line }> | null>(null);

  const [hoveredTopicId, setHoveredTopicId] = useState<string | null>(null);
  const [webglReady, setWebglReady] = useState<boolean | null>(null);
  const [renderError, setRenderError] = useState<string | null>(null);
  const [showLabels, setShowLabels] = useState(true);
  const [isFullscreen, setIsFullscreen] = useState(false);

  const selectedTopicRef = useRef<string | null>(selectedTopicId);

  const { nodes, edges } = useMemo(() => buildLandscape(topics), [topics]);

  const nodeByTopicId = useMemo(() => {
    const mapping = new Map<string, LandscapeNode>();
    nodes.forEach((node) => mapping.set(node.topic.topic_id, node));
    return mapping;
  }, [nodes]);

  const focusedTopicId = hoveredTopicId ?? selectedTopicId;
  const focusedNode = focusedTopicId ? nodeByTopicId.get(focusedTopicId) ?? null : null;
  const connectionEvidence = useMemo(
    () => buildConnectionEvidence(focusedNode, nodeByTopicId),
    [focusedNode, nodeByTopicId]
  );

  useEffect(() => {
    selectedTopicRef.current = selectedTopicId;
  }, [selectedTopicId]);

  useEffect(() => {
    if (typeof document === "undefined") {
      return;
    }
    const handleFullscreenChange = () => {
      const host = shellRef.current;
      const fullscreenElement = document.fullscreenElement;
      setIsFullscreen(Boolean(host && fullscreenElement === host));
    };
    document.addEventListener("fullscreenchange", handleFullscreenChange);
    handleFullscreenChange();
    return () => document.removeEventListener("fullscreenchange", handleFullscreenChange);
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") {
      setWebglReady(false);
      return;
    }
    try {
      const canvas = document.createElement("canvas");
      const gl = canvas.getContext("webgl2") ?? canvas.getContext("webgl");
      setWebglReady(Boolean(gl));
    } catch {
      setWebglReady(false);
    }
  }, []);

  useEffect(() => {
    const container = mountRef.current;
    if (!container || webglReady !== true) {
      return;
    }

    setRenderError(null);

    try {
      const scene = new THREE.Scene();
      scene.background = new THREE.Color("#070c12");
      scene.fog = new THREE.Fog("#070c12", 240, 620);

      const camera = new THREE.PerspectiveCamera(48, 1, 0.1, 2000);
      camera.position.set(0, 130, 250);
      cameraRef.current = camera;

      const renderer = new THREE.WebGLRenderer({ antialias: true, powerPreference: "high-performance" });
      renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
      renderer.outputColorSpace = THREE.SRGBColorSpace;
      container.innerHTML = "";
      container.appendChild(renderer.domElement);

      const controls = new OrbitControls(camera, renderer.domElement);
      controls.enableDamping = true;
      controls.dampingFactor = 0.07;
      controls.enablePan = false;
      controls.maxDistance = 420;
      controls.minDistance = 70;
      controls.maxPolarAngle = Math.PI / 2.1;
      controlsRef.current = controls;

      defaultViewRef.current = {
        position: camera.position.clone(),
        target: controls.target.clone()
      };

      const ambientLight = new THREE.AmbientLight(0xffffff, 0.68);
      scene.add(ambientLight);

      const directionalPrimary = new THREE.DirectionalLight(0xffffff, 1.1);
      directionalPrimary.position.set(90, 120, 30);
      scene.add(directionalPrimary);

      const directionalSecondary = new THREE.DirectionalLight(0xffffff, 0.5);
      directionalSecondary.position.set(-80, 40, -90);
      scene.add(directionalSecondary);

      const grid = new THREE.GridHelper(600, 48, "#1f3a5f", "#1f2937");
      grid.position.set(0, -32, 0);
      scene.add(grid);

      const meshByTopicId = new Map<string, THREE.Mesh>();
      const labelByTopicId = new Map<string, THREE.Sprite>();
      const lineSegments: Array<{ edge: LandscapeEdge; line: THREE.Line }> = [];
      const pointerStart = { x: 0, y: 0 };
      let pointerDown = false;
      let hoveredMesh: THREE.Mesh | null = null;

      const setMeshIdle = (mesh: THREE.Mesh, topicId: string) => {
        const node = nodeByTopicId.get(topicId);
        if (!node) return;
        const material = mesh.material as THREE.MeshStandardMaterial;
        material.emissive = new THREE.Color(selectedTopicRef.current === topicId ? node.color : "#111827");
        material.emissiveIntensity = selectedTopicRef.current === topicId ? 0.34 : 0.08;
        mesh.scale.set(1, 1, 1);
      };

      const applyHoverState = (nextMesh: THREE.Mesh | null) => {
        if (hoveredMesh === nextMesh) {
          return;
        }

        if (hoveredMesh) {
          const previousTopicId = String(hoveredMesh.userData.topicId ?? "");
          setMeshIdle(hoveredMesh, previousTopicId);
        }

        hoveredMesh = nextMesh;

        if (hoveredMesh) {
          const topicId = String(hoveredMesh.userData.topicId ?? "");
          const node = nodeByTopicId.get(topicId);
          if (!node) {
            return;
          }
          const material = hoveredMesh.material as THREE.MeshStandardMaterial;
          hoveredMesh.scale.set(1.14, 1.14, 1.14);
          material.emissive = new THREE.Color(node.color);
          material.emissiveIntensity = 0.28;
          setHoveredTopicId(topicId);
          renderer.domElement.style.cursor = "pointer";
          return;
        }

        setHoveredTopicId(null);
        renderer.domElement.style.cursor = "grab";
      };

      nodes.forEach((node) => {
        const geometry = new THREE.SphereGeometry(node.radius, 16, 16);
        const material = new THREE.MeshStandardMaterial({
          color: node.color,
          emissive: new THREE.Color(selectedTopicRef.current === node.topic.topic_id ? node.color : "#111827"),
          emissiveIntensity: selectedTopicRef.current === node.topic.topic_id ? 0.34 : 0.08,
          roughness: 0.36,
          metalness: 0.22
        });
        const mesh = new THREE.Mesh(geometry, material);
        mesh.position.set(node.position[0], node.position[1], node.position[2]);
        mesh.userData.topicId = node.topic.topic_id;
        scene.add(mesh);
        meshByTopicId.set(node.topic.topic_id, mesh);

        const label = createLabelSprite(node.topic.label);
        label.position.set(node.position[0], node.position[1] + node.radius + 4.2, node.position[2]);
        label.visible = showLabels;
        scene.add(label);
        labelByTopicId.set(node.topic.topic_id, label);
      });

      edges.forEach((edge) => {
        const source = nodeByTopicId.get(edge.sourceTopicId);
        const target = nodeByTopicId.get(edge.targetTopicId);
        if (!source || !target) {
          return;
        }
        const geometry = new THREE.BufferGeometry().setFromPoints([
          new THREE.Vector3(...source.position),
          new THREE.Vector3(...target.position)
        ]);
        const material = new THREE.LineBasicMaterial({
          color: edgeColor(edge.strength),
          transparent: true,
          opacity: 0.16 + edge.strength * 0.64
        });
        const line = new THREE.Line(geometry, material);
        scene.add(line);
        lineSegments.push({ edge, line });
      });

      meshByTopicIdRef.current = meshByTopicId;
      lineSegmentsRef.current = lineSegments;

      const raycaster = new THREE.Raycaster();
      const pointer = new THREE.Vector2();

      const updatePointerFromEvent = (event: MouseEvent) => {
        const rect = renderer.domElement.getBoundingClientRect();
        pointer.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
        pointer.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;
      };

      const pickHoveredMesh = (event: MouseEvent): THREE.Mesh | null => {
        updatePointerFromEvent(event);
        raycaster.setFromCamera(pointer, camera);
        const intersections = raycaster.intersectObjects([...meshByTopicId.values()], false);
        return (intersections[0]?.object as THREE.Mesh | undefined) ?? null;
      };

      const handlePointerDown = (event: MouseEvent) => {
        pointerDown = true;
        pointerStart.x = event.clientX;
        pointerStart.y = event.clientY;
      };

      const handlePointerMove = (event: MouseEvent) => {
        if (pointerDown) {
          const movedDistance = Math.hypot(event.clientX - pointerStart.x, event.clientY - pointerStart.y);
          if (movedDistance > 5) {
            applyHoverState(null);
            return;
          }
        }
        applyHoverState(pickHoveredMesh(event));
      };

      const handlePointerUp = (event: MouseEvent) => {
        const movedDistance = Math.hypot(event.clientX - pointerStart.x, event.clientY - pointerStart.y);
        pointerDown = false;
        if (movedDistance > 5) {
          return;
        }

        const hit = pickHoveredMesh(event);
        if (!hit) {
          return;
        }
        const topicId = String(hit.userData.topicId ?? "");
        const node = nodeByTopicId.get(topicId);
        if (node) {
          onSelectTopic(node.topic);
        }
      };

      const handlePointerLeave = () => {
        pointerDown = false;
        applyHoverState(null);
      };

      renderer.domElement.addEventListener("mousedown", handlePointerDown);
      renderer.domElement.addEventListener("mousemove", handlePointerMove);
      renderer.domElement.addEventListener("mouseup", handlePointerUp);
      renderer.domElement.addEventListener("mouseleave", handlePointerLeave);
      renderer.domElement.style.cursor = "grab";

      const resize = () => {
        const width = Math.max(container.clientWidth, 320);
        const height = Math.max(container.clientHeight, 420);
        renderer.setSize(width, height);
        camera.aspect = width / height;
        camera.updateProjectionMatrix();
      };
      resize();

      let frameId = 0;
      const animate = () => {
        frameId = window.requestAnimationFrame(animate);
        controls.update();
        renderer.render(scene, camera);
      };
      animate();
      window.addEventListener("resize", resize);

      return () => {
        window.cancelAnimationFrame(frameId);
        window.removeEventListener("resize", resize);
        renderer.domElement.removeEventListener("mousedown", handlePointerDown);
        renderer.domElement.removeEventListener("mousemove", handlePointerMove);
        renderer.domElement.removeEventListener("mouseup", handlePointerUp);
        renderer.domElement.removeEventListener("mouseleave", handlePointerLeave);

        controls.dispose();
        controlsRef.current = null;
        cameraRef.current = null;
        defaultViewRef.current = null;

        [...meshByTopicId.values()].forEach((mesh) => {
          mesh.geometry.dispose();
          (mesh.material as THREE.Material).dispose();
        });
        [...labelByTopicId.values()].forEach((sprite) => {
          const material = sprite.material as THREE.SpriteMaterial;
          material.map?.dispose();
          material.dispose();
        });
        lineSegments.forEach((segment) => {
          segment.line.geometry.dispose();
          (segment.line.material as THREE.Material).dispose();
        });

        meshByTopicIdRef.current = null;
        lineSegmentsRef.current = null;

        renderer.dispose();
        if (renderer.domElement.parentElement === container) {
          container.removeChild(renderer.domElement);
        }
      };
    } catch (error) {
      const reason = error instanceof Error ? error.message : "Unknown render error";
      setRenderError(reason);
      return;
    }
  }, [edges, nodeByTopicId, nodes, onSelectTopic, showLabels, webglReady]);

  useEffect(() => {
    const meshes = meshByTopicIdRef.current;
    if (!meshes) {
      return;
    }
    meshes.forEach((mesh, topicId) => {
      const node = nodeByTopicId.get(topicId);
      if (!node) return;
      const material = mesh.material as THREE.MeshStandardMaterial;
      const isSelected = selectedTopicId === topicId;
      const isHovered = hoveredTopicId === topicId;
      if (isHovered) {
        mesh.scale.set(1.14, 1.14, 1.14);
        material.emissive = new THREE.Color(node.color);
        material.emissiveIntensity = 0.28;
        return;
      }
      mesh.scale.set(1, 1, 1);
      material.emissive = new THREE.Color(isSelected ? node.color : "#111827");
      material.emissiveIntensity = isSelected ? 0.34 : 0.08;
    });
  }, [hoveredTopicId, nodeByTopicId, selectedTopicId]);

  useEffect(() => {
    const lineSegments = lineSegmentsRef.current;
    if (!lineSegments) {
      return;
    }
    const focusTopicId = hoveredTopicId ?? selectedTopicId;
    lineSegments.forEach(({ edge, line }) => {
      const material = line.material as THREE.LineBasicMaterial;
      const isFocused =
        focusTopicId !== null &&
        (edge.sourceTopicId === focusTopicId || edge.targetTopicId === focusTopicId);
      const visibleStrength = clamp(edge.strength, 0.12, 1);
      material.color = new THREE.Color(
        isFocused
          ? edgeColor(clamp(visibleStrength * 1.05, 0, 1))
          : edgeColor(clamp(visibleStrength * 0.82, 0, 1))
      );
      material.opacity = isFocused
        ? clamp(0.34 + visibleStrength * 0.58, 0.34, 1)
        : clamp(0.12 + visibleStrength * 0.46, 0.12, 0.74);
      material.needsUpdate = true;
    });
  }, [hoveredTopicId, selectedTopicId]);

  const handleRecenter = () => {
    const camera = cameraRef.current;
    const controls = controlsRef.current;
    const defaultView = defaultViewRef.current;
    if (!camera || !controls || !defaultView) {
      return;
    }

    camera.position.copy(defaultView.position);
    controls.target.copy(defaultView.target);
    controls.update();
  };

  const handleFullscreenToggle = async () => {
    if (typeof document === "undefined") {
      return;
    }
    const host = shellRef.current;
    if (!host) {
      return;
    }
    try {
      if (document.fullscreenElement === host) {
        await document.exitFullscreen();
      } else if (!document.fullscreenElement) {
        await host.requestFullscreen();
      }
    } catch {
      // ignore fullscreen API failures
    }
  };

  if (!topics.length) {
    return (
      <div className="treemap-empty">
        <p>No topics found for the current filter selection.</p>
      </div>
    );
  }

  if (webglReady === false) {
    return (
      <div className="landscape-fallback">
        <p>WebGL is unavailable, so 3D landscape mode can&apos;t run right now.</p>
        <p>Use Treemap mode, or enable hardware acceleration in your browser settings.</p>
      </div>
    );
  }

  if (renderError) {
    return (
      <div className="landscape-fallback">
        <p>3D landscape couldn&apos;t render in this browser session.</p>
        <p>{renderError}</p>
      </div>
    );
  }

  return (
    <div className={`landscape-shell${isFullscreen ? " is-fullscreen" : ""}`} ref={shellRef}>
      <div className="landscape-canvas" ref={mountRef} />

      <div className="landscape-legend">
        <span>Connected narratives</span>
        <span>{nodes.length} nodes</span>
        <span>{edges.length} links</span>
        <span className="landscape-strength-scale">
          <em>weak</em>
          <span className="landscape-strength-scale-bar" />
          <em>strong</em>
        </span>
      </div>

      <div className="landscape-actions">
        <button type="button" onClick={() => setShowLabels((current) => !current)}>
          {showLabels ? "Hide labels" : "Show labels"}
        </button>
        <button type="button" onClick={handleRecenter}>
          Recenter view
        </button>
        <button type="button" onClick={() => void handleFullscreenToggle()}>
          {isFullscreen ? "Exit fullscreen" : "Fullscreen"}
        </button>
      </div>

      <div className="landscape-hint">
        Drag to orbit · Scroll to zoom · Click to inspect · Brighter links = stronger connections
      </div>

      {focusedNode ? (
        <aside className="landscape-tooltip" role="status" aria-live="polite">
          <strong>{focusedNode.topic.label}</strong>
          <p>{focusedNode.topic.volume_now} stories</p>
          <p>{formatPercent(focusedNode.topic.momentum)} momentum</p>
          <p>{focusedNode.topic.diversity} sources</p>
        </aside>
      ) : null}

      {focusedNode ? (
        <aside className="landscape-connections" aria-label="Connection evidence">
          <h3>Why this is connected</h3>
          <p className="landscape-connections-subtitle">
            {focusedNode.topic.label} has {connectionEvidence.length} nearby narrative links.
          </p>
          {connectionEvidence.length === 0 ? (
            <p className="landscape-connections-empty">No direct nearby links in this current 3D subset.</p>
          ) : (
            <ul>
              {connectionEvidence.map((entry) => (
                <li key={`${focusedNode.topic.topic_id}:${entry.topic.topic_id}`}>
                  <button type="button" onClick={() => onSelectTopic(entry.topic)}>
                    {entry.topic.label}
                  </button>
                  <p>{entry.reason}</p>
                  {entry.sharedKeywords.length > 0 ? (
                    <span>Keywords: {entry.sharedKeywords.join(", ")}</span>
                  ) : null}
                  {entry.sharedEntities.length > 0 ? (
                    <span>Entities: {entry.sharedEntities.join(", ")}</span>
                  ) : null}
                </li>
              ))}
            </ul>
          )}
        </aside>
      ) : null}
    </div>
  );
}
