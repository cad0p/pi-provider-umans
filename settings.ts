/**
 * Provider-specific settings for pi-provider-umans, read from a JSON file so
 * users can tune behavior without env vars (env vars don't survive restarts,
 * aren't discoverable via /settings, and can't be scoped per-project).
 *
 * Schema (today only the concurrency multiplier lives here):
 *
 *   { "concurrencyMultiplier": 1.0 }
 *
 * Precedence (highest to lowest):
 *   1. UMANS_CONCURRENCY_LIMIT env var (absolute override, testing only —
 *      takes precedence over everything, bypasses the multiplier). Handled in
 *      concurrencyLimit(), not here.
 *   2. .pi/umans.json (project settings, deep-merged over global)
 *   3. ~/.pi/agent/umans.json (global settings)
 *   4. Default (concurrencyMultiplier: 1.0)
 *
 * Merge semantics mirror pi's own deepMergeSettings (~/.pi/agent/settings.json
 * + .pi/settings.json), so users get predictable, familiar config layering:
 *   - Nested objects → deep merged (project fields override only their key;
 *     global siblings survive).
 *   - Arrays → fully replaced (project array replaces global entirely, NOT
 *     concatenated). No arrays in the schema today, but documented so a future
 *     array field doesn't surprise.
 *   - Primitives → override (project wins).
 *
 * Malformed JSON → console.warn + defaults (don't crash the provider).
 * Missing file → defaults.
 * Invalid concurrencyMultiplier (0, negative, NaN, non-number) → warn + default.
 *
 * File format + read path mirror auth.json reading (fs.readFileSync +
 * JSON.parse). The settings file is owned by this package; pi's settings.json
 * schema is owned by the pi CLI and extensions don't add fields to it.
 */
import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

/** Default multiplier: 1.0 = use the full guaranteed concurrency. */
export const DEFAULT_CONCURRENCY_MULTIPLIER = 1.0;

/**
 * Sane maximum for the concurrency multiplier. The documented useful range is
 * 0.5–3.0; anything above ~10 has no legitimate use. A huge finite multiplier
 * (e.g. 1e300) bypasses the hard_cap clamp during the startup window before
 * /v1/usage populates it (Math.floor(4 * 1e300) = 4e300 → the gate always
 * reports free → self-DoS via 429s). Rejecting at validation time bounds the
 * multiplier at the edge, independent of hard_cap.
 */
const MAX_CONCURRENCY_MULTIPLIER = 100;

export interface UmansSettings {
  concurrencyMultiplier: number;
}

/** The default settings returned when files are missing or malformed. */
export function defaultSettings(): UmansSettings {
  return { concurrencyMultiplier: DEFAULT_CONCURRENCY_MULTIPLIER };
}

/**
 * Default file paths. Global lives under ~/.pi/agent (mirrors pi's global
 * settings + auth.json location); project lives under .pi (mirrors pi's
 * project settings location). Resolved lazily at call time so a test that
 * swaps HOME sees the new path.
 */
function defaultGlobalPath(): string {
  return join(homedir(), ".pi", "agent", "umans.json");
}
function defaultProjectPath(): string {
  return join(process.cwd(), ".pi", "umans.json");
}

/**
 * Options for readSettings. The path overrides are a DI seam so selfcheck can
 * point at temp files without monkey-patching homedir/cwd.
 */
export interface ReadSettingsOptions {
  globalPath?: string;
  projectPath?: string;
}

/**
 * Deep-merge two settings objects, mirroring pi's deepMergeSettings:
 *   - plain objects → recursively merged (project keys override; global
 *     siblings survive)
 *   - arrays → project array replaces global entirely (NOT concatenated)
 *   - primitives → project wins; undefined project value keeps global
 *
 * `unknown` is the input type (JSON.parse output); the guards narrow to
 * records vs arrays vs primitives. Exported so selfcheck can unit-test the
 * merge semantics directly.
 */
export function deepMergeSettings(global: unknown, project: unknown): unknown {
  // undefined project (key absent or explicitly undefined) keeps the global
  // value entirely. JSON never emits undefined, but a hand-built test object
  // or a future caller might pass it; treat it as "no override".
  if (project === undefined) return global;
  // Both must be plain objects to deep-merge; otherwise project wins outright
  // (covers the array-replaces-object + primitive-override + object-replaces-
  // array cases). isPlainObject rejects arrays (typeof [] === "object").
  if (!isPlainObject(global) || !isPlainObject(project)) return project;
  const out: Record<string, unknown> = { ...(global as Record<string, unknown>) };
  for (const [k, v] of Object.entries(project as Record<string, unknown>)) {
    out[k] = deepMergeSettings(out[k], v);
  }
  return out;
}

/** True for plain objects (not arrays, not null). typeof [] === "object" so arrays are excluded. */
function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

/**
 * Read + validate a single settings file. Returns the parsed JSON (unknown) on
 * success, or null on missing/malformed (caller falls back to defaults).
 * Malformed JSON warns so the user sees the file is being ignored.
 */
function readSettingsFile(path: string): unknown {
  if (!existsSync(path)) return null;
  let txt: string;
  try {
    txt = readFileSync(path, "utf8");
  } catch (err) {
    console.warn(`umans: settings file ${path} unreadable (${err instanceof Error ? err.message : err}); using defaults`);
    return null;
  }
  try {
    return JSON.parse(txt);
  } catch (err) {
    console.warn(`umans: settings file ${path} is malformed JSON (${err instanceof Error ? err.message : err}); using defaults`);
    return null;
  }
}

/**
 * Validate + coerce a parsed settings object into a typed UmansSettings.
 * concurrencyMultiplier must be a finite number > 0; otherwise warn + fall back
 * to the default. Extra/unknown keys are ignored (forward-compat: a future
 * field shouldn't be a hard error for an older provider version).
 */
function coerceSettings(parsed: unknown, sourceLabel: string): UmansSettings {
  const base = defaultSettings();
  if (!isPlainObject(parsed)) return base;
  const raw = parsed.concurrencyMultiplier;
  if (raw === undefined) return base; // key absent → default (no warn)
  // Number check: rejects strings, booleans, objects, null. Number.isFinite
  // rejects NaN + Infinity. The > 0 check rejects 0 + negatives.
  if (typeof raw !== "number" || !Number.isFinite(raw) || raw <= 0) {
    console.warn(`umans: settings ${sourceLabel} concurrencyMultiplier=${JSON.stringify(raw)} is invalid (must be a finite number > 0); using default ${DEFAULT_CONCURRENCY_MULTIPLIER}`);
    return base;
  }
  // Sane-maximum cap: reject a huge finite multiplier (e.g. 1e300) that would
  // bypass the hard_cap clamp during the startup window + self-DoS the account
  // via 429s. The documented useful range is 0.5–3.0; anything above 100 has no
  // legitimate use.
  if (raw > MAX_CONCURRENCY_MULTIPLIER) {
    console.warn(`umans: settings ${sourceLabel} concurrencyMultiplier=${raw} exceeds the maximum (${MAX_CONCURRENCY_MULTIPLIER}); using default ${DEFAULT_CONCURRENCY_MULTIPLIER}`);
    return base;
  }
  return { concurrencyMultiplier: raw };
}

/**
 * Read settings from the global + project files, deep-merged (project wins).
 * Malformed or missing files fall back to defaults. The multiplier validation
 * runs AFTER the merge so a project override of an invalid global value still
 * wins when the project value is valid (and vice versa: an invalid project
 * value falls back to the merged default).
 *
 * Accepts optional path overrides (DI seam) so selfcheck can point at temp
 * files without monkey-patching homedir/cwd.
 */
export function readSettings(opts?: ReadSettingsOptions): UmansSettings {
  const globalPath = opts?.globalPath ?? defaultGlobalPath();
  const projectPath = opts?.projectPath ?? defaultProjectPath();
  const globalParsed = readSettingsFile(globalPath);
  const projectParsed = readSettingsFile(projectPath);
  // Both missing → defaults (skip the merge + coerce work).
  if (globalParsed === null && projectParsed === null) return defaultSettings();
  // A missing/unreadable file (null) means "no layer", not "explicit null" —
  // treat null as absent so the present layer survives. Only deep-merge when
  // both layers are present; otherwise use whichever is present. (A real JSON
  // null value for the whole file is vanishingly unlikely + would coerce to
  // defaults via isPlainObject(null)===false below anyway.)
  const merged = globalParsed === null
    ? projectParsed
    : projectParsed === null
      ? globalParsed
      : deepMergeSettings(globalParsed, projectParsed);
  // Label for the warn message: project is the more specific layer, so cite it
  // when present (that's where a user-introduced invalid value most likely
  // lives); otherwise cite the global file.
  const sourceLabel = projectParsed !== null ? `project (${projectPath})` : `global (${globalPath})`;
  return coerceSettings(merged, sourceLabel);
}
