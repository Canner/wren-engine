import init, { WrenEngine as WasmEngine } from "./wren_core_wasm.js";

export interface WrenProfile {
  /** Data source root: URL prefix for remote Parquet, or any non-empty string for local mode. */
  source: string;
}

export interface WrenEngineOptions {
  /**
   * WASM binary source. Accepts:
   * - URL string or URL object (fetched in browser)
   * - BufferSource such as ArrayBuffer or Node.js Buffer (instantiated directly)
   *
   * Defaults to sibling wren_core_wasm_bg.wasm resolved via import.meta.url.
   */
  wasmUrl?: string | URL | BufferSource;
}

export class WrenEngine {
  private engine: WasmEngine;

  private constructor(engine: WasmEngine) {
    this.engine = engine;
  }

  /**
   * Initialize engine, loading WASM binary.
   * Call once per page lifecycle.
   */
  static async init(options?: WrenEngineOptions): Promise<WrenEngine> {
    if (options?.wasmUrl) {
      await init({ module_or_path: options.wasmUrl });
    } else {
      await init();
    }
    const engine = new WasmEngine();
    return new WrenEngine(engine);
  }

  /**
   * Load MDL manifest with profile source.
   *
   * @param mdl - MDL manifest object (will be JSON-serialized)
   * @param profile - Profile with source path/URL
   *   - `{ source: "https://cdn/data/" }` — URL mode: auto-registers ListingTables from remote Parquet
   *   - `{ source: "./data/" }` — local mode: expects pre-registered tables via registerParquet/registerJson
   *   - `{ source: "" }` — fallback: auto-detect from tableReference fields in MDL
   */
  async loadMDL(mdl: object, profile: WrenProfile): Promise<void> {
    const mdlJson = JSON.stringify(mdl);
    await this.engine.loadMDL(mdlJson, profile.source);
  }

  /**
   * Register Parquet data from an ArrayBuffer as a named table.
   * Call before loadMDL when using local mode.
   */
  async registerParquet(name: string, data: ArrayBuffer): Promise<void> {
    await this.engine.registerParquet(name, new Uint8Array(data));
  }

  /**
   * Register JSON data as a named table.
   * Call before loadMDL when using local mode.
   */
  async registerJson(name: string, data: object[]): Promise<void> {
    await this.engine.registerJson(name, JSON.stringify(data));
  }

  /**
   * Execute SQL query through the semantic layer.
   * Returns parsed result objects.
   */
  async query(sql: string): Promise<Record<string, unknown>[]> {
    const jsonStr = await this.engine.query(sql);
    if (!jsonStr) return [];
    return JSON.parse(jsonStr);
  }

  /** Release WASM memory. */
  free(): void {
    this.engine.free();
  }
}
