# Maritime Intelligence Globe

A polished local-first Cesium globe for exploring unusual global port activity through historical time. The browser speaks only to FastAPI; all normal queries are answered by local DuckDB views over Parquet.

## Start here: first-time setup on macOS or Linux

Downloading the GitHub ZIP gives you the source code and reproducible pipelines. It intentionally does not include Python or frontend dependencies, a compiled frontend, provider datasets, or the local database. Complete this setup once; afterward the included PyCharm configurations or `python run.py` launch the existing local application without automatically contacting upstream sources.

### 1. Install prerequisites

- Python 3.12
- Node.js 22 or newer, including Corepack
- PyCharm is optional but recommended for the included run configurations

Windows is not currently supported by the locked installation workflow. Only works with macOS or Linux

### 2. Open a terminal in the extracted project and run

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --require-hashes -r requirements.lock
python -m pip install --no-deps -e .
corepack enable
cd frontend && pnpm install --frozen-lockfile && pnpm run build && cd ..
python scripts/prepare_natural_earth.py
python -m maritime.pipeline.bootstrap --days 120
```

The Natural Earth preparation step is optional; without it, the build uses Cesium's smaller bundled imagery. The PortWatch bootstrap is required for the historical globe and performs the initial external-data download. It also attempts the optional NGA metadata enrichment without allowing NGA availability to block PortWatch.

### 3. Launch from PyCharm

1. Open the extracted project folder in PyCharm.
2. Select the project interpreter at `.venv/bin/python`.
3. Mark `src` as **Sources Root** if PyCharm has not detected it automatically.
4. Select **Maritime Intelligence Globe — Safari** or **Maritime Intelligence Globe — Chrome**, then press **Run**.

On macOS, those configurations open the named browser. On Linux, either configuration starts the same backend and opens the system-default browser because Safari is unavailable. If PyCharm reports that the saved module cannot be found after extracting a renamed ZIP folder, edit the configuration once and select the current project module. Chrome requires graphics acceleration/WebGL to be enabled for Cesium.

### 4. Launch without PyCharm

With the virtual environment active, run:

```bash
python run.py
```

Then open <http://127.0.0.1:8000>.

## Local-first guarantee

Normal globe use operates entirely from locally persisted DuckDB/Parquet data. Changing dates, dragging the timeline, selecting ports, loading charts, filtering, and viewing top anomalies **do not contact external data providers**. External sources are contacted only by the initial bootstrap, an explicit **Refresh PortWatch** action (including a TTL-controlled NGA metadata check), the explicitly started background AIS collector, or an explicit bounded **History → Build Weekly History** request. A query cache miss falls through to local DuckDB, never to an upstream API.

After bootstrap, disconnecting the machine from the internet does not prevent the application from loading, rendering its bundled Natural Earth globe, exploring stored dates, or querying port history. If a refresh fails, the last valid local dataset remains untouched and usable.

`requirements.lock` pins and hashes the complete Python 3.12 runtime, development, and imagery dependency set. `pyproject.toml` remains the human-edited dependency source; regenerate the lock with `pip-compile --all-extras --strip-extras --generate-hashes --output-file=requirements.lock pyproject.toml` after changing it. The frontend declares pnpm 11 in `package.json`, and `--frozen-lockfile` prevents an install from silently changing the committed dependency graph.

Bootstrap uses approximately four months by default so a meaningful 56-day baseline and 90-day history chart are available without requiring an unbounded first download. Increase `--days` for more local history. Bootstrap is resumable at the durable Parquet merge boundary; successful published files are never replaced by partial downloads.

Bootstrap also generates an optional local NGA World Port Index enrichment cache. It requests only the static port fields used by this project, checks at most once every seven days, and stores the result under ignored `data/`. The cache and enriched provider records are never committed; the adapter, matching rules, and configuration remain in source control so every installation can reproduce them. If NGA is unavailable, PortWatch ingestion continues with the last valid WPI cache or without WPI enrichment on a first run.

The imagery preparation command performs a one-time download of the public-domain 1:10m Natural Earth II raster and converts it into a local six-level tile pyramid. The verified download and generated tiles stay in the ignored `data/cache/natural-earth` directory and are never committed; the temporary 376 MB extracted TIFF is removed after preparation. At runtime Cesium requests only visible tiles, so zoomed-in geography is much sharper without adding a paid service or loading the complete raster in the browser. If imagery has not been prepared, the build automatically falls back to Cesium's smaller bundled Natural Earth tiles.

The space background uses Cesium's locally bundled Tycho star-catalog skybox. It requires no token, network request, or additional runtime service and can be disabled with the isolated `ENABLE_STAR_SKYBOX` flag in `frontend/src/main.ts`.

### Extend the historical range safely

Stop the running app, request the desired total history window, and restart it:

```bash
python -m maritime.pipeline.bootstrap --backfill-days 365
python run.py
```

`--days` and `--backfill-days` are aliases. When local data already exists, the command downloads only the missing older interval plus the normal recent 14-day revision interval. Large intervals are divided into sequential 31-day batches without increasing request concurrency. Completed batches are checkpointed as compact Parquet files until the final atomic merge succeeds, so retrying after an interruption skips them. It merges on `(date, port_id)`, atomically replaces the validated Parquet files, recalculates analytics across the complete local history, and preserves existing observations. This is an explicit external-data operation; the ordinary **Refresh PortWatch** button remains incremental and never starts a historical backfill.

### Enable the free live AIS layer

PortWatch remains the verified shipment-estimate layer. For preliminary activity through the current day, create a free server-side key at [AISStream](https://aisstream.io/account), then configure it locally:

```bash
cp .env.example .env
# Set AISSTREAM_API_KEY in .env, restart the application, then press Start Live AIS.
```

The collector starts only after that explicit button press. It keeps one compressed, reconnecting global WebSocket open, samples vessel positions into short-lived local routes, spatially matches positions to the locally stored port catalog, and persists a provisional call after at least 20 minutes within 8 km at no more than 3 knots. The app must remain running to collect events because the free stream has no durable historical replay. The key stays on the backend and is never sent to the browser.

The global stream remains lightweight locally by requesting only the three AIS position-report types and never writing every incoming event. Each vessel is processed at most once per minute; a route point is retained after two minutes only when the vessel moved at least 1 km or changed course by at least 15 degrees, with a ten-minute continuity point. Latest points are coalesced in memory and written in batches every 15 seconds or 1,500 points. Routes expire after 72 hours, and the local route API is capped at 800 vessels with 40 points each. Collection automatically pauses above 30,000 decoded messages or 24 MB of decoded payload per minute. Compression confirmation, finite frame/queue limits, expiring caches, jittered reconnects, an authentication circuit breaker, and a five-failure ceiling provide additional safeguards.

### Import optional NOAA historical routes

NOAA/MarineCadastre publishes historical U.S. Coast Guard AIS observations for U.S. waters. The application provides an explicit, resumable weekly importer covering the six complete years from **2018-01-01 through 2023-12-31**. Nothing is downloaded during startup, PortWatch refresh, **Start Live AIS**, or normal globe use.

In the running UI, choose **History**, review the transfer estimate, and press **Build Weekly History**. The fixed plan imports one nationwide Wednesday snapshot for each ISO week—313 sampled dates—and retains one position per vessel per 30-minute bucket. This is weekly snapshot coverage, not continuous movement between Wednesdays. The always-visible NOAA/USCG row in **Data Status** reports completed dates, percentage, and an ETA once the first date establishes a measured transfer/compaction rate. The History dialog additionally reports the active date, cumulative transfer, and compact Parquet size. Each completed date is added to the **Preliminary Live AIS** slider while the remaining history continues downloading. **Pause After Current Date** finishes and validates the active date, deletes its source files, and then stops; pressing the build button later resumes from the first incomplete date.

NOAA's daily national archives total about 552.8 GB across the six source years. Sampling one of every seven days is estimated to transfer about **79 GB cumulatively**. That transfer is never retained as a 79 GB local archive: the pipeline downloads exactly one daily ZIP, extracts one temporary CSV, uses DuckDB to project the seven route fields and sample them, writes and validates a Zstandard-compressed date-partitioned Parquet file, and then deletes the ZIP, CSV, DuckDB spill files, and partial download before moving to the next date. Startup also removes an interrupted day's orphan source artifacts. Only completed Parquet partitions and tiny completion checkpoints remain.

The equivalent command-line workflow is available for scripted imports. Stop the application before using the CLI so only one process writes the local DuckDB manifest:

```bash
# Example: one day around the New York / New Jersey approaches.
python -m maritime.pipeline.noaa_ais \
  --start 2023-06-07 \
  --end 2023-06-07 \
  --bounds 39.5,-75.5,41.5,-72.5

python run.py
```

The CLI remains available as an advanced, small-area importer. Bounds are required and use `south,west,north,east`. A CLI run is limited to seven days, requests one hour at a time through NOAA's ERDDAP subset service, enforces a 256 MB response ceiling per chunk, runs sequentially through the shared polite HTTP client, and defaults to one retained point per vessel per ten-minute bucket. The UI uses the resumable national weekly bulk pipeline instead.

Normalized observations are merged idempotently on `(source, MMSI, observed_at)` into ignored, UTC date-partitioned files under `data/silver/noaa_ais_routes/date=YYYY-MM-DD/`. A `weekly-national.complete.json` checkpoint is written only after the Parquet partition passes row and schema validation. Only pipeline code and configuration are committed; downloaded archives, temporary CSV files, generated Parquet partitions, checkpoints, DuckDB manifests, and credentials remain excluded by `.gitignore`.

When an imported NOAA date overlaps the selected PortWatch date, gold route lines appear as a contextual overlay while verified port metrics and scores remain unchanged. The **Preliminary Live AIS** timeline also includes locally imported NOAA dates. Its legend changes to **Historical AIS · NOAA/USCG route · U.S. waters only**, so regional historical coverage is never presented as a global or live feed. NOAA routes are for coastal/ocean-planning analysis, not navigation, enforcement, or regulatory decisions.

The data sources are intentionally not blended analytically:

- **Verified PortWatch:** published port calls, shipment-volume estimates, and the existing volume-plus-calls activity score.
- **Preliminary Live AIS:** locally derived calls and unique vessels. A separate calls-only score appears after seven prior collection days; before then the UI says that the baseline is building.
- **NOAA historical AIS:** optional sampled U.S.-waters route context. It does not create shipment volume, port calls, or activity scores.

Live calls never populate shipment weight. When PortWatch later publishes the same date, the verified layer is the authoritative shipment view while the provisional observations remain available for comparison.

The shared PyCharm configurations and `python run.py` only launch the local application; they do not bootstrap, refresh, or import provider data automatically.

### Development

```bash
uvicorn maritime.api.main:app --reload --app-dir src
cd frontend && pnpm run dev
```

Vite proxies `/api` to port 8000. The production/local build is served by FastAPI, so only `python run.py` is needed after building.

## Architecture

```text
Cesium + TypeScript ──local HTTP──> FastAPI
                                      │
                         snapshot / port services (local only)
                                      │
                         DuckDB views over Silver + Gold Parquet

PortWatch ──bootstrap/explicit refresh────────────> Silver Parquet → Gold analytics
NGA WPI ───TTL-controlled metadata enrichment────> ignored local port metadata cache

NOAA/USCG AIS ──explicit weekly bulk import───────> ignored daily route Parquet
                                                               │
                                                               v
                                                   historical route replay

AISStream ──explicit Start Live AIS──> in-memory coalescing ──> batched route points
                                                │                       │
                                                v                       v
                                   deduplicated local calls      live Cesium tracks
```

`maritime.ingestion`, `MaritimeRefreshService`, and `NoaaWeeklyHistoryService` form the external-access boundary. Storage repositories deliberately do not import the network package. PortWatch refresh and NOAA history import are independently guarded so repeated clicks return the active job instead of starting duplicate downloads. The AIS collector is also guarded and cannot start without both a server-side key and an explicit start action. The UI polls only local status and shows measured NOAA date, transfer, and compacted-storage progress instead of fabricated percentages.

## Official source interfaces reviewed September 2026

- **IMF PortWatch:** the official OGC Records [Search API](https://portwatch.imf.org/api/search/definition/) identifies the public `Daily_Ports_Data` ArcGIS FeatureServer and `PortWatch_ports_database` FeatureServer. Daily data supports `date`, `portcalls`, cargo/tanker calls, and estimated `import`/`export`; the layer advertises a 1,000-record page size. The adapter uses ordered sequential pagination, rejects repeated pages, requests only the revision-aware date window, and never issues per-port/per-day requests. PortWatch’s catalog records link to IMF terms and dataset-specific citation guidance. No general numeric request limit was published in the inspected interface, so the project defaults to two requests per host and at least 0.5 seconds between starts.
- **AISStream:** the official [WebSocket documentation](https://aisstream.io/documentation) describes a free global AIS stream, server-side API keys, up to three subscribed connections per account, required geographic bounding boxes, and typed position/static messages. It explicitly provides no SLA and no durable replay; clients must read continuously, use message filters, enable compression, and reconnect with backoff. This project uses one compressed global backend connection, stores sampled positions for only 72 hours, batches database writes, and retains deduplicated point-geofence/dwell call events for 180 days by default. Daily maintenance prunes expired live rows and checkpoints DuckDB so its allocated space can be reclaimed or reused. AISStream does not provide measured cargo weight, so its output is never shown as shipment volume.
- **NOAA/MarineCadastre historical AIS:** NOAA's [historical AIS catalog](https://marinecadastre.gov/ais/) and [ERDDAP access service](https://data.pmel.noaa.gov/pmel/erddap/search/index.html?searchFor=Automatic+Identification+System) publish one-minute-filtered U.S. Coast Guard NAIS observations for U.S. waters. NOAA identifies the data as U.S. Government material and applies a CC0 public-domain dedication to the ERDDAP copies; the catalog describes the intended use as coastal and ocean planning. The UI importer uses NOAA's official daily national ZIPs for large transfers, downloads sequentially, enforces compressed and expanded per-day limits, projects only route fields, samples every 30 minutes, atomically validates Parquet, and immediately discards source artifacts. The advanced CLI retains the bounded ERDDAP workflow. Reception gaps, publication lag, and terrestrial antenna coverage mean the tracks remain incomplete and regional.
- **NGA World Port Index:** NGA’s public [ArcGIS service](https://fgmod.nga.mil/nauticalpubs-feature/rest/services/WPI/World_Port_Index_Viewer/FeatureServer) supports querying and downloading worldwide port locations and attributes with a 3,000-record page limit. Bootstrap and explicit PortWatch refreshes invoke the isolated adapter only when the seven-day cache TTL has expired. The adapter discovers the live schema, requests only the fields used for classification and port details, validates the complete extract, and atomically replaces `data/silver/nga_world_port_index.parquet`. PortWatch ports are matched conservatively by unique UN/LOCODE, then unambiguous name/location evidence; unmatched records remain unmatched rather than being guessed. NGA failure never blocks PortWatch and certificate verification is never disabled. The configured `fgmod.nga.mil` public endpoint is used instead of the CAC-oriented `vcps.nga.mil` host so standard Python TLS verification remains enabled.
- **Natural Earth:** all raster and vector data is public domain under its [terms of use](https://www.naturalearthdata.com/about/terms-of-use/). The optional preparation step downloads the 1:10m Natural Earth II raster from Natural Earth's official S3 host and generates a six-level local tile pyramid. When that cache is absent, the build falls back to Cesium's smaller packaged NaturalEarthII tiles. Both paths load locally—there is no Cesium ion token, paid imagery service, or runtime tile dependency.

PortWatch revisions are expected. Refresh re-requests the configurable last 14 days, merges on `(date, port_id)`, and reports inserted versus revised rows. WPI is slow-moving and configured for a seven-day freshness TTL; its last check, last success, row count, and any stale-cache warning are recorded in the local ingestion manifest. Source `ETag`, `Last-Modified`, and check metadata have manifest columns ready for providers that return them. All downloads use bounded exponential backoff with jitter; 429 and `Retry-After` are honored. Routine tests use local fixtures and never contact public providers.

## Configuration

Behavior is split across `config/app.yaml`, `sources.yaml`, `network.yaml`, `storage.yaml`, `analytics.yaml`, and `visualization.yaml`. Important defaults include a 56-day verified baseline, 30-day minimum verified history, 65/35 volume-to-calls score weight, five retries, conservative concurrency, and a 14-day revision window. AIS geofence behavior, the 72-hour route retention, 180-day port-call retention, 24-hour DuckDB maintenance interval, and WPI cache, validation, name/proximity matching, and ambiguity thresholds are configurable in `sources.yaml`.

The activity score maps the weighted local z-score to 0–100: `0 ≤ score < 20` is strongly below normal, `20 ≤ score < 40` is below normal, `40 ≤ score < 60` is approximately normal, `60 ≤ score < 80` is elevated, `80 ≤ score < 95` is highly elevated, and `95 ≤ score ≤ 100` is extreme. Scores describe each port relative to its own trailing history—not absolute port size. Shipment volume and port calls retain their configured 65% / 35% weights; a component with no usable variation is treated as neutral rather than transferring its weight to the remaining component.

### Shipment-volume breakdown

The selected-day shipment panel separates PortWatch's estimated directional volume so the displayed total is not mistaken for inbound cargo alone:

- **Total throughput** is estimated imports plus estimated exports. This combined value remains the shipment-volume input to the existing activity score.
- **Imports** are PortWatch's estimated inbound volume for the selected port and day.
- **Exports** are PortWatch's estimated outbound volume for the selected port and day.
- **Import / export share** shows each direction as a percentage of total throughput when both directional estimates are available and their sum is greater than zero.
- **Net flow** is imports minus exports. A positive value means net inbound, a negative value means net outbound, and zero means balanced. Net flow is contextual only and is not an activity-score input.

PortWatch shipment estimates are stored and calculated in metric tonnes. The UI can display either metric tonnes (`t`) or pounds (`lb`), using `1 metric tonne = 2,204.62262185 pounds`. This toggle changes presentation only: stored values, historical calculations, z-scores, baselines, and activity scores remain in metric tonnes and are not recalculated. If one of the directional estimates needed for a share or net-flow calculation is unavailable, the UI displays an em dash instead of inventing a value.

Port marker shapes are stable across the timeline because they are classified from explicit PortWatch identity, conservatively matched NGA WPI terminal evidence, and the complete locally stored PortWatch call history—not the selected day. Diamonds and squares require explicit or dedicated oil/LNG terminal evidence; a general port is not labeled oil-specific merely because WPI reports an oil facility nearby. Triangles identify ports with at least 20 typed calls and an 80% tanker share, and circles represent cargo or mixed-use ports. Every WPI match retains its method, confidence, distance, WPI number, and retrieval date. Shape communicates port function while color continues to communicate activity.

## API

- `GET /api/health`
- `GET /api/dates`
- `GET /api/globe/snapshot?date=YYYY-MM-DD`
- `GET /api/ports/{port_id}?date=YYYY-MM-DD`
- `GET /api/ports/{port_id}/history?start=...&end=...`
- `GET /api/anomalies/top?date=...&limit=10`
- `GET /api/data/status`
- `GET /api/live/status`
- `POST /api/live/start` — explicitly starts the backend AISStream collector
- `POST /api/live/stop`
- `GET /api/live/snapshot?date=YYYY-MM-DD`
- `GET /api/live/routes` — bounded recent tracks served from local DuckDB
- `GET /api/live/ports/{port_id}/history?start=...&end=...`
- `GET /api/historical/routes/dates` — locally imported NOAA route dates and coverage label
- `GET /api/historical/routes?date=YYYY-MM-DD` — bounded route replay from one local UTC partition
- `POST /api/historical/import` — starts or resumes the guarded weekly national NOAA import
- `GET /api/historical/import/status`
- `POST /api/historical/import/cancel`
- `POST /api/refresh` — explicitly checks PortWatch
- `GET /api/refresh/status`

## Tests

```bash
pytest
cd frontend && pnpm run build
```

Tests cover configuration-sensitive scoring, baselines, revision windows, Retry-After parsing, health, snapshots, dates, histories, anomalies, AIS dwell detection, repeat-call deduplication, route sampling/batch persistence, NOAA query bounding/downsampling, weekly date selection, bulk ZIP-to-Parquet compaction, source cleanup, idempotent route merging, historical date partitions, guarded import jobs, and the calls-only baseline gate. The local-only API test replaces the HTTP client with a function that raises immediately; every normal exploration endpoint, including live and historical route retrieval, must still succeed.

## Data quality and limitations

AIS-derived estimates and routes can contain coverage/reception gaps, latency, revisions, and methodological changes, and they can differ materially from customs statistics. Live point geofences are approximations: closely spaced terminals, AIS gaps, noisy coordinates, and vessels that do not transmit can create missed or misclassified calls. NOAA historical tracks cover U.S. waters only and have publication lag. Route lines connect sampled observations and therefore indicate observed movement, not a guaranteed continuous voyage. Empty source fields remain null; the application never invents missing source values or cargo weight. Refresh validation rejects invalid coordinates, duplicates, high null rates, and suspiciously small port extracts before atomic promotion. This product is a maritime-activity intelligence view—not a navigation, enforcement, regulatory, or official customs-accounting system—and does not imply IMF, AISStream, NOAA, USCG, NGA, or Natural Earth endorsement.

To add a source, implement an adapter under `maritime.ingestion`, add source configuration and manifest metadata, normalize into the canonical port/activity schemas, and invoke it only from an explicit ingestion or refresh boundary. Never import a provider into a query repository or GET route.

## License

The original source code in this repository is available under the [MIT License](LICENSE).

The MIT License applies only to this project's original source code. Third-party software, data, imagery, and services—including IMF PortWatch, AISStream, NOAA/USCG AIS, NGA World Port Index, Natural Earth, and CesiumJS—remain subject to their respective licenses and terms. Downloaded provider datasets, generated NOAA route partitions, the generated WPI cache and enriched port records, locally captured AIS observations, databases, and credentials are excluded by `.gitignore` and are not distributed with this repository.

See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for bundled frontend dependencies. Production builds automatically copy the project license, CesiumJS's Apache-2.0 license and third-party manifests, and Chart.js's MIT license into `frontend/dist/licenses`, so those notices remain present in compiled releases and hosted bundles.
