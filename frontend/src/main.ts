import './style.css'
import './clarity.css'
import 'cesium/Build/Cesium/Widgets/widgets.css'
import { Billboard, BillboardCollection, Viewer, Cartesian3, Color, ConstantProperty, Ellipsoid, Entity, HeightReference, Material, NearFarScalar, PolylineCollection, TileMapServiceImageryProvider, Math as CesiumMath, Ion } from 'cesium'
import Chart from 'chart.js/auto'

type Layer = 'verified' | 'live'
type ShipmentUnit = 'metric' | 'pounds'
type ScoreDirection = 'above' | 'below' | 'neutral' | 'unavailable'
type AnomalyDirection = 'increase' | 'decrease'
type PortFunction = 'oil_terminal' | 'tanker_dominant' | 'cargo_or_mixed' | 'lng_lpg_terminal'
type PortFunctionFilter = 'all' | PortFunction
type Port = {
  port_id:string; name:string; country:string; latitude:number; longitude:number;
  activity_score:number|null; volume_z_score?:number|null; calls_z_score?:number|null;
  change_7d_pct?:number|null; change_30d_pct?:number|null; port_calls:number|null;
  total_volume?:number|null; import_volume?:number|null; export_volume?:number|null;
  importance:number; unique_vessels?:number|null;
  calls_expected?:number|null; baseline_days?:number|null; last_observation_at?:string|null;
  port_function?:PortFunction; port_function_label?:string; classification_basis?:string;
  classification_confidence?:string; tanker_call_share?:number|null; typed_call_count?:number;
}
type Dates = {earliest_available_date:string|null;latest_available_date:string|null;date_count:number;available_dates:string[]}
type LiveStatus = Dates & {
  state:string; stage:string; configured:boolean; connected_at:string|null; last_message_at:string|null;
  messages_received:number; calls_detected:number; reconnects:number; error:string|null;
  port_call_count:number; unique_vessel_count:number; last_observation_at:string|null;
  route_points_persisted:number; active_route_vessels:number; pending_route_points:number;
  last_route_flush_at:string|null;
}
type RoutePoint = {observed_at:string;latitude:number;longitude:number}
type VesselRoute = {
  mmsi:string; vessel_name:string|null; last_observed_at:string;
  speed_knots:number|null; course_degrees:number|null; points:RoutePoint[];
}
type RouteResult = {
  hours?:number;date?:string;vessel_count:number;provisional:boolean;historical?:boolean;
  source?:string;source_label?:string;coverage_label?:string;routes:VesselRoute[]
}
type HistoricalRouteDates = Dates & {source:string;source_label:string;coverage_label:string}
type NoaaImportStatus = {
  job_id:string|null;state:string;stage:string;current_date:string|null;
  completed_dates:number;total_dates:number;skipped_dates:number;
  rows_added:number;rows_updated:number;source_bytes_downloaded:number;
  parquet_bytes_written:number;started_at?:string|null;completed_at?:string|null;
  average_seconds_per_date?:number|null;estimated_seconds_remaining?:number|null;
  estimated_completion_at?:string|null;error:string|null
}

const api = async <T>(path:string, options?:RequestInit):Promise<T> => {
  const response = await fetch(path, options)
  if (!response.ok){const body=await response.text();let message=body;try{const parsed=JSON.parse(body);message=parsed.detail||body}catch{/* Keep a non-JSON provider response readable. */}throw new Error(message||`Request failed: ${response.status}`)}
  return response.json()
}

document.querySelector<HTMLDivElement>('#app')!.innerHTML = `
  <main class="terminal">
    <header class="topbar">
      <div class="brand"><div><h1>MARITIME INTELLIGENCE</h1><p>Verified shipments + preliminary live vessel activity</p></div></div>
      <div class="clock"><span>LOCAL TIME</span><strong id="local-time">—</strong></div>
    </header>
    <section class="workspace">
      <div id="cesium" aria-label="Interactive 3D maritime activity globe"></div>
      <div class="globe-vignette"></div>
      <aside class="panel anomalies-panel">
        <div class="panel-head"><span id="ranking-title">TOP ABOVE-NORMAL ACTIVITY</span><span id="ranking-label" class="signal-label">7D SIGNAL</span></div>
        <div id="anomaly-direction" class="anomaly-direction" role="group" aria-label="Activity range"><button data-direction="increase" class="active" aria-pressed="true">↑ ABOVE NORMAL</button><button data-direction="decrease" aria-pressed="false">↓ BELOW NORMAL</button></div>
        <div id="anomalies" class="anomaly-list"><div class="skeleton">Awaiting local dataset</div></div>
      </aside>
      <aside class="panel status-panel">
        <div class="panel-head"><span>DATA STATUS</span><div class="panel-actions"><button id="noaa-history" class="text-btn">HISTORY</button><button id="about" class="text-btn">SOURCES</button></div></div>
        <div class="source-status verified-source"><div><span class="source-badge verified">VERIFIED</span><b>PORTWATCH SHIPMENTS</b></div><strong id="data-through">—</strong><small>DATA THROUGH</small></div>
        <div class="source-status live-source"><div><span id="live-indicator" class="source-badge offline">OFF</span><b>LIVE AIS ACTIVITY</b></div><strong id="live-through">NO CALLS YET</strong><small id="last-ais">NOT STARTED</small></div>
        <div class="source-status history-source"><div><span id="noaa-indicator" class="source-badge offline">HISTORY</span><b>NOAA / USCG ROUTES</b></div><strong id="noaa-through">0 / 313 DATES</strong><small id="noaa-summary">PRESS HISTORY TO BUILD</small><progress id="noaa-meter" max="100" value="0" aria-label="Historical route import progress"></progress></div>
        <button id="refresh" class="refresh-btn"><span>↻</span> REFRESH PORTWATCH</button>
        <button id="live-toggle" class="live-btn"><span>●</span> START LIVE AIS</button>
        <div id="refresh-message" class="refresh-message">Globe exploration reads only local data</div>
      </aside>
      <aside id="detail" class="detail-panel" aria-hidden="true"><button id="close-detail" class="close">×</button><div id="detail-content"></div><canvas id="history-chart"></canvas></aside>
      <div id="map-key" class="map-key"></div>
    </section>
    <section class="timeline-shell">
      <div class="control-row">
        <div class="layer-switch" role="group" aria-label="Data layer"><button data-layer="verified" class="active"><span></span>VERIFIED PORTWATCH</button><button data-layer="live"><span></span>PRELIMINARY LIVE AIS</button></div>
        <div id="activity-filters" class="filters" role="group" aria-label="Activity filter"><button data-filter="all" class="active">ALL</button><button data-filter="above">ABOVE NORMAL</button><button data-filter="below">BELOW NORMAL</button><label>MIN SCORE <input id="minimum" type="number" min="0" max="100" value="0"/></label></div>
      </div>
      <div class="timeline"><button id="previous" aria-label="Previous day">‹</button><input id="date-picker" type="date" aria-label="Data date"/><input id="slider" type="range" min="0" max="0" value="0" aria-label="Data timeline"/><button id="next" aria-label="Next day">›</button><div class="selected"><span id="selected-layer-label">VERIFIED PORTWATCH ACTIVITY</span><strong id="selected-date">NO LOCAL DATA</strong></div></div>
    </section>
    <dialog id="noaa-dialog" class="import-dialog"><button class="close" id="close-noaa">×</button><h2>Build weekly U.S. route history</h2><p>Import one nationwide NOAA/USCG AIS snapshot every Wednesday from 2018 through 2023. Routes are sampled every 30 minutes and remain separate from shipment metrics and activity scores.</p><div class="history-plan"><div><strong>313</strong><span>WEEKLY DATES</span></div><div><strong>6 YEARS</strong><span>2018–2023</span></div><div><strong>≈79 GB</strong><span>TOTAL TRANSFER</span></div></div><p class="import-warning"><b>Source cleanup is automatic.</b> Each daily ZIP and extracted CSV is deleted immediately after its compressed Parquet partition is validated. Only compact local Parquet remains; the job can be paused and resumed without repeating completed dates.</p><div id="noaa-progress" class="import-progress">Ready · no download starts until you press the button</div><div class="import-actions"><button id="cancel-noaa" type="button" disabled>PAUSE AFTER CURRENT DATE</button><button id="start-noaa" type="button">BUILD WEEKLY HISTORY</button></div></dialog>
    <dialog id="about-dialog"><button class="close" id="close-about">×</button><h2>Data & methodology</h2><p>This globe deliberately separates fast preliminary vessel activity from slower shipment estimates. Every map interaction reads local DuckDB/Parquet data; external access occurs only through explicit ingestion actions.</p><dl><dt>Verified · IMF PortWatch</dt><dd>AIS-derived daily port calls and estimated import/export shipment activity. It is retained as the shipment-volume layer and may arrive with publication lag or revisions.</dd><dt>Historical routes · NOAA/USCG</dt><dd>Optional weekly U.S.-waters snapshots for 2018–2023, sampled to one observation per vessel every 30 minutes and stored as local Parquet. Imported Wednesdays replay alongside the date slider and never affect PortWatch shipment metrics or activity scores.</dd><dt>Port marker shapes</dt><dd>Shapes describe a stable port function calculated from the complete locally stored PortWatch history: diamonds are explicitly identified oil terminals, squares are explicitly identified LNG/LPG terminals, triangles are historically tanker-dominant ports, and circles are cargo or mixed-use ports. Tanker-dominant does not necessarily mean oil-specific.</dd><dt>Preliminary · AISStream</dt><dd>A live, free, global WebSocket feed. Position reports are coalesced in memory and sampled into short-lived local routes; the globe never contacts AISStream directly. This app also derives a port call when a vessel remains within 8 km of a port for at least 20 minutes at 3 knots or less. Results can be missed or misclassified because reception and point geofences are imperfect.</dd><dt>Live activity score</dt><dd>A calls-only score is calculated separately after seven prior collection days. Until then the UI says “baseline building” instead of inventing a score.</dd><dt>Setup</dt><dd>Create a free server-side key at <a href="https://aisstream.io/authenticate" target="_blank" rel="noreferrer">aisstream.io</a>, copy <code>.env.example</code> to <code>.env</code>, set <code>AISSTREAM_API_KEY</code>, restart the app, then press Start Live AIS. The app must remain running to collect events; the free stream provides no historical replay or uptime guarantee.</dd></dl><p class="disclaimer">Live and historical AIS routes are activity indicators—not cargo manifests, shipment weights, navigation guidance, or customs accounting. NOAA routes cover U.S. waters only. No endorsement by the IMF, AISStream, NOAA, USCG, NGA, or Natural Earth is implied.</p></dialog>
  </main>`

Ion.defaultAccessToken = ''
// Cesium's bundled Tycho star catalog is local, static, and inexpensive. Flip
// this single flag to return to the original blank-space background.
const ENABLE_STAR_SKYBOX = true
const viewerOptions: Viewer.ConstructorOptions = {animation:false,timeline:false,baseLayer:false,baseLayerPicker:false,geocoder:false,homeButton:false,sceneModePicker:false,navigationHelpButton:false,fullscreenButton:false,infoBox:false,selectionIndicator:false,terrainProvider:undefined,skyBox:ENABLE_STAR_SKYBOX?undefined:false,skyAtmosphere:false}
let viewer: Viewer
try {
  viewer = new Viewer('cesium', viewerOptions)
} catch (error) {
  // Some embedded/accelerated browser configurations advertise WebGL 2 but
  // cannot create its context. Keep WebGL 2 as the normal path and retry once
  // with Cesium's supported WebGL 1 renderer only for that specific failure.
  const message = error instanceof Error ? `${error.name}: ${error.message}` : String(error)
  if (!/webgl/i.test(message)) throw error
  document.querySelector('#cesium')!.replaceChildren()
  viewer = new Viewer('cesium', {...viewerOptions, contextOptions:{requestWebgl1:true}})
}
// FXAA softens Cesium's single-pixel catalog stars. Disabling only this
// full-frame pass keeps the same skybox, star size, density, and placement.
viewer.scene.postProcessStages.fxaa.enabled = false
// The skybox provides the subtle star field; hide Cesium's much brighter
// astronomical Sun sprite so it never distracts behind the globe.
if (viewer.scene.sun) viewer.scene.sun.show = false
viewer.scene.globe.baseColor = Color.fromCssColorString('#0b1c2c')
viewer.scene.backgroundColor = Color.fromCssColorString('#03090f')
viewer.scene.globe.showGroundAtmosphere = true
viewer.scene.globe.enableLighting = false
viewer.scene.globe.depthTestAgainstTerrain = true
const routeLines = viewer.scene.primitives.add(new PolylineCollection())
const vesselHeads = viewer.scene.primitives.add(new BillboardCollection())
TileMapServiceImageryProvider.fromUrl('/imagery/NaturalEarthII').then(provider => { viewer.imageryLayers.removeAll(); const imagery=viewer.imageryLayers.addImageryProvider(provider); imagery.brightness=.42; imagery.contrast=1.2; imagery.saturation=.25 })
viewer.camera.setView({destination:Cartesian3.fromDegrees(12,18,21000000),orientation:{heading:0,pitch:CesiumMath.toRadians(-90),roll:0}})

let verifiedDates:Dates = {earliest_available_date:null,latest_available_date:null,date_count:0,available_dates:[]}
let historicalRouteDates:HistoricalRouteDates = {earliest_available_date:null,latest_available_date:null,date_count:0,available_dates:[],source:'noaa_marinecadastre',source_label:'NOAA Historical AIS',coverage_label:'U.S. waters only'}
let liveStatus:LiveStatus | null = null
let layer:Layer = 'verified'
let selectedDate = ''
let allPorts:Port[] = []
let filter = 'all'
let portFunctionFilter:PortFunctionFilter = 'all'
let anomalyDirection:AnomalyDirection = 'increase'
let shipmentUnit:ShipmentUnit = localStorage.getItem('maritime-shipment-unit') === 'pounds' ? 'pounds' : 'metric'
let historyChart:Chart | null = null
let requestController:AbortController | null = null
let routeRequestController:AbortController | null = null
let routeRequestActive = false
let lastRouteFlushRendered:string|null = null
let routeOverlaySource:'live'|'noaa'|null = null
let cameraIsMoving = false
let pendingRouteResult:RouteResult|null = null
const entities = new Map<string,Entity>()
const topIncreasePortIds = new Set<string>()
const topDecreasePortIds = new Set<string>()
const portSurfacePositions = new Map<string,Cartesian3>()
const portFilterVisibility = new Map<string,boolean>()
const vesselHeadEntries:Array<{billboard:Billboard;surfacePosition:Cartesian3}> = []
const lastVisibilityCameraPosition = new Cartesian3()
const visibilitySurfaceNormal = new Cartesian3()
const visibilityToCamera = new Cartesian3()
let hasVisibilityCameraPosition = false
const PORT_MARKER_HEIGHT_METERS = 1_000
const VESSEL_MARKER_HEIGHT_METERS = 1_100
const MARKER_NEAR_DISTANCE_METERS = 500_000
const MARKER_FAR_DISTANCE_METERS = 24_000_000
const MARKER_HORIZON_HYSTERESIS = .006
const BELOW_NORMAL_SCORE = 40
const ABOVE_NORMAL_SCORE = 60

const fmt = (value:number|null|undefined,digits=0) => value==null?'—':new Intl.NumberFormat(undefined,{maximumFractionDigits:digits}).format(value)
const fmtDate = (value:string|null|undefined) => value ? new Date(value.length===10?value+'T12:00:00':value).toLocaleString(undefined,value.length===10?{year:'numeric',month:'short',day:'numeric'}:{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}) : '—'
const fmtDuration = (seconds:number|null|undefined) => {if(seconds==null||!Number.isFinite(seconds))return 'ESTIMATING AFTER FIRST DATE';const minutes=Math.max(1,Math.round(seconds/60));if(minutes<60)return `ABOUT ${minutes} MIN REMAINING`;const hours=Math.round(minutes/60);if(hours<48)return `ABOUT ${hours} HR REMAINING`;return `ABOUT ${Math.round(hours/24)} DAYS REMAINING`}
const localISODate = () => {const now=new Date();const pad=(value:number)=>String(value).padStart(2,'0');return `${now.getFullYear()}-${pad(now.getMonth()+1)}-${pad(now.getDate())}`}
const fmtShipment = (value:number|null|undefined,metricDigits=0) => value==null?'—':shipmentUnit==='pounds'?`${fmt(value*2204.62262185)} lb`:`${fmt(value,metricDigits)} t`
const finiteShipment = (value:unknown) => typeof value==='number'&&Number.isFinite(value)?value:null
const fmtSignedShipment = (value:number|null) => value==null?'—':`${value>0?'+':''}${fmtShipment(value)}`
const scoreColor = (score:number|null) => score==null?Color.fromCssColorString('#71d3dc'):score>=95?Color.fromCssColorString('#ff6247'):score>=80?Color.fromCssColorString('#ff9d4d'):score>=ABOVE_NORMAL_SCORE?Color.fromCssColorString('#f3d56b'):score<BELOW_NORMAL_SCORE?Color.fromCssColorString('#43a8c4'):Color.fromCssColorString('#8bc8c5')
const statusFor = (score:number|null) => score==null?'Baseline building':score>=95?'Extreme anomaly':score>=80?'Highly elevated':score>=ABOVE_NORMAL_SCORE?'Elevated':score<20?'Strongly below normal':score<BELOW_NORMAL_SCORE?'Below normal':'Approximately normal'
const directionFor = (score:number|null):ScoreDirection => score==null?'unavailable':score>=ABOVE_NORMAL_SCORE?'above':score<BELOW_NORMAL_SCORE?'below':'neutral'
const directionIndicator = (score:number|null) => {const direction=directionFor(score);return direction==='above'?'<em class="direction-indicator score-above">↑ ABOVE-NORMAL ACTIVITY</em>':direction==='below'?'<em class="direction-indicator score-below">↓ BELOW-NORMAL ACTIVITY</em>':direction==='neutral'?'<em class="direction-indicator score-neutral">• NORMAL RANGE</em>':''}
const directionSymbol = (score:number|null) => directionFor(score)==='above'?'↑':directionFor(score)==='below'?'↓':'•'
const markerColor = (port:Port) => topIncreasePortIds.has(port.port_id)?Color.fromCssColorString('#4ee391'):topDecreasePortIds.has(port.port_id)?Color.fromCssColorString('#ff9b55'):scoreColor(port.activity_score)
const markerShapeClass = (portFunction:PortFunction|undefined) => portFunction==='oil_terminal'?'shape-diamond':portFunction==='tanker_dominant'?'shape-triangle':portFunction==='lng_lpg_terminal'?'shape-square':'shape-circle'
const matchesPortFunction = (port:Port) => portFunctionFilter==='all'||(port.port_function||'cargo_or_mixed')===portFunctionFilter
const portFunctionFilterLabel = () => portFunctionFilter==='oil_terminal'?'OIL':portFunctionFilter==='tanker_dominant'?'TANKER':portFunctionFilter==='lng_lpg_terminal'?'LNG/LPG':portFunctionFilter==='cargo_or_mixed'?'CARGO/MIXED':''
const markerImages = new Map<string,HTMLCanvasElement>()
const markerImage = (portFunction:PortFunction|undefined,color:Color) => {
  const shape=portFunction||'cargo_or_mixed';const cssColor=color.toCssColorString();const key=`${shape}:${cssColor}`;const cached=markerImages.get(key);if(cached)return cached
  const canvas=document.createElement('canvas');canvas.width=64;canvas.height=64;const context=canvas.getContext('2d')!;context.beginPath()
  if(shape==='oil_terminal'){context.moveTo(32,7);context.lineTo(57,32);context.lineTo(32,57);context.lineTo(7,32);context.closePath()}
  else if(shape==='tanker_dominant'){context.moveTo(32,7);context.lineTo(58,55);context.lineTo(6,55);context.closePath()}
  else if(shape==='lng_lpg_terminal'){context.rect(10,10,44,44)}
  else context.arc(32,32,22,0,Math.PI*2)
  context.fillStyle=cssColor;context.strokeStyle='rgba(232,251,255,.78)';context.lineWidth=4;context.lineJoin='round';context.fill();context.stroke();markerImages.set(key,canvas);return canvas
}
const vesselMarkerImages = new Map<string,HTMLCanvasElement>()
const vesselMarkerImage = (color:Color) => {
  const key=color.toCssColorString();const cached=vesselMarkerImages.get(key);if(cached)return cached
  // Generous transparent padding keeps the small silhouette intact while
  // Cesium scales and clips billboards during camera movement.
  const canvas=document.createElement('canvas');canvas.width=64;canvas.height=80;const context=canvas.getContext('2d')!
  context.beginPath();context.moveTo(32,4);context.lineTo(54,25);context.lineTo(48,67);context.quadraticCurveTo(32,76,16,67);context.lineTo(10,25);context.closePath()
  context.fillStyle=key;context.strokeStyle='rgba(224,252,255,.9)';context.lineWidth=4;context.lineJoin='round';context.fill();context.stroke()
  context.fillStyle='rgba(4,18,27,.78)';context.fillRect(25,32,14,23)
  context.strokeStyle='rgba(224,252,255,.82)';context.lineWidth=3;context.strokeRect(25,32,14,23)
  vesselMarkerImages.set(key,canvas);return canvas
}
const portFunctionBadge = (port:Port) => {
  const label=port.port_function_label||'Cargo / mixed-use port';const basis=port.classification_basis||'Insufficient typed-call history for a specialized classification';const confidence=(port.classification_confidence||'low').toUpperCase()
  return `<div class="port-function-badge"><i class="marker-shape ${markerShapeClass(port.port_function)}"></i><span><b>${label}</b><small>${basis} · ${confidence} CONFIDENCE</small></span></div>`
}
const verifiedSignalExplanation = (activity:any) => {
  const selected=`Selected day: ${fmt(activity.port_calls)} calls and ${fmtShipment(activity.total_volume)} total throughput reported.`
  const trailing=`Trailing 7-day daily averages: ${fmt(activity.calls_7d,2)} calls versus ${fmt(activity.calls_expected,2)} baseline; ${fmtShipment(activity.volume_7d,1)} total throughput versus ${fmtShipment(activity.volume_expected,1)} baseline.`
  const volumeAvailable=activity.volume_z_score!=null
  const callsAvailable=activity.calls_z_score!=null
  const components=!volumeAvailable&&!callsAvailable?'Neither component has usable variation, so the score is neutral.':!volumeAvailable?'Shipment volume has no usable variation, so its 65% component is neutral; port calls retain their configured 35% weight.':!callsAvailable?'Port calls have no usable variation, so their 35% component is neutral; shipment volume retains its configured 65% weight.':'Shipment volume and port calls contribute at their configured 65% / 35% weights.'
  return `${selected} ${trailing} ${components}`
}
const shipmentVolumeTable = (activity:any) => {
  const imports=finiteShipment(activity.import_volume)
  const exports=finiteShipment(activity.export_volume)
  const bothReported=imports!=null&&exports!=null
  const directionalTotal=bothReported?imports+exports:null
  const importShare=imports!=null&&directionalTotal!=null&&directionalTotal>0?imports/directionalTotal*100:null
  const exportShare=exports!=null&&directionalTotal!=null&&directionalTotal>0?exports/directionalTotal*100:null
  const netFlow=bothReported?imports-exports:null
  const share=importShare==null||exportShare==null?'—':`${fmt(importShare,1)}% / ${fmt(exportShare,1)}%`
  const netDirection=netFlow==null?'BOTH ESTIMATES REQUIRED':netFlow>0?'NET INBOUND':netFlow<0?'NET OUTBOUND':'BALANCED'
  return `<div class="metrics shipment-metrics"><div class="selected-day"><span>TOTAL THROUGHPUT</span><strong>${fmtShipment(activity.total_volume)}</strong><small>IMPORTS + EXPORTS</small></div><div><span>IMPORTS</span><strong>${fmtShipment(imports)}</strong><small>ESTIMATED INBOUND</small></div><div><span>EXPORTS</span><strong>${fmtShipment(exports)}</strong><small>ESTIMATED OUTBOUND</small></div><div><span>IMPORT / EXPORT SHARE</span><strong class="share-value">${share}</strong><small>SELECTED DAY</small></div><div><span>NET FLOW</span><strong>${fmtSignedShipment(netFlow)}</strong><small>IMPORTS − EXPORTS · ${netDirection}</small></div><div><span>TRAILING 7-DAY AVG</span><strong>${fmtShipment(activity.volume_7d,1)}</strong><small>TOTAL THROUGHPUT</small></div><div><span>56-DAY BASELINE</span><strong>${fmtShipment(activity.volume_expected,1)}</strong><small>TOTAL THROUGHPUT</small></div><div><span>VOLUME Z-SCORE</span><strong>${fmt(activity.volume_z_score,2)}</strong><small>UNCHANGED SCORE INPUT</small></div></div><div class="flow-note">Net flow is imports minus exports: positive means net inbound and negative means net outbound. Total throughput remains the volume input to the existing activity score.</div>`
}
const currentDates = () => layer==='verified' ? verifiedDates.available_dates : [...new Set([...(liveStatus?.available_dates||[]),...historicalRouteDates.available_dates,localISODate()])].sort()

function updateMarkerVisibility(force=false){
  const cameraPosition=viewer.camera.positionWC
  if(!force&&hasVisibilityCameraPosition&&Cartesian3.equalsEpsilon(cameraPosition,lastVisibilityCameraPosition,1e-10,.01))return
  Cartesian3.clone(cameraPosition,lastVisibilityCameraPosition);hasVisibilityCameraPosition=true
  const isVisible=(surfacePosition:Cartesian3,current:boolean)=>{
    Ellipsoid.WGS84.geodeticSurfaceNormal(surfacePosition,visibilitySurfaceNormal)
    Cartesian3.subtract(cameraPosition,surfacePosition,visibilityToCamera)
    Cartesian3.normalize(visibilityToCamera,visibilityToCamera)
    const facing=Cartesian3.dot(visibilitySurfaceNormal,visibilityToCamera)
    return facing>MARKER_HORIZON_HYSTERESIS?true:facing<-MARKER_HORIZON_HYSTERESIS?false:current
  }
  entities.forEach((entity,id)=>{const surfacePosition=portSurfacePositions.get(id);const allowed=portFilterVisibility.get(id)??false;const show=allowed&&!!surfacePosition&&isVisible(surfacePosition,entity.show);if(entity.show!==show)entity.show=show})
  vesselHeadEntries.forEach(({billboard,surfacePosition})=>{const show=isVisible(surfacePosition,billboard.show);if(billboard.show!==show)billboard.show=show})
}
viewer.scene.preRender.addEventListener(()=>updateMarkerVisibility())
viewer.camera.moveStart.addEventListener(()=>{cameraIsMoving=true})
viewer.camera.moveEnd.addEventListener(()=>{
  cameraIsMoving=false
  if(!pendingRouteResult)return
  const result=pendingRouteResult;pendingRouteResult=null;renderLiveRoutes(result)
})

function renderKey(){
  const filterButton=(value:PortFunctionFilter,label:string,shape?:string,title?:string)=>`<button type="button" data-port-function="${value}" class="${portFunctionFilter===value?'active':''}" aria-pressed="${portFunctionFilter===value}" title="${title||label}">${shape?`<i class="marker-shape ${shape}"></i>`:''}${label}</button>`
  const shapes=`<div class="key-row shape-key" role="group" aria-label="Port type filter"><b>PORT TYPE</b>${filterButton('all','ALL TYPES',undefined,'Show every port type')}${filterButton('cargo_or_mixed','CARGO / MIXED','shape-circle')}${filterButton('oil_terminal','OIL TERMINAL','shape-diamond')}${filterButton('tanker_dominant','TANKER-DOMINANT','shape-triangle')}${filterButton('lng_lpg_terminal','LNG / LPG','shape-square')}</div>`
  const activity=layer==='verified'?'<div class="key-row activity-key"><b>ACTIVITY</b><span><i class="top-increase"></i>TOP ABOVE NORMAL</span><span><i class="top-decrease"></i>TOP BELOW NORMAL</span><span><i class="quiet"></i>QUIET</span><span><i class="normal"></i>NORMAL</span><span><i class="elevated"></i>ELEVATED</span><span><i class="extreme"></i>EXTREME</span></div>':routeOverlaySource==='noaa'?'<div class="key-row activity-key"><b>HISTORICAL AIS</b><span><i class="historical-vessel"></i>OBSERVED POSITION</span><span><i class="historical-route"></i>NOAA / USCG ROUTE</span><span class="key-note">U.S. WATERS ONLY</span></div>':'<div class="key-row activity-key"><b>LIVE AIS</b><span><i class="live-vessel"></i>LIVE VESSEL</span><span><i class="live-route"></i>RECENT ROUTE</span><span><i class="live-marker"></i>PORT CALL</span><span class="key-note">PROVISIONAL</span></div>'
  const historicalOverlay=layer==='verified'&&routeOverlaySource==='noaa'?'<div class="key-row activity-key"><b>HISTORICAL AIS</b><span><i class="historical-route"></i>NOAA / USCG ROUTES</span><span class="key-note">U.S. WATERS ONLY · CONTEXT ONLY</span></div>':''
  document.querySelector('#map-key')!.innerHTML=shapes+activity+historicalOverlay
}

function renderPorts(){
  const minimum=Number((document.querySelector<HTMLInputElement>('#minimum')!).value||0)
  allPorts.forEach(port=>{
    const score=port.activity_score
    const activityVisible=layer==='live'||((score??50)>=minimum&&(filter==='all'||(filter==='above'&&(score??50)>=ABOVE_NORMAL_SCORE)||(filter==='below'&&(score??50)<BELOW_NORMAL_SCORE)))
    const visible=matchesPortFunction(port)&&activityVisible
    portFilterVisibility.set(port.port_id,visible)
    let entity=entities.get(port.port_id)
    const size=layer==='live'?Math.max(7,Math.min(12,7+Math.log2(1+(port.port_calls??0))*1.15)):Math.max(6,Math.min(11,6+Math.log10(Math.max(1,port.importance))*1.5))
    const image=markerImage(port.port_function,markerColor(port))
    if(!entity){const surfacePosition=Cartesian3.fromDegrees(port.longitude,port.latitude);portSurfacePositions.set(port.port_id,surfacePosition);entity=viewer.entities.add({id:port.port_id,name:port.name,position:Cartesian3.fromDegrees(port.longitude,port.latitude,PORT_MARKER_HEIGHT_METERS),properties:{port_id:port.port_id},billboard:{image,width:size,height:size,scaleByDistance:new NearFarScalar(MARKER_NEAR_DISTANCE_METERS,.95,MARKER_FAR_DISTANCE_METERS,.58),heightReference:HeightReference.NONE,disableDepthTestDistance:Number.POSITIVE_INFINITY}});entities.set(port.port_id,entity)}
    if(entity.billboard){entity.billboard.image=new ConstantProperty(image);entity.billboard.width=new ConstantProperty(size);entity.billboard.height=new ConstantProperty(size)}
  })
  updateMarkerVisibility(true)
}

function clearLiveRoutes(){
  routeLines.removeAll()
  vesselHeads.removeAll()
  vesselHeadEntries.length=0
  pendingRouteResult=null
  lastRouteFlushRendered=null
  routeOverlaySource=null
  renderKey()
}

function renderLiveRoutes(result:RouteResult){
  routeLines.removeAll()
  vesselHeads.removeAll()
  vesselHeadEntries.length=0
  const historical=result.source==='noaa_marinecadastre'||result.historical===true
  routeOverlaySource=historical?'noaa':'live'
  const lineColor=Color.fromCssColorString(historical?'#e5c36c':'#51bfca').withAlpha(historical ? .48 : .38)
  const headColor=Color.fromCssColorString('#ff7ac8').withAlpha(.94)
  result.routes.forEach(route=>{
    const points=route.points.filter(point=>Number.isFinite(point.latitude)&&Number.isFinite(point.longitude))
    if(!points.length)return
    if(points.length>1){
      routeLines.add({
        positions:points.map(point=>Cartesian3.fromDegrees(point.longitude,point.latitude,900)),
        width:1.15,
        material:Material.fromType('Color',{color:lineColor}),
      })
    }
    const latest=points[points.length-1]
    const surfacePosition=Cartesian3.fromDegrees(latest.longitude,latest.latitude)
    const billboard=vesselHeads.add({
      position:Cartesian3.fromDegrees(latest.longitude,latest.latitude,VESSEL_MARKER_HEIGHT_METERS),
      image:vesselMarkerImage(headColor),
      width:11,
      height:15,
      disableDepthTestDistance:Number.POSITIVE_INFINITY,
    })
    vesselHeadEntries.push({billboard,surfacePosition})
  })
  updateMarkerVisibility(true)
  renderKey()
}

async function loadLiveRoutes(){
  const liveToday=layer==='live'&&selectedDate===localISODate()
  const historical=historicalRouteDates.available_dates.includes(selectedDate)
  if(!liveToday&&!historical){
    clearLiveRoutes()
    return
  }
  routeRequestActive=true
  routeRequestController?.abort()
  routeRequestController=new AbortController()
  const requestDate=selectedDate
  try{
    const path=liveToday?'/api/live/routes':`/api/historical/routes?date=${selectedDate}`
    const result=await api<RouteResult>(path,{signal:routeRequestController.signal})
    if(requestDate!==selectedDate)return
    if(cameraIsMoving)pendingRouteResult=result
    else renderLiveRoutes(result)
    lastRouteFlushRendered=liveToday?(liveStatus?.last_route_flush_at||null):null
  }catch(error){
    if((error as Error).name!=='AbortError')console.warn('Unable to load local live routes',error)
  }finally{routeRequestActive=false}
}

async function loadSnapshot(date:string){
  requestController?.abort();requestController=new AbortController()
  try{const path=layer==='verified'?`/api/globe/snapshot?date=${date}`:`/api/live/snapshot?date=${date}`;const result=await api<{date:string;ports:Port[]}>(path,{signal:requestController.signal});allPorts=result.ports;viewer.entities.removeAll();entities.clear();portSurfacePositions.clear();portFilterVisibility.clear();renderRanking();renderPorts();void loadLiveRoutes()}catch(error){if((error as Error).name!=='AbortError')showMessage((error as Error).message,true)}
}

function renderRanking(){
  const directionToggle=document.querySelector('#anomaly-direction')!;directionToggle.classList.toggle('hidden',layer==='live')
  topIncreasePortIds.clear();topDecreasePortIds.clear()
  const eligiblePorts=allPorts.filter(matchesPortFunction)
  const increases=layer==='verified'?[...eligiblePorts].filter(port=>(port.activity_score??50)>=ABOVE_NORMAL_SCORE).sort((a,b)=>(b.activity_score??50)-(a.activity_score??50)).slice(0,8):[]
  const decreases=layer==='verified'?[...eligiblePorts].filter(port=>(port.activity_score??50)<BELOW_NORMAL_SCORE).sort((a,b)=>(a.activity_score??50)-(b.activity_score??50)).slice(0,8):[]
  increases.forEach(port=>topIncreasePortIds.add(port.port_id));decreases.forEach(port=>topDecreasePortIds.add(port.port_id))
  const ranked=layer==='live'?[...eligiblePorts].sort((a,b)=>(b.port_calls??0)-(a.port_calls??0)).slice(0,8):anomalyDirection==='increase'?increases:decreases
  document.querySelector('#ranking-title')!.textContent=layer==='live'?'MOST ACTIVE AIS PORTS':anomalyDirection==='increase'?'TOP ABOVE-NORMAL ACTIVITY':'TOP BELOW-NORMAL ACTIVITY';document.querySelector('#ranking-label')!.textContent=portFunctionFilter==='all'?(layer==='live'?'PORT CALLS':'7D SIGNAL'):`${portFunctionFilterLabel()} · ${layer==='live'?'CALLS':'7D'}`
  const emptyScope=portFunctionFilter==='all'?'':`${portFunctionFilterLabel().toLowerCase()} `
  document.querySelector('#anomalies')!.innerHTML=ranked.length?ranked.map((port,index)=>{const direction=directionFor(port.activity_score);const score=Math.round(port.activity_score??50);const symbol=anomalyDirection==='decrease'?'↓':directionSymbol(port.activity_score);return `<button data-port="${port.port_id}" title="${layer==='live'?'Preliminary AIS-derived port calls':`${direction==='above'?'Above-normal activity':direction==='below'?'Below-normal activity':'Activity within the normal range'} · trailing 7-day average versus this port’s 56-day baseline`}"><span class="rank">${String(index+1).padStart(2,'0')}</span><span><strong>${port.name}</strong><small>${port.country}</small></span>${layer==='live'?`<b>${fmt(port.port_calls)}</b>`:`<b class="score-${direction}" aria-label="Activity score ${score}, ${direction}-normal activity"><span aria-hidden="true">${symbol}</span>${score}</b>`}</button>`}).join(''):`<div class="skeleton">${layer==='live'?`No ${emptyScope}AIS port calls collected for this date`:`No ${emptyScope}${anomalyDirection==='increase'?'above-normal':'below-normal'} activity for this date`}</div>`
}

function selectIndex(index:number){
  const available=currentDates();if(!available.length)return
  index=Math.max(0,Math.min(available.length-1,index));selectedDate=available[index]
  const slider=document.querySelector<HTMLInputElement>('#slider')!;slider.max=String(Math.max(0,available.length-1));slider.value=String(index)
  const picker=document.querySelector<HTMLInputElement>('#date-picker')!;picker.value=selectedDate;picker.min=available[0];picker.max=available[available.length-1]
  document.querySelector('#selected-date')!.textContent=new Date(selectedDate+'T12:00:00').toLocaleDateString(undefined,{year:'numeric',month:'long',day:'numeric'}).toUpperCase();void loadSnapshot(selectedDate)
}

async function openPort(portId:string,fly=true){const port=allPorts.find(value=>value.port_id===portId);if(!port)return;if(fly)viewer.camera.flyTo({destination:Cartesian3.fromDegrees(port.longitude,port.latitude,1200000),duration:1.4});if(layer==='live')await openLivePort(port);else await openVerifiedPort(port)}

async function openLivePort(port:Port){
  const available=currentDates();const start=available[Math.max(0,available.indexOf(selectedDate)-89)]||selectedDate;const history=await api<any>(`/api/live/ports/${port.port_id}/history?start=${start}&end=${selectedDate}`);const score=port.activity_score
  document.querySelector('#detail-content')!.innerHTML=`<div class="eyebrow">PRELIMINARY LIVE AIS / ${selectedDate}</div><h2>${port.name}</h2><p class="country">${port.country}</p>${portFunctionBadge(port)}<div class="provisional-banner"><span>●</span> EARLY ACTIVITY SIGNAL · NOT SHIPMENT TONNAGE</div><div class="score"><strong class="score-${directionFor(score)}">${score==null?'—':Math.round(score)}</strong><span>CALLS-ONLY ACTIVITY SCORE<br/><b>${statusFor(score)}</b>${directionIndicator(score)}</span></div><div class="score-explainer"><i>i</i><span>${score==null?`A score will appear after seven prior collection days. ${port.baseline_days??0} prior days are currently available.`:'Green/up indicates above-normal activity; red/down indicates below-normal activity. This calls-only score is calculated independently from PortWatch shipment volume.'}</span></div><div class="metric-heading"><span>PRELIMINARY PORT CALLS</span><small>POINT GEOFENCE + DWELL</small></div><div class="metrics"><div class="selected-day"><span>SELECTED DAY</span><strong>${fmt(port.port_calls)}</strong></div><div><span>UNIQUE VESSELS</span><strong>${fmt(port.unique_vessels)}</strong></div><div><span>PRIOR DAILY AVG</span><strong>${fmt(port.calls_expected,2)}</strong></div><div><span>BASELINE DAYS</span><strong>${fmt(port.baseline_days)}</strong></div></div><div class="method-note">Detected after ≥20 minutes within 8 km at ≤3 knots. AIS gaps and nearby terminals can cause missed or misclassified calls. No cargo weight is inferred.</div><div class="chart-label">COLLECTED DAILY PORT CALLS</div>`
  openDetail();renderChart(history.observations.map((x:any)=>x.date),history.observations.map((x:any)=>x.port_calls),'#71d3dc',undefined)
}

async function openVerifiedPort(port:Port){
  const [details,history]=await Promise.all([api<any>(`/api/ports/${port.port_id}?date=${selectedDate}`),api<any>(`/api/ports/${port.port_id}/history?start=${verifiedDates.available_dates[Math.max(0,verifiedDates.available_dates.indexOf(selectedDate)-89)]}&end=${selectedDate}`)]);const a=details.activity||port;const score=a.activity_score??port.activity_score;const profiledPort={...port,...details}
  document.querySelector('#detail-content')!.innerHTML=`<div class="eyebrow">VERIFIED PORTWATCH / ${selectedDate}</div><h2>${port.name}</h2><p class="country">${port.country}</p>${portFunctionBadge(profiledPort)}<div class="verified-banner">PUBLISHED SHIPMENT-ESTIMATE LAYER</div><div class="score"><strong class="score-${directionFor(score)}">${Math.round(score??50)}</strong><span>ACTIVITY SCORE · 7-DAY SIGNAL<br/><b>${statusFor(score)}</b>${directionIndicator(score)}</span></div><div class="score-explainer"><i>i</i><span>${verifiedSignalExplanation(a)}</span></div><div class="metric-heading shipment-heading"><span>SHIPMENT VOLUME · SELECTED DAY BREAKDOWN</span><div id="shipment-unit-toggle" class="unit-toggle" role="group" aria-label="Shipment volume unit"><button data-unit="metric" aria-pressed="${shipmentUnit==='metric'}" class="${shipmentUnit==='metric'?'active':''}">METRIC TONNES</button><button data-unit="pounds" aria-pressed="${shipmentUnit==='pounds'}" class="${shipmentUnit==='pounds'?'active':''}">POUNDS</button></div></div>${shipmentVolumeTable(a)}<div class="metric-heading"><span>PORT CALLS</span><small>PORTWATCH AIS ESTIMATE</small></div><div class="metrics"><div class="selected-day"><span>SELECTED DAY</span><strong>${fmt(a.port_calls)}</strong></div><div><span>TRAILING 7-DAY AVG</span><strong>${fmt(a.calls_7d,2)}</strong></div><div><span>56-DAY BASELINE</span><strong>${fmt(a.calls_expected,2)}</strong></div><div><span>CALLS Z-SCORE</span><strong>${fmt(a.calls_z_score,2)}</strong></div></div><div class="change-row"><div><span>7-DAY CHANGE</span><strong>${fmt(a.change_7d_pct,1)}%</strong></div><div><span>30-DAY CHANGE</span><strong>${fmt(a.change_30d_pct,1)}%</strong></div></div><div class="chart-label">90-DAY VERIFIED ACTIVITY SIGNAL</div>`
  document.querySelector('#shipment-unit-toggle')?.addEventListener('click',event=>{const button=(event.target as HTMLElement).closest<HTMLButtonElement>('button[data-unit]');if(!button||button.dataset.unit===shipmentUnit)return;shipmentUnit=button.dataset.unit as ShipmentUnit;localStorage.setItem('maritime-shipment-unit',shipmentUnit);void openPort(port.port_id,false)});openDetail();renderChart(history.observations.map((x:any)=>x.date),history.observations.map((x:any)=>x.activity_score),'#e8c56a',100)
}

function openDetail(){const panel=document.querySelector<HTMLElement>('#detail')!;panel.classList.add('open');panel.setAttribute('aria-hidden','false')}
function renderChart(labels:string[],data:number[],color:string,max:number|undefined){historyChart?.destroy();const canvas=document.querySelector<HTMLCanvasElement>('#history-chart')!;historyChart=new Chart(canvas,{type:'line',data:{labels,datasets:[{data,borderColor:color,backgroundColor:`${color}1f`,fill:true,tension:.3,pointRadius:0,borderWidth:1.5}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{display:false},y:{min:0,max,ticks:{color:'#79909e'},grid:{color:'rgba(120,150,165,.12)'}}}}})}
function showMessage(text:string,error=false){const element=document.querySelector('#refresh-message')!;element.textContent=text;element.classList.toggle('error',error)}

const noaaDialog=document.querySelector<HTMLDialogElement>('#noaa-dialog')!
let noaaPollTimer:number|undefined
let handledNoaaJobId:string|null=null
let lastNoaaCompletedDates=-1

function renderNoaaImportStatus(status:NoaaImportStatus){
  const running=status.state==='running'
  const complete=status.state==='complete'
  const progress=status.total_dates?Math.round(status.completed_dates/status.total_dates*100):0
  const downloaded=(status.source_bytes_downloaded/1024**3).toFixed(1)
  const compact=(status.parquet_bytes_written/1024**3).toFixed(2)
  const remaining=fmtDuration(status.estimated_seconds_remaining)
  const eta=status.estimated_completion_at?`ETA ${fmtDate(status.estimated_completion_at).toUpperCase()} · ${remaining}`:remaining
  const detail=running?`${status.stage} · ${status.completed_dates}/${status.total_dates} dates · ${progress}% · ${eta.toLowerCase()} · ${downloaded} GB transferred · ${compact} GB Parquet`:status.error?`${status.stage}: ${status.error}`:status.stage
  document.querySelector('#noaa-progress')!.textContent=detail
  const indicator=document.querySelector('#noaa-indicator')!;indicator.textContent=complete?'READY':running?'BUILDING':status.completed_dates?'PARTIAL':'HISTORY';indicator.className=`source-badge ${complete?'live':running?'warning':status.completed_dates?'verified':'offline'}`
  document.querySelector('#noaa-through')!.textContent=`${fmt(status.completed_dates)} / ${fmt(status.total_dates||313)} DATES`
  document.querySelector('#noaa-summary')!.textContent=complete?'READY IN PRELIMINARY LIVE AIS':running?`${progress}% · ${eta}`:status.error?'PAUSED · OPEN HISTORY FOR DETAILS':status.completed_dates?`${progress}% LOCAL · OPEN HISTORY TO RESUME`:'PRESS HISTORY TO BUILD'
  document.querySelector<HTMLProgressElement>('#noaa-meter')!.value=progress
  document.querySelector<HTMLButtonElement>('#start-noaa')!.disabled=running||complete
  document.querySelector<HTMLButtonElement>('#cancel-noaa')!.disabled=!running
  document.querySelector<HTMLButtonElement>('#start-noaa')!.textContent=complete?'WEEKLY HISTORY COMPLETE':status.completed_dates?'RESUME WEEKLY HISTORY':'BUILD WEEKLY HISTORY'
}

async function pollNoaaImportStatus(){
  try{
    const status=await api<NoaaImportStatus>('/api/historical/import/status')
    renderNoaaImportStatus(status)
    if(status.completed_dates!==lastNoaaCompletedDates){
      lastNoaaCompletedDates=status.completed_dates
      const updated=await api<HistoricalRouteDates>('/api/historical/routes/dates')
      historicalRouteDates=updated
      if(layer==='live'){
        const dates=currentDates();const slider=document.querySelector<HTMLInputElement>('#slider')!;slider.max=String(Math.max(0,dates.length-1))
        const selectedIndex=dates.indexOf(selectedDate);if(selectedIndex>=0)slider.value=String(selectedIndex)
        const picker=document.querySelector<HTMLInputElement>('#date-picker')!;if(dates.length){picker.min=dates[0];picker.max=dates[dates.length-1]}
      }
      renderKey()
    }
    if(status.state==='running')return
    if(noaaPollTimer){clearInterval(noaaPollTimer);noaaPollTimer=undefined}
    if(status.state==='complete'&&status.job_id&&status.job_id!==handledNoaaJobId){
      handledNoaaJobId=status.job_id
      await initialize(false)
      const imported=status.current_date
      if(imported&&historicalRouteDates.available_dates.includes(imported)){
        if(layer==='verified'&&verifiedDates.available_dates.includes(imported))selectIndex(verifiedDates.available_dates.indexOf(imported))
        else{await setLayer('live');const index=currentDates().indexOf(imported);if(index>=0)selectIndex(index)}
      }
    }
  }catch(error){document.querySelector('#noaa-progress')!.textContent=(error as Error).message}
}

function startNoaaPolling(){
  if(noaaPollTimer)clearInterval(noaaPollTimer)
  noaaPollTimer=window.setInterval(()=>void pollNoaaImportStatus(),1500)
  void pollNoaaImportStatus()
}

async function startNoaaImport(){
  try{
    const status=await api<NoaaImportStatus>('/api/historical/import',{method:'POST'})
    renderNoaaImportStatus(status);startNoaaPolling()
  }catch(error){document.querySelector('#noaa-progress')!.textContent=(error as Error).message}
}

async function refreshPortWatch(){const button=document.querySelector<HTMLButtonElement>('#refresh')!;button.disabled=true;try{let job=await api<any>('/api/refresh',{method:'POST'});showMessage(job.stage);while(job.state==='running'){await new Promise(resolve=>setTimeout(resolve,1200));job=await api<any>('/api/refresh/status');showMessage(job.stage)}if(job.state==='failed')throw new Error(`${job.error}. Cached PortWatch data remains available through ${job.newest_local_date||'the last successful refresh'}.`);showMessage(`PortWatch checked · ${job.rows_added.toLocaleString()} added · ${job.rows_updated.toLocaleString()} revised`);await initialize()}catch(error){showMessage((error as Error).message,true)}finally{button.disabled=false}}
async function toggleLive(){const running=['starting','running','reconnecting'].includes(liveStatus?.state||'');try{liveStatus=await api<LiveStatus>(running?'/api/live/stop':'/api/live/start',{method:'POST'});renderLiveStatus();if(liveStatus.state==='configuration_required')showMessage('Live AIS needs a free API key. Open Sources for the one-time setup.',true);else{showMessage(liveStatus.stage);if(layer==='live')selectIndex(currentDates().length-1)}}catch(error){showMessage((error as Error).message,true)}}

function renderLiveStatus(){
  if(!liveStatus)return
  const active=['starting','running','reconnecting'].includes(liveStatus.state);const indicator=document.querySelector('#live-indicator')!;indicator.textContent=liveStatus.state==='running'?'LIVE':liveStatus.state==='reconnecting'?'RETRY':liveStatus.configured?'OFF':'SETUP';indicator.className=`source-badge ${liveStatus.state==='running'?'live':liveStatus.state==='reconnecting'?'warning':'offline'}`
  document.querySelector('#live-through')!.textContent=liveStatus.latest_available_date?fmtDate(liveStatus.latest_available_date).toUpperCase():liveStatus.active_route_vessels?`${fmt(liveStatus.active_route_vessels)} VESSELS`:'NO CALLS YET';document.querySelector('#last-ais')!.textContent=liveStatus.last_message_at?`LAST MESSAGE ${fmtDate(liveStatus.last_message_at).toUpperCase()}`:(liveStatus.configured?'READY TO START':'API KEY REQUIRED')
  const button=document.querySelector<HTMLButtonElement>('#live-toggle')!;button.innerHTML=active?'<span>■</span> STOP LIVE AIS':'<span>●</span> START LIVE AIS';button.classList.toggle('active',active)
}

async function pollLiveStatus(){try{const previous=liveStatus?.latest_available_date;liveStatus=await api<LiveStatus>('/api/live/status');renderLiveStatus();if(layer==='live'&&previous!==liveStatus.latest_available_date)selectIndex(currentDates().length-1);else if(layer==='live'&&selectedDate===localISODate()&&liveStatus.last_route_flush_at!==lastRouteFlushRendered)void loadLiveRoutes()}catch{/* Verified offline data remains usable. */}}
async function setLayer(next:Layer){layer=next;filter='all';document.querySelectorAll<HTMLButtonElement>('.layer-switch button').forEach(button=>button.classList.toggle('active',button.dataset.layer===layer));document.querySelector('#activity-filters')!.classList.toggle('disabled',layer==='live');document.querySelector('#anomaly-direction')!.classList.toggle('hidden',layer==='live');document.querySelector('#selected-layer-label')!.textContent=layer==='live'?'PRELIMINARY LIVE AIS ACTIVITY':'VERIFIED PORTWATCH ACTIVITY';document.querySelector('#detail')!.classList.remove('open');if(layer!=='live')clearLiveRoutes();renderKey();selectIndex(currentDates().length-1)}

async function initialize(resetSelection=true){
  try{const [datesResult,statusResult,sourceStatus,historicalDatesResult,noaaStatus]=await Promise.all([api<Dates>('/api/dates'),api<LiveStatus>('/api/live/status'),api<any>('/api/data/status'),api<HistoricalRouteDates>('/api/historical/routes/dates'),api<NoaaImportStatus>('/api/historical/import/status')]);verifiedDates=datesResult;liveStatus=statusResult;historicalRouteDates=historicalDatesResult;lastNoaaCompletedDates=noaaStatus.completed_dates;document.querySelector('#data-through')!.textContent=verifiedDates.latest_available_date?fmtDate(verifiedDates.latest_available_date).toUpperCase():'NOT BOOTSTRAPPED';const last=sourceStatus.sources?.find((source:any)=>source.source==='portwatch')?.last_checked;const routeNote=historicalRouteDates.date_count?` · ${historicalRouteDates.date_count} NOAA route day${historicalRouteDates.date_count===1?'':'s'} local`:'';showMessage(last?`PortWatch last checked ${fmtDate(last)} · globe queries are local${routeNote}`:`Globe exploration reads only local data${routeNote}`);renderLiveStatus();renderNoaaImportStatus(noaaStatus);renderKey();if(noaaStatus.state==='running')startNoaaPolling();if(resetSelection)selectIndex(currentDates().length-1)}catch{showMessage('No local data yet. Run bootstrap or refresh PortWatch.',true)}
}

let debounce:number|undefined
document.querySelector('#slider')!.addEventListener('input',event=>{clearTimeout(debounce);const index=Number((event.target as HTMLInputElement).value);const value=currentDates()[index];if(value){selectedDate=value;document.querySelector<HTMLInputElement>('#date-picker')!.value=value;document.querySelector('#selected-date')!.textContent=new Date(value+'T12:00:00').toLocaleDateString(undefined,{year:'numeric',month:'long',day:'numeric'}).toUpperCase();debounce=window.setTimeout(()=>void loadSnapshot(value),160)}})
document.querySelector('#date-picker')!.addEventListener('change',event=>{const index=currentDates().indexOf((event.target as HTMLInputElement).value);if(index>=0)selectIndex(index)})
document.querySelector('#anomaly-direction')!.addEventListener('click',event=>{const button=(event.target as HTMLElement).closest<HTMLButtonElement>('button[data-direction]');if(!button||layer!=='verified'||button.dataset.direction===anomalyDirection)return;anomalyDirection=button.dataset.direction as AnomalyDirection;document.querySelectorAll<HTMLButtonElement>('#anomaly-direction button').forEach(item=>{const active=item.dataset.direction===anomalyDirection;item.classList.toggle('active',active);item.setAttribute('aria-pressed',String(active))});renderRanking();renderPorts()})
document.querySelector('#map-key')!.addEventListener('click',event=>{const button=(event.target as HTMLElement).closest<HTMLButtonElement>('button[data-port-function]');if(!button||button.dataset.portFunction===portFunctionFilter)return;portFunctionFilter=button.dataset.portFunction as PortFunctionFilter;document.querySelector('#detail')!.classList.remove('open');renderKey();renderRanking();renderPorts()})
document.querySelector('#previous')!.addEventListener('click',()=>selectIndex(Number(document.querySelector<HTMLInputElement>('#slider')!.value)-1));document.querySelector('#next')!.addEventListener('click',()=>selectIndex(Number(document.querySelector<HTMLInputElement>('#slider')!.value)+1));document.querySelector('#refresh')!.addEventListener('click',()=>void refreshPortWatch());document.querySelector('#live-toggle')!.addEventListener('click',()=>void toggleLive());document.querySelector('#minimum')!.addEventListener('input',renderPorts)
document.querySelector('#activity-filters')!.addEventListener('click',event=>{const button=(event.target as HTMLElement).closest<HTMLButtonElement>('button[data-filter]');if(!button||layer==='live')return;filter=button.dataset.filter!;document.querySelectorAll('#activity-filters button').forEach(item=>item.classList.remove('active'));button.classList.add('active');renderPorts()});document.querySelector('.layer-switch')!.addEventListener('click',event=>{const button=(event.target as HTMLElement).closest<HTMLButtonElement>('button[data-layer]');if(button&&button.dataset.layer!==layer)void setLayer(button.dataset.layer as Layer)});document.querySelector('#anomalies')!.addEventListener('click',event=>{const button=(event.target as HTMLElement).closest<HTMLButtonElement>('[data-port]');if(button)void openPort(button.dataset.port!)});viewer.selectedEntityChanged.addEventListener(entity=>{const id=entity?.id;if(id&&entities.has(id))void openPort(id)});document.querySelector('#close-detail')!.addEventListener('click',()=>document.querySelector('#detail')!.classList.remove('open'))
document.querySelector('#noaa-history')!.addEventListener('click',()=>{void pollNoaaImportStatus();noaaDialog.showModal()})
document.querySelector('#close-noaa')!.addEventListener('click',()=>noaaDialog.close())
document.querySelector('#start-noaa')!.addEventListener('click',()=>void startNoaaImport())
document.querySelector('#cancel-noaa')!.addEventListener('click',async()=>{try{const status=await api<NoaaImportStatus>('/api/historical/import/cancel',{method:'POST'});renderNoaaImportStatus(status)}catch(error){document.querySelector('#noaa-progress')!.textContent=(error as Error).message}})
const dialog=document.querySelector<HTMLDialogElement>('#about-dialog')!;document.querySelector('#about')!.addEventListener('click',()=>dialog.showModal());document.querySelector('#close-about')!.addEventListener('click',()=>dialog.close())
setInterval(()=>document.querySelector('#local-time')!.textContent=new Date().toLocaleString(undefined,{hour:'2-digit',minute:'2-digit',second:'2-digit',timeZoneName:'short'}),1000);setInterval(()=>void pollLiveStatus(),5000);void initialize()
