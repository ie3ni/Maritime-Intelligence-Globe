import { defineConfig } from 'vite'
import { viteStaticCopy } from 'vite-plugin-static-copy'
import { existsSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

const preparedImagery = fileURLToPath(new URL('../data/cache/natural-earth/tiles/NaturalEarthII', import.meta.url))
const bundledImagery = 'node_modules/cesium/Build/Cesium/Assets/Textures/NaturalEarthII'
const imagerySource = existsSync(`${preparedImagery}/tilemapresource.xml`) ? preparedImagery : bundledImagery
const projectLicense = fileURLToPath(new URL('../LICENSE', import.meta.url))
const thirdPartyNotices = fileURLToPath(new URL('../THIRD_PARTY_NOTICES.md', import.meta.url))
const cesiumLicense = fileURLToPath(new URL('./node_modules/cesium/LICENSE.md', import.meta.url))
const cesiumThirdParty = fileURLToPath(new URL('./node_modules/cesium/ThirdParty.json', import.meta.url))
const cesiumThirdPartyExtra = fileURLToPath(new URL('./node_modules/cesium/ThirdParty.extra.json', import.meta.url))
const chartJsLicense = fileURLToPath(new URL('./node_modules/chart.js/LICENSE.md', import.meta.url))

export default defineConfig({
  define: { CESIUM_BASE_URL: JSON.stringify('/cesium') },
  plugins: [viteStaticCopy({ targets: [
    { src:'node_modules/cesium/Build/Cesium/Workers', dest:'cesium' },
    { src:'node_modules/cesium/Build/Cesium/Assets', dest:'cesium' },
    { src:'node_modules/cesium/Build/Cesium/ThirdParty', dest:'cesium' },
    { src:'node_modules/cesium/Build/Cesium/Widgets', dest:'cesium' },
    { src:imagerySource, dest:'imagery' },
    { src:projectLicense, dest:'licenses/project' },
    { src:thirdPartyNotices, dest:'licenses' },
    { src:cesiumLicense, dest:'licenses/cesium' },
    { src:cesiumThirdParty, dest:'licenses/cesium' },
    { src:cesiumThirdPartyExtra, dest:'licenses/cesium' },
    { src:chartJsLicense, dest:'licenses/chart.js' }
  ]})],
  server: { proxy: { '/api':'http://127.0.0.1:8000' } },
  build: { target:'es2022' }
})
