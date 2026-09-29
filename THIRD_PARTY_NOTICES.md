# Third-Party Notices

Maritime Intelligence Globe incorporates third-party software. The project's
MIT License applies only to the original source code in this repository; the
components below remain under their own licenses.

## CesiumJS

- Project: <https://github.com/CesiumGS/cesium>
- License: Apache License 2.0
- Copyright: 2011–2026 CesiumJS Contributors
- Version: the exact version is recorded in `frontend/pnpm-lock.yaml`

CesiumJS includes additional third-party components and assets. A compiled
frontend contains the complete notices supplied by the installed CesiumJS
package at:

- `licenses/cesium/LICENSE.md`
- `licenses/cesium/ThirdParty.json`
- `licenses/cesium/ThirdParty.extra.json`

## Chart.js

- Project: <https://github.com/chartjs/Chart.js>
- License: MIT License
- Copyright: 2014–2024 Chart.js Contributors
- Version: the exact version is recorded in `frontend/pnpm-lock.yaml`

A compiled frontend contains the complete Chart.js license at
`licenses/chart.js/LICENSE.md`.

## NGA World Port Index data

- Source: National Geospatial-Intelligence Agency, Maritime Safety Office
- Service: <https://fgmod.nga.mil/nauticalpubs-feature/rest/services/WPI/World_Port_Index_Viewer/FeatureServer>

The application can generate a local World Port Index metadata cache during an
explicit bootstrap or refresh. NGA data is not licensed under this project's
MIT License, is not committed to the source repository, and must retain source
attribution and its retrieval date. The project does not use NGA seals or imply
NGA endorsement.

## NOAA / U.S. Coast Guard historical AIS data

- Source: NOAA Office for Coastal Management / MarineCadastre.gov
- Origin: U.S. Coast Guard Nationwide Automatic Identification System
- Catalog: <https://marinecadastre.gov/ais/>
- ERDDAP access: <https://data.pmel.noaa.gov/pmel/erddap/>
- Rights: U.S. Government material; NOAA's ERDDAP copies state a Creative
  Commons Zero 1.0 Universal public-domain dedication (CC0-1.0)

The optional importer stores requested U.S.-waters subsets only in ignored
local data directories. Historical AIS files are not licensed under the
project's MIT License and are not distributed with this repository. NOAA
describes the dataset's intended use as coastal and ocean planning. Route
replays are not suitable for navigation, enforcement, or regulatory decisions;
coverage and reception gaps are expected. Attribution does not imply NOAA or
U.S. Coast Guard endorsement.

## Distribution

The Vite production build copies this notice, the project's MIT License, and
the complete license files listed above into `frontend/dist/licenses`. Keep
that directory intact when publishing a compiled release, Docker image, or
hosted bundle. Development-only tools are not part of the compiled frontend;
their versions and licenses remain available through the package lockfile and
their installed package metadata.
