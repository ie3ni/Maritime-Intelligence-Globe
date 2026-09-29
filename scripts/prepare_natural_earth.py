#!/usr/bin/env python3
"""Prepare a local high-resolution Natural Earth II TMS tile pyramid."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import urllib.request
import zipfile
from pathlib import Path


SOURCE_URL = (
    "https://naturalearth.s3.amazonaws.com/10m_raster/"
    "NE2_LR_LC_SR_W_DR.zip"
)
SOURCE_SHA256 = "73eb7a00b182c702e69892332cbd380edc7aa00ccaf68ae0111deefb87303a0e"
SOURCE_NAME = "NE2_LR_LC_SR_W_DR.tif"
SOURCE_SIZE = (16_200, 8_100)
TILE_SIZE = 256
DEFAULT_MAX_LEVEL = 5
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "natural-earth"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(url: str, destination: Path) -> None:
    temporary = destination.with_suffix(destination.suffix + ".part")
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "maritime-intelligence-globe/0.1"},
    )
    print(f"Downloading Natural Earth II to {destination} …")
    with urllib.request.urlopen(request, timeout=60) as response, temporary.open("wb") as output:
        total = int(response.headers.get("Content-Length", "0"))
        received = 0
        while chunk := response.read(1024 * 1024):
            output.write(chunk)
            received += len(chunk)
            if total:
                print(f"\r  {received / total:6.1%}", end="", flush=True)
    if total:
        print()
    os.replace(temporary, destination)


def ensure_source(cache_dir: Path) -> Path:
    archive = cache_dir / "NE2_LR_LC_SR_W_DR.zip"
    source = cache_dir / SOURCE_NAME
    cache_dir.mkdir(parents=True, exist_ok=True)

    if not archive.exists():
        download(SOURCE_URL, archive)
    if sha256(archive) != SOURCE_SHA256:
        raise RuntimeError(
            f"Natural Earth archive checksum mismatch: {archive}. "
            "Delete it and run this command again."
        )

    if not source.exists():
        print(f"Extracting {SOURCE_NAME} …")
        temporary = source.with_suffix(".tif.part")
        with zipfile.ZipFile(archive) as package:
            with package.open(SOURCE_NAME) as compressed, temporary.open("wb") as output:
                shutil.copyfileobj(compressed, output, length=1024 * 1024)
        os.replace(temporary, source)
    return source


def tilemap_xml(max_level: int) -> str:
    tile_sets = "\n".join(
        f'        <TileSet href="{level}" units-per-pixel="{0.703125 / (2**level):.14f}" order="{level}"/>'
        for level in range(max_level + 1)
    )
    return f"""<?xml version="1.0" encoding="utf-8"?>
<TileMap version="1.0.0" tilemapservice="http://tms.osgeo.org/1.0.0">
  <Title>Natural Earth II 1:10m local tile pyramid</Title>
  <Abstract>Public-domain Natural Earth II imagery prepared locally.</Abstract>
  <SRS>EPSG:4326</SRS>
  <BoundingBox miny="-90" minx="-180" maxy="90" maxx="180"/>
  <Origin y="-90" x="-180"/>
  <TileFormat width="256" height="256" mime-type="image/jpeg" extension="jpg"/>
  <TileSets profile="geodetic">
{tile_sets}
  </TileSets>
</TileMap>
"""


def expected_metadata(max_level: int) -> dict[str, object]:
    return {
        "source": SOURCE_URL,
        "source_sha256": SOURCE_SHA256,
        "source_dimensions": list(SOURCE_SIZE),
        "tile_size": TILE_SIZE,
        "maximum_level": max_level,
        "scheme": "TMS EPSG:4326",
    }


def tiles_are_current(cache_dir: Path, max_level: int) -> bool:
    marker = cache_dir / "tiles" / "NaturalEarthII" / "source.json"
    try:
        return json.loads(marker.read_text(encoding="utf-8")) == expected_metadata(
            max_level
        )
    except (OSError, json.JSONDecodeError):
        return False


def prepare_tiles(source: Path, cache_dir: Path, max_level: int, force: bool) -> Path:
    try:
        from PIL import Image
    except ImportError as exc:
        raise RuntimeError(
            "Pillow is required. Install the imagery extra with "
            "`pip install -e '.[imagery]'`."
        ) from exc

    output = cache_dir / "tiles" / "NaturalEarthII"
    marker = output / "source.json"
    expected = expected_metadata(max_level)
    if not force and marker.exists():
        try:
            if json.loads(marker.read_text(encoding="utf-8")) == expected:
                print(f"High-resolution tiles are already ready at {output}")
                return output
        except (OSError, json.JSONDecodeError):
            pass

    staging = output.with_name(".NaturalEarthII.tmp")
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)

    Image.MAX_IMAGE_PIXELS = None
    with Image.open(source) as original:
        if original.size != SOURCE_SIZE:
            raise RuntimeError(
                f"Unexpected Natural Earth dimensions {original.size}; expected {SOURCE_SIZE}."
            )
        image = original.convert("RGB")
        for level in range(max_level + 1):
            columns = 2 ** (level + 1)
            rows = 2**level
            level_size = (columns * TILE_SIZE, rows * TILE_SIZE)
            print(
                f"Generating level {level}/{max_level} "
                f"({columns * rows:,} tiles, {level_size[0]}×{level_size[1]}) …"
            )
            level_image = image.resize(level_size, Image.Resampling.LANCZOS)
            for column in range(columns):
                column_dir = staging / str(level) / str(column)
                column_dir.mkdir(parents=True)
                for north_row in range(rows):
                    left = column * TILE_SIZE
                    top = north_row * TILE_SIZE
                    tile = level_image.crop(
                        (left, top, left + TILE_SIZE, top + TILE_SIZE)
                    )
                    tms_row = rows - north_row - 1
                    tile.save(
                        column_dir / f"{tms_row}.jpg",
                        "JPEG",
                        quality=84,
                        subsampling=2,
                    )
            level_image.close()
        image.close()

    (staging / "tilemapresource.xml").write_text(
        tilemap_xml(max_level), encoding="utf-8"
    )
    (staging / "source.json").write_text(
        json.dumps(expected, indent=2) + "\n", encoding="utf-8"
    )
    if output.exists():
        shutil.rmtree(output)
    os.replace(staging, output)
    print(f"Prepared {sum(2 * 4**level for level in range(max_level + 1)):,} tiles at {output}")
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download public-domain Natural Earth II and prepare local Cesium tiles."
    )
    parser.add_argument(
        "--cache-dir", type=Path, default=DEFAULT_CACHE_DIR, help=argparse.SUPPRESS
    )
    parser.add_argument(
        "--max-level", type=int, default=DEFAULT_MAX_LEVEL, choices=range(3, 6)
    )
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--keep-source",
        action="store_true",
        help="Keep the extracted 376 MB TIFF after the tiles are prepared.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        output = args.cache_dir / "tiles" / "NaturalEarthII"
        extracted_source = args.cache_dir / SOURCE_NAME
        if not args.force and tiles_are_current(args.cache_dir, args.max_level):
            if extracted_source.exists() and not args.keep_source:
                extracted_source.unlink()
                print("Removed the temporary extracted TIFF; the verified archive is retained.")
            print(f"High-resolution tiles are already ready at {output}")
            return 0
        source = ensure_source(args.cache_dir)
        prepare_tiles(source, args.cache_dir, args.max_level, args.force)
        if not args.keep_source:
            source.unlink(missing_ok=True)
            print("Removed the temporary extracted TIFF; the verified archive is retained.")
    except (OSError, RuntimeError, urllib.error.URLError, zipfile.BadZipFile) as exc:
        print(f"Imagery preparation failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
