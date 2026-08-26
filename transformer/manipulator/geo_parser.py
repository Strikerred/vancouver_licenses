"""
Turns the nested geo objects into two plain float columns, latitude and longitude.

The raw data carries the same point twice, in two shapes:

    geo_point_2d  {'lon': -123.121054, 'lat': 49.263014}
    geom          {'type': 'Feature',
                   'geometry': {'type': 'Point',
                                'coordinates': [-123.121054, 49.263014]},
                   'properties': {}}

I checked whether they ever disagree and on the rows where both are present they
match exactly, so geo_point_2d is the primary and geom is the fallback.

THE THING TO GET RIGHT: GeoJSON coordinates are [longitude, latitude], in that
order. Not [lat, lon]. Reading them the wrong way round puts Vancouver in the
Indian Ocean, and nothing errors, you just get a map with no pins on it. This is
the same family of bug as reading a UTC price against a local time volume: the
types are fine, the pipeline is green, the numbers are wrong. So the index is
spelled out below and the bounds check exists to catch it if someone edits this
later.

Coordinate strings are handled too. The API gives objects, but a CSV export of
the same dataset gives 'lat, lon' as text, and if we ever point the reader at one
of those I would rather this already worked than find out in production.
"""

from __future__ import annotations

import logging
import re

import pandas as pd

from transformer.configs import business_licences_configs as cfg

log = logging.getLogger(__name__)

_LON_INDEX, _LAT_INDEX = 0, 1

_COORD_STRING = re.compile(
    r"^\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*$"
)


def _from_geo_point(value: object) -> tuple[float | None, float | None]:
    if isinstance(value, dict) and "lat" in value and "lon" in value:
        try:
            return float(value["lat"]), float(value["lon"])
        except (TypeError, ValueError):
            return None, None
    return None, None


def _from_geom(value: object) -> tuple[float | None, float | None]:
    if not isinstance(value, dict):
        return None, None
    geometry = value.get("geometry") if "geometry" in value else value
    if not isinstance(geometry, dict):
        return None, None
    coords = geometry.get("coordinates")
    if not isinstance(coords, (list, tuple)) or len(coords) < 2:
        return None, None
    try:
        return float(coords[_LAT_INDEX]), float(coords[_LON_INDEX])
    except (TypeError, ValueError):
        return None, None


def _from_string(value: object) -> tuple[float | None, float | None]:
    """
    'lat, lon' as text, which is what the CSV export of this dataset gives.

    Note the order flips versus GeoJSON. Opendatasoft writes geo points as
    'lat, lon' in CSV and as [lon, lat] in GeoJSON, which is exactly the kind of
    inconsistency that makes this worth a function rather than an inline lambda.
    """
    if not isinstance(value, str):
        return None, None
    match = _COORD_STRING.match(value)
    if not match:
        return None, None
    return float(match.group(1)), float(match.group(2))


def parse_coordinates(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Add latitude and longitude float columns, drop the nested source columns.

    Returns the frame plus counts of what happened, so the caller can log how
    many rows have no location and how many had a value that is not on Earth.
    """
    out = frame.copy()
    latitudes: list[float | None] = []
    longitudes: list[float | None] = []

    point_col = "geo_point_2d" if "geo_point_2d" in out.columns else None
    geom_col = "geom" if "geom" in out.columns else None

    for _, row in out.iterrows():
        lat = lon = None
        if point_col is not None:
            lat, lon = _from_geo_point(row[point_col])
            if lat is None:
                lat, lon = _from_string(row[point_col])
        if lat is None and geom_col is not None:
            lat, lon = _from_geom(row[geom_col])
            if lat is None:
                lat, lon = _from_string(row[geom_col])
        latitudes.append(lat)
        longitudes.append(lon)

    out["latitude"] = pd.array(latitudes, dtype="float64")
    out["longitude"] = pd.array(longitudes, dtype="float64")

    has_point = out["latitude"].notna() & out["longitude"].notna()

    lat_lo, lat_hi = cfg.LATITUDE_BOUNDS
    lon_lo, lon_hi = cfg.LONGITUDE_BOUNDS
    off_earth = has_point & ~(out["latitude"].between(lat_lo, lat_hi)
                              & out["longitude"].between(lon_lo, lon_hi))

    bc = cfg.BC_BOUNDING_BOX
    outside_bc = has_point & ~off_earth & ~(
        out["latitude"].between(*bc["lat"]) & out["longitude"].between(*bc["lon"])
    )

    out["has_coordinates"] = has_point.astype("boolean")
    out = out.drop(columns=[c for c in cfg.GEO_SOURCE_COLUMNS if c in out.columns])

    counts = {
        "with_coordinates": int(has_point.sum()),
        "without_coordinates": int((~has_point).sum()),
        "off_earth": int(off_earth.sum()),
        "outside_bc": int(outside_bc.sum()),
    }

    log.info("coordinates: %d parsed, %d without a point",
             counts["with_coordinates"], counts["without_coordinates"])
    if counts["off_earth"]:
        log.warning("%d row(s) have coordinates outside valid lat/lon bounds",
                    counts["off_earth"])
    if counts["outside_bc"]:
        log.info("%d row(s) plot outside BC, expected for out of town addresses",
                 counts["outside_bc"])

    return out, counts
