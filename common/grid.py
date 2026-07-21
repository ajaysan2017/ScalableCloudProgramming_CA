"""
Shared region-bucketing logic used identically by the batch layer,
the speed layer, and the live producer, so that "region" means the
exact same thing everywhere in the pipeline.

We bucket every event into a 10-degree latitude/longitude grid cell.
This avoids having to parse USGS's free-text `place` field (e.g.
"24km SSW of Whittier, CA"), which is inconsistent and not reliably
machine-parseable.
"""

import math

GRID_SIZE_DEGREES = 10


def grid_key(lat: float, lon: float, size: int = GRID_SIZE_DEGREES) -> str:
    """
    Map a (lat, lon) pair to a coarse grid-cell region id, e.g. "30_-120".

    The cell id is the lower-left corner of the size x size degree box
    the point falls into.
    """
    lat_bucket = int(math.floor(lat / size) * size)
    lon_bucket = int(math.floor(lon / size) * size)
    return f"{lat_bucket}_{lon_bucket}"


if __name__ == "__main__":
    # quick sanity check
    examples = [
        (38.83, -122.80),   # Cobb, CA
        (44.66, -110.48),   # Mammoth, WY
        (-18.36, -175.75),  # Tonga
    ]
    for lat, lon in examples:
        print(f"({lat}, {lon}) -> region {grid_key(lat, lon)}")
