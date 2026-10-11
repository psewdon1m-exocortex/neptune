#!/usr/bin/env python3
"""Validate checked-in Windows icon dimensions without third-party modules."""

from pathlib import Path
import struct


ROOT = Path(__file__).resolve().parents[1]
ASSETS = ROOT / "packaging" / "windows" / "Assets"


def png_dimensions(path: Path) -> tuple[int, int]:
    data = path.read_bytes()
    if data[:8] != b"\x89PNG\r\n\x1a\n" or data[12:16] != b"IHDR":
        raise AssertionError(f"{path.relative_to(ROOT)} is not a PNG")
    width, height = struct.unpack(">II", data[16:24])
    if data[24] != 8 or data[25] != 6:
        raise AssertionError(f"{path.relative_to(ROOT)} must be 8-bit RGBA")
    return width, height


def ico_sizes(path: Path) -> set[tuple[int, int]]:
    data = path.read_bytes()
    reserved, image_type, count = struct.unpack("<HHH", data[:6])
    if reserved != 0 or image_type != 1 or count == 0:
        raise AssertionError("neptune.ico has an invalid directory")
    sizes: set[tuple[int, int]] = set()
    for index in range(count):
        offset = 6 + index * 16
        width, height, _colors, reserved, _planes, bits, length, image_offset = struct.unpack(
            "<BBBBHHII", data[offset : offset + 16]
        )
        width = width or 256
        height = height or 256
        if reserved != 0 or bits != 32 or image_offset + length > len(data):
            raise AssertionError(f"neptune.ico entry {index} is invalid")
        sizes.add((width, height))
    return sizes


def main() -> None:
    expected_pngs = {
        ROOT / "assets" / "windows" / "neptune-master.png": (1254, 1254),
        ASSETS / "Square44x44Logo.png": (44, 44),
        ASSETS / "Square150x150Logo.png": (150, 150),
        ASSETS / "StoreLogo.png": (50, 50),
        ASSETS / "Wide310x150Logo.png": (310, 150),
        ASSETS / "SplashScreen.png": (620, 300),
    }
    for size in (16, 24, 32, 48, 256):
        expected_pngs[ASSETS / f"Square44x44Logo.targetsize-{size}.png"] = (size, size)
        expected_pngs[ASSETS / f"Square44x44Logo.targetsize-{size}_altform-unplated.png"] = (size, size)
    for path, expected in expected_pngs.items():
        actual = png_dimensions(path)
        if actual != expected:
            raise AssertionError(f"{path.relative_to(ROOT)} is {actual}, expected {expected}")

    expected_ico = {(size, size) for size in (16, 20, 24, 32, 40, 48, 64, 96, 128, 256)}
    actual_ico = ico_sizes(ROOT / "assets" / "windows" / "neptune.ico")
    if actual_ico != expected_ico:
        raise AssertionError(f"neptune.ico sizes are {sorted(actual_ico)}, expected {sorted(expected_ico)}")

    print(f"Validated {len(expected_pngs)} PNG assets and {len(actual_ico)} ICO sizes.")


if __name__ == "__main__":
    main()
