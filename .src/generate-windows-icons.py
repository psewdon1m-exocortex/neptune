"""Generate the Windows Neptune icon family from the checked-in master PNG.

Requires Pillow. The master is expected to be a full-bleed square RGBA image;
this script keeps that square composition identical at every icon size.
"""

from pathlib import Path

from PIL import Image, ImageFilter


ROOT = Path(__file__).resolve().parents[1]
MASTER = ROOT / "assets" / "windows" / "neptune-master.png"
ICON = ROOT / "assets" / "windows" / "neptune.ico"
PACKAGE = ROOT / "packaging" / "windows" / "Assets"
ICO_SIZES = (16, 20, 24, 32, 40, 48, 64, 96, 128, 256)


def square(master: Image.Image, size: int) -> Image.Image:
    image = master.resize((size, size), Image.Resampling.LANCZOS)
    if size <= 48:
        image = image.filter(ImageFilter.UnsharpMask(radius=0.65, percent=170, threshold=2))
    return image


def canvas(master: Image.Image, width: int, height: int, icon_size: int) -> Image.Image:
    output = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    icon = square(master, icon_size)
    output.alpha_composite(icon, ((width - icon_size) // 2, (height - icon_size) // 2))
    return output


def main() -> None:
    PACKAGE.mkdir(parents=True, exist_ok=True)
    with Image.open(MASTER) as source:
        master = source.convert("RGBA")
        if master.width != master.height:
            raise ValueError("Neptune icon master must be square")

        master.save(ICON, format="ICO", sizes=[(size, size) for size in ICO_SIZES], bitmap_format="png")
        square(master, 44).save(PACKAGE / "Square44x44Logo.png")
        square(master, 150).save(PACKAGE / "Square150x150Logo.png")
        square(master, 50).save(PACKAGE / "StoreLogo.png")
        canvas(master, 310, 150, 132).save(PACKAGE / "Wide310x150Logo.png")
        canvas(master, 620, 300, 224).save(PACKAGE / "SplashScreen.png")

        for size in (16, 24, 32, 48, 256):
            target = square(master, size)
            target.save(PACKAGE / f"Square44x44Logo.targetsize-{size}.png")
            target.save(PACKAGE / f"Square44x44Logo.targetsize-{size}_altform-unplated.png")


if __name__ == "__main__":
    main()
