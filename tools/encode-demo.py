"""Encode paired, real Playwright screenshots; no UI or message content is mocked."""
import json
import sys
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

manifest = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
frames = []
for index, (left_path, right_path) in enumerate(manifest["frames"]):
    with Image.open(left_path) as left, Image.open(right_path) as right:
        canvas = Image.new("RGB", (left.width + right.width, max(left.height, right.height) + 52), "#141414")
        draw = ImageDraw.Draw(canvas)
        draw.text((12, 6), "CLIENT A", fill="#ededed", font=ImageFont.load_default(size=14))
        draw.text((left.width + 12, 6), "CLIENT B", fill="#ededed", font=ImageFont.load_default(size=14))
        draw.text((12, 28), manifest["captions"][index], fill="#22c55e", font=ImageFont.load_default(size=14))
        canvas.paste(left.convert("RGB"), (0, 52))
        canvas.paste(right.convert("RGB"), (left.width, 52))
        frames.append(canvas.quantize(colors=192))
output = Path(manifest["output"])
output.parent.mkdir(parents=True, exist_ok=True)
frames[0].save(output, save_all=True, append_images=frames[1:], duration=600, loop=0, optimize=False)
with Image.open(output) as check:
    assert check.n_frames > 1
    print(json.dumps({"frames": check.n_frames, "width": check.width, "height": check.height, "bytes": output.stat().st_size}))
