import itertools

import cv2
import numpy as np

#
# Generate combinations of job definition payloads for testing
#

def generate_job_definition_payloads():
    base_images = [
        {"name": "img1.png", "order": 0},
        {"name": "img2.jpg", "order": 1},
        {"name": "img3.tif", "order": 2},
    ]

    payloads = []
    for combo in itertools.product([False, True], repeat=3):
        flags = dict(zip(["meta_json_required", "alto_required", "page_required"], combo))
        payloads.append({
            "images": base_images[:2] if any(combo) else base_images[:3],
            **flags,
        })
    return payloads

def job_definition_payload_id(payload):
    """Generate a readable ID for pytest logs."""
    image_count = len(payload["images"])
    # collect unique extensions
    exts = {img["name"].split(".")[-1] for img in payload["images"]}
    flags = [
        name.replace("_required", "")
        for name, val in payload.items()
        if name.endswith("_required") and val
    ]
    flags_str = "+".join(flags) if flags else "none"
    return f"{image_count}-imgs:{'+'.join(sorted(exts))}:{flags_str}"

JOB_DEFINITION_PAYLOADS = generate_job_definition_payloads()

def make_white_image_bytes(ext: str = ".jpg"):
    """
    Create a 1×1 pixel valid image using OpenCV and return (bytes, content_type).
    ext can be '.jpg', '.png', or '.tif' depending on what you want to test.
    """
    # Create a simple white 1×1 RGB image
    img = np.full((128, 128, 3), 255, dtype=np.uint8)

    # Encode the image into memory (OpenCV always returns tuple (ok, buf))
    ok, buf = cv2.imencode(ext, img)
    assert ok, f"Failed to encode {ext} image with OpenCV"

    # Choose appropriate MIME type
    content_type = {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".tif": "image/tiff",
        ".tiff": "image/tiff",
    }.get(ext.lower(), "application/octet-stream")

    return buf.tobytes(), content_type

VALID_ALTO_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<alto xmlns="http://www.loc.gov/standards/alto/ns-v4#">
  <Layout><Page><PrintSpace><TextBlock><TextLine><String CONTENT="x"/></TextLine></TextBlock></PrintSpace></Page></Layout>
</alto>"""


VALID_PAGE_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<PcGts xmlns="http://schema.primaresearch.org/PAGE/gts/pagecontent/2019-07-15">
  <Metadata><Creator>test</Creator></Metadata>
  <Page imageWidth="1" imageHeight="1" imageFilename="x">
    <TextRegion id="r1"/>
  </Page>
</PcGts>
"""