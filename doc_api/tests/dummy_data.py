import cv2
import numpy as np

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