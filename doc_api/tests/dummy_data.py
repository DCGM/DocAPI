import itertools
import io
import zipfile
from xml.etree import ElementTree

import cv2
import numpy as np


def make_xml_payload(kind, variant):
    """Build bounded XML cases at test runtime, without checked-in large files."""
    namespace = ("http://www.loc.gov/standards/alto/ns-v4#" if kind == "alto" else
                 "http://schema.primaresearch.org/PAGE/gts/pagecontent/2019-07-15")
    if variant == "validation_combinations":
        namespace = "urn:unsupported-test-namespace"
    def tag(name):
        return f"{{{namespace}}}{name}"

    root = ElementTree.Element(tag("alto" if kind == "alto" else "PcGts"))
    parent = ElementTree.SubElement(root, tag("Layout")) if kind == "alto" else root
    parent = ElementTree.SubElement(parent, tag("Page"))
    if variant != "validation_combinations":
        if variant == "deep":
            for _ in range(64):
                parent = ElementTree.SubElement(parent, tag("Group"))
        for index in range(15000 if variant == "large" else 1):
            line = ElementTree.SubElement(parent, tag("TextLine"), id=f"line-{index}")
            text = "Příliš žluťoučký kůň — Ελληνικά 中文 😀 & < > " * 2
            if kind == "alto":
                ElementTree.SubElement(line, tag("String"), CONTENT=text)
            else:
                ElementTree.SubElement(line, tag("Unicode")).text = text
    payload = ElementTree.tostring(root, encoding="utf-16" if variant == "utf16" else "utf-8",
                                   xml_declaration=True)
    if variant == "truncated":
        return payload[:-20]
    if variant == "invalid_encoding":
        return payload.replace(b"</ns0:Page>", b"\xff</ns0:Page>")
    if variant == "entity":
        return b'<!DOCTYPE root [<!ENTITY injected "text">]><root>&injected;</root>'
    if variant == "empty":
        return b""
    return payload


def make_zip_payload(variant):
    """Generate real archives, including multipart-spooling and rejection cases."""
    buffer = io.BytesIO()
    compression = zipfile.ZIP_DEFLATED if variant == "compressed" else zipfile.ZIP_STORED
    with zipfile.ZipFile(buffer, "w", compression=compression) as archive:
        if variant == "many_entries":
            for index in range(400):
                archive.writestr(f"pages/{index:04d}.xml", f"<page id='{index}'/>")
        elif variant == "large":
            archive.writestr("large.bin", bytes(range(256)) * 12288)  # 3 MiB, no compression.
        elif variant == "compressed":
            archive.writestr("repeated.txt", b"generated text\n" * 100000)
        elif variant == "unicode":
            archive.writestr("výsledky/中文 😀.xml", "<text>Příliš žluťoučký kůň</text>")
            archive.writestr("empty-directory/", b"")
        elif variant != "empty":
            archive.writestr("page.xml", "<page/>")
    payload = buffer.getvalue()
    if variant == "truncated":
        return payload[:-22]  # Remove the end-of-central-directory record.
    if variant == "invalid_directory":
        return payload.replace(b"PK\x01\x02", b"BAD!", 1)
    return payload

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

def job_definition_payload_id(payload, app_code=None):
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
    payload_id = f"{image_count}-imgs:{'+'.join(sorted(exts))}:{flags_str}"
    if app_code is not None:
        return f"{app_code}:{payload_id}"
    return payload_id

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

VALID_ZIP = (b"PK\x03\x04"                      # local file header signature
             b"\x14\x00\x00\x00\x00\x00"        # version / flags / compression / mod time/date
             b"\x00\x00\x00\x00"                # crc32
             b"\x00\x00\x00\x00"                # comp / uncomp sizes
             b"\x00\x00"                        # filename length
             b"\x00\x00"                        # extra length
             b"PK\x01\x02"                      # central directory header
             b"\x14\x00\x14\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
             b"\x00\x00\x00\x00\x00\x00\x00\x00"
             b"PK\x05\x06" + b"\x00" * 18)       # end of central directory
