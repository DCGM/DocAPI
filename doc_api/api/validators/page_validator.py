from typing import Dict, Optional
from defusedxml import ElementTree as ET

PAGE_NS_BASE = "http://schema.primaresearch.org/PAGE/gts/pagecontent/"

def _localname(tag: str) -> str:
    return tag.split("}", 1)[1] if tag.startswith("{") else tag

def _namespace(tag: str) -> Optional[str]:
    return tag[1:].split("}", 1)[0] if tag.startswith("{") else None

def validate_page_basic(xml_bytes: bytes) -> Dict[str, bool]:
    checks = {"root": False, "namespace": False, "has_page": False, "has_text": False}

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        # not even XML
        return checks

    # Root element must be PcGts
    if _localname(root.tag) == "PcGts":
        checks["root"] = True

    # Allow missing ns OR any PAGE ns variant by prefix
    ns = _namespace(root.tag)
    if ns is None or ns.startswith(PAGE_NS_BASE):
        checks["namespace"] = True

    # Require at least one <Page>
    page = root.find(".//{*}Page")
    if page is not None:
        checks["has_page"] = True

    # Presence of text-bearing structures anywhere
    if root.find(".//{*}TextRegion") is not None or root.find(".//{*}TextLine") is not None:
        checks["has_text"] = True

    return checks
