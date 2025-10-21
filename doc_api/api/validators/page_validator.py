from typing import Dict, Optional
from defusedxml import ElementTree as ET

# Accept any PAGE schema date variant by prefix
PAGE_NS_BASE = "http://schema.primaresearch.org/PAGE/gts/pagecontent/"

def _localname(tag: str) -> str:
    return tag.split("}", 1)[1] if tag.startswith("{") else tag

def _namespace(tag: str) -> Optional[str]:
    return tag[1:].split("}", 1)[0] if tag.startswith("{") else None

def validate_page_basic(xml_bytes: bytes) -> Dict[str, bool]:
    checks = {
        "root": False,
        "namespace": False,
        "has_page": False,
        "has_text": False,
    }

    root = ET.fromstring(xml_bytes)

    # Root element must be PcGts
    if _localname(root.tag) == "PcGts":
        checks["root"] = True

    # Namespace: allow missing ns, or any PAGE ns variant by prefix
    ns = _namespace(root.tag)
    if ns.startswith(PAGE_NS_BASE):
        checks["namespace"] = True

    # Require at least one <Page>
    if root.find(".//{*}Page") is not None:
        checks["has_page"] = True

    # Loosely consider presence of text-bearing structures
    if (
        root.find(".//{*}TextRegion") is not None
        or root.find(".//{*}TextLine") is not None
    ):
        checks["has_text"] = True

    return checks
