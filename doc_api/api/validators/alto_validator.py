from typing import Dict, Optional
from defusedxml import ElementTree as ET

ALLOWED_NS = {
    "http://www.loc.gov/standards/alto/ns-v2#",
    "http://www.loc.gov/standards/alto/ns-v3#",
    "http://www.loc.gov/standards/alto/ns-v4#",
}


def _localname(tag: str) -> str:
    return tag.split("}", 1)[1] if tag.startswith("{") else tag


def _namespace(tag: str) -> Optional[str]:
    return tag[1:].split("}", 1)[0] if tag.startswith("{") else None


def validate_alto_basic(xml_bytes: bytes) -> Dict[str, bool]:
    checks = {
        "root": False,
        "namespace": False,
        "has_layout": False,
        "has_page": False,
        "has_text": False,
    }

    root = ET.fromstring(xml_bytes)

    if _localname(root.tag) == "alto":
        checks["root"] = True

    ns = _namespace(root.tag)
    if ns in ALLOWED_NS:
        checks["namespace"] = True

    if root.find(".//{*}Layout") is not None:
        checks["has_layout"] = True

    if root.find(".//{*}Page") is not None:
        checks["has_page"] = True

    if (
        root.find(".//{*}String") is not None
        or root.find(".//{*}TextLine") is not None
        or root.find(".//{*}TextBlock") is not None
    ):
        checks["has_text"] = True

    return checks
