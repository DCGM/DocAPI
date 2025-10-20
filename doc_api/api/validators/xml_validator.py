from defusedxml import ElementTree as ET
from defusedxml.common import DefusedXmlException

def is_well_formed_xml(xml_bytes: bytes) -> bool:
    """
    Returns True if the given bytes represent well-formed XML.
    Uses defusedxml for safe parsing.
    """
    try:
        ET.fromstring(xml_bytes)
        return True
    except (ET.ParseError, DefusedXmlException):
        return False