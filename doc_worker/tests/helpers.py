"""Test data for worker downloads, independent of the API test fixtures."""
from unittest.mock import Mock

import cv2
import numpy as np
import requests


def input_download_response(kind, status=200):
    response = requests.Response()
    response.status_code = status
    response.encoding = "utf-8"
    if kind == "image":
        encoded, image = cv2.imencode(".jpg", np.full((128, 128, 3), 255, dtype=np.uint8))
        assert encoded
        response._content = image.tobytes()
    else:
        response._content = b"<document>linked input</document>"
    response._content_consumed = True
    response.json = Mock(side_effect=ValueError("not a DocAPI JSON envelope"))
    return response
