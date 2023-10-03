from unittest.mock import patch, Mock

import pytest
import requests  # noqa
from pydantic import BaseModel
from requests import JSONDecodeError, Timeout

from control_plane.provisioning.http_helper import HttpRequestHandler, HttpHandlingError


class DummyRequestBody(BaseModel):
    x: int


class DummyResponseBody(BaseModel):
    y: str


BASE_URL = "https://example.com/"
BEARER_TOKEN = "token"
PATH = "/path"
FULL_URL = "https://example.com/path"

REQUEST_BODY = DummyRequestBody(x=5)
RESPONSE_BODY = DummyResponseBody(y="q")

REQUEST_BODY_DICT = {"x": 5}
RESPONSE_BODY_DICT = {"y": "q"}

MALFORMED_RESPONSE_STRING = '{"y": "q}'
MALFORMED_RESPONSE_DICT = {"z": 99.0}

ERROR_MESSAGE = "some reason"
ERROR_RESPONSE_BODY_DICT = {"detail": ERROR_MESSAGE}


def test_with_200() -> None:
    http_handler = HttpRequestHandler(BASE_URL, BEARER_TOKEN)

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = RESPONSE_BODY_DICT

    with patch(f"{__name__}.requests.post") as mock_post:
        mock_post.return_value = mock_response

        actual_response_body = http_handler.post(PATH, REQUEST_BODY, DummyResponseBody)

        mock_post.assert_called_with(
            url=FULL_URL, json=REQUEST_BODY_DICT, headers={"Authorization": f"Bearer {BEARER_TOKEN}"}
        )

        assert actual_response_body == RESPONSE_BODY


def test_with_201() -> None:
    # 201 is also recognised as a valid success code

    http_handler = HttpRequestHandler(BASE_URL, BEARER_TOKEN)

    mock_response = Mock()
    mock_response.status_code = 201
    mock_response.json.return_value = RESPONSE_BODY_DICT

    with patch(f"{__name__}.requests.post") as mock_post:
        mock_post.return_value = mock_response
        actual_response_body = http_handler.post(PATH, REQUEST_BODY, DummyResponseBody)
        assert actual_response_body == RESPONSE_BODY


def test_with_200_when_json_is_unparseable() -> None:
    http_handler = HttpRequestHandler(BASE_URL, BEARER_TOKEN)

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.side_effect = JSONDecodeError(ERROR_MESSAGE, MALFORMED_RESPONSE_STRING, 13)

    with patch(f"{__name__}.requests.post") as mock_post:
        mock_post.return_value = mock_response

        with pytest.raises(HttpHandlingError) as exc_info:
            http_handler.post(PATH, REQUEST_BODY, DummyResponseBody)

        assert "Status code: 200" in str(exc_info.value)
        assert "Cannot parse response as JSON" in str(exc_info.value)
        assert ERROR_MESSAGE in str(exc_info.value)


def test_with_200_when_json_does_not_fit_response_model() -> None:
    http_handler = HttpRequestHandler(BASE_URL, BEARER_TOKEN)

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = MALFORMED_RESPONSE_DICT

    with patch(f"{__name__}.requests.post") as mock_post:
        mock_post.return_value = mock_response

        with pytest.raises(HttpHandlingError) as exc_info:
            http_handler.post(PATH, REQUEST_BODY, DummyResponseBody)

        assert "Status code: 200" in str(exc_info.value)
        assert "Cannot deserialise response:" in str(exc_info.value)


def test_with_400() -> None:
    http_handler = HttpRequestHandler(BASE_URL, BEARER_TOKEN)

    mock_response = Mock()
    mock_response.status_code = 400
    mock_response.json.return_value = ERROR_RESPONSE_BODY_DICT

    with patch(f"{__name__}.requests.post") as mock_post:
        mock_post.return_value = mock_response

        with pytest.raises(HttpHandlingError) as exc_info:
            http_handler.post(PATH, REQUEST_BODY, DummyResponseBody)

        assert "Status code: 400" in str(exc_info.value)
        assert f"Detail: {ERROR_MESSAGE}" in str(exc_info.value)


def test_with_404() -> None:
    # 404 is another possible failure code

    http_handler = HttpRequestHandler(BASE_URL, BEARER_TOKEN)

    mock_response = Mock()
    mock_response.status_code = 404
    mock_response.json.return_value = ERROR_RESPONSE_BODY_DICT

    with patch(f"{__name__}.requests.post") as mock_post:
        mock_post.return_value = mock_response

        with pytest.raises(HttpHandlingError) as exc_info:
            http_handler.post(PATH, REQUEST_BODY, DummyResponseBody)

        assert "Status code: 404" in str(exc_info.value)


def test_with_400_when_json_is_unparseable() -> None:
    http_handler = HttpRequestHandler(BASE_URL, BEARER_TOKEN)

    mock_response = Mock()
    mock_response.status_code = 400
    mock_response.json.side_effect = JSONDecodeError(ERROR_MESSAGE, MALFORMED_RESPONSE_STRING, 13)

    with patch(f"{__name__}.requests.post") as mock_post:
        mock_post.return_value = mock_response

        with pytest.raises(HttpHandlingError) as exc_info:
            http_handler.post(PATH, REQUEST_BODY, DummyResponseBody)

        assert "Status code: 400" in str(exc_info.value)
        assert ERROR_MESSAGE in str(exc_info.value)


def test_with_400_when_json_does_not_fit_error_response_model() -> None:
    http_handler = HttpRequestHandler(BASE_URL, BEARER_TOKEN)

    mock_response = Mock()
    mock_response.status_code = 400
    mock_response.json.return_value = MALFORMED_RESPONSE_DICT

    with patch(f"{__name__}.requests.post") as mock_post:
        mock_post.return_value = mock_response

        with pytest.raises(HttpHandlingError) as exc_info:
            http_handler.post(PATH, REQUEST_BODY, DummyResponseBody)

        assert "Status code: 400" in str(exc_info.value)
        assert "Cannot deserialise response:" in str(exc_info.value)


def test_with_500() -> None:
    http_handler = HttpRequestHandler(BASE_URL, BEARER_TOKEN)

    mock_response = Mock()
    mock_response.status_code = 500

    with patch(f"{__name__}.requests.post") as mock_post:
        mock_post.return_value = mock_response

        with pytest.raises(HttpHandlingError) as exc_info:
            http_handler.post(PATH, REQUEST_BODY, DummyResponseBody)

        assert "Status code: 500" in str(exc_info.value)


def test_when_request_times_out() -> None:
    http_handler = HttpRequestHandler(BASE_URL, BEARER_TOKEN)

    with patch(f"{__name__}.requests.post") as mock_post:
        mock_post.side_effect = Timeout(ERROR_MESSAGE)

        with pytest.raises(HttpHandlingError) as exc_info:
            http_handler.post(PATH, REQUEST_BODY, DummyResponseBody)

        assert ERROR_MESSAGE in str(exc_info.value)
