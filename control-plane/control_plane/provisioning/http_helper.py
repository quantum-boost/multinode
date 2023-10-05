import urllib.parse
from typing import TypeVar

import requests
from pydantic import BaseModel, ValidationError
from requests import JSONDecodeError, RequestException, Response

T = TypeVar("T", bound=BaseModel)


class HttpHandlingError(BaseException):
    pass


class HttpStandardErrorResponse(BaseModel):
    detail: str


class HttpRequestHandler:
    def __init__(self, base_url: str, bearer_token: str):
        self._base_url = base_url
        self._bearer_token = bearer_token

    def post(
        self, path: str, request_body: BaseModel, response_body_type: type[T]
    ) -> T:
        url = urllib.parse.urljoin(self._base_url, path)
        request_dict = request_body.model_dump()
        headers = {"Authorization": f"Bearer {self._bearer_token}"}

        try:
            response = requests.post(url=url, json=request_dict, headers=headers)
        except RequestException as ex:
            raise HttpHandlingError(f"Request failed: {str(ex)}")

        if 200 <= response.status_code <= 299:
            return self._extract_object_from_response(response, response_body_type)

        elif 400 <= response.status_code <= 499:
            error_response_body = self._extract_object_from_response(
                response, HttpStandardErrorResponse
            )
            raise HttpHandlingError(
                f"Status code: {response.status_code}, Detail: {error_response_body.detail}"
            )

        else:
            raise HttpHandlingError(f"Status code: {response.status_code}")

    @staticmethod
    def _extract_object_from_response(response: Response, object_type: type[T]) -> T:
        try:
            response_dict = response.json()
        except JSONDecodeError as ex:
            raise HttpHandlingError(
                f"Status code: {response.status_code}, Cannot parse response as JSON: {str(ex)}"
            )

        try:
            return object_type.model_validate(response_dict)
        except ValidationError as ex:
            raise HttpHandlingError(
                f"Status code: {response.status_code}, Cannot deserialise response: {str(ex)}"
            )
