FROM python:3.11.5-slim-bullseye
WORKDIR /app
COPY dist/ /app/
RUN pip install /app/control_plane-1.0.0-py3-none-any.whl
