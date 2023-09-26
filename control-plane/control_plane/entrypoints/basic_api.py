"""
A basic API, used to test our build process. Will eventually replace with real code.
"""
import uvicorn
from fastapi import FastAPI


def build_app() -> FastAPI:
    app = FastAPI()

    @app.get("/")
    def hello() -> str:
        return "Hello world"

    return app


def main() -> None:
    app = build_app()
    uvicorn.run(app, host="0.0.0.0", port=5000)


if __name__ == "__main__":
    main()
