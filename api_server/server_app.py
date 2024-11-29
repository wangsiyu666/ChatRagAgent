import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import RedirectResponse
import argparse
import sys
from copilotkit.api_server.kb_routes import kb_router
from utils import MakeFastAPIOffline
from file_api import file_router

def create_app(run_mode: str = None):
    app = FastAPI(title="CopilotKit")
    MakeFastAPIOffline(app)
    OPEN_CROSS_DOMAIN = True
    if OPEN_CROSS_DOMAIN:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"]
        )

    @app.get("/", summary="swagger 文档", include_in_schema=False)
    async def document():
        return RedirectResponse(url="/docs")


    app.include_router(kb_router)
    return app


def run_api(host, port, **kwargs):
    if kwargs.get("ssl_keyfile") and kwargs.get("ssl_certfile"):
        uvicorn.run(
            app,
            host=host,
            port=port,
            ssl_keyfile=kwargs.get("ssl_keyfile"),
            ssl_certfile=kwargs.get("ssl_certfile"),
        )
    else:
        uvicorn.run(app, host=host, port=port)


app = create_app()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog="copilotKit",
        description="About toolkit"
    )

    parser.add_argument("--host", type=str, default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--ssl_keyfile", type=str)
    parser.add_argument("--ssl_certfile", type=str)

    args = parser.parse_args()
    args_dict = vars(args)
    run_api(
        host=args.host,
        port=args.port,
        ssl_keyfile=args.ssl_keyfile,
        ssl_certfile=args.ssl_certfile,
    )
