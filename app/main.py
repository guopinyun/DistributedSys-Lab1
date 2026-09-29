"""Producer service (port 8080).

Runs the HTTP API only. The consumer is a separate process
(`python -m app.consumer`) so it can be killed and restarted on its own, which
is what exercises 1.3 and the harness's criterion 2 require.

The interactive docs are switched off so the service really does expose exactly
POST /process and GET /result/{id}.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from . import config, store
from .routes import router

log = logging.getLogger("producer")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    store.init()
    log.info("producer ready: queue=%s db=%s", config.QUEUE_NAME, config.DB_PATH)
    log.info("start the consumer separately: python -m app.consumer")
    yield


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

app = FastAPI(
    title="COMP41720 Lab 1 - async AI processing",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

app.include_router(router)
