from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.router import router
from app.core.database import engine


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield
    engine.dispose()


app = FastAPI(title="Oil Flows Backend", lifespan=lifespan)
app.include_router(router)
