from fastapi import APIRouter

from app.api.routes.verification import router as verification_router

router = APIRouter()

router.include_router(verification_router)


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
