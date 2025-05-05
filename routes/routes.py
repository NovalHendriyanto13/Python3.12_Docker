from fastapi import APIRouter
import routes.api as apiRouter

router = APIRouter()

router.include_router(apiRouter.authRoutes.router)
router.include_router(apiRouter.setupRoutes.router)