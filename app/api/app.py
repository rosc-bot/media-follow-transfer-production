from fastapi import FastAPI

from app.api.channels import router as channels_router
from app.api.follow import router as follow_router
from app.api.ingest import router as ingest_router
from app.api.resources import router as resources_router
from app.api.settings import router as settings_router
from app.api.transfer import router as transfer_router
from app.core.config import get_settings

app=FastAPI(title='media-follow-transfer')
app.include_router(follow_router); app.include_router(ingest_router); app.include_router(resources_router)
app.include_router(transfer_router); app.include_router(channels_router); app.include_router(settings_router)

@app.get('/health')
async def health(): return {'status':'ok','app_env':get_settings().app_env}

@app.get('/ready')
async def ready(): return {'status':'ok','cloud_write_enabled':get_settings().cloud_write_enabled}
