from fastapi import APIRouter

from app.core.config import get_settings

router=APIRouter(prefix='/settings',tags=['settings'])

@router.get('')
async def settings_summary():
    settings=get_settings()
    return {'app_env':settings.app_env,'cloud_write_enabled':settings.cloud_write_enabled,'resource_messages_db':settings.resource_messages_db}
