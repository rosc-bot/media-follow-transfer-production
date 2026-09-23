from app.core.constants import CHANNEL_ROLE_MANUAL_INGEST, CHANNEL_ROLE_RESOURCE


def accepts_message(setting, *, is_forward: bool) -> bool:
    if setting is None or getattr(setting, 'enabled', True) is False:
        return False
    role = getattr(setting, 'role', None)
    if role == CHANNEL_ROLE_MANUAL_INGEST:
        return bool(is_forward and getattr(setting, 'accept_forward', False))
    return role == CHANNEL_ROLE_RESOURCE
