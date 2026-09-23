from app.transfer.adapters import BaseAdapter


class MobileAdapter(BaseAdapter):
    provider = 'mobile'

    async def transfer(self, payload: dict):
        return await super().transfer(payload)
