from app.transfer.adapters import BaseAdapter


class AlistAdapter(BaseAdapter):
    provider = 'alist'

    async def transfer(self, payload: dict):
        return await super().transfer(payload)
