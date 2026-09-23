import logging


class RedactingFormatter(logging.Formatter):
    _SENSITIVE = ('token', 'password', 'secret', 'authorization', 'api_hash')

    def format(self, record: logging.LogRecord) -> str:
        text = super().format(record)
        for key in self._SENSITIVE:
            text = text.replace(key, f'{key[:2]}***')
        return text


def configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(RedactingFormatter('%(asctime)s %(levelname)s %(name)s %(message)s'))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)
