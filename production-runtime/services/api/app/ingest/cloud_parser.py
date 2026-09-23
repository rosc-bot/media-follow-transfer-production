from urllib.parse import urlparse


def provider_from_url(url: str) -> str:
    host = urlparse(url if '://' in url else f'https://{url}').netloc.lower()
    if 'guangya' in host or 'gypan' in host: return 'guangya'
    if 'alist' in host: return 'alist'
    if 'mobile' in host: return 'mobile'
    return 'unknown'
