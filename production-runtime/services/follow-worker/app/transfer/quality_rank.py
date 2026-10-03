import re

_QUALITY_RANKS = [
    (100, re.compile(r'(?i)\b(?:remux|bdremux|uhd[- .]?remux)\b')),
    (80, re.compile(r'(?i)\b(?:2160p|4k|uhd)\b')),
    (60, re.compile(r'(?i)\b(?:1080p|fhd)\b')),
    (40, re.compile(r'(?i)\b(?:720p|hd)\b')),
    (20, re.compile(r'(?i)\b(?:576p|480p|sd)\b')),
]

_ENCODING_RANKS = [
    (15, re.compile(r'(?i)\b(?:bluray|bdrip)\b')),
    (10, re.compile(r'(?i)\b(?:web[- .]?dl|webrip)\b')),
    (5, re.compile(r'(?i)\b(?:hdtv)\b')),
]

def extract_quality_score(text: str) -> int:
    score = 0
    raw = str(text or "")
    for s, pat in _QUALITY_RANKS:
        if pat.search(raw):
            score += s
            break
    for s, pat in _ENCODING_RANKS:
        if pat.search(raw):
            score += s
            break
    return score
