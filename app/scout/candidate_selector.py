import re
from dataclasses import dataclass, field

from app.follow.incremental_logic import title_scoped_text


@dataclass(frozen=True)
class SelectedCandidate:
    message: object
    episode_key: str
    score: int
    matched_url: str | None = None
    rejections: tuple[str, ...] = field(default_factory=tuple)


def _season_episode(episode_key: str) -> tuple[int, int] | None:
    matched = re.fullmatch(r'S(\d{1,3})E(\d{1,4})', str(episode_key or '').upper())
    if not matched:
        return None
    return int(matched.group(1)), int(matched.group(2))


def _episode_number(episode_key: str) -> int | None:
    matched = re.search(r'E(\d+)', str(episode_key or ''), re.IGNORECASE)
    return int(matched.group(1)) if matched else None


def evaluate(candidate, *, title: str, episode_key: str) -> tuple[bool, str | None]:
    """Return (accepted, matched_url_or_None). Rejection reason surfaced when False."""
    text = getattr(candidate, 'text', None) or ''
    urls = getattr(candidate, 'urls', None) or []
    if not urls:
        return False, 'no_url'
    if not title_scoped_text(title, text):
        return False, 'title_mismatch'
    pair = _season_episode(episode_key)
    if pair is not None:
        season, episode = pair
        lowered = text.lower()
        forms = {
            f's{season:02d}e{episode:02d}',
            f's{season}e{episode:02d}',
            f's{season:02d}e{episode}',
            f'第{season}季第{episode}集',
            f'第{season}季 第{episode}集',
            f'se{episode:02d}',
            f'ep{episode:02d}',
            f'第{episode}集',
            f'第{episode}話',
            f'e{episode:02d}',
        }
        if not any(form.lower() in lowered for form in forms) and not re.search(
            rf'\bE\s*0*{episode}\b', text, re.IGNORECASE
        ):
            # Loose fallback only for cross-season references (E1-13 style ranges).
            return False, 'episode_mismatch'
    matched_url = next((u for u in urls if 'guangyapan' in u or 'gypan' in u), None) or (urls[0] if urls else None)
    if matched_url is None:
        return False, 'no_url'
    return True, matched_url


class CandidateSelector:
    @staticmethod
    def select(candidates, *, title: str, episode_key: str, limit: int = 5):
        """Rank valid candidates; expose rejection reason per candidate."""
        valid: list[SelectedCandidate] = []
        for candidate in candidates:
            _matched, matched_url = evaluate(candidate, title=title, episode_key=episode_key)
            score = 100 if title.lower() in (getattr(candidate, 'text', None) or '').lower() else 50
            valid.append(SelectedCandidate(
                candidate, episode_key, score, matched_url=matched_url,
            ))
        return sorted(valid, key=lambda item: (-item.score, int(getattr(item.message, 'message_id', 0) or 0)))[:limit]

    @staticmethod
    def explain(candidates, *, title: str, episode_key: str) -> dict:
        """Phase 2B: structured per-candidate decision record."""
        entries = []
        accepted = 0
        for candidate in (candidates or []):
            ok, matched_url = evaluate(candidate, title=title, episode_key=episode_key)
            entry = {
                'chat_id': str(getattr(candidate, 'chat_id', '')),
                'message_id': int(getattr(candidate, 'message_id', 0) or 0),
                'accepted': ok,
                'matched_url': matched_url,
                'has_url': bool(getattr(candidate, 'urls', None)),
            }
            if ok:
                accepted += 1
            else:
                reason = 'no_url' if not getattr(candidate, 'urls', None) else (
                    'title_mismatch' if not title_scoped_text(title, getattr(candidate, 'text', None) or '')
                    else 'episode_mismatch'
                )
                entry['rejection_reason'] = reason
            entries.append(entry)
        return {
            'candidate_count': len(entries),
            'accepted_count': accepted,
            'candidates': entries,
        }
