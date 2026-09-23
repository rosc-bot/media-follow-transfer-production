from app.follow.incremental_logic import title_scoped_text


def matches(candidate, *, title: str, episode_key: str) -> bool:
    return title_scoped_text(title, candidate.text) and episode_key.upper() in candidate.text.upper()
