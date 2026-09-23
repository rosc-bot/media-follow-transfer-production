"""One canonical TMDB-backed media destination builder.

The historical media bot exposed a ``MediaClassifier.classify(task, tmdb_data)``
contract and created a media root plus a second-level category before the work
folder.  The current follow/transfer service had retained only the lifecycle
(root) split, so this module restores that contract in one provider-neutral
place.  It is deliberately pure: it never calls TMDB or a cloud provider.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

TV_CATEGORIES = (
    "国产剧",
    "日韩剧",
    "欧美剧",
    "日番",
    "国漫",
    "欧美动漫",
    "纪录片",
    "儿童",
    "综艺",
    "其他剧",
)
MOVIE_CATEGORIES = (
    "华语电影",
    "日韩电影",
    "欧美电影",
    "动画电影",
    "纪录片",
    "儿童",
    "综艺",
    "其他电影",
)

_TV_TYPES = frozenset({"tv", "series", "anime", "电视剧", "动漫", "show"})
_MOVIE_TYPES = frozenset({"movie", "film", "电影"})
_ENDED = frozenset({"ended", "canceled", "cancelled"})
_CN_COUNTRIES = frozenset({"CN"})
_JP_KR_COUNTRIES = frozenset({"JP", "KR"})
_WESTERN_COUNTRIES = frozenset({
    "US", "GB", "CA", "AU", "NZ", "IE", "FR", "DE", "IT", "ES", "PT", "NL", "BE",
    "LU", "CH", "AT", "DK", "SE", "NO", "FI", "IS", "PL", "CZ", "HU", "RO", "GR",
    "UA", "RU", "ZA", "BR", "MX", "AR", "CL", "CO",
})
_ANIMATION_IDS = frozenset({16})
_DOCUMENTARY_IDS = frozenset({99})
_KIDS_IDS = frozenset({10762})
_VARIETY_IDS = frozenset({10764, 10767})


def _text(value: object) -> str:
    return str(value or "").strip()


def _normalise_kind(value: object) -> str:
    value = _text(value).casefold()
    if value in _MOVIE_TYPES:
        return "movie"
    if value in _TV_TYPES or not value:
        return "tv"
    return value


def _list_values(value: object) -> list[object]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        for key in ("keywords", "results", "items", "data"):
            if key in value:
                return _list_values(value[key])
        return []
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return list(value)
    return [value]


def _genre_names(metadata: Mapping[str, Any]) -> tuple[str, ...]:
    values: list[str] = []
    for raw in _list_values(metadata.get("genres")):
        if isinstance(raw, Mapping):
            value = _text(raw.get("name") or raw.get("english_name"))
        else:
            value = _text(raw)
        if value and value not in values:
            values.append(value)
    return tuple(values)


def _genre_ids(metadata: Mapping[str, Any]) -> frozenset[int]:
    values: set[int] = set()
    raw_values = list(_list_values(metadata.get("genre_ids")))
    for raw in _list_values(metadata.get("genres")):
        if isinstance(raw, Mapping):
            raw_values.append(raw.get("id"))
    for raw in raw_values:
        try:
            values.add(int(raw))
        except (TypeError, ValueError):
            continue
    return frozenset(values)


def _keywords(metadata: Mapping[str, Any]) -> frozenset[str]:
    values: set[str] = set()
    for raw in _list_values(metadata.get("keywords")):
        if isinstance(raw, Mapping):
            value = _text(raw.get("name") or raw.get("keyword"))
        else:
            value = _text(raw)
        if value:
            values.add(value.casefold())
    return frozenset(values)


def _countries(metadata: Mapping[str, Any]) -> frozenset[str]:
    values: set[str] = set()
    sources = [metadata.get("origin_country"), metadata.get("production_countries")]
    for source in sources:
        for raw in _list_values(source):
            if isinstance(raw, Mapping):
                raw = raw.get("iso_3166_1") or raw.get("country_code") or raw.get("code")
            value = _text(raw).upper()
            if value:
                values.add(value)
    return frozenset(values)


def _language(metadata: Mapping[str, Any]) -> str:
    return _text(metadata.get("original_language")).casefold()


def _contains(names: tuple[str, ...], *needles: str) -> bool:
    haystack = " ".join(names).casefold()
    return any(needle.casefold() in haystack for needle in needles)


def _country_class(countries: frozenset[str]) -> str | None:
    if countries & _CN_COUNTRIES:
        return "cn"
    if countries & _JP_KR_COUNTRIES:
        return "jpkr"
    if countries & _WESTERN_COUNTRIES:
        return "western"
    return None


@dataclass(frozen=True)
class CategoryResolution:
    media_root: str
    category: str
    origin_country: tuple[str, ...]
    original_language: str
    genres: tuple[str, ...]
    genre_ids: tuple[int, ...]
    keywords: tuple[str, ...]
    animation: bool
    documentary: bool
    kids: bool
    variety: bool
    confidence: str
    evidence: tuple[str, ...]

    @property
    def sub_category(self) -> str:
        return self.category

    def as_dict(self) -> dict[str, Any]:
        return {
            "media_root": self.media_root,
            "category": self.category,
            "media_category": self.category,
            "sub_category": self.category,
            "origin_country": list(self.origin_country),
            "original_language": self.original_language,
            "genres": list(self.genres),
            "genre_ids": list(self.genre_ids),
            "keywords": list(self.keywords),
            "animation": self.animation,
            "documentary": self.documentary,
            "kids": self.kids,
            "variety": self.variety,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True)
class CanonicalDestination:
    destination_kind: str
    root_label: str
    media_root: str
    media_category: str
    item_name: str
    season_name: str | None
    resolution: CategoryResolution

    @property
    def series_or_movie_root(self) -> str:
        return self.item_name

    @property
    def relative_item_path(self) -> str:
        return f"{self.media_root}/{self.media_category}/{self.item_name}"

    @property
    def inventory_prefix(self) -> str:
        parts = [self.relative_item_path]
        if self.season_name:
            parts.append(self.season_name)
        return "/".join(parts)

    @property
    def archive_directory(self) -> str:
        parts = [self.root_label, self.media_root, self.media_category, self.item_name]
        if self.season_name:
            parts.append(self.season_name)
        return " / ".join(parts)

    def as_dict(self) -> dict[str, Any]:
        return {
            "destination_kind": self.destination_kind,
            "root_label": self.root_label,
            "media_root": self.media_root,
            "media_category": self.media_category,
            "sub_category": self.media_category,
            "item_name": self.item_name,
            "series_folder_name": self.item_name,
            "season_name": self.season_name,
            "season_folder_name": self.season_name,
            "relative_item_path": self.relative_item_path,
            "inventory_prefix": self.inventory_prefix,
            "archive_directory": self.archive_directory,
            "category_resolution": self.resolution.as_dict(),
        }


class DestinationMetadataIncomplete(ValueError):
    """TMDB identity/metadata is insufficient for a safe destination decision."""


class CanonicalDestinationBuilder:
    """Build every ongoing/completed/movie path from the same category result."""

    @staticmethod
    def resolve_category(metadata: Mapping[str, Any]) -> CategoryResolution:
        if not isinstance(metadata, Mapping):
            raise DestinationMetadataIncomplete("TMDB metadata object is required")
        tmdb_id = metadata.get("id") or metadata.get("tmdb_id")
        if tmdb_id is None:
            raise DestinationMetadataIncomplete("TMDB id is required for category routing")
        kind = _normalise_kind(metadata.get("media_type"))
        countries = _countries(metadata)
        language = _language(metadata)
        genres = _genre_names(metadata)
        genre_ids = _genre_ids(metadata)
        keywords = _keywords(metadata)
        searchable = (*genres, *keywords)
        animation = bool(genre_ids & _ANIMATION_IDS or _contains(searchable, "animation", "动画", "anime"))
        documentary = bool(genre_ids & _DOCUMENTARY_IDS or _contains(searchable, "documentary", "纪录"))
        kids = bool(genre_ids & _KIDS_IDS or _contains(searchable, "kids", "children", "儿童", "family"))
        variety = bool(genre_ids & _VARIETY_IDS or _contains(searchable, "reality", "variety", "talk", "综艺", "真人秀"))
        country_class = _country_class(countries)
        evidence: list[str] = []
        if countries:
            evidence.append(f"origin_country={','.join(sorted(countries))}")
        if language:
            evidence.append(f"original_language={language}")
        if genre_ids:
            evidence.append(f"genre_ids={','.join(str(value) for value in sorted(genre_ids))}")

        if kind == "movie":
            media_root = "电影"
            if documentary:
                category = "纪录片"
            elif kids:
                category = "儿童"
            elif variety:
                category = "综艺"
            elif animation:
                category = "动画电影"
            elif country_class == "cn":
                category = "华语电影"
            elif country_class == "jpkr":
                category = "日韩电影"
            elif country_class == "western":
                category = "欧美电影"
            else:
                category = "其他电影"
        else:
            media_root = "电视剧"
            if documentary:
                category = "纪录片"
            elif kids:
                category = "儿童"
            elif variety:
                category = "综艺"
            elif animation and country_class == "jpkr":
                category = "日番"
            elif animation and country_class == "cn":
                category = "国漫"
            elif animation and country_class == "western":
                category = "欧美动漫"
            elif animation:
                category = "其他剧"
            elif country_class == "cn":
                category = "国产剧"
            elif country_class == "jpkr":
                category = "日韩剧"
            elif country_class == "western":
                category = "欧美剧"
            else:
                category = "其他剧"
        confidence = "HIGH" if country_class or documentary or kids or variety or animation else "LOW"
        return CategoryResolution(
            media_root=media_root,
            category=category,
            origin_country=tuple(sorted(countries)),
            original_language=language,
            genres=genres,
            genre_ids=tuple(sorted(genre_ids)),
            keywords=tuple(sorted(keywords)),
            animation=animation,
            documentary=documentary,
            kids=kids,
            variety=variety,
            confidence=confidence,
            evidence=tuple(evidence),
        )

    @staticmethod
    def _item_name(*, title: str, year: int | None, tmdb_id: int) -> str:
        clean_title = _text(title)
        if not clean_title:
            raise DestinationMetadataIncomplete("title is required for destination routing")
        parts = [clean_title]
        if year is not None:
            parts.append(f"({int(year)})")
        parts.append(f"{{tmdbid-{int(tmdb_id)}}}")
        return " ".join(parts)

    @classmethod
    def build(
        cls,
        *,
        metadata: Mapping[str, Any],
        tmdb_id: int,
        media_type: str,
        title: str,
        year: int | None = None,
        destination_kind: str,
        season: int | None = None,
    ) -> CanonicalDestination:
        if int(tmdb_id) <= 0:
            raise DestinationMetadataIncomplete("tmdb_id must be positive")
        kind = _normalise_kind(media_type)
        if destination_kind not in {"ongoing", "completed"}:
            raise ValueError(f"unsupported destination_kind={destination_kind!r}")
        resolution = cls.resolve_category({**dict(metadata), "id": tmdb_id, "media_type": kind})
        item_name = cls._item_name(title=title, year=year, tmdb_id=int(tmdb_id))
        season_name = None
        if kind != "movie":
            try:
                season_number = int(season or 1)
            except (TypeError, ValueError) as exc:
                raise DestinationMetadataIncomplete("season is required for TV destination routing") from exc
            if season_number < 1:
                raise DestinationMetadataIncomplete("season must be positive")
            raw_seasons = metadata.get("seasons")
            if not isinstance(raw_seasons, Sequence) or isinstance(raw_seasons, (str, bytes, bytearray)) or not raw_seasons:
                raise DestinationMetadataIncomplete("TMDB relevant seasons are required for TV layout routing")
            relevant_seasons: set[int] = set()
            for item in raw_seasons:
                if not isinstance(item, Mapping):
                    continue
                try:
                    number = int(item.get("season_number") or 0)
                except (TypeError, ValueError):
                    continue
                if number > 0:
                    relevant_seasons.add(number)
            if not relevant_seasons:
                raise DestinationMetadataIncomplete("TMDB season list contains no relevant seasons")
            if season_number not in relevant_seasons:
                raise DestinationMetadataIncomplete(
                    f"requested season S{season_number:02d} is absent from TMDB relevant seasons"
                )
            if len(relevant_seasons) > 1:
                season_name = f"S{season_number:02d}"
        return CanonicalDestination(
            destination_kind=destination_kind,
            root_label="未完结追新" if destination_kind == "ongoing" else "影视转存总目录",
            media_root=resolution.media_root,
            media_category=resolution.category,
            item_name=item_name,
            season_name=season_name,
            resolution=resolution,
        )

    @staticmethod
    def category_change_status(current_category: str | None, resolved_category: str) -> str:
        return "UNCHANGED" if _text(current_category) == _text(resolved_category) else "CATEGORY_MISMATCH_REVIEW"

    @staticmethod
    def relative_path_from_payload(payload: Mapping[str, Any], file_name: str) -> str | None:
        prefix = _text(payload.get("inventory_prefix") or payload.get("remote_rel_path_prefix"))
        name = _text(file_name)
        if not prefix or not name:
            return None
        return f"{prefix.strip('/')}/{name}"


__all__ = [
    "MOVIE_CATEGORIES",
    "TV_CATEGORIES",
    "CanonicalDestination",
    "CanonicalDestinationBuilder",
    "CategoryResolution",
    "DestinationMetadataIncomplete",
]
