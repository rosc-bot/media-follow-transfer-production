VIDEO_EXTENSIONS = frozenset({'.mkv', '.mp4', '.avi', '.mov', '.m4v', '.ts'})


def classify_media(file_names: list[str]) -> str:
    lowered = [name.lower() for name in file_names]
    if any(name.endswith(tuple(VIDEO_EXTENSIONS)) for name in lowered):
        return 'video'
    return 'unknown'
