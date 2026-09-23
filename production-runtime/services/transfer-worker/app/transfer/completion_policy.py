def verified_completion(expected_files: list[str], remote_files: list[str], *, provider_verified: bool) -> bool:
    """Verify a transfer only against a non-empty set of remote file names.

    * provider must report verified,
    * the observed remote listing must contain at least one file,
    * when expected_files is non-empty every expected name must be present.

    ``expected_files == []`` no longer auto-passes: with no expected list the
    adapter already verified against the share's selected video names, so we only
    require the readback to contain at least one file (a truly empty target
    directory can never be a successful transfer).
    """
    if not provider_verified:
        return False
    observed = {x.strip().lower() for x in remote_files if x.strip()}
    if not observed:
        return False
    expected = {x.strip().lower() for x in expected_files if x.strip()}
    if not expected:
        return True
    return expected.issubset(observed)
