import re
from pathlib import Path


def test_alembic_revision_identifiers_fit_the_version_table_column():
    versions = Path(__file__).parents[1] / 'migrations' / 'versions'
    identifiers = []
    for path in versions.glob('*.py'):
        match = re.search(r"^revision\s*=\s*'([^']+)'", path.read_text(), flags=re.MULTILINE)
        if match:
            identifiers.append(match.group(1))

    assert identifiers
    assert all(len(identifier) <= 32 for identifier in identifiers)
