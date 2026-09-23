import subprocess
import sys
from pathlib import Path


def test_watchlist_migration_cli_runs_as_a_direct_script():
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, 'migration_tools/import_watchlist.py', '--help'],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert '--report' in result.stdout
