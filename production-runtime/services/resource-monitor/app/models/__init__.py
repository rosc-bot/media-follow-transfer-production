from app.models.auto_ingest_history import AutoIngestHistory
from app.models.bot_settings import BotSettings
from app.models.channel import ChannelSetting
from app.models.cloud import CloudConfig, CloudDiskInventory
from app.models.failed_scout_push import FailedScoutPush
from app.models.ignored_missing import IgnoredMissing
from app.models.ingest import ChannelIngestJob, ChannelIngestMessage
from app.models.resource import Resource
from app.models.resource_candidate import ResourceCandidate
from app.models.settings import AppSetting
from app.models.transfer import TransferJob, TransferQueueTask
from app.models.watchlist import SeriesWatchlist


def import_all_models() -> None:
    """Import hook used before metadata creation; imports are intentionally explicit."""
    return


__all__ = [
    'AppSetting', 'AutoIngestHistory', 'BotSettings', 'ChannelIngestJob',
    'ChannelIngestMessage', 'ChannelSetting', 'CloudConfig', 'CloudDiskInventory',
    'FailedScoutPush', 'IgnoredMissing', 'Resource', 'SeriesWatchlist',
    'TransferJob', 'TransferQueueTask', 'ResourceCandidate',
]
