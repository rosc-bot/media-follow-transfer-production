from app.ingest.media_identity import clean_title


def test_clean_title_excludes_episode_and_share_url():
    assert clean_title('人工剧 S01E01 https://pan.guangyapan.com/s/manual') == '人工剧'
