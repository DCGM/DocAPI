import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest import mock

import pytest

from doc_worker.doc_worker_wrapper import JobHistory


NOW = datetime(2026, 11, 11, 12, tzinfo=timezone.utc)


def record(history, job_id='job-1', attempt=0, status='ok', finished=NOW):
    job = SimpleNamespace(id=job_id, previous_attempts=attempt, images=[1, 2],
                          engine_name='engine', engine_version='v1', engine_id=None,
                          engine_definition={'api_key': 'must-not-be-stored'})
    history.record(job, started_at=finished - timedelta(seconds=60), finished_at=finished,
                   duration_seconds=60, status=status)


def test_history_survives_restart_and_counts_retries_and_time_windows(tmp_path):
    path = tmp_path / 'history.jsonl'
    history = JobHistory(path)
    record(history, finished=NOW - timedelta(days=8))
    record(history, status='error', finished=NOW - timedelta(days=2))
    record(JobHistory(path), attempt=1, finished=NOW)
    record(history, job_id='job-2', status='fail', finished=NOW - timedelta(days=1))
    stats = JobHistory(path).statistics(NOW)
    assert stats['total']['attempts'] == 4 and stats['total']['jobs'] == 2
    assert stats['total']['outcomes']['ok'] == 2
    assert stats['total']['duration_seconds'] == 240
    assert stats['last_7_days']['attempts'] == 3
    assert stats['last_24_hours']['attempts'] == 2
    assert stats['last_24_hours']['outcomes']['fail'] == 1
    contents = path.read_text()
    assert 'must-not-be-stored' not in contents and 'engine_definition' not in contents
    entry = json.loads(contents.splitlines()[-1])
    assert entry['job_id'] == 'job-2' and entry['image_count'] == 2
    assert entry['finished_at'] == (NOW - timedelta(days=1)).isoformat()
    assert entry['engine_name'] == 'engine'


def test_new_history_has_zero_statistics_without_creating_file(tmp_path):
    path = tmp_path / 'history.jsonl'
    stats = JobHistory(path).statistics(NOW)
    assert all(stats[name]['attempts'] == 0 for name in ('total', 'last_7_days', 'last_24_hours'))
    assert not path.exists()


def test_window_boundaries_use_completion_time_and_timezone(tmp_path):
    history = JobHistory(tmp_path / 'history.jsonl')
    record(history, finished=NOW - timedelta(days=7))
    record(history, finished=NOW - timedelta(days=7, microseconds=1))
    record(history, finished=(NOW - timedelta(days=1)).astimezone(timezone(timedelta(hours=2))))
    record(history, finished=NOW + timedelta(seconds=1))
    stats = history.statistics(NOW)
    assert stats['total']['attempts'] == 4
    assert stats['last_7_days']['attempts'] == 2
    assert stats['last_24_hours']['attempts'] == 1


@pytest.mark.parametrize('contents', ['not json', '{"version": 2, "attempts": []}',
                                    '{"version": 1, "attempts": [{}]}'])
def test_corrupt_history_is_preserved_and_new_records_remain_readable(tmp_path, contents):
    path = tmp_path / 'history.jsonl'
    path.write_text(contents)
    history = JobHistory(path)
    record(history)
    assert path.read_text().startswith(contents + '\n')
    assert history.statistics(NOW)['total']['attempts'] == 1
    assert history.statistics(NOW)['invalid_records'] == 1


def test_failed_cache_replacement_keeps_appended_history_for_recovery(tmp_path):
    path = tmp_path / 'history.jsonl'
    history = JobHistory(path)
    record(history)
    before = history.stats_path.read_bytes()
    with mock.patch('doc_worker.doc_worker_wrapper.os.replace', side_effect=OSError('disk failure')):
        with pytest.raises(OSError):
            record(history, job_id='job-2')
    assert len(path.read_text().splitlines()) == 2
    assert history.stats_path.read_bytes() == before
    assert sorted(p.name for p in tmp_path.iterdir()) == ['history.jsonl', 'history.stats.json']
    assert JobHistory(path).statistics(NOW)['total']['attempts'] == 2


def test_parallel_statistics_and_recording_do_not_observe_partial_files(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    history = JobHistory(tmp_path / 'history.jsonl')

    def append(index):
        record(history, job_id=f'job-{index}')
        return history.statistics(NOW)['total']['attempts']

    with ThreadPoolExecutor(max_workers=4) as pool:
        counts = list(pool.map(append, range(12)))
    assert all(1 <= count <= 12 for count in counts)
    assert JobHistory(history.path).statistics(NOW)['total']['jobs'] == 12


def test_cached_append_and_report_do_not_read_existing_history(tmp_path):
    path = tmp_path / 'history.jsonl'
    history = JobHistory(path)
    record(history)
    first_line = path.read_bytes()
    real_open = type(path).open

    def checked_open(opened_path, mode='r', *args, **kwargs):
        if opened_path == path and mode in ('rb', 'r'):
            raise AssertionError('Existing history should not be read with a current cache')
        return real_open(opened_path, mode, *args, **kwargs)

    with mock.patch.object(type(path), 'open', checked_open):
        record(history, job_id='job-2')
        stats = history.statistics(NOW)
    assert stats['total']['attempts'] == 2
    assert path.read_bytes().startswith(first_line)


@pytest.mark.parametrize('damage', ['missing', 'invalid'])
def test_statistics_cache_can_be_rebuilt_from_history(tmp_path, damage):
    history = JobHistory(tmp_path / 'history.jsonl')
    record(history)
    record(history, job_id='job-2', status='error')
    if damage == 'missing':
        history.stats_path.unlink()
    else:
        history.stats_path.write_text('bad cache')
    stats = JobHistory(history.path).statistics(NOW)
    assert stats['total']['attempts'] == 2 and stats['total']['outcomes']['error'] == 1
    assert json.loads(history.stats_path.read_text())['history_bytes'] == history.path.stat().st_size


def test_stale_cache_reads_only_missing_tail(tmp_path):
    history = JobHistory(tmp_path / 'history.jsonl')
    record(history)
    cached = history.stats_path.read_bytes()
    record(history, job_id='job-2')
    history.stats_path.write_bytes(cached)
    offset = json.loads(cached)['history_bytes']
    real_open = type(history.path).open
    reads = []

    def checked_open(path, mode='r', *args, **kwargs):
        stream = real_open(path, mode, *args, **kwargs)
        if path == history.path and mode == 'rb':
            proxy = mock.MagicMock(wraps=stream)
            proxy.__enter__.return_value = proxy
            proxy.__exit__.side_effect = lambda *exc: stream.close()
            proxy.__iter__.side_effect = lambda: iter(stream)
            reads.append(proxy)
            return proxy
        return stream

    with mock.patch.object(type(history.path), 'open', checked_open):
        assert JobHistory(history.path).statistics(NOW)['total']['attempts'] == 2
    assert len(reads) == 1
    reads[0].seek.assert_called_once_with(offset)


def test_cached_rolling_windows_expire_without_reading_old_history(tmp_path):
    history = JobHistory(tmp_path / 'history.jsonl')
    record(history)
    record(history, job_id='job-2', finished=NOW - timedelta(days=2))
    stats = history.statistics(NOW + timedelta(days=6))
    assert stats['total']['attempts'] == 2
    assert stats['last_7_days']['attempts'] == 1
    assert stats['last_24_hours']['attempts'] == 0
    assert len(json.loads(history.stats_path.read_text())['recent']) == 1


def test_older_history_and_statistics_cache_preserve_job_status_on_rebuild(tmp_path):
    history = JobHistory(tmp_path / 'history.jsonl')
    record(history)
    entry = json.loads(history.path.read_text())
    entry.pop('reported_to_api')
    history.path.write_text(json.dumps(entry) + '\n')
    cache = json.loads(history.stats_path.read_text())
    cache['version'] = 1
    cache['total'].pop('report_failures')
    history.stats_path.write_text(json.dumps(cache))
    stats = history.statistics(NOW)
    assert stats['total']['attempts'] == 1
    assert stats['total']['outcomes']['ok'] == 1
    assert stats['total']['report_failures'] == 0
