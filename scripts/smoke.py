import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request
import uuid
import socket

ROOT = Path(__file__).resolve().parents[1]
JAVA = os.environ.get('PIPELINE_JAVA') or (str(Path(os.environ['JAVA_HOME']) / 'bin' / ('java.exe' if os.name == 'nt' else 'java')) if os.environ.get('JAVA_HOME') else 'java')
DATA = ROOT / 'target' / ('smoke-' + uuid.uuid4().hex)
BASE = 'http://127.0.0.1:18083'
DATA.mkdir(parents=True)


def request(route, payload=None):
    body = None if payload is None else json.dumps(payload).encode()
    command = urllib.request.Request(BASE + route, data=body, headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(command, timeout=4) as response:
        return json.load(response)


def wait_for(predicate, seconds=45):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result:
                return result
        except (OSError, ValueError):
            pass
        time.sleep(0.15)
    raise AssertionError('Timed out waiting for expected condition')


def start():
    log = open(DATA / ('server-' + uuid.uuid4().hex + '.log'), 'wb')
    process = subprocess.Popen([JAVA, '-jar', str(ROOT / 'target/recoverable-pipeline-0.1.0.jar'),
                                '--server.port=18083', '--pipeline.data=' + str(DATA)],
                               cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                               creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    log.close()
    try:
        wait_for(lambda: process.poll() is None and request('/api/tasks') is not None)
    except BaseException:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
        raise
    return process


process = None
report = []
try:
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 18083))
    process = start()
    with urllib.request.urlopen(BASE, timeout=4) as response:
        assert 'Mock' in response.read().decode('utf-8')
    report.append('PASS: homepage and HTTP interface')
    task = request('/api/tasks', {'asset': uuid.uuid4().hex, 'durationMs': 50, 'failStep': 3, 'failTimes': 1})['taskId']
    result = wait_for(lambda: (detail if (detail := request('/api/tasks/' + task))['task']['STATUS'] == 'SUCCEEDED' else None))
    assert [step['ATTEMPTS'] for step in result['steps']] == [1, 1, 2, 1]
    assert request('/api/tasks/' + task + '/artifacts/4')['mock'] is True
    report.append('PASS: transient failure, stage retry, upstream reuse')
    task = request('/api/tasks', {'asset': uuid.uuid4().hex, 'durationMs': 900})['taskId']
    before = wait_for(lambda: (detail if (detail := request('/api/tasks/' + task))['steps'][2]['STATUS'] == 'RUNNING' else None))
    (DATA / 'before-kill.json').write_text(json.dumps(before, indent=2), encoding='utf-8')
    process.kill()
    process.wait(timeout=10)
    process = start()
    result = wait_for(lambda: (detail if (detail := request('/api/tasks/' + task))['task']['STATUS'] == 'SUCCEEDED' else None))
    (DATA / 'after-restart.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    assert [step['ATTEMPTS'] for step in result['steps']] == [1, 1, 2, 1], result
    assert any(attempt['STATUS'] == 'ABANDONED' for attempt in result['attempts'])
    report.append('PASS: real process kill/restart, completed stages preserved')
    Path(result['steps'][1]['ARTIFACT']).write_text('corrupted', encoding='utf-8')
    request('/api/tasks/' + task + '/retry', {})
    result = wait_for(lambda: (detail if (detail := request('/api/tasks/' + task))['task']['STATUS'] == 'SUCCEEDED' else None))
    assert result['steps'][1]['ATTEMPTS'] == 2
    report.append('PASS: corrupted artifact detected and regenerated')
finally:
    if process and process.poll() is None:
        process.kill()
        process.wait(timeout=10)
    (DATA / 'smoke-report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
print('\n'.join(report))
print('Evidence:', DATA)
