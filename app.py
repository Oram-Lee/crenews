#!/usr/bin/env python3
"""
CRE Daily Brief — Flask 미니 서버
===================================
역할: 브라우저에서 버튼 클릭으로 수집 스크립트를 실행하고,
      실시간 로그를 SSE(Server-Sent Events)로 스트리밍

실행:
  python app.py
  → http://localhost:8000

엔드포인트:
  GET  /                          index.html 서빙
  GET  /data/<filename>           data/ 정적 파일 서빙
  POST /api/collect/indicators    collect.py 실행 + 스트림
  POST /api/collect/news          collect_news.py 실행 + 스트림
  GET  /api/status                서버 상태 확인
"""

import os
import sys
import json
import shutil
import subprocess
import threading
from datetime import datetime
from flask import Flask, Response, request, send_from_directory, jsonify, stream_with_context, make_response

# ─────────────────────────────────────────────
#  시작 시 __pycache__ 강제 삭제
#  → 구버전 .pyc 바이트코드가 신버전 .py를 덮는 현상 방지
# ─────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def _clear_pycache():
    count = 0
    for root, dirs, files in os.walk(BASE_DIR):
        for d in dirs:
            if d == '__pycache__':
                target = os.path.join(root, d)
                try:
                    shutil.rmtree(target)
                    count += 1
                except Exception:
                    pass
    if count:
        print(f"  🧹 __pycache__ {count}개 삭제 완료")

_clear_pycache()


# ─────────────────────────────────────────────
#  Flask 앱 — 내장 정적 파일 서빙 비활성화
#  (static_folder=None → Flask가 /static 경로를 자동 캐시하지 않음)
# ─────────────────────────────────────────────
app = Flask(__name__, static_folder=None)

# 캐시 금지 헤더 상수
NO_CACHE_HEADERS = {
    'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
    'Pragma':        'no-cache',
    'Expires':       '0',
}

def no_cache_response(resp):
    """응답 객체에 캐시 금지 헤더를 일괄 적용"""
    for key, val in NO_CACHE_HEADERS.items():
        resp.headers[key] = val
    return resp


# ─────────────────────────────────────────────
#  현재 실행 중인 프로세스 관리 (중복 실행 방지)
# ─────────────────────────────────────────────
running_jobs = {}
lock = threading.Lock()

# ─────────────────────────────────────────────
#  자동 수집 로그 버퍼 (브라우저 폴링용)
# ─────────────────────────────────────────────
from collections import deque
_auto_log_buffer = deque(maxlen=500)   # 최근 500줄 유지
_auto_collect_status = {
    'running': False,
    'done': False,
    'success': False,
}

def _log(msg: str):
    """터미널 출력 + 버퍼 동시 저장"""
    print(msg)
    _auto_log_buffer.append(msg)


# ─────────────────────────────────────────────
#  라우트: 정적 파일 (모두 캐시 금지)
# ─────────────────────────────────────────────

@app.route('/')
def index():
    resp = make_response(send_from_directory(BASE_DIR, 'index.html'))
    return no_cache_response(resp)


@app.route('/<path:filename>')
def serve_static(filename):
    """
    index.html 외 모든 정적 파일 서빙 (JS, CSS 포함)
    캐시 금지 헤더 강제 적용 — 파일 교체 즉시 반영됨
    """
    # data/ 경로는 아래 별도 라우트로 처리
    if filename.startswith('data/'):
        return serve_data(filename[5:])

    # 보안: 상위 디렉토리 탈출 방지
    safe_path = os.path.normpath(os.path.join(BASE_DIR, filename))
    if not safe_path.startswith(BASE_DIR):
        return jsonify({'error': 'forbidden'}), 403

    if not os.path.isfile(safe_path):
        return jsonify({'error': f'{filename} not found'}), 404

    file_dir  = os.path.dirname(safe_path)
    file_name = os.path.basename(safe_path)
    resp = make_response(send_from_directory(file_dir, file_name))
    return no_cache_response(resp)


@app.route('/data/<path:filename>')
def serve_data(filename):
    """data/ 폴더 JSON 파일 서빙 — 뉴스·지표 JSON도 캐시 금지"""
    data_dir = os.path.join(BASE_DIR, 'data')
    resp = make_response(send_from_directory(data_dir, filename))
    return no_cache_response(resp)


# ─────────────────────────────────────────────
#  라우트: 상태 확인
# ─────────────────────────────────────────────

@app.route('/api/status')
def status():
    data_dir = os.path.join(BASE_DIR, 'data')
    indicators_ok = os.path.exists(os.path.join(data_dir, 'indicators.json'))
    news_ok       = os.path.exists(os.path.join(data_dir, 'news.json'))

    def mtime(name):
        path = os.path.join(data_dir, name)
        if os.path.exists(path):
            return datetime.fromtimestamp(os.path.getmtime(path)).strftime('%Y-%m-%d %H:%M:%S')
        return None

    return jsonify({
        'server':          'CRE Daily Brief Flask',
        'indicators_json': {'exists': indicators_ok, 'updated': mtime('indicators.json')},
        'news_json':        {'exists': news_ok,       'updated': mtime('news.json')},
        'running_jobs':     list(running_jobs.keys()),
    })


@app.route('/api/auto-collect/status')
def auto_collect_status():
    """브라우저가 폴링해서 자동수집 진행상황을 가져가는 엔드포인트"""
    since = request.args.get('since', 0, type=int)
    logs  = list(_auto_log_buffer)
    new_logs = logs[since:]
    resp = jsonify({
        'running': _auto_collect_status['running'],
        'done':    _auto_collect_status['done'],
        'success': _auto_collect_status['success'],
        'logs':    new_logs,
        'total':   len(logs),
    })
    return no_cache_response(resp)


# ─────────────────────────────────────────────
#  헬퍼: SSE 스트리밍 실행기
# ─────────────────────────────────────────────

def run_script_stream(job_id: str, cmd: list):
    """
    subprocess로 스크립트 실행 → 한 줄씩 SSE data: 이벤트로 전송
    완료/오류 시 done / error 이벤트 발송
    """
    with lock:
        if job_id in running_jobs:
            yield "data: ⚠️ 이미 실행 중입니다\n\n"
            yield "event: error\ndata: already_running\n\n"
            return
        running_jobs[job_id] = True

    try:
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        env['PYTHONUTF8']       = '1'
        env['PYTHONDONTWRITEBYTECODE'] = '1'   # ← 이 프로세스에서 .pyc 생성 방지

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=BASE_DIR,
            encoding='utf-8',
            errors='replace',
            env=env
        )

        for line in proc.stdout:
            line = line.rstrip('\n')
            if line:
                yield f"data: {line}\n\n"

        proc.wait()
        if proc.returncode == 0:
            yield "event: done\ndata: SUCCESS\n\n"
        else:
            yield f"event: error\ndata: EXIT_CODE_{proc.returncode}\n\n"

    except Exception as e:
        yield f"data: ❌ 서버 오류: {str(e)}\n\n"
        yield "event: error\ndata: EXCEPTION\n\n"
    finally:
        with lock:
            running_jobs.pop(job_id, None)


@app.route('/api/logs/news')
def view_news_log():
    """data/collect_news.log 내용을 브라우저에서 바로 확인"""
    log_path = os.path.join(BASE_DIR, 'data', 'collect_news.log')
    if not os.path.exists(log_path):
        return Response(
            "로그 파일 없음 — 아직 수집이 실행되지 않았거나 이전 버전입니다.",
            mimetype='text/plain; charset=utf-8',
            headers=NO_CACHE_HEADERS
        )
    with open(log_path, encoding='utf-8', errors='replace') as f:
        content = f.read()
    resp = Response(content, mimetype='text/plain; charset=utf-8')
    for k, v in NO_CACHE_HEADERS.items():
        resp.headers[k] = v
    return resp


# ─────────────────────────────────────────────
#  라우트: 경제지표 수집
# ─────────────────────────────────────────────

@app.route('/api/collect/indicators', methods=['POST'])
def collect_indicators():
    script = os.path.join(BASE_DIR, 'collect.py')
    if not os.path.exists(script):
        return jsonify({'error': 'collect.py를 찾을 수 없습니다'}), 404

    return Response(
        stream_with_context(run_script_stream('indicators', [sys.executable, script])),
        mimetype='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'},
    )


# ─────────────────────────────────────────────
#  라우트: 뉴스 수집
# ─────────────────────────────────────────────

@app.route('/api/collect/news', methods=['POST'])
def collect_news():
    script = os.path.join(BASE_DIR, 'collect_news.py')
    if not os.path.exists(script):
        return jsonify({'error': 'collect_news.py를 찾을 수 없습니다'}), 404

    body      = request.get_json(silent=True) or {}
    date_from = body.get('date_from', '')
    date_to   = body.get('date_to',   '')
    days      = body.get('days', 3)

    cmd = [sys.executable, script]
    if date_from and date_to:
        cmd += ['--from-date', date_from, '--to-date', date_to]
    else:
        cmd += ['--days', str(days)]

    return Response(
        stream_with_context(run_script_stream('news', cmd)),
        mimetype='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'},
    )


# ─────────────────────────────────────────────
#  진입점
# ─────────────────────────────────────────────

def _auto_collect_news():
    """서버 시작 시 collect_news.py 자동 실행 (백그라운드 스레드)"""
    script = os.path.join(BASE_DIR, 'collect_news.py')
    if not os.path.exists(script):
        _log("  ⚠️ collect_news.py 없음 — 자동 수집 건너뜀")
        return

    _auto_collect_status['running'] = True
    _auto_collect_status['done']    = False
    _auto_collect_status['success'] = False
    _auto_log_buffer.clear()
    _log("  🔄 뉴스 자동 수집 시작...")

    try:
        env = os.environ.copy()
        env['PYTHONIOENCODING']        = 'utf-8'
        env['PYTHONUTF8']              = '1'
        env['PYTHONDONTWRITEBYTECODE'] = '1'
        proc = subprocess.Popen(
            [sys.executable, script],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=BASE_DIR,
            encoding='utf-8',
            errors='replace',
            env=env,
        )
        for line in proc.stdout:
            _log(line.rstrip('\n'))
        proc.wait()
        if proc.returncode == 0:
            _log("  ✅ 뉴스 자동 수집 완료")
            _auto_collect_status['success'] = True
        else:
            _log(f"  ❌ 뉴스 수집 종료 코드: {proc.returncode}")
            _auto_collect_status['success'] = False
    except Exception as e:
        _log(f"  ❌ 뉴스 자동 수집 오류: {e}")
        _auto_collect_status['success'] = False
    finally:
        _auto_collect_status['running'] = False
        _auto_collect_status['done']    = True


if __name__ == '__main__':
    print("=" * 55)
    print("  🏢 CRE Daily Brief — Flask 미니 서버")
    print("=" * 55)
    print(f"  📂 작업 디렉토리: {BASE_DIR}")
    print(f"  🌐 접속 주소:     http://localhost:8000")
    print("  Ctrl+C 로 종료")
    print("=" * 55)

    # PYTHONDONTWRITEBYTECODE=1 — 이 프로세스도 .pyc 생성 안 함
    os.environ['PYTHONDONTWRITEBYTECODE'] = '1'

    # ⛔ 자동 수집 비활성화 — 수집은 브라우저 버튼으로 수동 실행
    # t = threading.Thread(target=_auto_collect_news, daemon=True)
    # t.start()

    port = int(os.environ.get('PORT', 8000))
    app.run(
        host='0.0.0.0',
        port=port,
        debug=False,    # True 시 subprocess 스트림 타이밍 이슈
        threaded=True,  # SSE 동시 연결 허용
    )
