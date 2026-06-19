#!/usr/bin/env python3
"""
CRE Daily Brief — Flask 미니 서버 (백그라운드 + 폴링 방식)
=============================================================
역할: 브라우저에서 버튼 클릭으로 수집 스크립트를 실행
      → POST 요청은 즉시 202 응답하고 백그라운드에서 실행
      → 브라우저는 /api/job/status를 폴링해서 진행상황 표시

⭐ 변경 이력 (이전: SSE 방식)
  - SSE는 Render Free 티어 900초 타임아웃에 걸려 502로 끊김
  - 백그라운드 스레드로 실행하면 HTTP 요청 시간과 무관하게 작업 진행됨
  - 폴링은 매 호출이 0.1초 안에 끝나므로 타임아웃 무관

실행:
  [로컬 개발]  python app.py
               → http://localhost:8000

엔드포인트:
  GET  /                          index.html 서빙
  GET  /data/<filename>           data/ 정적 파일 서빙
  POST /api/collect/indicators    백그라운드 시작 (202 즉시 응답)
  POST /api/collect/news          백그라운드 시작 (202 즉시 응답)
  GET  /api/job/status?job=news   작업 진행상황 조회 (폴링용)
  GET  /api/auto-collect/status   레거시 호환 (news job과 동일)
  GET  /api/status                서버 상태 확인
"""

import os
import sys
import json
import shutil
import subprocess
import threading
from collections import deque
from datetime import datetime
from flask import Flask, Response, request, send_from_directory, jsonify, make_response

# ─────────────────────────────────────────────
#  시작 시 __pycache__ 강제 삭제
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
        print(f"  🧹 __pycache__ {count}개 삭제 완료", flush=True)

_clear_pycache()
os.environ['PYTHONDONTWRITEBYTECODE'] = '1'


# ─────────────────────────────────────────────
#  Flask 앱
# ─────────────────────────────────────────────
app = Flask(__name__, static_folder=None)

NO_CACHE_HEADERS = {
    'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
    'Pragma':        'no-cache',
    'Expires':       '0',
}

def no_cache_response(resp):
    for key, val in NO_CACHE_HEADERS.items():
        resp.headers[key] = val
    return resp


# ─────────────────────────────────────────────
#  ⭐ 작업 상태 저장소 (백그라운드 + 폴링용)
# ─────────────────────────────────────────────
#
#  _jobs[job_id] = {
#      'running': bool,    # 실행 중 여부
#      'done':    bool,    # 완료 여부 (성공/실패 무관)
#      'success': bool,    # 성공 여부 (done=True 일 때 의미 있음)
#      'logs':    deque,   # stdout 로그 버퍼
#      'started': str,     # 시작 시각
#      'ended':   str,     # 종료 시각
#      'process': Popen,   # 실행 중인 subprocess (중단용)
#  }
#
def _make_job_state():
    return {
        'running': False,
        'done':    False,
        'success': False,
        'logs':    deque(maxlen=3000),   # collect_news.py가 큰 스크립트 → 여유있게
        'started': None,
        'ended':   None,
        'process': None,
    }

_jobs = {
    'news':       _make_job_state(),
    'indicators': _make_job_state(),
    'weekly':     _make_job_state(),
}
_jobs_lock = threading.Lock()


def _job_log(job_id: str, msg: str):
    """터미널 출력 + 작업 로그 버퍼 동시 저장"""
    print(f"[{job_id}] {msg}", flush=True)
    job = _jobs.get(job_id)
    if job is not None:
        job['logs'].append(msg)


# ─────────────────────────────────────────────
#  ⭐ 백그라운드 작업 실행 함수
# ─────────────────────────────────────────────
def _run_job(job_id: str, cmd: list):
    """
    subprocess로 스크립트를 실행하며 stdout을 _jobs[job_id]['logs']에 누적.
    HTTP 요청과 분리된 별도 스레드에서 실행되므로 Render 타임아웃 무관.
    """
    job = _jobs[job_id]

    # 상태 초기화 (이전 실행 흔적 제거)
    job['logs'].clear()
    job['running'] = True
    job['done']    = False
    job['success'] = False
    job['started'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    job['ended']   = None
    job['process'] = None

    _job_log(job_id, f"🚀 {job_id} 수집 시작 — {' '.join(cmd[1:] if cmd[0] == sys.executable else cmd)}")

    try:
        env = os.environ.copy()
        env['PYTHONIOENCODING']        = 'utf-8'
        env['PYTHONUTF8']              = '1'
        env['PYTHONDONTWRITEBYTECODE'] = '1'
        env['PYTHONUNBUFFERED']        = '1'   # ⭐ 출력 즉시 flush

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=BASE_DIR,
            encoding='utf-8',
            errors='replace',
            env=env,
        )
        job['process'] = proc

        # readline은 line-buffered 모드에서 한 줄씩 즉시 반환
        for line in iter(proc.stdout.readline, ''):
            line = line.rstrip('\n')
            if line:
                _job_log(job_id, line)

        proc.stdout.close()
        proc.wait()

        if proc.returncode == 0:
            _job_log(job_id, f"✅ {job_id} 수집 완료 (exit 0)")
            job['success'] = True
        else:
            _job_log(job_id, f"❌ {job_id} 수집 실패 (exit {proc.returncode})")
            job['success'] = False

    except Exception as e:
        _job_log(job_id, f"❌ 서버 오류: {str(e)}")
        job['success'] = False

    finally:
        job['running'] = False
        job['done']    = True
        job['ended']   = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        job['process'] = None


# ─────────────────────────────────────────────
#  라우트: 정적 파일
# ─────────────────────────────────────────────
@app.route('/')
def index():
    resp = make_response(send_from_directory(BASE_DIR, 'index.html'))
    return no_cache_response(resp)


@app.route('/<path:filename>')
def serve_static(filename):
    if filename.startswith('data/'):
        return serve_data(filename[5:])

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
    data_dir = os.path.join(BASE_DIR, 'data')
    resp = make_response(send_from_directory(data_dir, filename))
    return no_cache_response(resp)


# ─────────────────────────────────────────────
#  라우트: 서버 상태
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
        'server':          'CRE Daily Brief Flask (background+polling)',
        'indicators_json': {'exists': indicators_ok, 'updated': mtime('indicators.json')},
        'news_json':        {'exists': news_ok,       'updated': mtime('news.json')},
        'jobs': {
            jid: {
                'running': j['running'],
                'done':    j['done'],
                'success': j['success'],
                'started': j['started'],
                'ended':   j['ended'],
                'log_count': len(j['logs']),
            }
            for jid, j in _jobs.items()
        },
    })


# ─────────────────────────────────────────────
#  ⭐ 라우트: 작업 상태 조회 (폴링용)
# ─────────────────────────────────────────────
@app.route('/api/job/status')
def job_status():
    """
    브라우저가 폴링해서 작업 진행상황을 가져가는 엔드포인트.

    Query params:
      job=news|indicators  (default: news)
      since=N              이미 본 로그 개수 (그 이후 줄만 받음)
    """
    job_id = request.args.get('job', 'news')
    since  = request.args.get('since', 0, type=int)

    job = _jobs.get(job_id)
    if job is None:
        return jsonify({'error': f'unknown job: {job_id}'}), 404

    logs = list(job['logs'])
    new_logs = logs[since:] if since < len(logs) else []

    resp = jsonify({
        'job':     job_id,
        'running': job['running'],
        'done':    job['done'],
        'success': job['success'],
        'started': job['started'],
        'ended':   job['ended'],
        'logs':    new_logs,
        'total':   len(logs),
    })
    return no_cache_response(resp)


# ─────────────────────────────────────────────
#  레거시 호환: /api/auto-collect/status
#  (기존 startAutoCollectPolling()이 이 경로를 사용)
# ─────────────────────────────────────────────
@app.route('/api/auto-collect/status')
def auto_collect_status():
    """뉴스 작업 상태로 매핑 (호환용)"""
    since = request.args.get('since', 0, type=int)
    job   = _jobs['news']
    logs  = list(job['logs'])
    new_logs = logs[since:] if since < len(logs) else []
    resp = jsonify({
        'running': job['running'],
        'done':    job['done'],
        'success': job['success'],
        'logs':    new_logs,
        'total':   len(logs),
    })
    return no_cache_response(resp)


# ─────────────────────────────────────────────
#  ⭐ 라우트: 경제지표 수집 (백그라운드)
# ─────────────────────────────────────────────
@app.route('/api/collect/indicators', methods=['POST'])
def collect_indicators():
    script = os.path.join(BASE_DIR, 'collect.py')
    if not os.path.exists(script):
        return jsonify({'error': 'collect.py를 찾을 수 없습니다'}), 404

    with _jobs_lock:
        if _jobs['indicators']['running']:
            return jsonify({
                'error':   '이미 실행 중',
                'job':     'indicators',
                'running': True,
            }), 409

    cmd = [sys.executable, '-u', script]
    threading.Thread(
        target=_run_job, args=('indicators', cmd), daemon=True
    ).start()

    return jsonify({
        'started': True,
        'job':     'indicators',
        'message': '백그라운드 수집 시작됨 — /api/job/status?job=indicators 폴링하세요',
    }), 202


# ─────────────────────────────────────────────
#  ⭐ 라우트: 뉴스 수집 (백그라운드)
# ─────────────────────────────────────────────
@app.route('/api/collect/news', methods=['POST'])
def collect_news():
    script = os.path.join(BASE_DIR, 'collect_news.py')
    if not os.path.exists(script):
        return jsonify({'error': 'collect_news.py를 찾을 수 없습니다'}), 404

    with _jobs_lock:
        if _jobs['news']['running']:
            return jsonify({
                'error':   '이미 실행 중',
                'job':     'news',
                'running': True,
            }), 409

    body      = request.get_json(silent=True) or {}
    date_from = body.get('date_from', '')
    date_to   = body.get('date_to', '')
    days      = body.get('days', 3)
    category  = body.get('category', '')

    # ⭐ 런타임 오버라이드 기록 — 프런트(localStorage)에서 보낸
    #    키워드 검색어 교체 + 매체 공신력 조정을 파일로 남겨 collect_news.py에 전달.
    #    (데이터가 커서 CLI 인자로 못 넘기므로 파일 경유)
    overrides = {
        'keyword_override': bool(body.get('keyword_override', False)),
        'keywords':         body.get('keywords') or {},
        'source_authority': body.get('source_authority') or {},
        'source_domains':   body.get('source_domains') or {},
        'prompt_global':     body.get('prompt_global') or '',
        'prompt_categories': body.get('prompt_categories') or {},
        'keyword_combo':     bool(body.get('keyword_combo', False)),
        'keyword_groups':    body.get('keyword_groups') or {},
        'authority_min':     int(body.get('authority_min') or 0),
    }
    ov_path = os.path.join(BASE_DIR, 'data', 'run_overrides.json')
    try:
        os.makedirs(os.path.join(BASE_DIR, 'data'), exist_ok=True)
        with open(ov_path, 'w', encoding='utf-8') as f:
            json.dump(overrides, f, ensure_ascii=False)
        kw_on  = overrides['keyword_override']
        sa_cnt = len(overrides['source_authority'])
        sd_cnt = len(overrides['source_domains'])
        print(f"[news] 🔧 오버라이드 기록: 키워드교체={kw_on}, 공신력조정={sa_cnt}개, 도메인등록={sd_cnt}개", flush=True)
    except Exception as e:
        print(f"[news] ⚠️ 오버라이드 기록 실패: {e}", flush=True)

    cmd = [sys.executable, '-u', script]
    if date_from and date_to:
        cmd += ['--from-date', date_from, '--to-date', date_to]
    else:
        cmd += ['--days', str(days)]
    if category:
        cmd += ['--category', category]
    cmd += ['--overrides', ov_path]

    threading.Thread(
        target=_run_job, args=('news', cmd), daemon=True
    ).start()

    return jsonify({
        'started': True,
        'job':     'news',
        'message': '백그라운드 수집 시작됨 — /api/job/status?job=news 폴링하세요',
    }), 202


# ─────────────────────────────────────────────
#  ⭐ 라우트: 하이브리드 위클리 (수집전용 풀 / 주간 큐레이션 / 풀 현황)
# ─────────────────────────────────────────────
def _write_run_overrides(body: dict) -> str:
    """프런트 오버라이드(키워드·공신력·도메인·프롬프트·활성카테고리)를 파일로 기록."""
    overrides = {
        'keyword_override':   bool(body.get('keyword_override', False)),
        'keywords':           body.get('keywords') or {},
        'source_authority':   body.get('source_authority') or {},
        'source_domains':     body.get('source_domains') or {},
        'prompt_global':      body.get('prompt_global') or '',
        'prompt_categories':  body.get('prompt_categories') or {},
        'enabled_categories': body.get('enabled_categories') or [],
        'keyword_combo':      bool(body.get('keyword_combo', False)),
        'keyword_groups':     body.get('keyword_groups') or {},
        'authority_min':      int(body.get('authority_min') or 0),
    }
    ov_path = os.path.join(BASE_DIR, 'data', 'run_overrides.json')
    os.makedirs(os.path.join(BASE_DIR, 'data'), exist_ok=True)
    with open(ov_path, 'w', encoding='utf-8') as f:
        json.dump(overrides, f, ensure_ascii=False)
    return ov_path


@app.route('/api/weekly/collect', methods=['POST'])
def weekly_collect():
    """매일 호출용: 수집 전용 → 주간 풀 누적 (LLM 미사용 → 사실상 무료)."""
    script = os.path.join(BASE_DIR, 'collect_news.py')
    if not os.path.exists(script):
        return jsonify({'error': 'collect_news.py 없음'}), 404
    with _jobs_lock:
        if _jobs['weekly']['running']:
            return jsonify({'error': '이미 실행 중', 'job': 'weekly', 'running': True}), 409
    body = request.get_json(silent=True) or {}
    try:
        ov_path = _write_run_overrides(body)
    except Exception as e:
        ov_path = None
        print(f"[weekly] ⚠️ 오버라이드 기록 실패: {e}", flush=True)
    cmd = [sys.executable, '-u', script, '--mode', 'collect']
    if body.get('date_from') and body.get('date_to'):
        cmd += ['--from-date', body['date_from'], '--to-date', body['date_to']]
    else:
        cmd += ['--days', str(body.get('days', 1))]
    if ov_path:
        cmd += ['--overrides', ov_path]
    threading.Thread(target=_run_job, args=('weekly', cmd), daemon=True).start()
    return jsonify({'started': True, 'job': 'weekly',
                    'message': '일일 수집(풀 누적) 시작 — /api/job/status?job=weekly'}), 202


@app.route('/api/weekly/curate', methods=['POST'])
def weekly_curate():
    """주간 큐레이션 + 하이라이트 (풀 기간 추출 → 2단 선별 → AI 요약 → 하이라이트)."""
    script = os.path.join(BASE_DIR, 'collect_news.py')
    if not os.path.exists(script):
        return jsonify({'error': 'collect_news.py 없음'}), 404
    with _jobs_lock:
        if _jobs['weekly']['running']:
            return jsonify({'error': '이미 실행 중', 'job': 'weekly', 'running': True}), 409
    body = request.get_json(silent=True) or {}
    try:
        ov_path = _write_run_overrides(body)
    except Exception as e:
        ov_path = None
        print(f"[weekly] ⚠️ 오버라이드 기록 실패: {e}", flush=True)
    cmd = [sys.executable, '-u', script, '--mode', 'weekly', '--top', str(int(body.get('top', 5)))]
    if body.get('date_from') and body.get('date_to'):
        cmd += ['--from-date', body['date_from'], '--to-date', body['date_to']]
    else:
        cmd += ['--days', str(body.get('days', 7))]
    if ov_path:
        cmd += ['--overrides', ov_path]
    threading.Thread(target=_run_job, args=('weekly', cmd), daemon=True).start()
    return jsonify({'started': True, 'job': 'weekly',
                    'message': '주간 큐레이션 시작 — /api/job/status?job=weekly'}), 202


@app.route('/api/weekly/pool')
def weekly_pool():
    """주간 풀 현황 (총 건수·일자별·카테고리별)."""
    pool_path = os.path.join(BASE_DIR, 'data', 'weekly_pool.json')
    try:
        with open(pool_path, encoding='utf-8') as f:
            pool = json.load(f)
    except Exception:
        return jsonify({'total': 0, 'updated_at': None, 'by_date': {}, 'by_category': {}})
    items = pool.get('items', [])
    by_date, by_cat = {}, {}
    for it in items:
        d = it.get('collected_at') or (it.get('pub_date', '')[:10]) or '미상'
        by_date[d] = by_date.get(d, 0) + 1
        c = it.get('category_name') or it.get('category') or '미상'
        by_cat[c] = by_cat.get(c, 0) + 1
    return jsonify({'total': len(items), 'updated_at': pool.get('updated_at'),
                    'by_date': dict(sorted(by_date.items())), 'by_category': by_cat})


# ─────────────────────────────────────────────
#  ⭐ 라우트: 작업 중단
# ─────────────────────────────────────────────
@app.route('/api/job/stop', methods=['POST'])
def job_stop():
    """현재 실행 중인 작업을 중단"""
    body   = request.get_json(silent=True) or {}
    job_id = body.get('job', 'news')

    job = _jobs.get(job_id)
    if job is None:
        return jsonify({'error': f'unknown job: {job_id}'}), 404

    proc = job.get('process')
    if not job['running'] or proc is None:
        return jsonify({'stopped': False, 'message': '실행 중인 작업이 없습니다'}), 200

    try:
        proc.terminate()
        _job_log(job_id, "⏹ 사용자가 작업을 중단했습니다")
        return jsonify({'stopped': True, 'job': job_id}), 200
    except Exception as e:
        return jsonify({'stopped': False, 'error': str(e)}), 500


# ─────────────────────────────────────────────
#  레거시: 뉴스 로그 파일 뷰어
# ─────────────────────────────────────────────
@app.route('/api/logs/news')
def view_news_log():
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
#  진입점
# ─────────────────────────────────────────────
if __name__ == '__main__':
    print("=" * 55)
    print("  🏢 CRE Daily Brief — Flask 미니 서버 (백그라운드+폴링)")
    print("=" * 55)
    print(f"  📂 작업 디렉토리: {BASE_DIR}")
    print(f"  🌐 접속 주소:     http://localhost:8000")
    print(f"  💡 수집 방식:     POST → 즉시 202 → /api/job/status 폴링")
    print("  Ctrl+C 로 종료")
    print("=" * 55)

    port = int(os.environ.get('PORT', 8000))
    app.run(
        host='0.0.0.0',
        port=port,
        debug=False,
        threaded=True,
    )
