#!/usr/bin/env python3
"""
CRE Daily Brief — 뉴스 수집 + AI 큐레이션 v2.0
=================================================
소스: Naver News API + Google News RSS (API 키 불필요)
AI:   Claude API (short_summary 15자 + summary 60자)
출력: data/news.json
=================================================
사용법:
  python collect_news.py
  python collect_news.py --days 5
  python collect_news.py --from-date 2026-03-10 --to-date 2026-03-12
"""

import os
import sys
import json
import re
import hashlib
import time
import argparse
import requests
import warnings
import urllib3
import xml.etree.ElementTree as ET
import html as html_lib
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
from difflib import SequenceMatcher
from dataclasses import dataclass
from urllib.parse import quote_plus

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')

# Windows cp949 환경 UTF-8 강제
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ('utf-8', 'utf8'):
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)
if sys.stderr.encoding and sys.stderr.encoding.lower() not in ('utf-8', 'utf8'):
    sys.stderr = open(sys.stderr.fileno(), mode='w', encoding='utf-8', buffering=1)

KST = timezone(timedelta(hours=9))
NOW = datetime.now(KST)


# ================================================================
#  로그 파일 — data/collect_news.log 동시 저장
#  print()를 전역 후킹하여 기존 코드 변경 없이 모든 출력을 캡처
# ================================================================
import builtins as _builtins

_LOG_FILE_HANDLE = None  # main()에서 초기화
_ORIGINAL_PRINT  = _builtins.print  # 원본 print 보관

def _tee_print(*args, **kwargs):
    """터미널 출력 + 로그 파일 동시 저장"""
    _ORIGINAL_PRINT(*args, **kwargs)
    if _LOG_FILE_HANDLE:
        text = kwargs.get('sep', ' ').join(str(a) for a in args)
        end  = kwargs.get('end', '\n')
        try:
            _LOG_FILE_HANDLE.write(text + end)
            _LOG_FILE_HANDLE.flush()
        except Exception:
            pass

_builtins.print = _tee_print  # 전역 교체


def _init_log_file():
    """로그 파일 초기화 — main() 시작 시 호출"""
    global _LOG_FILE_HANDLE
    os.makedirs('data', exist_ok=True)
    log_path = os.path.join('data', 'collect_news.log')
    _LOG_FILE_HANDLE = open(log_path, 'w', encoding='utf-8', buffering=1)
    _tee_print(f"{'='*60}")
    _tee_print(f"📋 로그 시작: {NOW.strftime('%Y-%m-%d %H:%M:%S KST')}")
    _tee_print(f"{'='*60}")


# ================================================================
#  설정
# ================================================================

@dataclass
class NewsConfig:
    NAVER_CLIENT_ID:     str = ""
    NAVER_CLIENT_SECRET: str = ""
    CLAUDE_API_KEY:      str = ""
    GEMINI_API_KEY:      str = ""

    NEWS_AGE_DAYS:         int   = 3
    MAX_NEWS_PER_CATEGORY: int   = 20      # 카테고리당 상한 (실질적 제한은 AI 선별)
    SIMILARITY_THRESHOLD:  float = 0.55  # 낮출수록 유사 기사 더 공격적으로 제거

    # Claude 모델 — 큐레이션(필터링)용: Haiku(빠름·저렴)
    CLAUDE_MODEL: str = "claude-haiku-4-5-20251001"
    # Claude 요약 전용 모델 — Sonnet(고품질 개조식 생성)
    CLAUDE_SUMMARY_MODEL: str = "claude-sonnet-4-6"
    # Gemini 모델 우선순위 (자동 폴백)
    GEMINI_MODEL: str = "gemini-2.0-flash"

    def __post_init__(self):
        self.NAVER_CLIENT_ID     = os.environ.get('NAVER_CLIENT_ID',     self.NAVER_CLIENT_ID)
        self.NAVER_CLIENT_SECRET = os.environ.get('NAVER_CLIENT_SECRET', self.NAVER_CLIENT_SECRET)
        self.CLAUDE_API_KEY      = os.environ.get('CLAUDE_API_KEY',      self.CLAUDE_API_KEY)
        self.GEMINI_API_KEY      = os.environ.get('GEMINI_API_KEY',      self.GEMINI_API_KEY)

        cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
        if os.path.exists(cfg_path):
            try:
                with open(cfg_path, 'r', encoding='utf-8') as f:
                    cfg = json.load(f)
                if not self.NAVER_CLIENT_ID:
                    self.NAVER_CLIENT_ID     = cfg.get("NAVER_CLIENT_ID", "")
                if not self.NAVER_CLIENT_SECRET:
                    self.NAVER_CLIENT_SECRET = cfg.get("NAVER_CLIENT_SECRET", "")
                if not self.CLAUDE_API_KEY:
                    self.CLAUDE_API_KEY      = cfg.get("CLAUDE_API_KEY", "")
                if not self.GEMINI_API_KEY:
                    self.GEMINI_API_KEY      = cfg.get("GEMINI_API_KEY", "")
            except Exception as e:
                print(f"  ⚠️ config.json 로드 실패: {e}")


# ================================================================
#  카테고리 정의
# ================================================================

# ── 공통 노이즈 차단 패턴 ────────────────────────────────────────
# 어느 카테고리든 이 패턴이 포함된 기사는 1차 수집 단계에서 제외
COMMON_NOISE_TITLE_PATTERNS = [
    '[특징주]', '[오늘의 주요일정]', '[오늘 재경부', '[N2 모닝',
    '오늘의 채권', '채권ㆍ외환', '채권·외환',
    '[클릭 e종목]', '[주총]',
    '모닝 경제 브리핑', '주요일정', '경제 브리핑',
    '상한가', '하한가',  # 주식 시황 기사
]

# ── 공통 must_not_keywords (모든 카테고리에 추가) ────────────────
BASE_MUST_NOT = [
    # 주거용 부동산 (CRE 아님)
    '주택', '아파트', '분양', '청약', '재건축', '재개발', '빌라', '오피스텔',
    # 가상자산
    '비트코인', '암호화폐', '블록체인', 'NFT', '코인',
    # 정치
    '정치', '국회', '선거', '의원', '대통령',
    # 일정표·시황 브리핑 (내용 없는 형식 기사)
    '주요일정', '채권·외환', '채권ㆍ외환', '모닝 브리핑', '경제 브리핑',
    '[특징주]', '[클릭 e종목]', '[오늘의',
    # 순수 인사 기사 (부동산 거래·시장 내용 없는 것)
    '대표 선임', '대표이사 선임', '신임 대표', '사장 선임',
    # 해외 부동산 (국내 CRE 시장 중심)
    '일본 부동산', '중국 부동산', '미국 부동산', '일본 물류',
    '일본 오피스', '중국 오피스', '해외 부동산 투자',
]

REAL_ESTATE_MARKET_CATEGORY = {
    "id": "real_estate_market", "name": "부동산 시장 & 정책",
    "ai_definition": "국내 상업용 부동산(오피스·리츠·상업시설) 시장 동향, 투자 수익률, 캡레이트, 부동산PF, 금리/환율의 CRE 영향, 관련 정책·규제가 핵심 주제인 기사. 해외 부동산·주택·주식 시황이 주인 기사는 제외.",
    "icon": "📊", "label": "MARKET",
    "search_queries": [
        '기준금리 상업용부동산', '캡레이트 오피스', '부동산PF 오피스',
        '리츠 수익률 오피스', '공모리츠 오피스', '외국인 빌딩 매입',
        '상업용부동산 투자수익률', '상업용부동산 시장 전망',
        '부동산 규제 상업용', '취득세 법인 부동산',
        '서울 오피스 투자', '오피스 캡레이트 서울',
    ],
    "rss_queries": [
        '상업용부동산 캡레이트', '부동산PF 리스크 오피스', '리츠 수익률 오피스',
        '외국인 빌딩 매입', '상업용부동산 시장 전망', '서울 오피스 투자',
    ],
    "must_have_keywords": [
        '상업용부동산', '캡레이트', '오피스 투자수익률', '리츠', 'REITs',
        '부동산PF 오피스', '부동산PF 상업', '외국인 빌딩', '글로벌 자본',
        '오피스 투자', '상업용 부동산 규제', '법인 부동산 취득세',
        '부동산 전망 오피스', '오피스 캡레이트', '빌딩 투자 수익',
    ],
    "must_not_keywords": BASE_MUST_NOT,  # 세부 판단은 AI에 위임
    "min_relevance_score": 0,  # AI가 관련성 판단
}

OFFICE_LEASE_CATEGORY = {
    "id": "office_lease", "name": "오피스 임대차",
    "ai_definition": "서울 권역(CBD/GBD/YBD/BBD) 오피스 빌딩의 공실률·임대료·렌트프리·사옥 이전·신규 공급이 핵심 주제인 기사. 리츠 배당/주가가 주제거나 오피스 임대를 단순 언급만 하는 기사는 제외.",
    "icon": "🏢", "label": "OFFICE",
    "search_queries": [
        '프라임 오피스 임대', 'A급 오피스 공실률',
        '오피스 공실률 서울', '오피스 임대료 상승',
        '강남 오피스 임대료', '여의도 오피스 임대료',
        '렌트프리 오피스', '사옥 이전 서울',
        '본사 이전 서울 오피스', '순흡수면적 오피스',
        '마곡 오피스 임대', '판교 오피스 임대', '용산 오피스 임대',
        'CBD 오피스 공실률', 'GBD 오피스 임대료',
    ],
    "rss_queries": [
        '프라임 오피스 임대', 'A급 오피스 공실률', '서울 오피스 공실률',
        '강남 오피스 임대료', '여의도 오피스 임대료', '렌트프리 오피스',
        '기업 사옥 이전 서울 오피스', '오피스 신규 공급 서울',
    ],
    "must_have_keywords": [
        'A급 오피스', '프라임 오피스', '공실률', '오피스 임대료', '렌트프리',
        'CBD 오피스', 'GBD 오피스', 'YBD 오피스',
        '오피스 임대차', '오피스 사옥 이전', '본사 이전 서울',
        '오피스 신규 공급', '순흡수면적', 'NOC', '실질 임대료',
        '오피스 공실', '임대차 계약 오피스',
    ],
    "must_not_keywords": BASE_MUST_NOT,  # 세부 판단은 AI에 위임
    "min_relevance_score": 0,
}

ASSET_TRANSACTION_CATEGORY = {
    "id": "asset_transaction", "name": "자산 매입·매각",
    "ai_definition": "국내 상업용 부동산(오피스·빌딩) 매입·매각·딜 클로징·세일즈앤리스백 등 실제 자산 거래가 핵심 주제인 기사. 금융·주식·채권 기사에서 부동산펀드가 곁가지로 언급된 기사는 제외.",
    "icon": "💼", "label": "DEAL",
    "search_queries": [
        '사옥 매입 빌딩', '빌딩 매입 자산운용',
        '오피스 매각 부동산펀드', '세일즈앤리스백 오피스',
        '자산운용사 오피스 매입', '부동산펀드 오피스 투자',
        'IFRS17 부동산 매각', '보험사 빌딩 매각',
        '캐피탈마켓 오피스 딜', '딜 클로징 오피스',
        '매각 자문 오피스', '투자 자문 빌딩',
    ],
    "rss_queries": [
        '빌딩 매입 매각 오피스', '세일즈앤리스백 오피스',
        '자산운용사 오피스 매입', '부동산펀드 오피스 투자',
        '보험사 빌딩 매각', 'IFRS17 오피스 부동산',
    ],
    "must_have_keywords": [
        '빌딩 매입', '빌딩 매각', '오피스 매입', '오피스 매각',
        '사옥 매입 계약', '세일즈앤리스백', 'S&LB',
        '자산운용사 오피스', '부동산펀드 오피스',
        '딜 클로징', '딜 성사', 'IFRS17 오피스',
        '랜드마크 빌딩 매각', '매각 자문 오피스', '투자 자문 빌딩',
        '오피스 인수', '빌딩 인수합병',
    ],
    "must_not_keywords": BASE_MUST_NOT,  # 세부 판단은 AI에 위임
    "min_relevance_score": 0,
}

CORPORATE_SPACE_CATEGORY = {
    "id": "corporate_space", "name": "기업 공간 전략",
    "ai_definition": "기업의 오피스 공간 전략(하이브리드 근무·공유오피스·사무공간 효율화·리모델링·워크플레이스 혁신)이 기사의 핵심 주제인 경우. 기업 성장기사에서 오피스를 한 줄 언급하거나, 외식·유통 매장 리뉴얼 기사는 제외.",
    "icon": "🏗️", "label": "WORKSPACE",
    "search_queries": [
        '워크플레이스 트렌드 기업', '하이브리드 근무 오피스 전략',
        '거점 오피스 전략 기업', '코워킹스페이스 확대',
        '사무공간 효율화 기업', '오피스 면적 축소 전략',
        '오피스 리모델링 기업', '직원 경험 오피스 환경',
        '스마트오피스 구축', '플렉스오피스 기업',
    ],
    "rss_queries": [
        '워크플레이스 트렌드 기업', '하이브리드 근무 오피스 전략',
        '거점 오피스 전략', '코워킹스페이스 확대',
        '사무공간 효율화 기업', '오피스 리모델링 기업',
    ],
    "must_have_keywords": [
        '워크플레이스 전략', '하이브리드 근무 오피스', '재택근무 정책',
        '거점 오피스 전략', '공유 오피스 확대', '코워킹 기업',
        '사무공간 효율화', '오피스 면적', '오피스 리모델링',
        '스마트오피스', '플렉스오피스', '직원 경험 오피스',
        '오피스 환경 전략', '좌석 활용률', '근무지 유연화 오피스',
    ],
    "must_not_keywords": BASE_MUST_NOT,  # 세부 판단은 AI에 위임
    "min_relevance_score": 0,
}

INDUSTRIAL_ASSET_CATEGORY = {
    "id": "industrial_asset", "name": "산업용 자산",
    "ai_definition": "국내 물류센터·데이터센터·지식산업센터의 임대·매매·공실·투자 등 부동산 자산으로서의 내용이 핵심인 기사. IT기업의 데이터센터 사업전략, 해외 물류시설, 전자제품 제조·수출, 반도체·배터리 기술 기사는 제외.",
    "icon": "🏭", "label": "INDUSTRIAL",
    # ⚠️ 핵심 수정: 부동산 자산으로서의 데이터센터/물류센터만
    # IT기업의 데이터센터 사업, 전자제품 제조 기사는 차단
    "search_queries": [
        '물류센터 공실률', '수도권 물류센터 임대', '물류센터 임대료',
        '물류센터 매입', '물류센터 매각', '물류 리츠',
        '데이터센터 부동산', '데이터센터 개발 부동산',
        '데이터센터 리츠', '하이퍼스케일 데이터센터 투자',
        '데이터센터 임대 부동산', '데이터센터 매입',
        '지식산업센터 분양', '지식산업센터 공실',
        '풀필먼트센터 임대', '이커머스 물류센터',
    ],
    "rss_queries": [
        '물류센터 공실률 수도권', '물류센터 임대료 수도권',
        '데이터센터 부동산 투자', '데이터센터 리츠',
        '지식산업센터 공실 임대', '풀필먼트센터 부동산',
        '하이퍼스케일 데이터센터 개발',
    ],
    "must_have_keywords": [
        # 물류 자산 (부동산 관점)
        '물류센터 공실', '물류센터 임대', '물류센터 임대료',
        '물류센터 매입', '물류센터 매각', '물류센터 투자',
        '물류 리츠', '풀필먼트센터 부동산',
        # 데이터센터 (부동산 관점 - IT기업 사업과 구분)
        '데이터센터 부동산', '데이터센터 개발 투자',
        '데이터센터 리츠', '데이터센터 임대 시장',
        '하이퍼스케일 데이터센터 개발',
        # 지식산업센터
        '지식산업센터 분양', '지식산업센터 공실',
        '지식산업센터 임대', '지식산업센터 가격',
        '산업단지 분양', '반도체 클러스터 부동산',
    ],
    "must_not_keywords": BASE_MUST_NOT + [
        # IT기업/전자기업 제품·사업 기사
        '전시회', '밀라노', '유럽 시장', '히트펌프', '칠러',
        '공조 수출', '공조 전시', '공조 시스템 수출',
        '스마트팩토리 방문',
        # 주식·투자 기사
        '[특징주]', '[클릭 e종목]', '상한가', '급등',
        'ETF 수익률', '관련주', '연초 수익률',
        # 기업 CEO/경영 전략
        'CEO 발표', '주총', 'AI 올인', 'AI 전략',
        '경영혁신 선포', '통합 서비스',
        # 비부동산 AI 데이터센터 기사
        'GPU 클러스터', 'NVIDIA GTC', 'Arm CPU',
        '광반도체', 'CPO', '광통신', '오케스트레이션',
        '소프트웨어 시장', 'AI 서비스',
        # 조선·제조·해운
        '자율운항', '조선', '선박',
        # 유통·식품
        '스타필드', '유통', '현장경영',
        # 금융 시황
        '바퀴벌레', '대폭락', '월가', '月가',
        # 원자력/SMR (부동산 자산 아님)
        'SMR', '원자력', '핵연료',
        # 배터리·반도체 제조
        'K배터리', '반도체 수출', '반도체 장비',
        # 해외 물류·DC 투자 (국내 CRE 무관)
        '일본 부동산', '일본 투자', '일본 물류', '일본 데이터센터',
        '미국 물류', '유럽 물류', '싱가포르 데이터센터',
        # 인사·조직 기사 (자산 내용 없음)
        '대표 선임', '대표이사 선임', '신임 대표', '대표 취임',
        '부사장 선임', '전무 선임', '인사 발령', '임원 선임',
    ],
    "min_relevance_score": 2,  # 물류/데이터센터 부동산 관련 2개 이상 매칭 필요
}

SMART_ESG_CATEGORY = {
    "id": "smart_esg", "name": "스마트 빌딩 & ESG",
    "ai_definition": "국내 오피스·상업용 빌딩의 스마트화·에너지 효율·ESG 인증·프롭테크 적용이 핵심 주제인 기사. 빌딩과 무관한 제조업 ESG, 화재 사고 수사, 해외 반도체 공장의 LEED 인증 한 줄 언급 기사는 제외.",
    "icon": "🌿", "label": "ESG",
    "search_queries": [
        '스마트빌딩 부동산', '프롭테크 상업용부동산',
        '탄소중립 건물 오피스', 'LEED 인증 빌딩',
        'ESG 부동산', '그린 리모델링 빌딩',
        'RE100 빌딩', '중대재해처벌법 빌딩 관리',
        '디지털 트윈 FM 빌딩', '그린빌딩 인증 오피스',
        '건물 에너지 절감', '프롭테크 오피스 데이터',
    ],
    "rss_queries": [
        '스마트빌딩 자동화 부동산', '프롭테크 상업용부동산',
        '탄소중립 빌딩 오피스', 'LEED 그린빌딩 인증',
        'ESG 부동산 경영', '중대재해처벌법 빌딩 관리',
        '프롭테크 AI 부동산 데이터',
    ],
    "must_have_keywords": [
        '스마트빌딩', '빌딩 자동화', '빌딩 에너지',
        '프롭테크 부동산', '부동산 AI', '부동산 데이터',
        '탄소중립 빌딩', '넷제로 건물', 'RE100 빌딩',
        'LEED 인증', '그린빌딩 인증', 'ESG 부동산',
        '디지털 트윈 빌딩', '건물 에너지 절감',
        '온실가스 건물', '그린 리모델링', 'FM 디지털',
    ],
    "must_not_keywords": BASE_MUST_NOT + [
        # 화재 사고/수사 기사 (중대재해처벌법 걸림 방지)
        '화재 원인', '화재 수사', '화재 감식', '화재현장',
        '희생자', '사상자', '신원 확인', '합동감식',
        '입건', '압수수색', '현장감식',
        '나트륨 보관', '샌드위치 패널',
        # 일반 ESG (부동산 미관련)
        '배터리 ESG', '반도체 ESG', '조선 ESG',
        # 주거/토지 프롭테크
        '아파트 프롭테크', '주거 AI',
        # 관련 없는 반도체 기사
        '반도체 관련주',
        # 해외 반도체/제조 공장 기사 (LEED 한 줄 언급돼도 한국 CRE 무관)
        '반도체 공장', '반도체 제조', '반도체 테스트', '태국', '베트남 공장',
        '말레이시아 공장', '해외 제조', '해외 공장', '설립 완공',
    ],
    "min_relevance_score": 0,  # AI가 관련성 판단
}

ALL_CATEGORIES = [
    REAL_ESTATE_MARKET_CATEGORY,
    OFFICE_LEASE_CATEGORY,
    ASSET_TRANSACTION_CATEGORY,
    CORPORATE_SPACE_CATEGORY,
    INDUSTRIAL_ASSET_CATEGORY,
    SMART_ESG_CATEGORY,
]


# ================================================================
#  유틸리티
# ================================================================

def clean_html(text: str) -> str:
    """HTML 태그 제거 + HTML 엔티티 디코딩 (&quot; &amp; &nbsp; 등)"""
    if not text:
        return ""
    text = re.sub(r'<[^>]+>', '', text)   # 태그 제거
    text = html_lib.unescape(text)         # &quot; → " , &nbsp; → 공백 등
    text = re.sub(r' ', ' ', text)      # non-breaking space → 일반 공백
    text = re.sub(r'\s+', ' ', text)       # 연속 공백 정리
    return text.strip()


def clean_description(text: str) -> str:
    """Naver/RSS 스니펫 완전 정제
    - HTML 태그 & 엔티티 제거
    - 잘린 앞부분·한글 자모 제거
    - 언론사명·기자명·출처 꼬리 제거
    """
    if not text:
        return ""
    # 1) HTML 태그 제거 + 엔티티 디코딩 (clean_html 재호출)
    text = clean_html(text)

    # 2) 잔여 HTML 엔티티 제거 (&quot; 디코딩 후 남은 따옴표 정리)
    text = re.sub(r'&[a-zA-Z#0-9]+;', '', text)

    # 3) 앞부분 잘림 패턴 제거
    text = re.sub(r'^[.…""\'\"]+\s*', '', text).strip()          # 따옴표·말줄임표로 시작
    text = re.sub(r'^[\u3131-\u318F]+', '', text).strip()      # 한글 자모(ㅓ ㄱ 등)
    parts = text.split(' ', 2)
    if len(parts) >= 2 and len(parts[0]) == 1 and re.fullmatch(r'[가-힣]', parts[0]):
        text = ' '.join(parts[1:])                                  # 1자 조사 잔재
    text = re.sub(r'^[a-z0-9]{1,3}\s+', '', text).strip()       # 소문자 영숫자 잔재
    # 디코딩된 따옴표(" ")는 제거 (스니펫 내부 인용부호 → 텍스트 가독성 저해)
    text = text.replace('"', '').replace('"', '').replace('"', '')

    # 4) 앞에 붙는 출처 태그 제거: [시장경제] (뉴시스) 【더벨】 등
    text = re.sub(r'^[\[\(【（][^\]\)】）]{1,25}[\]\)】）]\s*', '', text).strip()

    # 5) 뒤에 붙는 언론사·기자 꼬리 제거
    # "투데이코리아", "=네이트", "=연합뉴스" 같은 패턴
    text = re.sub(r'\s+[가-힣a-zA-Z]{2,20}(뉴스|코리아|미디어|신문|투데이|일보|경제|타임즈?)$', '', text).strip()
    # "김철수 기자", "홍길동 특파원" 등
    text = re.sub(r'\s+[가-힣]{2,4}\s+(기자|특파원|선임기자|에디터)\s*$', '', text).strip()
    # "=네이트", "= 연합뉴스" 꼬리
    text = re.sub(r'\s*=\s*[가-힣a-zA-Z\s]{1,20}$', '', text).strip()
    # "[기자 사진]", "[사진=...]" 같은 잔재
    text = re.sub(r'\s*\[[^\]]{1,20}\]\s*$', '', text).strip()

    # 6) 연속 공백 정리
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def sanitize_json_strings(json_str: str) -> str:
    """JSON 문자열 값 안의 이스케이프 안된 따옴표를 공백으로 대체 (상태 머신)"""
    result = []
    in_string  = False
    escape_next = False
    for i, c in enumerate(json_str):
        if escape_next:
            result.append(c)
            escape_next = False
        elif c == '\\':
            result.append(c)
            escape_next = True
        elif c == '"':
            if not in_string:
                in_string = True
                result.append(c)
            else:
                rest = json_str[i + 1:].lstrip(' \t\r\n')
                if rest and rest[0] in ':,}]':
                    in_string = False
                    result.append(c)
                else:
                    result.append(' ')  # 내부 따옴표 → 공백으로 제거
        else:
            result.append(c)
    return ''.join(result)


def word_cut(text: str, max_len: int) -> str:
    """max_len 이내에서 단어 경계(공백)로 자름 — 절대 단어 중간 컷 없음.
    공백이 없는 긴 단어는 max_len+5까지 허용해 통째로 포함.
    """
    if not text:
        return ""
    text = text.strip()
    if len(text) <= max_len:
        return text
    # max_len 이내 마지막 공백 찾기
    cut = text[:max_len + 1]  # +1 여유
    sp = cut.rfind(' ')
    if sp >= 3:
        return text[:sp].strip()
    # 공백이 없으면 (한 단어가 매우 길다) → 다음 공백까지 포함 (최대 max_len+5)
    next_sp = text.find(' ', max_len)
    if 0 < next_sp <= max_len + 5:
        return text[:next_sp].strip()
    # 그래도 없으면 max_len 그대로
    return text[:max_len].strip()


def extract_summary(description: str, max_len: int = 70) -> str:
    """description에서 완전한 첫 문장을 추출 (max_len 이내)"""
    desc = clean_description(description)
    if not desc:
        return ""
    for sep in ['다. ', '다.\n', '. ', '.\n']:
        idx = desc.find(sep)
        if 15 < idx < max_len:
            return desc[:idx + 1].strip()
    if len(desc) <= max_len:
        return desc
    cut = desc[:max_len]
    sp  = cut.rfind(' ')
    return (cut[:sp] if sp > 10 else cut).strip()


def text_similarity(a: str, b: str) -> float:
    ca = re.sub(r'[^\w가-힣]', '', a.lower())
    cb = re.sub(r'[^\w가-힣]', '', b.lower())
    return SequenceMatcher(None, ca, cb).ratio()


def _extract_keywords(text: str) -> set:
    """제목에서 의미 있는 핵심 단어 추출 — 3자 이상 한국어 + 2자 이상 영문·숫자 혼합"""
    # 한국어 3자 이상
    ko_words = set(re.findall(r'[가-힣]{3,}', text))
    # 영문 대문자 약어·고유명사 2자 이상 (GRESB, IPO, ESG 등)
    en_words = set(re.findall(r'[A-Z]{2,}', text))
    # 불용어 제거 (지나치게 일반적인 단어)
    stopwords = {'있다', '없다', '이후', '위해', '통해', '대한', '관련', '기자', '뉴스', '기사'}
    return (ko_words | en_words) - stopwords


def is_same_event(title_a: str, title_b: str, min_overlap: int = 3) -> bool:
    """핵심 키워드 겹침으로 동일 사건 여부 판단
    - min_overlap개 이상 공통 키워드 → 같은 사건으로 간주
    - 단, 총 키워드가 너무 적은 경우(3개 미만) 오탐 방지를 위해 False
    """
    kw_a = _extract_keywords(title_a)
    kw_b = _extract_keywords(title_b)
    if len(kw_a) < 2 or len(kw_b) < 2:
        return False
    overlap = kw_a & kw_b
    return len(overlap) >= min_overlap


def generate_hash(title: str, desc: str = "") -> str:
    keywords = re.findall(r'[가-힣]+', title + desc)
    return hashlib.md5(''.join(sorted(keywords[:10])).encode()).hexdigest()


# ================================================================
#  기사 원문 크롤링
# ================================================================

# 크롤링 차단 도메인 (SNS, 포털 앱, 로그인 필요 사이트)
_CRAWL_BLOCKED_DOMAINS = (
    'instagram.com', 'facebook.com', 'twitter.com', 'x.com',
    'youtube.com', 'tiktok.com', 'blog.naver.com', 'cafe.naver.com',
    'reddit.com', 'dcinside.com', 'theqoo.net',
)

_CRAWL_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/124.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.5',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}


# 더벨 모바일 목록 페이지에서 수집 가능한 카테고리 ID
_THEBELL_CATEGORIES = {
    "real_estate_market", "office_lease", "asset_transaction",
}

def fetch_thebell_mobile(date_from: datetime, date_to: datetime) -> List[Dict]:
    """더벨 모바일 무료 목록 페이지에서 기사 수집.

    구조 분석 결과:
    - 기사 데이터는 <script> 안에 JS 객체로 내장됨:
        attr={"newskey":"...", "subject":"제목", "subsubject":"부제목",
              "freedtm":"2026-04-03 08:22:39"};
    - href=null (링크는 newskey로 조합): newsview.asp?svccode=00&newskey={newskey}
    - 추가 페이지는 newsdata.asp POST API로 JSON 수신 가능

    HTML 파싱 불필요 — JS 객체 regex 추출로 제목·부제·날짜·링크 전부 확보.
    subsubject(부제목)를 description으로 사용하므로 본문 크롤링 없이도 품질 확보.
    """
    items: List[Dict] = []
    seen_hashes: set = set()

    base_url  = "https://m.thebell.co.kr"
    list_url  = f"{base_url}/m/news.asp?svccode=00&sort=FREE_DTM&searchtxt="
    data_url  = f"{base_url}/m/newsdata.asp"

    mobile_headers = {
        'User-Agent': (
            'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) '
            'AppleWebKit/605.1.15 (KHTML, like Gecko) '
            'Version/17.0 Mobile/15E148 Safari/604.1'
        ),
        'Accept-Language': 'ko-KR,ko;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Referer': base_url + '/',
    }

    # ── JS 객체 파싱 헬퍼 ─────────────────────────────────────────
    # attr={"newskey":"...","subject":"...","subsubject":"...","freedtm":"..."};
    _ATTR_PATTERN = re.compile(
        r'attr\s*=\s*(\{[^}]+\})\s*;',
        re.S
    )

    def _parse_attr_block(raw_json: str) -> Optional[Dict]:
        """attr={...} 블록을 dict로 파싱. HTML 엔티티 디코딩 포함."""
        try:
            d = json.loads(raw_json)
            d['subject']    = html_lib.unescape(d.get('subject',    ''))
            d['subsubject'] = html_lib.unescape(d.get('subsubject', ''))
            return d
        except Exception:
            return None

    def _make_item(attr: Dict) -> Optional[Dict]:
        """attr dict → collect_news 아이템 dict 변환."""
        newskey = attr.get('newskey', '').strip()
        title   = attr.get('subject', '').strip()
        desc    = attr.get('subsubject', '').strip() or title
        freedtm = attr.get('freedtm', '').strip()

        if not newskey or not title or len(title) < 5:
            return None

        pub_date = parse_date(freedtm) if freedtm else NOW
        if pub_date is None:
            pub_date = NOW
        if pub_date.tzinfo is None:
            pub_date = pub_date.replace(tzinfo=KST)

        if not is_within_range(pub_date, date_from, date_to):
            return None

        link = (f"{base_url}/m/newsview.asp"
                f"?svccode=00&newskey={newskey}&sort=FREE_DTM&searchtxt=")

        h = generate_hash(title)
        return {
            "title":       title,
            "description": desc,
            "link":        link,
            "naver_link":  "",
            "source":      "더벨",
            "pub_date":    pub_date.isoformat(),
            "hash_id":     h,
        }

    # ── 1단계: 목록 페이지 HTML의 <script> 에서 attr={} 파싱 ─────
    try:
        resp = requests.get(list_url, headers=mobile_headers, timeout=15, verify=False)
        if resp.status_code != 200:
            print(f"  ⚠️ 더벨 모바일 HTTP {resp.status_code}")
            return []

        if resp.encoding and resp.encoding.lower() in ('iso-8859-1', 'windows-1252'):
            resp.encoding = resp.apparent_encoding or 'euc-kr'

        for m in _ATTR_PATTERN.finditer(resp.text):
            attr = _parse_attr_block(m.group(1))
            if not attr:
                continue
            item = _make_item(attr)
            if item and item['hash_id'] not in seen_hashes:
                seen_hashes.add(item['hash_id'])
                items.append(item)

    except Exception as e:
        print(f"  ❌ 더벨 모바일 목록 오류: {e}")
        return []

    # ── 2단계: newsdata.asp POST API로 추가 페이지 수집 ──────────
    # page=2 부터 date_from 이전 기사가 나올 때까지 반복
    try:
        ajax_headers = {**mobile_headers, 'X-Requested-With': 'XMLHttpRequest',
                        'Content-Type': 'application/x-www-form-urlencoded'}
        page = 2
        while page <= 5:   # 최대 5페이지(100건)로 제한
            payload = {
                'svccode': '00', 'sort': 'FREE_DTM',
                'searchtxt': '', 'page': page, 'pagesize': 20, 'total': 999999,
            }
            resp2 = requests.post(
                data_url, data=payload, headers=ajax_headers,
                timeout=15, verify=False
            )
            if resp2.status_code != 200:
                break

            try:
                data = resp2.json()
            except Exception:
                break

            news_list = data.get('list', [])
            if not news_list:
                break

            oldest_on_page = None
            for entry in news_list:
                attr = {
                    'newskey':    entry.get('newskey', ''),
                    'subject':    html_lib.unescape(entry.get('subject',    '')),
                    'subsubject': html_lib.unescape(entry.get('subsubject', '')),
                    'freedtm':    entry.get('freedtm', ''),
                }
                item = _make_item(attr)
                if item:
                    if oldest_on_page is None:
                        oldest_on_page = parse_date(attr['freedtm'])
                    if item['hash_id'] not in seen_hashes:
                        seen_hashes.add(item['hash_id'])
                        items.append(item)

            # 이 페이지 최신 기사가 date_from 이전이면 더 볼 필요 없음
            if oldest_on_page and oldest_on_page < date_from - timedelta(days=1):
                break

            page += 1
            time.sleep(0.3)

    except Exception as e:
        print(f"  ⚠️ 더벨 newsdata.asp 오류: {e}")

    print(f"  📰 더벨 모바일: {len(items)}건 수집")
    return items


def fetch_article_body(url: str, timeout: int = 8) -> str:
    """기사 URL에서 본문 텍스트를 크롤링한다.

    - requests + regex 만 사용 (BeautifulSoup 의존성 없음)
    - 실패·차단 시 빈 문자열 반환 (파이프라인 중단 없음)
    - 반환값: 최대 1000자 정제된 본문 텍스트

    추출 우선순위:
      1) <article> 태그 내부
      2) id/class 에 'article|content|body|news' 포함 <div>
      3) 전체 HTML에서 <p> 단락 병합
    """
    if not url or not url.startswith('http'):
        return ""
    if any(b in url for b in _CRAWL_BLOCKED_DOMAINS):
        return ""

    try:
        resp = requests.get(
            url,
            headers=_CRAWL_HEADERS,
            timeout=timeout,
            verify=False,
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return ""

        html = resp.text

        # ── 1) 노이즈 태그 제거 ─────────────────────────────────
        html = re.sub(r'<script[^>]*>[\s\S]*?</script>', ' ', html, flags=re.I)
        html = re.sub(r'<style[^>]*>[\s\S]*?</style>',  ' ', html, flags=re.I)
        html = re.sub(r'<!--[\s\S]*?-->',               ' ', html)

        # ── 2) 본문 블록 추출 ───────────────────────────────────
        # 2-a) 네이버 뉴스 전용 셀렉터 (n.news.naver.com 캐시 뷰어)
        m_naver = re.search(
            r'<div[^>]+(?:id|class)=["\'][^"\']*'
            r'(?:newsct_article|dic_area|news_body_area|articleCont|'
            r'art_txt|article_body_contents|news-article-body)[^"\']*["\'][^>]*>'
            r'([\s\S]{100,8000}?)</div>',
            html, re.I
        )
        # 2-b) <article> 태그
        m_article = re.search(r'<article[^>]*>([\s\S]*?)</article>', html, re.I)
        # 2-c) 일반 div id/class 패턴
        m_div = re.search(
            r'<div[^>]+(?:id|class)=["\'][^"\']*'
            r'(?:article|article-body|news-body|content-body|'
            r'article_body|newsBody|articleBody|news_content)[^"\']*["\'][^>]*>'
            r'([\s\S]{100,6000}?)</div>',
            html, re.I
        )
        if m_naver:
            block = m_naver.group(1)
        elif m_article:
            block = m_article.group(1)
        elif m_div:
            block = m_div.group(1)
        else:
            block = html

        # ── 3) 태그 제거 + 엔티티 디코딩 ───────────────────────
        text = re.sub(r'<[^>]+>', ' ', block)
        text = html_lib.unescape(text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{2,}', '\n', text).strip()

        # ── 4) 의미 있는 단락만 수집 (30자 이상) ───────────────
        paragraphs: List[str] = []
        for seg in re.split(r'\n|(?<=다\.)\s{1,3}', text):
            seg = seg.strip()
            # 광고·네비·기자명 패턴 제거
            if len(seg) < 30:
                continue
            if re.search(r'(기자\s*=|저작권|무단\s*전재|구독|광고|로그인|회원가입)', seg):
                continue
            paragraphs.append(seg)

        body = ' '.join(paragraphs)
        return body[:1000]

    except Exception:
        return ""


def _enrich_with_article_body(items: List[Dict], timeout: int = 8) -> None:
    """선별된 기사에 대해 원문 크롤링 후 'full_body' 필드를 in-place 추가.

    full_body 가 있으면 _build_summary_prompt 에서 description 대신 사용.
    크롤링 실패 시 full_body = "" → 기존 description 폴백.

    시도 순서:
      1) link (originallink — 원본 언론사 URL)
      2) naver_link (n.news.naver.com — 네이버 캐시 뷰어)
    """
    success = 0
    for item in items:
        url        = item.get('link', '')
        naver_url  = item.get('naver_link', '')

        body = fetch_article_body(url, timeout=timeout)

        # originallink 실패 시 → 네이버 뷰어 URL로 재시도
        if not body and naver_url and naver_url != url:
            body = fetch_article_body(naver_url, timeout=timeout)

        item['full_body'] = body
        if body:
            success += 1
        time.sleep(0.3)   # 서버 부하 분산
    print(f"  🌐 원문 크롤링: {success}/{len(items)}건 성공")


def parse_date(date_str: str) -> Optional[datetime]:
    """다양한 날짜 포맷 파싱 — RSS pubDate 포맷 포함"""
    if not date_str:
        return None
    date_str = date_str.strip()
    formats = [
        '%a, %d %b %Y %H:%M:%S %z',   # RSS: Tue, 17 Mar 2026 10:00:00 +0900
        '%a, %d %b %Y %H:%M:%S GMT',   # RSS GMT
        '%Y-%m-%dT%H:%M:%S%z',
        '%Y-%m-%dT%H:%M:%SZ',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d',
        '%Y.%m.%d',
    ]
    for fmt in formats:
        try:
            parsed = datetime.strptime(date_str, fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=KST)
            return parsed
        except ValueError:
            continue
    return None


def is_within_range(date_obj: Optional[datetime],
                    date_from: datetime, date_to: datetime) -> bool:
    if not date_obj:
        return False
    if date_obj.tzinfo is None:
        date_obj = date_obj.replace(tzinfo=KST)
    return date_from <= date_obj <= date_to


# ================================================================
#  뉴스 수집: Naver News API
# ================================================================

def _source_from_url(url: str) -> str:
    """originallink URL 도메인으로 신문사명 반환"""
    if not url:
        return "네이버뉴스"
    DOMAIN_MAP = {
        'hankyung.com':      '한국경제',
        'mk.co.kr':          '매일경제',
        'sedaily.com':       '서울경제',
        'edaily.co.kr':      '이데일리',
        'mt.co.kr':          '머니투데이',
        'newsis.com':        '뉴시스',
        'newspim.com':       '뉴스핌',
        'thebell.co.kr':     '더벨',
        'bloter.net':        '블로터',
        'zdnet.co.kr':       'ZDNet',
        'etnews.com':        '전자신문',
        'chosun.com':        '조선일보',
        'donga.com':         '동아일보',
        'joongang.co.kr':    '중앙일보',
        'hani.co.kr':        '한겨레',
        'khan.co.kr':        '경향신문',
        'ohmynews.com':      '오마이뉴스',
        'yna.co.kr':         '연합뉴스',
        'yonhapnewstv.co.kr':'연합뉴스TV',
        'kbs.co.kr':         'KBS',
        'mbc.co.kr':         'MBC',
        'sbs.co.kr':         'SBS',
        'jtbc.co.kr':        'JTBC',
        'mbn.co.kr':         'MBN',
        'tvchosun.com':      'TV조선',
        'ytn.co.kr':         'YTN',
        'asiae.co.kr':       '아시아경제',
        'ajunews.com':       '아주경제',
        'heraldcorp.com':    '헤럴드경제',
        'bizchosun.com':     '비즈조선',
        'dailian.co.kr':     '데일리안',
        'fnnews.com':        '파이낸셜뉴스',
        'paxetv.com':        '팍스경제TV',
        'inews24.com':       '아이뉴스24',
        'financial.co.kr':   '파이낸셜포스트',
        'engnews24h.com':    '공학신문',
        'biz.chosun.com':    '비즈조선',
        'news.naver.com':    '네이버뉴스',
        'n.news.naver.com':  '네이버뉴스',
    }
    try:
        from urllib.parse import urlparse
        host = urlparse(url).netloc.lower().lstrip('www.')
        for domain, name in DOMAIN_MAP.items():
            if host == domain or host.endswith('.' + domain):
                return name
    except Exception:
        pass
    return "네이버뉴스"


def fetch_naver_news(config: NewsConfig, queries: List[str],
                     date_from: datetime, date_to: datetime) -> List[Dict]:
    """Naver 뉴스 — display=100 페이지네이션으로 date_from 기사까지 탐색"""
    if not config.NAVER_CLIENT_ID:
        print("  ⚠️ Naver API 키 없음 — 건너뜀")
        return []

    headers = {
        "X-Naver-Client-Id":     config.NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": config.NAVER_CLIENT_SECRET,
    }

    all_items: List[Dict] = []
    seen_hashes: set = set()

    for query in queries:
        start = 1

        while start <= 901:
            try:
                resp = requests.get(
                    "https://openapi.naver.com/v1/search/news.json",
                    params={"query": query, "display": 100,
                            "start": start, "sort": "date"},
                    headers=headers, timeout=15, verify=False
                )
                if resp.status_code != 200:
                    print(f"  ⚠️ Naver HTTP {resp.status_code}: {query[:30]}")
                    break

                items = resp.json().get("items", [])
                if not items:
                    break

                oldest_pub = None
                for item in items:
                    title    = clean_html(item.get("title", ""))
                    desc     = clean_html(item.get("description", ""))
                    pub_date = parse_date(item.get("pubDate", ""))

                    if pub_date is None:
                        continue
                    if pub_date.tzinfo is None:
                        pub_date = pub_date.replace(tzinfo=KST)

                    if oldest_pub is None or pub_date < oldest_pub:
                        oldest_pub = pub_date

                    # 범위 밖이면 skip (중단 없음)
                    if not is_within_range(pub_date, date_from, date_to):
                        continue

                    h = generate_hash(title, desc)
                    if h in seen_hashes:
                        continue
                    seen_hashes.add(h)

                    all_items.append({
                        "title":       title,
                        "description": desc[:500],
                        "link":        item.get("originallink") or item.get("link", ""),
                        "naver_link":  item.get("link", ""),   # n.news.naver.com 폴백용
                        "source":      _source_from_url(item.get("originallink") or item.get("link", "")),
                        "pub_date":    pub_date.isoformat(),
                        "hash_id":     h,
                    })

                # 이 페이지 가장 오래된 기사가 date_from보다 5일 이상 오래됐으면 중단
                if oldest_pub and oldest_pub < date_from - timedelta(days=5):
                    break

                if len(items) < 100:
                    break
                start += 100
                time.sleep(0.2)

            except Exception as e:
                print(f"  ❌ Naver 에러 ({query[:20]}): {e}")
                break

        time.sleep(0.15)

    print(f"  📰 Naver: {len(all_items)}건 수집")
    return all_items


# ================================================================
#  뉴스 수집: Google News RSS (API 키 불필요)
# ================================================================

# 신뢰 언론사 도메인 → 출처명 매핑
RSS_SOURCE_MAP = {
    'hankyung.com':    '한국경제',
    'mk.co.kr':        '매일경제',
    'chosun.com':      '조선비즈',
    'mt.co.kr':        '머니투데이',
    'edaily.co.kr':    '이데일리',
    'sedaily.com':     '서울경제',
    'fnnews.com':      '파이낸셜뉴스',
    'newsis.com':      '뉴시스',
    'thebell.co.kr':   '더벨',
    'dealsite.co.kr':  '딜사이트',
    'bizwatch.co.kr':  '비즈워치',
    'theqoo.net':      None,  # None → 차단
    'blog.naver.com':  None,
    'cafe.naver.com':  None,
    'instagram.com':   None,
    'facebook.com':    None,
    'twitter.com':     None,
    'x.com':           None,
    'tiktok.com':      None,
    'youtube.com':     None,
    'reddit.com':      None,
    'dcinside.com':    None,
}

# SNS / 커뮤니티 / 블로그 차단 도메인
BLOCKED_DOMAINS = {
    'instagram.com', 'facebook.com', 'twitter.com', 'x.com',
    'tiktok.com', 'youtube.com', 'linkedin.com', 'pinterest.com',
    'blog.naver.com', 'cafe.naver.com', 'post.naver.com',
    'blog.daum.net', 'cafe.daum.net',
    'dcinside.com', 'theqoo.net', 'clien.net',
    'ppomppu.co.kr', 'mlbpark.donga.com',
    'reddit.com', 'threads.net',
}


def _build_rss_url(query: str) -> str:
    """Google News RSS URL — 날짜 연산자 제거 (Google RSS가 무시함)
    날짜 필터는 is_within_range()에서 pubDate 기준으로 100% 처리.
    """
    encoded = quote_plus(query)
    return (
        f"https://news.google.com/rss/search"
        f"?q={encoded}&hl=ko&gl=KR&ceid=KR:ko"
    )


def fetch_google_rss(queries: List[str],
                     date_from: datetime, date_to: datetime) -> List[Dict]:
    """Google News RSS 수집 — API 키 불필요, pubDate 항상 포함"""
    all_items:   List[Dict] = []
    seen_hashes: set        = set()

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "ko-KR,ko;q=0.9",
    }

    for query in queries:
        url = _build_rss_url(query)
        try:
            resp = requests.get(url, headers=headers, timeout=20, verify=False)
            if resp.status_code != 200:
                print(f"  ⚠️ RSS HTTP {resp.status_code}: {query[:30]}")
                time.sleep(0.5)
                continue

            # XML 파싱
            try:
                root = ET.fromstring(resp.content)
            except ET.ParseError as e:
                print(f"  ⚠️ RSS XML 파싱 오류 ({query[:20]}): {e}")
                time.sleep(0.5)
                continue

            channel = root.find('channel')
            if channel is None:
                continue

            items = channel.findall('item')
            for item in items:
                title_raw = item.findtext('title', '') or ''
                link      = item.findtext('link',  '') or ''
                pub_str   = item.findtext('pubDate', '') or ''
                desc_raw  = item.findtext('description', '') or ''

                # ── 차단 도메인 필터 ──
                if any(bd in link for bd in BLOCKED_DOMAINS):
                    continue

                # ── 출처 식별 ──
                source = "Google News"
                for domain, name in RSS_SOURCE_MAP.items():
                    if domain in link:
                        if name is None:
                            source = None  # 차단
                        else:
                            source = name
                        break
                if source is None:
                    continue

                # ── 날짜 파싱 및 범위 필터 ──
                pub_date = parse_date(pub_str)
                if pub_date is None:
                    # RSS에서 날짜 없는 경우는 거의 없지만, 있으면 제외
                    continue
                if pub_date.tzinfo is None:
                    pub_date = pub_date.replace(tzinfo=KST)
                if not is_within_range(pub_date, date_from, date_to):
                    continue

                # ── 제목 정제 ──
                # Google News RSS 제목: "기사 제목 - 언론사명" 형태
                title = clean_html(title_raw)
                title = re.sub(r'\s*[-–—]\s*[^-–—]{2,30}$', '', title).strip()

                # ── 설명 정제 ──
                desc = clean_html(desc_raw)[:500]

                h = generate_hash(title, desc)
                if h in seen_hashes:
                    continue
                seen_hashes.add(h)

                # Google RSS description이 없거나 극히 짧은 경우만 제외
                # (제목과 일부 겹치는 것은 허용 — 유효 기사 대량 탈락의 주요 원인이었음)
                desc_body = re.sub(r'\s*([-–—]?\s*\S+\.(com|net|co\.kr|kr|news).*)?$', '', desc).strip()
                if not desc_body or len(desc_body) < 10:
                    continue

                all_items.append({
                    "title":       title,
                    "description": desc,
                    "link":        link,
                    "source":      source,
                    "pub_date":    pub_date.isoformat(),
                    "hash_id":     h,
                })

        except Exception as e:
            print(f"  ❌ RSS 에러 ({query[:20]}): {e}")

        time.sleep(0.4)  # Google RSS 속도 제한 방지

    print(f"  📰 Google RSS: {len(all_items)}건 수집")
    return all_items


# ================================================================
#  필터링 & 중복 제거
# ================================================================


def _kw_match(kw: str, text: str) -> bool:
    """키워드 매칭 — 복합 키워드(공백 포함)는 완전 일치 우선,
    실패 시 구성 단어가 모두 텍스트 안에 있으면 부분 매칭으로 허용.
    예: '물류센터 임대료' → '물류센터'+'임대료' 각각 존재하면 매칭.
    단어 2자 미만이면 완전 일치만 허용 (오탐 방지).
    """
    if kw in text:
        return True
    parts = kw.split()
    if len(parts) >= 2 and all(len(p) >= 2 for p in parts):
        return all(p in text for p in parts)
    return False

def filter_and_dedupe(items: List[Dict], category: Dict,
                      threshold: float = 0.65) -> List[Dict]:
    """노이즈 차단 + 제목 유사도 중복 제거 (관련성 판단은 AI에 위임)

    역할 분리:
    - 이 함수: 명백한 비CRE 노이즈만 차단 (주식 시황표, 주택, 암호화폐 등)
    - AI 큐레이션: 카테고리 적합성·맥락·관련성 판단 전담
    must_have_keywords 점수는 AI 배치 상위 정렬용으로만 사용 (게이트 역할 없음)
    """
    must_have = category["must_have_keywords"]
    # must_not은 BASE_MUST_NOT 기반의 명백한 비CRE 항목만 포함 (카테고리별 세부 목록은 AI가 판단)
    must_not  = category["must_not_keywords"]

    # 0) 공통 노이즈 제목 패턴 1차 차단 (주식 시황표, 채권 일정 등)
    def _is_noise_title(title: str) -> bool:
        return any(pat in title for pat in COMMON_NOISE_TITLE_PATTERNS)

    items = [item for item in items if not _is_noise_title(item.get("title", ""))]

    # 1) BASE_MUST_NOT 기반 명백한 비CRE 키워드 차단 (제목+설명)
    filtered = [
        item for item in items
        if not any(kw in item["title"] + " " + item["description"]
                   for kw in must_not)
    ]

    # 2) must_have 스코어링 — AI 배치 상위 정렬용 (게이트 아님, min_score 적용 안 함)
    for item in filtered:
        text = item["title"] + " " + item["description"]
        item["relevance_score"] = sum(1 for kw in must_have if _kw_match(kw, text))

    # 스코어 높은 순 → AI가 배치 상위 기사를 먼저 검토
    filtered.sort(key=lambda x: x["relevance_score"], reverse=True)

    # 4) 해시 + 제목 유사도 중복 제거
    unique:       List[Dict] = []
    seen_hashes:  set        = set()

    for item in filtered:
        if item["hash_id"] in seen_hashes:
            continue
        is_dup = any(
            text_similarity(item["title"], u["title"]) > threshold
            or is_same_event(item["title"], u["title"])
            for u in unique
        )
        if not is_dup:
            unique.append(item)
            seen_hashes.add(item["hash_id"])

    return unique


# ================================================================
#  AI 큐레이션: Gemini API (Claude API 폴백 지원)
# ================================================================

def _build_curate_prompt(batch: List[Dict], cat_name: str, category: Dict = None) -> str:
    """AI 큐레이션 프롬프트 빌더 — 맥락 기반 관련성 판단 중심

    설계 원칙:
    - AI가 기사의 '핵심 주제'를 먼저 파악하게 함 (키워드 존재 여부가 아님)
    - category["ai_definition"]으로 카테고리 정의를 자연어로 전달
    - 통과/차단 판단 근거를 AI가 스스로 서술하게 해 정확도 향상
    """
    # desc가 너무 짧은 기사는 제외 (요약 불가)
    valid = [item for item in batch if len(item.get('description', '').strip()) >= 15]
    if not valid:
        valid = batch

    news_text = "\n\n".join(
        f"[{i+1}] {item['source']} | {item['title']}\n{item['description'][:500]}"
        for i, item in enumerate(valid)
    )

    # 카테고리 정의 (ai_definition 우선, 없으면 name 사용)
    cat_id         = category.get("id", "") if category else ""
    cat_definition = (category or {}).get("ai_definition", cat_name)

    # 카테고리별 핵심 수치 포인트 (요약 작성용)
    key_focus_map = {
        "real_estate_market": "캡레이트(%), 투자 수익률(%), 거래 규모(억원), 금리 변동폭(bp), 리츠 배당수익률(%)",
        "office_lease":       "공실률(%), 실질 임대료(원/평), 렌트프리 개월수, 이전 면적(평/㎡), 권역(CBD/GBD/YBD/BBD)",
        "asset_transaction":  "거래 규모(억원/조원), 매입·매각 주체(운용사명), 빌딩명, 수익률(%), 딜 구조",
        "corporate_space":    "오피스 면적 변화(평/㎡), 워크플레이스 전략 내용, 하이브리드 비율(%), 좌석 활용률(%)",
        "industrial_asset":   "공실률(%), 임대료(원/평), 거래 규모(억원), 위치(수도권/경기), 면적(㎡/평)",
        "smart_esg":          "에너지 절감률(%), 인증 등급(LEED 등), 탄소 감축량, 빌딩명, 적용 기술명",
    }
    key_focus = key_focus_map.get(cat_id, "핵심 수치와 주체")

    return f"""당신은 서울 상업용 부동산(CRE) 전문 뉴스 큐레이터입니다.
아래 뉴스 {len(valid)}건을 검토해, [{cat_name}] 카테고리에 진정으로 부합하는 기사만 엄선하세요.

{news_text}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
【카테고리 정의】
{cat_definition}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
【선별 방법 — 순서대로 판단할 것】

STEP 1. 기사의 핵심 주제 파악
  이 기사가 주로 다루는 것은 무엇인가?
  제목과 본문 첫 단락이 가장 많은 분량을 할애하는 내용 = 핵심 주제.
  특정 키워드가 등장하더라도 그것이 곁가지 언급이면 핵심 주제가 아님.
  예) "LEED 인증 받은 태국 반도체 공장" → 핵심=반도체 제조, LEED는 부수적 언급
  예) "롯데리아 매장이 오피스 상권에 위치" → 핵심=외식업 매출, 오피스는 배경

STEP 2. 카테고리 정의와 핵심 주제 일치 여부 판단
  위에서 파악한 핵심 주제가 카테고리 정의에 부합하는가?
  YES → 선별 후보
  NO  → 즉시 제외 (이유 불필요)

STEP 3. 공통 제외 기준 (어느 카테고리든 무조건 제외)
  - 주택·아파트·분양·재건축이 핵심인 기사
  - 주식 시황표·채권 일정표·경제 브리핑 형식의 기사
  - 암호화폐·NFT·코인이 핵심인 기사
  - 기사 전체가 해외(일본·중국·미국 등) 부동산만 다루는 기사
  - 인사 발령·임원 선임만 다루는 기사 (부동산 거래·시장 내용 없음)

STEP 4. 애매한 경우 → 제외
  선별 여부가 50:50이면 과감히 제외. 명확히 부합하는 것만 선별.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
【요약 작성 규칙 — 선별된 기사에만 적용】

■ short_summary (40~60자)
  - 형식: [주체] + [행위/결과] + [핵심수치]로 구성된 완결 문장
  - 완결 어미 필수: "~했다", "~됐다", "~달했다", "~나타났다" 등
  - 예) "코람코자산신탁이 서울 CBD 에티버스타워를 2,400억원에 인수했다."
  - 수치가 없으면 주체+결과로 완결 ("~전략을 발표했다", "~계약을 체결했다")
  - 금지: 조사·어간으로 끝나는 미완결, 원문 그대로 발췌

■ summary: 빈 문자열 "" 로 고정 출력 (별도 전용 처리됨)

■ comment: CRE 임대차·투자 실무자 시각의 한 줄 코멘트
■ tags: 핵심 키워드 2~4개 (예: ["CBD", "공실률", "A급오피스"])

■ summary — 개조식 불릿 (각 줄 "▪ "로 시작)

  【작성 방식】
    기사 내용을 읽고 핵심을 새로 요약해 작성한다.
    원문 문장을 그대로 복사하거나 글자 수 단위로 잘라 붙이는 것은 절대 금지.
    불릿 1개(1줄)로도 충분하면 1줄만 출력한다. 억지로 2줄 만들지 말 것.

  【각 줄 필수 조건 — 아래 조건을 하나라도 어기면 그 줄은 출력하지 말 것】
    ① 그 자체로 의미가 완결되어야 한다 (단독으로 읽어도 뜻이 통해야 함)
    ② 반드시 완결어미·완결명사로 끝나야 한다
       허용 어미: ~했다 / ~됐다 / ~됩니다 / ~있다 / ~한다 / ~예정 / ~전망 / ~체결 / ~확보 / ~성장 / ~선정 / ~확대 / ~추진 등
    ③ 조사(을·를·이·가·의·며·고·에서·가운데·통해·위해·등·및)로 끝나는 줄 출력 금지
    ④ 앞 줄의 문장이 이어지는 내용은 출력 금지 (각 줄은 독립)

  【불릿 2개를 쓸 경우 — 서로 다른 정보여야 함】
    · 첫째 줄: 핵심 사실 — 주체+행위+규모·결과. 포함 우선: {key_focus}
    · 둘째 줄: 첫째 줄과 다른 정보 — 위치·스펙·타임라인·배경·시사점

  【작성 예】
    ✅ 올바른 예 (2줄):
      "▪ DL이앤씨, 4,000억 규모 코리안리 신사옥 공사 우선협상대상자 선정"
      "▪ 종로 수송동에 21층 프라임 오피스+콘서트홀+녹지 복합시설 2026년 5월 착공 예정"
    ✅ 올바른 예 (1줄 — 내용이 충분하지 않을 때):
      "▪ 마스턴투자운용·삼성물산, 상업용 부동산 스마트빌딩 플랫폼 협력 MOU 체결"
    ❌ 금지 (조사로 끝남):
      "▪ 선호 투자 전략에서는 밸류애드 전략이 3년 연속 가장 높은 선호도를" ← '를'로 끝남
    ❌ 금지 (한 문장을 절반으로 절단):
      "▪ 냉방은 26도 이상"  +  "▪ 난방은 18도 이하로 운영" ← 하나의 열거를 쪼갬

■ comment: CRE 임대차·투자 실무자 시각의 한 줄 코멘트
■ tags: 핵심 키워드 2~4개 (예: ["CBD", "공실률", "A급오피스"])

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
출력 형식 — JSON 배열만 출력 (설명·코드블록·마크다운 없이):
[
  {{
    "index": <기사번호 정수>,
    "relevance": "HIGH",
    "short_summary": "<완결 문장 40~60자>",
    "summary": "",
    "comment": "<CRE 실무자 한줄평>",
    "reason": "<이 카테고리 핵심 주제와 부합하는 이유 1문장>",
    "tags": ["태그1", "태그2"]
  }}
]
JSON만 출력. 선별 기사 없으면 []만 출력."""


def _build_summary_prompt(items: List[Dict], category: Dict) -> str:
    """개조식 요약 전용 프롬프트.
    상세요약(2줄 개조식) 먼저 작성 → 그 내용에서 한줄요약 도출.
    """
    cat_id = category.get("id", "")
    key_focus_map = {
        "real_estate_market": "캡레이트(%), 거래규모(억원), 투자수익률(%), 금리",
        "office_lease":       "공실률(%), 임대료(원/평), 권역(CBD/GBD/YBD/BBD)",
        "asset_transaction":  "거래규모(억/조원), 빌딩명, 매입·매각 주체",
        "corporate_space":    "이전지역, 면적(평), 워크플레이스 전략",
        "industrial_asset":   "공실률(%), 임대료(원/평), 거래규모(억원), 위치",
        "smart_esg":          "인증등급, 에너지절감(%), 빌딩명, 적용기술",
    }
    key_focus = key_focus_map.get(cat_id, "핵심 수치와 주체")

    def _item_body(item: Dict) -> str:
        """full_body(크롤링 원문) 우선, 없으면 description 폴백."""
        body = item.get('full_body', '').strip()
        if body:
            return body[:900]
        return item.get('description', '')[:500]

    articles_text = "\n\n".join(
        f"[{i+1}] 제목: {item['title']}\n본문: {_item_body(item)}"
        for i, item in enumerate(items)
    )

    return f"""한국 CRE 뉴스 에디터로서 아래 기사들을 각각 개조식으로 요약하세요.
우선 포함할 수치: {key_focus}

{articles_text}

━━━━━━━━━━━━━━━━━━━━
【작성 규칙 — 전부 지킬 것】
1. summary: "▪ " 로 시작하는 개조식 줄. 최대 2줄. 각 줄 40자 이하.
   - 줄1: 핵심사실 (주체+행위+수치)
   - 줄2: 줄1과 다른 정보 (위치·배경·전망). 없으면 1줄만.
2. short_summary: summary 핵심을 1줄로. 40자 이하.
3. 모든 줄은 완결어미로 끝낼 것: ~했다 / ~됐다 / ~예정 / ~체결 / ~선정 / ~전망 / ~확대
4. 조사(을·를·이·가·의·에서·통해·위해·등·및)로 끝나는 줄 출력 금지.
5. 원문 문장 그대로 복사 금지. 반드시 새로 압축해 쓸 것.

【예시】
기사: "DL이앤씨가 코리안리 신사옥 4000억 공사 우선협상자로 선정됐다. 종로 수송동에 21층 규모로 2026년 착공 예정."
  summary: "▪ DL이앤씨, 코리안리 신사옥 4,000억 우선협상자 선정\n▪ 종로 수송동 21층, 2026년 5월 착공 예정"
  short_summary: "DL이앤씨, 코리안리 신사옥 4,000억 공사 수주"

━━━━━━━━━━━━━━━━━━━━
출력 — JSON만 (코드블록 없이):
[
  {{
    "index": 1,
    "summary": "▪ 줄1\\n▪ 줄2",
    "short_summary": "한줄요약"
  }}
]"""


def _summarize_with_claude_sonnet(config: NewsConfig, items: List[Dict],
                                   category: Dict) -> List[Dict]:
    """Claude Sonnet 먼저, Gemini 폴백. 둘 다 실패 시 에러 메시지만 출력 (Python 폴백 없음)."""
    if not items:
        return items

    prompt = _build_summary_prompt(items, category)

    # ── 1순위: Claude Sonnet ───────────────────────────────────────
    if config.CLAUDE_API_KEY:
        SONNET_MODELS = list(dict.fromkeys([
            config.CLAUDE_SUMMARY_MODEL,   # claude-sonnet-4-6
            "claude-sonnet-4-5",
            "claude-3-5-sonnet-20241022",
            config.CLAUDE_MODEL,           # Haiku 최후 폴백
        ]))
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=config.CLAUDE_API_KEY)
            for model_name in SONNET_MODELS:
                try:
                    print(f"  ✍️  Claude 요약 생성... 모델={model_name} ({len(items)}건)")
                    resp = client.messages.create(
                        model=model_name,
                        max_tokens=2000,
                        temperature=0.2,
                        messages=[{"role": "user", "content": prompt}]
                    )
                    result_text = resp.content[0].text.strip()
                    print(f"  📝 응답: {result_text[:80]}")
                    summaries = _parse_summary_json(result_text)
                    if summaries is not None:
                        updated = _apply_summaries(items, summaries)
                        print(f"  ✅ Claude 요약 완료: {len(updated)}건")
                        return updated
                    print(f"  ⚠️ {model_name} JSON 파싱 실패 → 다음 모델")
                except Exception as e:
                    err = str(e)
                    if '404' in err or 'not found' in err.lower():
                        print(f"  ⚠️ {model_name} 없음 → 다음 모델")
                    elif '529' in err or 'overloaded' in err.lower():
                        print(f"  ⚠️ {model_name} 과부하 → 다음 모델")
                    else:
                        print(f"  ⚠️ {model_name} 오류: {type(e).__name__}: {e}")
        except ImportError:
            print("  ❌ anthropic 패키지 없음 — pip install anthropic")

    # ── 2순위: Gemini ──────────────────────────────────────────────
    if config.GEMINI_API_KEY:
        try:
            import google.generativeai as genai
            genai.configure(api_key=config.GEMINI_API_KEY)
            GEMINI_MODELS = list(dict.fromkeys([
                "gemini-1.5-pro-002",
                config.GEMINI_MODEL,
                "gemini-2.0-flash",
            ]))
            for model_name in GEMINI_MODELS:
                try:
                    print(f"  ✍️  Gemini 요약 생성... 모델={model_name}")
                    gm = genai.GenerativeModel(model_name)
                    resp = gm.generate_content(
                        prompt,
                        generation_config=genai.GenerationConfig(temperature=0.2, max_output_tokens=2000)
                    )
                    result_text = resp.text.strip()
                    summaries = _parse_summary_json(result_text)
                    if summaries is not None:
                        updated = _apply_summaries(items, summaries)
                        print(f"  ✅ Gemini 요약 완료: {len(updated)}건")
                        return updated
                except Exception as e:
                    err = str(e)
                    if '429' in err or 'quota' in err.lower() or 'RESOURCE_EXHAUSTED' in err:
                        print(f"  ⚠️ {model_name} 한도 초과 → 다음 모델")
                    elif '404' in err or 'not found' in err.lower():
                        print(f"  ⚠️ {model_name} 없음 → 다음 모델")
                    else:
                        print(f"  ⚠️ {model_name} 오류: {type(e).__name__}: {e}")
        except ImportError:
            print("  ❌ google-generativeai 패키지 없음 — pip install google-generativeai")

    # ── API 모두 실패 → 에러 메시지 출력, Python 폴백 없음 ────────
    print("  ❌ [요약 생성 실패] Claude/Gemini API를 확인하세요.")
    print("     - Claude API 키: config.json 또는 CLAUDE_API_KEY 환경변수")
    print("     - Gemini API 키: config.json 또는 GEMINI_API_KEY 환경변수")
    print("     - Gemini 일일 한도: 내일 자동 초기화됩니다.")
    # 빈 요약으로 반환 (항목은 유지)
    for item in items:
        item.setdefault('ai_summary', '▪ (API 오류 — 요약 생성 실패)')
        item.setdefault('ai_short_summary', '(API 오류)')
    return items


def _parse_summary_json(text: str) -> Optional[List[Dict]]:
    """요약 전용 JSON 파싱 [{index, summary, short_summary}, ...] 형태"""
    try:
        clean = re.sub(r'```(?:json)?|```', '', text).strip()
        for old, new in [('\u201c', '"'), ('\u201d', '"'), ('\u2018', "'"), ('\u2019', "'")]:
            clean = clean.replace(old, new)
        m = re.search(r'\[[\s\S]*\]', clean)
        if not m:
            return None
        data = json.loads(m.group())
        if isinstance(data, list) and all(
            isinstance(d, dict) and 'index' in d and 'summary' in d
            for d in data
        ):
            return data
    except Exception:
        pass
    return None


def _apply_summaries(items: List[Dict], summaries: List[Dict]) -> List[Dict]:
    """요약 JSON을 아이템에 적용. summary와 short_summary 모두 처리."""
    summary_map   = {s['index']: s.get('summary', '')       for s in summaries}
    short_map     = {s['index']: s.get('short_summary', '') for s in summaries}

    for i, item in enumerate(items):
        idx = i + 1

        # ── 상세요약(2줄 개조식) ─────────────────────────────────
        raw_summary = summary_map.get(idx, "").strip()
        item['ai_summary'] = _validate_and_fix_summary(raw_summary, item)

        # ── 한줄요약 ─────────────────────────────────────────────
        raw_short = short_map.get(idx, "").strip()
        # 비어있으면 summary 첫 줄에서 추출
        if not raw_short or len(raw_short) < 5:
            first_line = re.split(r'\n', item['ai_summary'])[0]
            raw_short  = re.sub(r'^▪\s*', '', first_line).strip()
        item['ai_short_summary'] = raw_short

    return items

    return items


def _compress_to_gaejoesik(text: str, max_len: int = 45) -> str:
    """산문 문장을 개조식 서술구로 압축. max_len(기본 45자) 이내로.
    완결어미 위치까지 자르되, 핵심 수치·고유명사는 우선 보존.
    """
    text = text.strip().rstrip('.')
    if len(text) <= max_len:
        return text

    # max_len 범위 내 마지막 완결어미 위치 탐색
    ENDINGS = re.compile(
        r'(?:[다됩됐습니겠요음]|확보|예정|전망|체결|선정|발표|착공|준공|완료|'
        r'추진|성장|상승|하락|확대|강화|진출|구축|출범|출시|인수|매각|진행|강화|완화)'
        r'(?=[,\s]|$)'
    )
    best = -1
    for m in ENDINGS.finditer(text[:max_len + 8]):
        if m.end() <= max_len + 3:
            best = m.end()
    if best >= 10:
        return text[:best].strip()

    # 완결어미 없으면 마지막 공백에서 자름
    sp = text[:max_len].rfind(' ')
    if sp >= 10:
        return text[:sp].strip()
    return text[:max_len].strip()


def _validate_and_fix_summary(summary: str, item: Dict) -> str:
    """AI가 생성한 요약 품질 검증.
    압축·절단은 하지 않는다 — AI가 이미 요약한 텍스트를 코드가 자르면
    오히려 잘린 조사·어간으로 끝나는 쓰레기가 나오기 때문.
    조사로 끝나는 줄만 드롭하고, 나머지는 AI 생성 그대로 반환.
    """
    if not summary:
        return _fallback_summary_for_item(item)

    # \\n 리터럴 → 실제 줄바꿈
    summary = summary.replace('\\n', '\n')

    # ▪ 기준 줄 분리
    raw_lines = re.split(r'\n', summary)
    lines = []
    for ln in raw_lines:
        ln = ln.strip()
        if not ln:
            continue
        if not ln.startswith('▪'):
            ln = '▪ ' + ln.lstrip('- •·')
        lines.append(ln)

    BAD_ENDINGS = re.compile(
        r'[을를이가의에서가운데통해위해등및하는하며이며되며하고이고되고으로서]$'
    )
    good_lines = []
    seen_content = set()
    for ln in lines:
        content = re.sub(r'^▪\s*', '', ln).strip()
        if len(content) < 8:
            continue
        # 조사로 끝나는 줄 — AI 생성 실패로 간주, 드롭
        if BAD_ENDINGS.search(content):
            continue
        key = content[:15]
        if key in seen_content:
            continue
        seen_content.add(key)
        good_lines.append(f"▪ {content}")

    if not good_lines:
        return _fallback_summary_for_item(item)
    if len(good_lines) == 1:
        return good_lines[0]
    return '\n'.join(good_lines[:2])


def _fallback_summary_for_item(item: Dict) -> str:
    """개별 기사용 rule-based 폴백. 완결문장 추출 후 45자로 압축."""
    desc  = item.get('description', '')
    title = item.get('title', '')

    # 노이즈 제거
    cleaned = re.sub(r'\[[^\]]{1,40}(?:제공|사진|=|기자)[^\]]{0,20}\]', '', desc)
    cleaned = re.sub(r'사진=[^\s,]+\s*', '', cleaned)
    cleaned = re.sub(r'[◆▶▷■●▲◇★☆※]\s*', '', cleaned)
    cleaned = re.sub(r'[가-힣]{2,4}\s*기자\s*[=·]?\s*', '', cleaned)
    cleaned = re.sub(r'^[가-힣A-Za-z0-9·\-\s]{2,30}(?:사옥|빌딩|건물)\s*\.?\s*', '', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()

    # 완결문장 분리
    sentences = re.split(r'(?<=[다됩됐습니겠요음])\.\s+', cleaned)
    sentences = [s.strip().rstrip('.') for s in sentences if len(s.strip()) >= 15]

    COMPLETE = re.compile(
        r'[다됩됐습니겠요음]$|'
        r'(?:확보|예정|전망|체결|선정|발표|착공|준공|완료|추진|성장|상승|하락|확대|강화|진출|구축|출범)$'
    )
    complete = [s for s in sentences if COMPLETE.search(s)]

    if complete:
        line1 = _compress_to_gaejoesik(complete[0], 45)
        if len(complete) >= 2:
            line2 = _compress_to_gaejoesik(complete[1], 45)
            if line2 and line2[:12] != line1[:12]:
                return f"▪ {line1}\n▪ {line2}"
        if line1:
            return f"▪ {line1}"

    # 완결문장 없으면 제목(정제) 45자 이내
    t = re.sub(r'^\[[^\]]{1,40}\]\s*', '', title).strip()
    t = re.sub(r'[【】《》「」\'\'""…]', '', t).strip()
    return f"▪ {_compress_to_gaejoesik(t, 45)}"


def _apply_fallback_summaries(items: List[Dict]) -> List[Dict]:
    """모든 AI 실패 시 전체 아이템에 rule-based 폴백 적용"""
    for item in items:
        item['ai_summary'] = _fallback_summary_for_item(item)
    return items



def _parse_curate_json(result_text: str) -> Optional[List[Dict]]:
    """AI 응답에서 JSON 배열 파싱 (3단계 내성)"""
    try:
        clean = re.sub(r'```(?:json)?|```', '', result_text).strip()
        for old_q, new_q in [('\u201c', '"'), ('\u201d', '"'), ('\u2018', "'"), ('\u2019', "'")]:
            clean = clean.replace(old_q, new_q)
        json_match = re.search(r'\[[\s\S]*\]', clean)
        if not json_match:
            return None
        json_str = json_match.group()
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            json_str = re.sub(r'[\x00-\x1f\x7f]', ' ', json_str)
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                return json.loads(sanitize_json_strings(json_str))
    except Exception:
        return None


def _apply_curations(batch: List[Dict], curations: List[Dict],
                     model_name: str = "unknown") -> List[Dict]:
    """파싱된 curation 결과를 뉴스 아이템에 적용"""
    curated = []
    for cur in curations:
        idx = cur.get("index", 0) - 1
        if 0 <= idx < len(batch):
            item = batch[idx].copy()
            # AI가 직접 작성한 완결 문장 — word_cut 없이 그대로 사용
            item["ai_model"]         = model_name   # ← 어떤 AI가 처리했는지 기록
            item["ai_short_summary"] = (cur.get("short_summary") or "").strip()
            item["ai_summary"]       = (cur.get("summary")       or "").strip()
            item["ai_comment"]       = cur.get("comment", "")
            item["ai_reason"]        = cur.get("reason",  "")
            item["ai_relevance"]     = cur.get("relevance", "MEDIUM")
            item["ai_tags"]          = cur.get("tags", [])
            curated.append(item)
    return curated


def _build_combined_prompt(batch: List[Dict], cat_name: str, category: Dict) -> str:
    """선별 + 요약을 단일 호출로 처리하는 통합 프롬프트.

    - 기존 _build_curate_prompt (선별) + _build_summary_prompt (요약) 를 1개로 통합
    - 입력 기사에 full_body 가 있으면 본문 우선 사용 (Issue 1 크롤링 결과 활용)
    - 선별된 기사만 JSON 에 포함 (미선별은 index 자체를 생략)
    """
    cat_id         = category.get("id", "")
    cat_definition = category.get("ai_definition", cat_name)

    key_focus_map = {
        "real_estate_market": "캡레이트(%), 거래규모(억원), 투자수익률(%)",
        "office_lease":       "공실률(%), 임대료(원/평), 권역(CBD/GBD/YBD/BBD)",
        "asset_transaction":  "거래규모(억/조원), 빌딩명, 매입·매각 주체",
        "corporate_space":    "이전지역, 면적(평), 워크플레이스 전략",
        "industrial_asset":   "공실률(%), 임대료(원/평), 거래규모(억원)",
        "smart_esg":          "인증등급, 에너지절감(%), 빌딩명, 적용기술",
    }
    key_focus = key_focus_map.get(cat_id, "핵심 수치와 주체")

    def _item_text(item: Dict) -> str:
        body = item.get('full_body', '').strip()
        return body[:900] if body else item.get('description', '')[:400]

    news_text = "\n\n".join(
        f"[{i+1}] {item['source']} | {item['title']}\n{_item_text(item)}"
        for i, item in enumerate(batch)
    )

    return f"""한국 CRE 뉴스 에디터입니다. 아래 {len(batch)}건에서 [{cat_name}] 카테고리 기사를 선별하고, 선별된 기사만 개조식으로 요약하세요.

【카테고리 정의】
{cat_definition}

{news_text}

━━━━━━━━━━━━━━━━━━━━
【STEP 1 — 선별】
- 기사의 핵심 주제가 카테고리 정의에 부합하면 선별.
- 주택·아파트·분양·주식시황·해외 부동산만 다루는 기사는 제외.
- 애매하면 제외. 명확히 부합하는 것만 선별.

【STEP 2 — 선별된 기사만 요약】
우선 포함할 수치: {key_focus}
1. summary: "▪ "로 시작하는 개조식 줄. 최대 2줄. 각 줄 40자 이하.
   - 줄1: 핵심사실 (주체+행위+수치)
   - 줄2: 줄1과 다른 정보 (위치·배경·전망). 없으면 1줄만.
2. short_summary: summary 핵심을 1줄로. 40자 이하.
3. 완결어미 필수: ~했다 / ~됐다 / ~예정 / ~체결 / ~선정 / ~전망 / ~확대
4. 조사(을·를·이·가·의·에서·통해·위해·등·및)로 끝나는 줄 출력 금지.
5. 원문 문장 그대로 복사 금지.

【예시】
기사: "DL이앤씨가 코리안리 신사옥 4000억 공사 우선협상자로 선정됐다. 종로 수송동에 21층 규모로 2026년 착공 예정."
  summary: "▪ DL이앤씨, 코리안리 신사옥 4,000억 우선협상자 선정\\n▪ 종로 수송동 21층, 2026년 5월 착공 예정"
  short_summary: "DL이앤씨, 코리안리 신사옥 4,000억 공사 수주"

━━━━━━━━━━━━━━━━━━━━
출력 — 선별된 기사만 JSON 배열 (코드블록 없이):
[
  {{
    "index": <기사번호 정수>,
    "relevance": "HIGH",
    "summary": "▪ 줄1\\n▪ 줄2",
    "short_summary": "한줄요약",
    "comment": "<CRE 실무자 한줄평>",
    "tags": ["태그1", "태그2"]
  }}
]
선별 기사 없으면 [] 출력."""


def _apply_combined_results(batch: List[Dict], results: List[Dict],
                             model_name: str = "unknown") -> List[Dict]:
    """통합 선별+요약 결과를 batch 아이템에 적용.

    _apply_curations + _apply_summaries 를 합친 단일 함수.
    AI 생성 요약은 절단하지 않고 그대로 사용.
    """
    selected = []
    for res in results:
        idx = res.get("index", 0) - 1
        if not (0 <= idx < len(batch)):
            continue
        item = batch[idx].copy()

        # ── 큐레이션 메타 ─────────────────────────────────────────
        item["ai_model"]     = model_name
        item["ai_relevance"] = res.get("relevance", "HIGH")
        item["ai_comment"]   = res.get("comment", "")
        item["ai_reason"]    = res.get("reason", "")
        item["ai_tags"]      = res.get("tags", [])

        # ── 상세요약 ──────────────────────────────────────────────
        raw_summary = res.get("summary", "").strip()
        item["ai_summary"] = _validate_and_fix_summary(raw_summary, item)

        # ── 한줄요약 ──────────────────────────────────────────────
        raw_short = res.get("short_summary", "").strip()
        # 비어있으면 summary 첫 줄에서 추출
        if not raw_short or len(raw_short) < 5:
            first_line = re.split(r'\n', item["ai_summary"])[0]
            raw_short  = re.sub(r'^▪\s*', '', first_line).strip()
        item["ai_short_summary"] = raw_short

        selected.append(item)
    return selected


def ai_curate(config: NewsConfig, news_items: List[Dict],
              category: Dict) -> List[Dict]:
    """Claude Sonnet 단일 호출로 선별+요약 동시 처리.
    Claude 실패 시 Gemini 폴백, 모두 실패 시 rule-based fallback + AI 요약."""
    if not news_items:
        return []

    batch    = news_items[:20]
    cat_name = category.get("name", "상업용 부동산")

    # ── 크롤링을 API 호출 전에 실행 (선별+요약 통합 프롬프트에 본문 제공) ──
    _enrich_with_article_body(batch)

    prompt = _build_combined_prompt(batch, cat_name, category)

    # ── 1순위: Claude Sonnet (선별+요약 통합) ─────────────────────
    # 큐레이션+요약 동시 처리이므로 Haiku 아닌 Sonnet 사용
    if config.CLAUDE_API_KEY:
        try:
            import anthropic
            CLAUDE_MODELS = list(dict.fromkeys([
                config.CLAUDE_SUMMARY_MODEL,   # claude-sonnet-4-6 (통합 기본)
                "claude-sonnet-4-5",
                "claude-3-5-sonnet-20241022",
                config.CLAUDE_MODEL,           # Haiku 최후 폴백
            ]))
            client = anthropic.Anthropic(api_key=config.CLAUDE_API_KEY)
            for model_name in CLAUDE_MODELS:
                try:
                    print(f"  🤖 Claude 선별+요약 통합... 모델={model_name} ({len(batch)}건)")
                    resp = client.messages.create(
                        model=model_name, max_tokens=4000, temperature=0.15,
                        messages=[{"role": "user", "content": prompt}]
                    )
                    result_text = resp.content[0].text.strip()
                    print(f"  📝 응답: {result_text[:100]}")
                    results = _parse_curate_json(result_text)
                    if results is not None:
                        selected = _apply_combined_results(batch, results, model_name=model_name)
                        print(f"  ✅ Claude 통합 완료: {len(selected)}건 선별+요약")
                        return selected
                    print(f"  ⚠️ {model_name} JSON 파싱 실패 → 다음 모델")
                except Exception as e:
                    err = str(e)
                    if '529' in err or 'overloaded' in err.lower():
                        print(f"  ⚠️ {model_name} 과부하 → 다음 모델")
                    elif '404' in err or 'not found' in err.lower():
                        print(f"  ⚠️ {model_name} 없음 → 다음 모델")
                    else:
                        print(f"  ⚠️ Claude {model_name} 실패: {type(e).__name__}: {e}")
        except ImportError:
            print("  ❌ anthropic 패키지 없음 — pip install anthropic")

    # ── 2순위: Gemini (선별+요약 통합) ───────────────────────────
    if config.GEMINI_API_KEY:
        try:
            import google.generativeai as genai
            genai.configure(api_key=config.GEMINI_API_KEY)
            GEMINI_MODELS = list(dict.fromkeys([
                config.GEMINI_MODEL,
                "gemini-2.0-flash-lite",
                "gemini-1.5-flash-002",
                "gemini-1.5-flash-001",
            ]))
            for model_name in GEMINI_MODELS:
                MAX_RETRIES = 3
                for attempt in range(MAX_RETRIES):
                    try:
                        print(f"  🤖 Gemini 선별+요약 통합... 모델={model_name} ({len(batch)}건)" +
                              (f" [재시도 {attempt}]" if attempt else ""))
                        gemini_model = genai.GenerativeModel(model_name)
                        resp = gemini_model.generate_content(
                            prompt,
                            generation_config=genai.GenerationConfig(
                                temperature=0.15, max_output_tokens=4000,
                            )
                        )
                        result_text = resp.text.strip()
                        print(f"  📝 응답: {result_text[:100]}")
                        results = _parse_curate_json(result_text)
                        if results is not None:
                            selected = _apply_combined_results(batch, results, model_name=model_name)
                            print(f"  ✅ Gemini 통합 완료: {len(selected)}건 선별+요약")
                            return selected
                        print(f"  ⚠️ JSON 파싱 실패 → 다음 모델")
                        break
                    except Exception as e:
                        err_str = str(e)
                        if '429' in err_str or 'RESOURCE_EXHAUSTED' in err_str:
                            is_daily = any(k in err_str.lower() for k in ['limit: 0', 'quota', 'daily'])
                            if is_daily:
                                print(f"  ⚠️ {model_name} 일일 한도 소진 → 다음 모델")
                                break
                            else:
                                delay = 30
                                m = re.search(r'retryDelay["\s:]+(\d+)', err_str)
                                if m:
                                    delay = max(int(m.group(1)) // 1000 + 5, 15)
                                print(f"  ⏳ {delay}초 대기 후 재시도 ({attempt+1}/{MAX_RETRIES})")
                                time.sleep(delay)
                                continue
                        elif '404' in err_str or 'not found' in err_str.lower():
                            print(f"  ⚠️ {model_name} 모델 없음 → 다음 모델")
                            break
                        else:
                            print(f"  ⚠️ {model_name} 오류: {type(e).__name__}: {e}")
                            break
        except ImportError:
            print("  ❌ google-generativeai 패키지 없음 — pip install google-generativeai")
        except Exception as e:
            print(f"  ⚠️ Gemini 초기화 실패: {e}")

    # ── 모두 실패 → 키워드 점수 선별 + AI 요약 (batch 이미 크롤링됨) ──
    print("  ❌ 통합 API 모두 실패 — 키워드 점수 기반 선별 후 AI 요약 시도")
    fallback_items = _fallback_curate(news_items)
    # batch 가 이미 _enrich_with_article_body() 처리됐으므로 재호출 불필요
    # (fallback_items 는 news_items 의 slice 참조 → full_body 공유)
    return _summarize_with_claude_sonnet(config, fallback_items, category)

def _fallback_curate(news_items: List[Dict]) -> List[Dict]:
    """AI API 실패 시 폴백 — 완결된 문장만 사용, 미완결 텍스트 강제 처리"""
    print("  ⚠️ [FALLBACK] AI 없이 키워드 점수 기반 큐레이션")

    # 완결 어미 패턴
    COMPLETE_ENDINGS = re.compile(r'[다요됩됐습니겠][\.!?\s]*$')
    # 완결 지점 찾기용 패턴 (위치 탐색)
    SENTENCE_END = re.compile(r'(?:다\.|됩니다\.?|했다\.?|있다\.?|된다\.?|한다\.?|됐다\.?|겠다\.?|습니다\.?)')

    def cut_to_last_complete(text: str) -> str:
        """텍스트를 마지막 완결 어미 위치까지만 잘라냄"""
        if not text:
            return text
        # 이미 완결 어미로 끝나면 그대로
        if COMPLETE_ENDINGS.search(text):
            return text.strip()
        # 마지막 완결 지점 찾기
        last_pos = -1
        for m in SENTENCE_END.finditer(text):
            last_pos = m.end()
        if last_pos > 20:
            return text[:last_pos].strip()
        # 완결 지점 없음 → 빈 문자열 반환 (caller가 대체값 사용)
        return ""

    for item in news_items:
        raw_title = item.get("title", "")
        # 제목 정제
        title = re.sub(r'^\[[^\]]{1,40}\]\s*', '', raw_title).strip()
        title = re.sub(r'[\[【】《》\]]', '', title).strip()
        # 제목의 말줄임표는 제거하지 않음 (원문 그대로 유지)

        # description 정제
        desc = item.get("description", "")
        cleaned = clean_description(desc)
        # 끝 말줄임 제거
        cleaned = re.sub(r'\s*[\.…⋯]{2,}\s*$', '', cleaned).strip()
        # 중간 말줄임(...) 처리: 말줄임 이전 완결 문장까지만 사용
        mid_el = re.search(r'[\.…⋯]{2,}', cleaned)
        if mid_el:
            before = cleaned[:mid_el.start()].strip()
            last_end = -1
            for pat in ['됩니다.', '됩니다', '했다.', '했다', '있다.', '있다',
                        '된다.', '된다', '한다.', '한다', '됐다.', '됐다',
                        '겠다.', '겠다', '습니다.', '습니다', '다.', '다,']:
                pos = before.rfind(pat)
                if pos > last_end:
                    last_end = pos + len(pat)
            if last_end > 20:
                cleaned = before[:last_end].strip()
            elif len(before) > 20:
                cleaned = before

        # ── ai_short_summary ─────────────────────────────────────
        short = ""
        if cleaned:
            work = cleaned
            # 접속어 시작이면 다음 완결 문장으로 이동
            if re.match(r'^(이후|또한|반면|하지만|그러나|따라서|이에|하며|로서|한편|같은|이런)\s', work):
                nxt = re.search(r'[다요됩됐]\s', work[10:])
                if nxt:
                    work = work[10 + nxt.end():].strip()
            # 완결 어미 있는 첫 문장 추출 (20~80자)
            m = re.search(r'^(.{20,80}?[다요됩됐음])[\s\.,]', work + ' ')
            if m and m.group(1).strip() != title[:len(m.group(1).strip())]:
                short = m.group(1).strip()

        # 완결 문장 없거나 제목과 동일하면 → 빈값 (title로 대체)
        if not short or short == title or title.startswith(short[:15]):
            short = ""  # index.html의 _xlsShort에서 title 사용

        item["ai_short_summary"] = short

        # ── ai_summary: 공통 폴백 함수 사용 ─────────────────────
        item["ai_summary"]   = _fallback_summary_for_item(item)
        item["ai_model"]     = "fallback"
        item["ai_comment"]   = ""
        item["ai_reason"]    = ""
        item["ai_relevance"] = "MEDIUM"
        item["ai_tags"]      = []

    valid = [item for item in news_items if item.get("relevance_score", 0) > 0]
    if not valid:
        valid = news_items
    return valid




# ================================================================
#  카테고리 간 중복 제거 (cross-category dedup)
# ================================================================

# 카테고리 특화도 우선순위 — 숫자가 낮을수록 더 특화(우선 보존)
# 동일 기사가 여러 카테고리에 배정됐을 때 가장 특화된 카테고리 하나만 남긴다
CAT_PRIORITY = {
    "asset_transaction":   1,  # 개별 딜 기사 — 가장 구체적
    "office_lease":        2,  # 임대차 특화
    "industrial_asset":    3,  # 물류·DC 특화
    "corporate_space":     4,  # 기업 공간 전략
    "smart_esg":           5,  # ESG·프롭테크
    "real_estate_market":  6,  # 시장·정책 — 가장 포괄적
}


def cross_category_dedup(all_categories_output: List[Dict]) -> List[Dict]:
    """
    전체 카테고리 결과에서 동일·유사 기사를 제거한다.

    기준:
    1. hash_id 완전 동일 → CAT_PRIORITY 낮은(더 특화된) 카테고리 하나만 보존
    2. 제목 유사도 >= 0.72 → 마찬가지로 특화도 높은 쪽 보존
    3. 제거 후 카테고리별 count·rank 재정렬
    """
    TITLE_SIM_THRESHOLD = 0.50  # 카테고리 간 중복도 공격적으로 제거 (0.50: GRESB류 유사기사 포함)

    # 전체 기사를 플랫 리스트로 펼침: (cat_index, item_index, item)
    flat: List[tuple] = []
    for ci, cat in enumerate(all_categories_output):
        for ii, item in enumerate(cat["items"]):
            flat.append((ci, ii, item))

    to_remove: set = set()

    # ── 1) hash_id 완전 중복 처리 ───────────────────────────────
    hash_seen: Dict[str, tuple] = {}
    for ci, ii, item in flat:
        h = item.get("hash_id", "")
        if not h:
            continue
        if h in hash_seen:
            prev_ci, prev_ii = hash_seen[h]
            prev_pri = CAT_PRIORITY.get(all_categories_output[prev_ci]["id"], 99)
            curr_pri = CAT_PRIORITY.get(all_categories_output[ci]["id"],      99)
            if curr_pri < prev_pri:
                to_remove.add((prev_ci, prev_ii))
                hash_seen[h] = (ci, ii)
            else:
                to_remove.add((ci, ii))
        else:
            hash_seen[h] = (ci, ii)

    # ── 2) 제목 유사도 중복 처리 ────────────────────────────────
    survivors = [(ci, ii, item) for ci, ii, item in flat if (ci, ii) not in to_remove]
    for i in range(len(survivors)):
        ci_a, ii_a, item_a = survivors[i]
        if (ci_a, ii_a) in to_remove:
            continue
        for j in range(i + 1, len(survivors)):
            ci_b, ii_b, item_b = survivors[j]
            if (ci_b, ii_b) in to_remove:
                continue
            if text_similarity(item_a["title"], item_b["title"]) >= TITLE_SIM_THRESHOLD:
                pri_a = CAT_PRIORITY.get(all_categories_output[ci_a]["id"], 99)
                pri_b = CAT_PRIORITY.get(all_categories_output[ci_b]["id"], 99)
                if pri_b < pri_a:
                    to_remove.add((ci_a, ii_a))
                else:
                    to_remove.add((ci_b, ii_b))

    if not to_remove:
        print("  ✅ 카테고리 간 중복 없음")
        return all_categories_output

    # ── 3) 제거 적용 + 로그 + rank 재정렬 ──────────────────────
    removed_total = 0
    for ci, cat in enumerate(all_categories_output):
        before = len(cat["items"])
        cat["items"] = [
            item for ii, item in enumerate(cat["items"])
            if (ci, ii) not in to_remove
        ]
        for rank, item in enumerate(cat["items"], 1):
            item["rank"] = rank
        removed = before - len(cat["items"])
        removed_total += removed
        if removed:
            print(f"  🗑️  [{cat['name']}] {removed}건 제거 → {len(cat['items'])}건 남음")
        cat["count"] = len(cat["items"])

    print(f"  📊 cross-category 중복 제거: 총 {removed_total}건")
    return all_categories_output

# ================================================================
#  메인 수집 파이프라인 (카테고리 단위)
# ================================================================

def collect_category(config: NewsConfig, category: Dict,
                     date_from: datetime, date_to: datetime) -> List[Dict]:

    print(f"\n{'='*60}")
    print(f"📰 [{category['icon']} {category['name']}] 수집 시작")
    print(f"📅 기간: {date_from.strftime('%Y-%m-%d')} ~ {date_to.strftime('%Y-%m-%d')}")
    print(f"{'='*60}")

    # ── 1단계: 수집 ──────────────────────────────────────────────
    print("\n[1단계] 뉴스 수집...")
    # Naver: 핵심 쿼리 10개만 사용 (쿼리 과다 시 중복 기사로 AI 풀이 항상 채워짐)
    naver_queries = category["search_queries"][:10]
    naver_items = fetch_naver_news(
        config, naver_queries, date_from, date_to
    )
    rss_queries = category.get("rss_queries", category["search_queries"])
    rss_items   = fetch_google_rss(rss_queries, date_from, date_to)

    all_items = naver_items + rss_items

    # ── 더벨 모바일 보강 (관련 카테고리만) ──────────────────────
    if category["id"] in _THEBELL_CATEGORIES:
        thebell_items = fetch_thebell_mobile(date_from, date_to)
        all_items += thebell_items

    # ── Safety net: 날짜 범위 완전 재검증 ──────────────────────
    # Google RSS가 날짜 필터를 무시 + Naver도 date_to 이후 기사 유입 가능
    # pub_date는 이미 isoformat() 문자열 → datetime.fromisoformat으로 직접 파싱
    def _safe_parse_stored(pub_str: str):
        """저장된 isoformat pub_date 파싱 (timezone aware 보장)"""
        if not pub_str:
            return None
        try:
            dt = datetime.fromisoformat(pub_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=KST)
            return dt
        except Exception:
            return parse_date(pub_str)  # fallback

    before_count = len(all_items)
    all_items = [
        item for item in all_items
        if _safe_parse_stored(item.get("pub_date", "")) is None
        or _safe_parse_stored(item.get("pub_date", "")) <= date_to
    ]
    removed = before_count - len(all_items)
    if removed:
        print(f"  🗓️ date_to 초과 기사 {removed}건 제거")

    thebell_count = len([i for i in all_items if i.get("source") == "더벨"])
    print(f"\n  📊 총 수집: {len(all_items)}건 "
          f"(Naver {len(naver_items)} + RSS {len(rss_items)}"
          f"{f' + 더벨 {thebell_count}' if thebell_count else ''}, 날짜 검증 후)")

    if not all_items:
        print(f"  📭 {date_from.strftime('%Y-%m-%d')} ~ {date_to.strftime('%Y-%m-%d')} "
              f"범위 내 수집된 뉴스 없음")
        return []

    # ── 2단계: 필터링 & 중복 제거 ────────────────────────────────
    print("\n[2단계] 필터링 & 중복 제거...")
    filtered = filter_and_dedupe(all_items, category, config.SIMILARITY_THRESHOLD)
    print(f"  ✅ 필터링 후: {len(filtered)}건")

    if not filtered:
        print("  📭 관련 뉴스 없음")
        return []

    # ── 3단계: AI 큐레이션 ────────────────────────────────────────
    print("\n[3단계] AI 큐레이션...")
    curated = ai_curate(config, filtered, category)

    # ── 4단계: 최종 정리 — pub_date ISO 문자열 내림차순 ─────────
    curated.sort(key=lambda x: (x.get("pub_date") or ""), reverse=True)
    for i, item in enumerate(curated):
        item["rank"]          = i + 1
        item["category"]      = category["id"]
        item["category_name"] = category["name"]
        item["category_icon"] = category["icon"]
        item.pop("relevance_score", None)

    return curated[:config.MAX_NEWS_PER_CATEGORY]


# ================================================================
#  진입점
# ================================================================

def main():
    _init_log_file()   # ← 로그 파일 초기화 (data/collect_news.log)

    parser = argparse.ArgumentParser(description="CRE Daily Brief 뉴스 수집 v2.0")
    parser.add_argument("--days",       type=int, default=3,
                        help="최근 N일 (기본: 3)")
    parser.add_argument("--from-date",  dest="date_from",
                        help="시작일 YYYY-MM-DD")
    parser.add_argument("--to-date",    dest="date_to",
                        help="종료일 YYYY-MM-DD")
    args = parser.parse_args()

    # 날짜 범위 결정
    if args.date_from and args.date_to:
        date_from = datetime.strptime(
            args.date_from, "%Y-%m-%d"
        ).replace(hour=0, minute=0, second=0, tzinfo=KST)
        date_to = datetime.strptime(
            args.date_to, "%Y-%m-%d"
        ).replace(hour=23, minute=59, second=59, tzinfo=KST)
    else:
        date_to   = NOW.replace(hour=23, minute=59, second=59)
        date_from = (NOW - timedelta(days=args.days)).replace(hour=0, minute=0, second=0)

    config = NewsConfig()

    print(f"\n🏢 CRE Daily Brief — 뉴스 수집 v2.0  (Google News RSS)")
    print(f"⏰ {NOW.strftime('%Y-%m-%d %H:%M:%S KST')}")
    print(f"📅 수집 기간: {date_from.strftime('%Y-%m-%d')} ~ "
          f"{date_to.strftime('%Y-%m-%d')}")
    print(f"\n🔑 API 키 상태:")
    print(f"  Naver:  {'✅' if config.NAVER_CLIENT_ID else '❌ 없음'}")
    print(f"  Claude: {'✅' if config.CLAUDE_API_KEY  else '❌ 없음'}")
    print(f"  Google News RSS: ✅ (API 키 불필요)")

    if not config.NAVER_CLIENT_ID and not config.CLAUDE_API_KEY:
        print("\n⚠️  Naver API 키 없음 — RSS 단독 수집으로 진행합니다")

    # Naver API 연결 확인
    if config.NAVER_CLIENT_ID:
        print(f"\n🔌 Naver API 연결 테스트...")
        try:
            test = requests.get(
                "https://openapi.naver.com/v1/search/news.json",
                params={"query": "오피스", "display": 1},
                headers={
                    "X-Naver-Client-Id":     config.NAVER_CLIENT_ID,
                    "X-Naver-Client-Secret": config.NAVER_CLIENT_SECRET,
                },
                timeout=10, verify=False
            )
            print(f"  Naver: HTTP {test.status_code} "
                  f"{'✅' if test.status_code == 200 else '❌'}")
        except Exception as e:
            print(f"  Naver: ❌ {e}")

    # Google News RSS 연결 확인
    print(f"🔌 Google News RSS 연결 테스트...")
    try:
        test_url = _build_rss_url("오피스 공실률")
        test = requests.get(
            test_url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10, verify=False
        )
        print(f"  Google RSS: HTTP {test.status_code} "
              f"{'✅' if test.status_code == 200 else '❌'}")
        if test.status_code == 200:
            try:
                root  = ET.fromstring(test.content)
                cnt   = len(root.findall('.//item'))
                print(f"  샘플 결과: {cnt}건")
            except ET.ParseError:
                print(f"  XML 파싱 실패")
    except Exception as e:
        print(f"  Google RSS: ❌ {e}")

    # ── 전체 카테고리 수집 ──────────────────────────────────────
    all_categories_output = []
    total_count = 0

    for category in ALL_CATEGORIES:
        cat_news = collect_category(config, category, date_from, date_to)
        all_categories_output.append({
            "id":    category["id"],
            "name":  category["name"],
            "icon":  category["icon"],
            "label": category["label"],
            "count": len(cat_news),
            "items": cat_news,
        })
        total_count += len(cat_news)

    # ── 카테고리 간 중복 제거 ────────────────────────────────────
    print(f"\n{'='*60}")
    print("🔍 카테고리 간 중복 제거 중...")
    all_categories_output = cross_category_dedup(all_categories_output)
    total_count = sum(cat["count"] for cat in all_categories_output)

    # ── 저장 ────────────────────────────────────────────────────
    output = {
        "generated_at": NOW.strftime("%Y-%m-%d %H:%M:%S"),
        "date_from":    date_from.strftime("%Y-%m-%d"),
        "date_to":      date_to.strftime("%Y-%m-%d"),
        "categories":   all_categories_output,
        "total_count":  total_count,
    }

    os.makedirs("data", exist_ok=True)
    with open("data/news.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*60}")
    print(f"✅ 완료: {total_count}건 → data/news.json")
    print(f"{'='*60}")

    for cat_data in all_categories_output:
        print(f"\n{cat_data['icon']} [{cat_data['name']}] {cat_data['count']}건")
        for item in cat_data["items"][:3]:
            print(f"  {item['rank']}. [{item.get('ai_relevance','')}] "
                  f"{item['title'][:50]}...")


if __name__ == "__main__":
    main()
