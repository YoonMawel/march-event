# -*- coding: utf-8 -*-
"""
마스토돈 전투 커맨드 로그봇 (단일 파일 + 큐/워커 + 429 재시도 버전)

기능:
- 봇 계정에 들어오는 멘션 중, 전투 커맨드 형식([공격 1] 등)이 포함된 것만 골라 처리
- 러너 닉네임, 계정, 멘션 내용, 커맨드, 대상, 형식 오류를 구글 시트에 누적 기록
- 형식 검사:
    1) 커맨드 대괄호 누락 여부
    2) [공격/1] 같은 슬래시(/) 잘못 사용
    3) 정의되지 않은 커맨드 문자열
    4) 대상 대괄호에 / 가 없는 경우 (최소한의 검사)
- 구글 시트 기록은 큐에 쌓고, 워커 스레드가 천천히 처리
- 429(Too Many Requests) 발생 시 backoff 하며 재시도
"""

import logging
import re
import time
import threading
import queue
from datetime import datetime

import pytz
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError
from mastodon import Mastodon, StreamListener

# ============================================================
# 설정 영역 (네 환경에 맞게 수정)
# ============================================================

# Mastodon 설정
MASTODON_BASE_URL = "https://marchen1210d.site"   # 예: "https://marchen1210d.site"
MASTODON_ACCESS_TOKEN = "x0FF-jzuurbAK6DrYqgxp9D-zoYR6jLy1OtzNMfS34g"

# Google Service Account JSON 경로
GOOGLE_SERVICE_JSON = "march-credential.json"

# 구글 시트 설정
SHEET_NAME = "전투로그문서"  # 문서 제목
TAB_LOG    = "전투로그"      # 탭 이름

# 타임존
KST = pytz.timezone("Asia/Seoul")

# 유효한 커맨드 리스트 (띄어쓰기는 이 형태를 기준)
VALID_COMMANDS = {
    "공격 1",
    "공격 2",
    "방어 1",
    "방어 2",
    "치유 1",
    "치유 2",
    "지원 1",
    "지원 2",
    "지원 3",
    "지원 4",
    "사용/아티팩트",
}

REQUIRED_TARGET_MIN = {
    "공격 1": 0,
    "공격 2": 0,
    "방어 1": 1,
    "방어 2": 2,
    "치유 1": 1,
    "치유 2": 2,
    "지원 1": 1,
    "지원 2": 1,
    "지원 3": 0,
    "지원 4": 0
}

# 잘못된 슬래시 커맨드 패턴 예: "공격/1", "방어/2", "치유/1", "지원/3"
BAD_SLASH_CMD_RE = re.compile(r"^(공격|방어|치유|지원)\s*/\s*\d+$")

# 전투 커맨드 후보를 대충 골라낼 키워드
TRIGGER_KEYWORDS = ["공격", "방어", "치유", "지원", "사용/아티팩트"]

# 대상이 꼭 필요한 아티팩트 이름 목록 (예시, 나중에 네가 채워넣으면 됨)
REQUIRES_TARGET_ARTIFACTS = {
    "아티팩트_지원"
}

# HTML 태그 제거용 정규식
HTML_TAG_RE    = re.compile(r"<[^>]+>")
# 대괄호 안 내용 추출용 정규식
BRACKET_RE     = re.compile(r"\[([^\]]*)\]")

# 로그 큐 (멘션 내용을 여기 쌓아두고 워커가 처리)
LOG_QUEUE      = queue.Queue(maxsize=1000)

# ============================================================
# 유틸 함수
# ============================================================

def html_to_text(html: str) -> str:
    """아주 단순하게 HTML 태그를 제거하고 공백을 정리."""
    if not html:
        return ""
    text = HTML_TAG_RE.sub(" ", html)
    return " ".join(text.split())


def extract_bracket_tokens(text: str):
    """
    "[공격 1] [해리/지니] (지문)" → ["공격 1", "해리/지니"]
    """
    return BRACKET_RE.findall(text)


def should_handle(text: str) -> bool:
    """
    이 멘션을 전투 커맨드 후보로 처리할지 여부.
    - 대괄호가 최소 하나는 있어야 함
    - 트리거 키워드 중 하나라도 포함되어야 함
    """
    if "[" not in text or "]" not in text:
        return False
    return any(kw in text for kw in TRIGGER_KEYWORDS)

def get_required_target_min(cmd: str, target_tokens, text: str) -> int:
    """
    커맨드와 전체 텍스트를 보고 '최소 몇 개의 대상 대괄호가 필요하냐'를 결정한다.
    - 기본값은 REQUIRED_TARGET_MIN에서 가져오고
    - 사용/아티팩트인 경우에는 텍스트 내 아티 이름 등에 따라 예외 처리 가능
    """
    base = REQUIRED_TARGET_MIN.get(cmd, 0)

    if cmd == "사용/아티팩트":
        # 기본적으로는 대상 없어도 된다고 가정 (필요하면 base를 0으로 세팅)
        required = base

        # 예: 텍스트 안에 "치유의 곡옥" 같은 특정 아티 이름이 들어있으면
        #     대상 1개를 필수로 둔다.
        for name in REQUIRES_TARGET_ARTIFACTS:
            if name in text:
                required = max(required, 1)

        return required

    return base

def validate_command(text: str):
    """
    멘션 전체 텍스트를 받아 전투 커맨드 형식을 검사.

    반환값:
        is_valid: bool
        cmd: str 또는 None (실제 행동 커맨드, 앞뒤 공백 제거 버전)
        targets_str: str (대상 대괄호들을 그대로 붙인 문자열)
        error_msg: str (쉼표로 이어붙인 오류 설명들)

    특이 사항:
        - 첫 대괄호가 [대리 선언]이면, 두 번째 대괄호를 실제 커맨드로 본다.
    """
    errors = []
    tokens = extract_bracket_tokens(text)

    if not tokens:
        errors.append("커맨드 대괄호가 없습니다.")
        return False, None, "", ", ".join(errors)

    # 0) [대리 선언] 프리픽스 처리
    idx_cmd = 0
    is_proxy = False

    first = tokens[0].strip()
    if first == "대리 선언":
        is_proxy = True
        idx_cmd = 1
        if len(tokens) <= 1:
            # [대리 선언]만 있고 실제 커맨드가 없는 경우
            errors.append("대리 선언 뒤에 실제 행동 커맨드가 없습니다.")
            return False, None, "", ", ".join(errors)

    # 1) 커맨드 토큰 결정
    raw_cmd = tokens[idx_cmd]
    cmd = raw_cmd.strip()

    # [공격/1] 같은 잘못된 형식
    if BAD_SLASH_CMD_RE.match(cmd):
        errors.append(
            f"커맨드는 [공격 1]처럼 공백으로 쓰고, 슬래시(/)는 사용하지 않습니다: [{raw_cmd}]"
        )

    # 허용된 커맨드인지 체크
    if cmd not in VALID_COMMANDS:
        errors.append(f"알 수 없는 커맨드입니다: [{raw_cmd}]")

    effective_cmd = cmd if cmd in VALID_COMMANDS else None

    # 2) 대상 토큰들 (대리 선언/커맨드 대괄호를 제외한 나머지)
    target_tokens = tokens[idx_cmd + 1 :]
    targets_str = ""
    if target_tokens:
        targets_str = "".join(f"[{t}]" for t in target_tokens)

    # 3) 대상 형식 및 개수 검사
    # - / 없는 대상만 잡는다
    # - 커맨드별 최소 대상 개수는 get_required_target_min()으로 결정
    if target_tokens:
        for t in target_tokens:
            if "/" not in t:
                errors.append(f"대상 대괄호 안에 '/'가 없습니다: [{t}]")

    # "최소 N개의 대상이 필요" 조건 검사
    required_min = 0
    if effective_cmd is not None:
        required_min = get_required_target_min(effective_cmd, target_tokens, text)

    if required_min > 0:
        if len(target_tokens) < required_min:
            if target_tokens:
                errors.append(
                    f"커맨드 [{cmd}] 에는 최소 {required_min}개의 대상 대괄호가 필요합니다. "
                    f"(현재 {len(target_tokens)}개)"
                )
            else:
                errors.append(
                    f"커맨드 [{cmd}] 에는 최소 {required_min}개의 대상 대괄호가 필요합니다. "
                    f"현재 대상이 전혀 지정되지 않았습니다."
                )

    # 4) 최종 결과
    is_valid = len(errors) == 0
    error_msg = ", ".join(errors)

    # cmd는 '실제 행동 커맨드'만 반환 (대리 선언 여부는 여기선 따로 안 기록)
    return is_valid, (effective_cmd), targets_str, error_msg


# ============================================================
# 구글 시트 관련
# ============================================================

_SHEET_CACHE = None  # 전역 워크시트 캐시

def get_sheet():
    """구글 시트 워크시트를 한 번 열어두고 캐시."""
    global _SHEET_CACHE
    if _SHEET_CACHE is not None:
        return _SHEET_CACHE

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(GOOGLE_SERVICE_JSON, scopes=scopes)
    client = gspread.authorize(creds)
    ss = client.open(SHEET_NAME)
    ws = ss.worksheet(TAB_LOG)
    _SHEET_CACHE = ws
    logging.info("Google Sheet 연결 완료: %s / %s", SHEET_NAME, TAB_LOG)
    return ws


def append_log_row(nickname: str, handle: str, text: str,
                   is_valid: bool, cmd: str, targets: str, error_msg: str):
    """전투 로그 한 줄을 시트에 추가."""
    ws = get_sheet()
    ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")

    row = [
        ts,                              # A: 타임스탬프
        nickname,                        # B: 러너 닉네임
        f"@{handle}" if handle else "",  # C: 계정
        text,                            # D: 멘션 본문
        cmd or "",                       # E: 커맨드
        targets or "",                   # F: 대상 대괄호들
        "O" if is_valid else "X",        # G: 형식 정상 여부
        error_msg,                       # H: 오류 사유
    ]

    ws.append_row(row, value_input_option="USER_ENTERED")
    logging.info(
        "시트 기록 완료 | nick=%s handle=%s valid=%s cmd=%s errors=%s",
        nickname, handle, is_valid, cmd, error_msg
    )


# ============================================================
# 로그 워커 (큐 소비 + 429 재시도)
# ============================================================

def log_worker():
    """
    큐에 쌓인 로그를 하나씩 꺼내서 구글 시트에 기록하는 워커.

    - 기본적으로 요청 사이에 짧게 sleep해서 속도 제한
    - 429(Too Many Requests) 발생 시 backoff 하며 재시도
    """
    while True:
        item = LOG_QUEUE.get()  # (nickname, handle, text, is_valid, cmd, targets, error_msg)
        if item is None:
            LOG_QUEUE.task_done()
            break

        nickname, handle, text, is_valid, cmd, targets, error_msg = item

        max_attempts = 5
        delay = 1.0  # 첫 재시도 대기 시간(초)

        for attempt in range(1, max_attempts + 1):
            try:
                append_log_row(nickname, handle, text, is_valid, cmd, targets, error_msg)
                break  # 성공 시 루프 탈출
            except APIError as e:
                # 429가 아니면 그냥 포기
                if "429" not in str(e):
                    logging.exception("시트 API 오류 발생 (429 아님), 재시도하지 않음")
                    break

                logging.warning(
                    "시트 429 오류, %s초 후 재시도 (%d/%d)",
                    delay, attempt, max_attempts
                )
                time.sleep(delay)
                delay *= 2  # backoff
            except Exception:
                logging.exception("시트 기록 중 알 수 없는 오류, 재시도하지 않음")
                break

        # 너무 빠르게 연속해서 쓰지 않도록 기본 속도 제한
        time.sleep(0.7)

        LOG_QUEUE.task_done()


# ============================================================
# Mastodon 리스너
# ============================================================

class BattleLogListener(StreamListener):
    def __init__(self, api: Mastodon):
        super().__init__()
        self.api = api

    def on_notification(self, notification):
        # 멘션만 처리
        if notification.get("type") != "mention":
            return

        status = notification.get("status") or {}
        content_html = status.get("content") or ""
        text = html_to_text(content_html)

        # 전투 커맨드 후보가 아니면 무시
        if not should_handle(text):
            return

        account = status.get("account") or {}
        nickname = account.get("display_name") or account.get("acct") or ""
        handle = account.get("acct") or ""

        is_valid, cmd, targets, error_msg = validate_command(text)

        logging.info(
            "멘션 처리 | nick=%s handle=%s valid=%s cmd=%s targets=%s errors=%s text=%s",
            nickname, handle, is_valid, cmd, targets, error_msg, text
        )

        # 시트에 직접 쓰지 않고 큐에 넣어서 워커가 처리하게 한다.
        try:
            LOG_QUEUE.put_nowait(
                (nickname, handle, text, is_valid, cmd, targets, error_msg)
            )
        except queue.Full:
            logging.error("로그 큐가 가득 찼습니다. 이 멘션은 시트에 기록되지 않습니다.")


# ============================================================
# main
# ============================================================

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    logging.info("Mastodon 연결 시도")
    api = Mastodon(
        api_base_url=MASTODON_BASE_URL,
        access_token=MASTODON_ACCESS_TOKEN,
    )

    # 로그 워커 스레드 시작
    worker_thread = threading.Thread(target=log_worker, daemon=True)
    worker_thread.start()
    logging.info("로그 워커 스레드 시작")

    listener = BattleLogListener(api)

    logging.info("전투 로그 스트림 시작")
    while True:
        try:
            api.stream_user(listener, run_async=False, reconnect_async=True)
        except Exception:
            logging.exception("스트림 에러 발생, 5초 후 재시도")
            time.sleep(5)


if __name__ == "__main__":
    main()
