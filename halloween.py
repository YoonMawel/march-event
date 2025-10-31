# -*- coding: utf-8 -*-
"""
마스토돈 [사탕] 자동봇 (단일 파일, 스트리밍 + 큐/워커, 한국어 헤더, '꽝' 지원)
- 키워드: [사탕]
- 1~10개 랜덤 지급 (단, 스크립트가 [꽝]이면 0개)
- 러너별 1시간 쿨다운 (꽝이어도 0개로 로그 남겨 쿨다운 적용)
- 스트리밍(user stream)으로 '연결 이후' 멘션만 처리
- 로그 시트: '이벤트' 문서 내 '할로윈' 탭 (헤더: 시각, 닉네임, 아이디, 사탕개수)
- 답글 스크립트: '할로윈_스크립트' 탭 A열 (헤더: 문구)에서 랜덤 선택
  • A열 문구에 "[꽝]"이 포함되면 '꽝'으로 처리(사탕 0, 합계 안 증가)
  • {n}은 지급 개수로 치환됨(꽝이면 0으로 치환)
"""

import re
import time
import random
from datetime import datetime
from typing import Optional, Tuple

import pytz
import gspread
from google.oauth2.service_account import Credentials
from mastodon import Mastodon, MastodonAPIError, MastodonNetworkError, StreamListener

# ======================
# 설정값 (요청하신 값 사용)
# ======================
MASTODON_BASE_URL     = "https://marchen1210d.site"
MASTODON_ACCESS_TOKEN = "5okhjnSVE3DJrxa9dHNK93Z5um1gRjPQpW-XufG0CIg"

GOOGLE_SERVICE_JSON = "march-credential.json"  # 서비스계정 JSON 경로
SHEET_NAME = "이벤트"                    # 문서 제목
TAB_LOG    = "할로윈"                    # 로그 탭
TAB_SCRIPT = "할로윈_스크립트"           # 스크립트 탭(A열, 헤더 '문구')

TRIGGER_KEYWORD   = "[사탕]"
MIN_CANDY         = 1
MAX_CANDY         = 10
COOLDOWN_SECONDS  = 3600                 # 1시간
TIMEZONE_NAME     = "Asia/Seoul"
REPLY_VISIBILITY  = "public"             # public / unlisted / private / direct

# 큐/워커 설정(시트 append 직렬화)
import queue, threading
QUEUE_MAXSIZE    = 1024
WORKER_SLEEP_SEC = 0.05

# 간단 HTML 제거용
TAG_RE = re.compile(r"<[^>]+>")

def strip_html(html_text: str) -> str:
    if not html_text:
        return ""
    return TAG_RE.sub("", html_text).strip()

def now_kr_dt() -> datetime:
    return datetime.now(pytz.timezone(TIMEZONE_NAME))

def now_kr_text() -> str:
    # 시트에 기록하는 가독형 시각
    return now_kr_dt().strftime("%Y-%m-%d %H:%M:%S")


# ======================
# 시트 핸들러
# ======================
class SheetClient:
    def __init__(self):
        # Drive 검색 403 방지: drive.readonly 스코프 추가
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.readonly",
        ]
        creds = Credentials.from_service_account_file(GOOGLE_SERVICE_JSON, scopes=scopes)
        self.gc = gspread.authorize(creds)
        self.ss = self.gc.open(SHEET_NAME)  # 제목으로 열기(Drive 스코프 필요)

        self.ws_log = self._ensure_log_tab()
        self.ws_script = self._get_script_tab()

    def _ensure_log_tab(self):
        header = ["시각", "닉네임", "아이디", "사탕개수"]
        try:
            ws = self.ss.worksheet(TAB_LOG)
        except gspread.WorksheetNotFound:
            ws = self.ss.add_worksheet(title=TAB_LOG, rows=2000, cols=10)
            ws.append_row(header, value_input_option="USER_ENTERED")
            return ws
        # 헤더 보정(1행 고정)
        first = ws.row_values(1)
        if first != header:
            try:
                ws.delete_rows(1)
            except Exception:
                pass
            ws.insert_row(header, 1)
        return ws

    def _get_script_tab(self):
        try:
            return self.ss.worksheet(TAB_SCRIPT)
        except gspread.WorksheetNotFound:
            return None

    def _load_script_candidates(self):
        if not self.ws_script:
            return []
        vals = self.ws_script.col_values(1)
        return [v for v in vals[1:] if v.strip()]  # 1행 헤더 제외

    def pick_script(self, n_random: int) -> Tuple[str, bool, int]:
        """
        스크립트 1개를 선택해 (문구, is_miss, award_n)를 반환.
        - 문구에 "[꽝]" 포함시: is_miss=True, award_n=0
        - 아니면: is_miss=False, award_n=n_random
        - {n} 치환은 반환 직전에 적용
        """
        candidates = self._load_script_candidates()
        if not candidates:
            candidates = [
                "할로윈 바구니에서 {n}개를 챙겼다.",
                "달달한 향기를 따라 {n}개의 사탕을 손에 넣었다.",
                "오늘의 횡재! 사탕 {n}개 확보.",
                "[꽝] 텅 빈 그릇만 남았다…",
            ]
        raw = random.choice(candidates)
        is_miss = ("[꽝]" in raw)
        award = 0 if is_miss else n_random
        # 출력용 문구 정리: [꽝] 태그 제거, {n} 치환(꽝이면 0)
        text = raw.replace("[꽝]", "").strip()
        text = text.replace("{n}", str(award))
        return text, is_miss, award

    def append_log(self, display_name: str, acct: str, n: int):
        # n=0 도 기록(꽝 시도도 쿨다운 적용 위해)
        row = [now_kr_text(), display_name or "", acct or "", n]
        self.ws_log.append_row(row, value_input_option="USER_ENTERED")

    def get_last_claim_time_text(self, acct: str) -> Optional[str]:
        # 작은 규모 가정: 전체 기록 후방 탐색 (32명/시간당 1회 수준 충분)
        records = self.ws_log.get_all_records()
        for row in reversed(records):
            if row.get("아이디") == acct:
                return row.get("시각")
        return None


# ======================
# 봇 본체 + 스트리밍 리스너
# ======================
class CandyBot:
    def __init__(self):
        self.m = Mastodon(
            access_token=MASTODON_ACCESS_TOKEN,
            api_base_url=MASTODON_BASE_URL,
            ratelimit_method="pace",
        )
        self.sheets = SheetClient()
        self.trigger_re = re.compile(re.escape(TRIGGER_KEYWORD))

        # 큐 + 워커: 시트 append를 직렬화하여 동시참여 안전 처리
        self.queue = queue.Queue(maxsize=QUEUE_MAXSIZE)
        self.worker = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker.start()

    def _worker_loop(self):
        while True:
            notif = self.queue.get()
            try:
                self._handle_claim_serial(notif)
            except Exception as e:
                print(f"[worker] error: {e}")
            finally:
                self.queue.task_done()
                time.sleep(WORKER_SLEEP_SEC)

    def _cooldown_left(self, last_text: Optional[str]) -> int:
        if not last_text:
            return 0
        try:
            # 로그에는 'YYYY-MM-DD HH:MM:SS' (KST) 텍스트로 저장됨
            last_dt = pytz.timezone(TIMEZONE_NAME).localize(
                datetime.strptime(last_text, "%Y-%m-%d %H:%M:%S")
            )
        except Exception:
            return 0
        elapsed = (now_kr_dt() - last_dt).total_seconds()
        left = int(COOLDOWN_SECONDS - elapsed)
        return max(0, left)

    def handle_claim(self, notif: dict):
        # 스트리밍에서 멘션 감지 시 큐에 적재 → 워커가 순차 처리
        self.queue.put(notif)

    def _handle_claim_serial(self, notif: dict):
        status = notif.get("status") or {}
        account = notif.get("account") or {}
        content = strip_html(status.get("content") or "")

        if not self.trigger_re.search(content):
            return  # 키워드 아님

        acct = account.get("acct") or ""
        display = account.get("display_name") or account.get("username") or acct
        status_id = status.get("id")

        # 쿨다운 확인(꽝이어도 쿨다운 적용)
        last_text = self.sheets.get_last_claim_time_text(acct)
        left = self._cooldown_left(last_text)
        if left > 0:
            mins, secs = divmod(left, 60)
            self._reply(status_id, acct, f"{display} 님, 쿨다운 남음: {mins}분 {secs}초.")
            return

        # 스크립트 선택 → 꽝 여부/지급 개수 결정
        n_random = random.randint(MIN_CANDY, MAX_CANDY)
        script_text, is_miss, award_n = self.sheets.pick_script(n_random)

        # 로그 기록: 꽝이면 0, 아니면 award_n
        self.sheets.append_log(display, acct, award_n)

        # 응답
        if is_miss:
            self._reply(status_id, acct, f"{display}이/가 호박을 잡기 위해 움직인다 . . . \n\n {script_text}")
        else:
            self._reply(status_id, acct, f"{display}이/가 호박을 잡기 위해 움직인다 . . . \n\n {script_text}")

    def _reply(self, status_id: str, acct: str, text: str):
        try:
            body = f"@{acct} {text}"   # 알림 보장을 위해 멘션을 본문에 직접 포함
            self.m.status_post(
                status=body,
                in_reply_to_id=status_id,
                visibility=REPLY_VISIBILITY
            )
        except (MastodonAPIError, MastodonNetworkError) as e:
            print(f"[reply] error: {e}")

    def run(self):
        print("[bot] start user stream…")
        listener = CandyListener(self)
        while True:
            try:
                # 스트림은 연결 이후 이벤트만 수신
                self.m.stream_user(listener)
            except Exception as e:
                print(f"[stream] error: {e}, retry in 5s")
                time.sleep(5)


class CandyListener(StreamListener):
    def __init__(self, bot: CandyBot):
        super().__init__()
        self.bot = bot

    def on_notification(self, notif):
        # 알림 타입이 멘션일 때만 큐에 적재
        if notif.get("type") == "mention":
            self.bot.handle_claim(notif)

    def handle_heartbeat(self):
        pass

    def on_abort(self, err):
        print(f"[stream] abort: {err}")


if __name__ == "__main__":
    CandyBot().run()
