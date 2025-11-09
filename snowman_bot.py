import json
import random
import re  # 정규표현식 모듈 추가
from datetime import datetime, timedelta, MINYEAR
import gspread
from mastodon import Mastodon, StreamListener
import os # os 모듈 추가

# ==============================================================================
# ⚙️ 설정값 및 데이터 구조 (여기를 실제 값으로 반드시 수정하세요!)
# ==============================================================================
MASTODON_INSTANCE = 'https://marchen1210d.site'
ACCESS_TOKEN = '99WpPDjDzatu5KYfbRLEBQGMPleah-oKffEfLbVMa-k' # 실제 토큰으로 수정해야 함
SHEET_NAME = '눈사람 굴리기 게임 데이터'
SERVICE_ACCOUNT_FILE = 'service_account.json'

# 목표 크기 설정
PERFECT_HEAD = 137
PERFECT_BODY = 274  # 💡 최종 목표 크기: 274로 설정
COOL_DOWN_HOURS = 1  # 그룹 쿨타임은 1시간으로 설정
DB_FILE = 'player_db.json'

# 게임 데이터 구조 (💡 장식 획득 확률 및 획득 개수, 점수 반영)
DECORATION_DATA = {
    '[장식/당근]': {'prob': 0.20, 'count': 1, 'score': 10, 'row': 3},  # 20%
    '[장식/가지]': {'prob': 0.20, 'count': 1, 'score': 10, 'row': 4},  # 20%
    '[장식/초코볼]': {'prob': 0.20, 'count': 1, 'score': 10, 'row': 5},  # 20%
    '[장식/솔잎*솔방울]': {'prob': 0.10, 'count': 1, 'score': 20, 'row': 6},  # 10%
    '[장식/검은색 조약돌]': {'prob': 0.10, 'count': 1, 'score': 20, 'row': 7},  # 10%
    '[장식/나뭇가지]': {'prob': 0.10, 'count': 1, 'score': 20, 'row': 8},  # 10%
    '[장식/목도리]': {'prob': 0.05, 'count': 1, 'score': 30, 'row': 9},  # 5%
    '[장식/거대 캔디케인]': {'prob': 0.05, 'count': 1, 'score': 30, 'row': 10},  # 5%
}

# 쿨타임 그룹 정의
SNOWMAN_COOL_DOWN_CMDS = ['[눈사람/굴리기]', '[눈사람/깎기]', '[눈사람/던지기]']
DECORATION_COMMAND = '[눈사람/장식]'
REGISTRATION_COMMANDS = ['[눈사람/머리]', '[눈사람/몸통]']

SNOWMAN_COMMANDS = SNOWMAN_COOL_DOWN_CMDS
ALL_COMMANDS = [DECORATION_COMMAND] + SNOWMAN_COMMANDS + REGISTRATION_COMMANDS


# ==============================================================================
# 데이터베이스 및 쿨타임 관리 함수
# ==============================================================================

def load_db():
    """JSON 파일에서 사용자 데이터베이스 로드 및 시간 객체 변환"""
    default_cooldown_time = datetime(MINYEAR, 1, 1)

    try:
        with open(DB_FILE, 'r', encoding='utf-8') as f:
            db = json.load(f)
            for user_id in db:
                if 'cooldown_times' not in db[user_id]:
                    db[user_id]['cooldown_times'] = {}

                for group, time_str in db[user_id]['cooldown_times'].items():
                    # 빈 문자열을 확실히 None으로 처리하기 위한 로직
                    if time_str == "" or time_str is None:
                        db[user_id]['cooldown_times'][group] = None
                        continue

                    if isinstance(time_str, str):
                        try:
                            # 1. datetime 문자열 변환 시도
                            db[user_id]['cooldown_times'][group] = datetime.fromisoformat(time_str)
                        except ValueError:
                            # 2. 유효하지 않은 문자열이면 기본값 (과거)
                            db[user_id]['cooldown_times'][group] = default_cooldown_time
                    else:
                        # 기타 예외 처리
                        db[user_id]['cooldown_times'][group] = default_cooldown_time

                if 'last_cmd' in db[user_id]:
                    del db[user_id]['last_cmd']

            return db
    except (FileNotFoundError, json.JSONDecodeError):
        print(f"경고: {DB_FILE} 파일을 찾을 수 없거나 형식이 잘못되었습니다. 빈 DB를 시작합니다.")
        return {}


def save_db(db):
    """사용자 데이터베이스를 JSON 파일에 저장 및 시간 객체 문자열 변환"""
    db_to_save = db.copy()
    for user_id in db_to_save:
        if 'cooldown_times' in db_to_save[user_id]:
            for group, time_obj in db_to_save[user_id]['cooldown_times'].items():
                if time_obj and isinstance(time_obj, datetime):
                    db_to_save[user_id]['cooldown_times'][group] = time_obj.isoformat()
                else:
                    # None일 경우 JSON에서 null 대신 빈 문자열로 저장하여 load_db에서 안정적으로 처리
                    db_to_save[user_id]['cooldown_times'][group] = ""

    with open(DB_FILE, 'w', encoding='utf-8') as f:
        json.dump(db_to_save, f, indent=4, ensure_ascii=False)


def _get_cooldown_group(command):
    """명령어에 해당하는 쿨타임 그룹 이름을 반환"""
    if command in SNOWMAN_COOL_DOWN_CMDS:
        return 'snowman_cmd'
    elif command == DECORATION_COMMAND:
        return 'decoration_cmd'
    return None


def check_group_cooldown(user_data, command):
    """특정 명령 그룹의 쿨타임을 확인"""
    group = _get_cooldown_group(command)
    if not group:
        return True, "등록 명령. 쿨타임 없음."

    if 'cooldown_times' not in user_data:
        user_data['cooldown_times'] = {}

    cooldown_time = user_data.get('cooldown_times', {}).get(group)

    if not isinstance(cooldown_time, datetime):
        return True, "쿨타임 정보 없음. 명령 실행 가능."

    time_since_last = datetime.now() - cooldown_time

    if time_since_last.total_seconds() > COOL_DOWN_HOURS * 3600:
        return True, "쿨타임 해제. 명령 실행 가능."
    else:
        remaining = timedelta(hours=COOL_DOWN_HOURS) - time_since_last
        minutes = int(remaining.total_seconds() // 60)
        seconds = int(remaining.total_seconds() % 60)

        # 쿨타임 메시지 템플릿 (볼드체 제거)
        cooldown_msg = f"""
손이 녹을 때까지 잠시 기다리자.

대기 시간 ― {minutes}분 {seconds}초
"""
        return False, cooldown_msg.strip()


# ==============================================================================
# SnowmanBot 클래스 (메인 로직)
# ==============================================================================

class SnowmanBot:
    def __init__(self):
        # 1. DB 로드 (시작 시 최초 1회)
        self.player_db = load_db()

        # 2. Gspread 인증 및 시트 연결
        try:
            self.gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
            self.spreadsheet = self.gc.open(SHEET_NAME)
            print("Gspread 인증 및 시트 연결 완료.")
        except Exception as e:
            print(f"Gspread 연결 오류: {e}")
            exit()

        # 3. Mastodon 연결
        try:
            self.m = Mastodon(
                access_token=ACCESS_TOKEN,
                api_base_url=MASTODON_INSTANCE
            )
            # 봇 계정 정보를 미리 로드하여 중복 처리 방지에 사용
            self.bot_acct = self.m.account_verify_credentials()['acct']
            print("마스토돈 인증 완료.")
        except Exception as e:
            print(f"마스토돈 연결/인증 오류: {e}")
            exit()

    # --- ID 자동 획득 및 DB 갱신 함수 ---
    def _resolve_user_id(self, username, user_id):
        """사용자명(ACCT)을 통해 DB에서 사용자를 찾아냅니다."""

        if user_id in self.player_db:
            return user_id, self.player_db[user_id]

        if username in self.player_db:
            print(f"ID 자동 획득: @{username}의 ID({user_id})를 찾아 DB 키를 갱신합니다.")

            user_data = self.player_db.pop(username)
            self.player_db[user_id] = user_data

            save_db(self.player_db)

            return user_id, user_data

        return None, None

    # --- 내부 도우미 함수 ---

    def _handle_registration(self, status, command, user_id, username):
        """[눈사람/머리] 또는 [눈사람/몸통] 명령 처리 (역할 할당)"""

        user_data = self.player_db[user_id]
        sheet_name = user_data.get('sheet_name')

        # 오류 메시지 수정: DB 정보 없음
        if not sheet_name:
            return "등록된 캐릭터가 아닙니다. 운영 계정(@MARCH)으로 문의해 주십시오."

        # 오류 메시지 수정: 이미 역할 할당됨
        if user_data.get('role'):
            return f"{sheet_name}의 {user_data['role']} 역할이 이미 할당되었습니다. 운영 계정(@MARCH)으로 문의해 주십시오."

        new_role = '머리' if command == '[눈사람/머리]' else '몸통'
        new_col = 'A' if command == '[눈사람/머리]' else 'B'

        is_role_taken = False
        for uid, data in self.player_db.items():
            if data.get('sheet_name') == sheet_name and data.get('role') == new_role:
                is_role_taken = True
                break

        # 오류 메시지 수정: 역할 중복
        if is_role_taken:
            return f"{sheet_name}의 {new_role} 역할이 이미 존재합니다. 운영 계정(@MARCH)으로 문의해 주십시오."

        self.player_db[user_id]['role'] = new_role
        self.player_db[user_id]['col'] = new_col

        try:
            team_sheet = self.spreadsheet.worksheet(sheet_name)
            col_index = ord(new_col) - ord('A') + 1
            team_sheet.update_cell(1, col_index, username)
            team_sheet.update_cell(2, col_index, 200)

            self._update_scores(team_sheet)

        # 오류 메시지 수정: 시트 업데이트 오류
        except Exception as e:
            print(f"Gspread registration update error for @{username}: {e}")
            return "연동 오류가 발생하였습니다. 운영 계정(@MARCH)으로 문의해 주십시오."

        if 'cooldown_times' not in self.player_db[user_id]:
            self.player_db[user_id]['cooldown_times'] = {'snowman_cmd': None, 'decoration_cmd': None}

        save_db(self.player_db)

        # 등록 스크립트 템플릿 적용 (볼드체 제거)
        registration_reply = f"""
눈사람의 {new_role} 을/를 멋지게 만들어 보자.

조 이름 ― {sheet_name}
눈덩이 크기 ― 200
"""
        return registration_reply.strip()

    def _update_snowman_size(self, team_sheet, role, col_char, current_size, command):
        """눈덩이 크기 조절 및 응답 메시지 생성 로직"""

        if command == '[눈사람/굴리기]':
            new_size = current_size + 10
        elif command == '[눈사람/깎기]':
            new_size = current_size - 10
        else:
            new_size = current_size + random.randint(-10, 10)

        col_index = ord(col_char) - ord('A') + 1
        team_sheet.update_cell(2, col_index, new_size)

        response_message = ""

        if role == '머리':
            # 7. 80 이하
            if new_size <= 80:
                response_message = '눈덩이가 투덜거린다. “이렇게 작은 머리로 뭘 보라는 거야?”'
            # 7. 190 이상
            elif new_size >= 190:
                response_message = '눈덩이가 화를 낸다. “무거워, 무거워, 무거워! 이러다 무너지겠어!”'
            # 9. 81~130, 140~189
            elif (81 <= new_size <= 130) or (140 <= new_size <= 189):
                response_message = '눈덩이가 격려의 말을 던진다. “조금 더 노력해 봐. 거의 다 왔어!”'
            # 10. 131~139 (완벽 범위)
            elif 131 <= new_size <= 139:
                response_message = '눈덩이가 자신감에 겨워 외친다. “올해의 가장 완벽한 눈사람은 분명 나일 거야!”'
            else:
                response_message = '눈덩이가 잠잠하다.'

        elif role == '몸통':
            # 8. 220 이하
            if new_size <= 220:
                response_message = '눈덩이가 투덜거린다. “이렇게나 작게 만들 거면 차라리 나를 머리로 올리지 그래?”'
            # 8. 330 이상
            elif new_size >= 330:
                response_message = '눈덩이가 비아냥 댄다. “온 사방의 눈이란 눈은 다 끌어 모았군. 너무 뚱뚱해!”'
            # 9. 221~270, 280~329
            elif (221 <= new_size <= 270) or (280 <= new_size <= 329):
                response_message = '눈덩이가 격려의 말을 던진다. “조금 더 노력해 봐. 거의 다 왔어!”'
            # 10. 271~279 (완벽 범위)
            elif 271 <= new_size <= 279:
                response_message = '눈덩이가 자신감에 겨워 외친다. “올해의 가장 완벽한 눈사람은 분명 나일 거야!”'
            else:
                response_message = '눈덩이가 잠잠하다.'

        return new_size, response_message

    def _try_get_decoration(self, team_sheet, role, col_char):
        """[눈사람/장식] 명령 처리: 가중치에 따라 하나의 장식을 획득하고 응답 메시지를 생성"""

        items = list(DECORATION_DATA.keys())
        weights = [data['prob'] for data in DECORATION_DATA.values()]

        # 1. 가중치에 따라 획득할 장식 선택
        acquired_command = random.choices(items, weights=weights, k=1)[0]
        deco_info = DECORATION_DATA[acquired_command]

        col_index = ord(col_char) - ord('A') + 1
        row_index = deco_info['row']
        count_to_add = deco_info['count']

        current_count_str = team_sheet.cell(row_index, col_index).value
        try:
            current_count = int(current_count_str)
        except (ValueError, TypeError):
            current_count = 0

            # 2. 시트 업데이트
        new_count = current_count + count_to_add
        team_sheet.update_cell(row_index, col_index, new_count)

        item_name = acquired_command.split('/')[1].replace(']', '')

        # [눈사람/장식] 스크립트 템플릿 (볼드체 제거)
        response_template = f"""
장식들이 담긴 주머니를 뒤적거리자⋯

{item_name} 이/가 나왔다! 어디에 장식해야 예쁠까?

획득 ― {item_name}
보유 현황 ― {new_count} 개
"""
        return response_template.strip()

    def _update_scores(self, team_sheet):
        """크기 및 장식 점수를 계산하고 시트에 최종 점수를 업데이트 (Batch Update 적용)"""
        try:
            # 1. 데이터 읽기 (2행 ~ 10행)
            data = team_sheet.get(f'A2:B10')

            head_size = 200
            body_size = 200

            if data and len(data) > 0:
                row_size = data[0]

                if len(row_size) > 0 and str(row_size[0]).isdigit():
                    head_size = int(row_size[0])

                if len(row_size) > 1 and str(row_size[1]).isdigit():
                    body_size = int(row_size[1])

            max_deco_rows = len(list(DECORATION_DATA.values()))
            head_counts = []
            body_counts = []

            for row_index in range(max_deco_rows):
                if len(data) <= row_index + 1:
                    head_counts.append(0)
                    body_counts.append(0)
                    continue

                row = data[row_index + 1]

                head_count = 0
                if len(row) > 0 and str(row[0]).isdigit():
                    head_count = int(row[0])
                head_counts.append(head_count)

                body_count = 0
                if len(row) > 1 and str(row[1]).isdigit():
                    body_count = int(row[1])
                body_counts.append(body_count)

            # 2. 크기 점수 계산
            head_size_score = max(0, 100 - abs(head_size - PERFECT_HEAD))
            body_size_score = max(0, 100 - abs(body_size - PERFECT_BODY))

            # 3. 장식 점수 계산
            deco_rows = list(DECORATION_DATA.values())
            head_deco_score = sum(deco_rows[i]['score'] * head_counts[i] for i in range(len(deco_rows)))
            body_deco_score = sum(deco_rows[i]['score'] * body_counts[i] for i in range(len(deco_rows)))

            # 4. 최종 점수 계산
            final_score = head_size_score + body_size_score + head_deco_score + body_deco_score

            # 5. 시트에 모든 점수를 단일 요청(Batch Update)으로 업데이트
            update_data = [
                [head_size_score, body_size_score],  # 11행 (A11: 크기 점수-머리, B11: 크기 점수-몸통)
                [head_deco_score, body_deco_score],  # 12행 (A12: 장식 점수-머리, B12: 장식 점수-몸통)
                [final_score]  # 13행 (A13: 최종 점수)
            ]

            # A11:B13 범위에 데이터 업데이트 (단일 API 호출)
            team_sheet.update('A11:B13', update_data)

        except Exception as e:
            # 시트 업데이트 실패 시 로깅
            print(f"FATAL GSPREAD UPDATE ERROR in _update_scores: {e}")

    # --- 메인 명령 처리 함수 ---

    def handle_command(self, status):
        """툿을 받아 명령을 처리하고 응답을 생성하는 메인 함수"""

        self.player_db = load_db()

        content = status['content'].lower()

        incoming_user_id = str(status['account']['id'])
        incoming_username = status['account']['acct']

        final_user_id, user_data = self._resolve_user_id(incoming_username, incoming_user_id)

        # 오류 메시지 수정: DB에 없는 사용자 ID
        if final_user_id is None:
            # NOTE: DB에 없는 사용자에게 응답을 보낼 필요가 없다면 아래 3줄을 주석 처리할 수 있습니다.
            self.m.status_reply(status, "참여가 확인되지 않았습니다. 운영 계정(@MARCH)으로 문의해 주십시오.")
            return

        command_found = None
        for cmd in ALL_COMMANDS:
            if cmd.lower() in content:
                command_found = cmd
                break

        # ======================================================================
        # 🚨 명령어 유효성 검사 및 응답 분기
        # ======================================================================
        # 1. 툿에서 대괄호로 둘러싸인 텍스트가 있는지 정규식으로 확인
        bracketed_text_search = re.search(r'\[.*?\]', content)

        # 2. 유효한 명령어가 발견되지 않았을 경우
        if not command_found:

            if bracketed_text_search:
                # 2-A. 대괄호는 있으나 유효한 명령어와 일치하지 않는 경우 (오타)
                error_message = "존재하지 않는 커맨드입니다. 오타가 없는지 점검 부탁드리며, 오기재 · 미등록 등으로 판단될 시 운영 계정(@MARCH)으로 문의해 주십시오."
                print(f"DEBUG: @{incoming_username}의 툿에 오타가 포함되어 응답: {error_message}")
                self.m.status_reply(status, error_message)
                return
            else:
                # 2-B. 대괄호가 전혀 없는 경우 (이전 요청대로 응답 안 함)
                print(f"DEBUG: @{incoming_username}의 툿에 유효한 명령어나 대괄호가 없습니다. 응답하지 않습니다.")
                return

        # 3. 유효한 명령어가 발견된 경우 (기존 로직 수행)
        if command_found in REGISTRATION_COMMANDS:
            reply_text = self._handle_registration(status, command_found, final_user_id, incoming_username)
            self.m.status_reply(status, reply_text)
            return

        # 오류 메시지 수정: 역할 할당 필요
        if not user_data.get('role'):
            self.m.status_reply(status,
                                "역할이 할당되지 않았습니다. [눈사람/머리] · [눈사람/몸통] 역할 등록이 완료되었는지 확인 부탁드리며, 미등록으로 판단될 시 운영 계정(@MARCH)으로 문의해 주십시오.")
            return

        can_act, cooldown_msg = check_group_cooldown(user_data, command_found)
        if not can_act:
            print(f"DEBUG: Cooldown active for @{incoming_username}")
            self.m.status_reply(status, cooldown_msg)
            return

        sheet_name = user_data['sheet_name']
        role = user_data['role']
        col_char = user_data['col']
        team_sheet = self.spreadsheet.worksheet(sheet_name)

        reply_text = ""

        if command_found in SNOWMAN_COOL_DOWN_CMDS:
            # 눈덩이 크기 로드
            col_index = ord(col_char) - ord('A') + 1
            current_size_str = team_sheet.cell(2, col_index).value
            current_size = int(current_size_str) if current_size_str and current_size_str.isdigit() else 200

            new_size, response_message = self._update_snowman_size(team_sheet, role, col_char, current_size,
                                                                   command_found)

            # 눈덩이 관련 명령 스크립트 템플릿 적용
            if command_found == '[눈사람/굴리기]':
                cmd_message = "눈덩이를 데굴데굴 굴리자⋯"
            elif command_found == '[눈사람/깎기]':
                cmd_message = "눈덩이를 조심스레 깎아내자⋯"
            else:  # [눈사람/던지기]
                cmd_message = "눈덩이를 휙 던지자⋯"

            # 기존 스크립트 출력 형식 유지 (볼드체 제거)
            reply_text = f"""
{cmd_message}
{response_message}

현재 크기 ― {new_size}
"""

        elif command_found == DECORATION_COMMAND:
            reply_text = self._try_get_decoration(team_sheet, role, col_char)

        self._update_scores(team_sheet)

        cooldown_group = _get_cooldown_group(command_found)

        if cooldown_group:
            if 'cooldown_times' not in self.player_db[final_user_id]:
                self.player_db[final_user_id]['cooldown_times'] = {}

            self.player_db[final_user_id]['cooldown_times'][cooldown_group] = datetime.now()
            print(
                f"DEBUG: Cooldown updated for user {final_user_id} group {cooldown_group} at {datetime.now().isoformat()}")

        save_db(self.player_db)

        # 멘션 중복 제거 (본문만 final_reply에 담음)
        final_reply = reply_text.strip()

        print(f"DEBUG: Replying to @{incoming_username} with: {final_reply[:50]}...")
        try:
            self.m.status_reply(status, final_reply)
            print(f"DEBUG: Reply to @{incoming_username} SUCCESS.")
        except Exception as e:
            print(f"FATAL REPLY ERROR for @{incoming_username}: {e}")

        return

    # --- 마스토돈 스트리밍 리스너 설정 ---
    def start_streaming(self):
        """마스토돈 스트리밍 시작"""

        class Listener(StreamListener):
            def __init__(self, bot_instance):
                self.bot = bot_instance

            # 멘션이 포함된 툿을 '알림(Notification)'을 통해 받아서 처리
            def on_notification(self, notification):
                if notification['type'] == 'mention':
                    status = notification['status']
                    self.bot.handle_command(status)

            # '업데이트(Update)'는 새로운 툿이 올라올 때 발생.
            # on_notification과의 중복 방지를 위해 멘션에 대한 처리를 제거함.
            def on_update(self, status):
                # if status['in_reply_to_id'] is None and any( # ⚠️ 중복 유발 코드였음. 주석 또는 삭제하여 중복 응답을 방지.
                #         tag['acct'] == self.bot.bot_acct for tag in status['mentions']):
                #     self.bot.handle_command(status)
                pass

            def on_error(self, error):
                print(f"스트리밍 오류 발생: {error}")

        print("마스토돈 스트리밍 시작...")
        # 봇 계정 ACCT 정보를 사용하여 on_update 로직에서 중복 검사를 할 수 있었지만,
        # 가장 간단한 해결책은 on_update에서 멘션 처리를 완전히 제거하는 것임.
        self.m.stream_user(Listener(self), run_async=False, reconnect_async=True)


if __name__ == '__main__':
    print("--------------------------------------------------")
    print("⛄ 눈사람 협동 게임 자동봇 시작 준비")
    print("--------------------------------------------------")

    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        print(f"FATAL ERROR: {SERVICE_ACCOUNT_FILE} 파일을 찾을 수 없습니다. 구글 설정이 필요합니다.")
        exit()

    try:
        bot = SnowmanBot()
        bot.start_streaming()
    except Exception as e:
        print(f"치명적인 봇 실행 오류: {e}")