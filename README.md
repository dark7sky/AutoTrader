# KIS AI Scalper

한국투자증권 KIS Open API와 KRX 장중 데이터를 이용하는 국내 주식 단타 자동매매 프로젝트입니다. 이 저장소의 코드는 **운영을 시작할 수 있도록 준비된 상태**이며, 실전 검증이 완료되었다는 의미는 아닙니다. 실제 주문 접수, 체결·미체결 응답, Telegram 운영, 잔고 대조는 장중 KIS 모의계좌에서 확인해야 합니다.

운영자는 먼저 `trading-service`를 모의계좌로 실행하고, 점검이 끝난 뒤 Telegram에서 resume합니다. 실계좌 전환은 모의계좌 장중 검증 결과를 확인한 후 별도로 판단합니다.

## 운영 핵심

- Docker의 기본 장기 실행 대상은 `trading-service` 단독 서비스입니다. `app`, smoke, collector, `telegram-poll`, `auto-trade-cycle`은 별도 도구 또는 프로파일입니다.
- 서비스는 시작할 때마다 runtime `paused=true`를 기록합니다. 기존 DB가 running이었어도 자동으로 재개하지 않습니다.
- `paused` 상태에서도 preflight와 게이트가 유효하면 주문·체결 상태 supervisor가 계속 잔고와 주문을 대조합니다. 단, 신규 주문은 제출하지 않습니다. Telegram `/resume`으로만 자동매매를 시작합니다.
- Telegram `/env demo`와 `/env real` 전환은 paused 상태에서만 가능합니다. real은 challenge 확인 절차가 추가됩니다.
- 주문 전 KRX `XKRX` 거래소 캘린더를 확인합니다. 캘린더가 준비되지 않으면 broker preflight가 fail-closed로 동작합니다.
- 브로커에만 있는 기존 잔고를 local 포지션으로 자동 채택하지 않습니다. local에만 있거나 수량이 다른 포지션도 자동 청산하지 않습니다.
- 잔고·주문·체결 불일치가 있으면 신규 진입을 차단하고 `operator_review`를 기록한 뒤 Telegram으로 문의합니다. 운영자가 원인을 확인하기 전에는 resume하지 않습니다.
- 미체결 BUY는 기본 60초, SELL은 기본 30초 후 취소를 요청합니다. 취소 HTTP 응답은 확정 체결 취소가 아니므로 KIS의 terminal 상태를 다시 확인할 때까지 `CANCEL_PENDING`으로 둡니다.
- 고위험 AI BUY만 운영자 승인 대기로 보냅니다. 승인 요청은 2분 후 만료되며, 승인 전에는 주문하지 않습니다.
- OpenAI 비용 조회 전용 키는 선택 사항입니다. 비용 키가 없으면 AI 판단과 별개로 비용 리포트만 `unavailable`이 될 수 있습니다.
- `data/`에는 SQLite, KIS token cache, runtime state가 남습니다. 운영에서는 반드시 영속 볼륨과 백업 정책을 사용합니다.

## 현재 상태와 검증 범위

구현된 주요 경로:

- KIS REST/WebSocket 인증과 국내주식 현재가·체결가 수집
- KIS 실시간 체결통보 worker: 실전 `H0STCNI0`, 모의 `H0STCNI9`; `KIS_HTS_ID`는 HTS 사용자/로그인 ID이며 event-driven 체결 처리에 사실상 필수입니다. worker를 사용할 수 없을 때는 REST 주문 supervisor가 보완합니다.
- SQLite 기반 tick, 1분봉, AI 판단, broker 주문·체결 ledger
- KRX 캘린더 기반 장중 gate와 신규진입 시간 gate
- deterministic risk engine, 잔고·local position 대조, 주문 상태 재조회
- BUY/SELL 미체결 주문 만료 취소와 취소 확정 대기
- 일반 BUY 및 손절·익절·타임스탑·장 마감 SELL 처리
- 고위험 BUY의 Telegram 승인 요청과 2분 만료
- Telegram pause/resume/status/report/cost/control/environment/positions/orders/fills/approvals
- Docker healthcheck, service lease, heartbeat, 데이터 보존 정리

아직 장중 모의계좌에서 확인해야 하는 항목:

- KIS 모의계좌 BUY·SELL의 실제 접수와 체결 결과
- 부분 체결·미체결·취소 확정 응답의 실제 형태
- KIS 잔고와 local ledger 대조가 운영 상황에서 의도대로 멈추는지
- Telegram 승인, pause, 불일치 문의, 사후 리포트
- OpenAI 호출 비용·권한과 비용 제한

따라서 문서의 상태 표현은 “코드 준비 완료, 장중 모의 검증 필요”입니다. “실전 검증 완료”로 해석하지 마세요.

## 필수 설정

비밀값은 로컬 `.env` 또는 Portainer Stack 환경변수에만 입력합니다. 아래 예시는 모두 자리표시자입니다.

```dotenv
LIVE_TRADING_ENABLED=false
KIS_ENV=demo

KIS_DEMO_APP_KEY=<KIS_DEMO_APP_KEY>
KIS_DEMO_APP_SECRET=<KIS_DEMO_APP_SECRET>
KIS_DEMO_ACCOUNT_NO=<KIS_DEMO_ACCOUNT_NO>
KIS_DEMO_ACCOUNT_PRODUCT_CODE=01

TELEGRAM_BOT_TOKEN=<TELEGRAM_BOT_TOKEN>
TELEGRAM_ALLOWED_CHAT_ID=<TELEGRAM_ALLOWED_CHAT_ID>
# 그룹에서 사용할 때만 선택: TELEGRAM_ALLOWED_USER_ID=<TELEGRAM_ALLOWED_USER_ID>

OPENAI_API_KEY=<OPENAI_API_KEY>
OPENAI_MODEL=gpt-4o-mini
AUTO_TRADE_AI=openai
AUTO_TRADE_MAX_QUANTITY=1
AUTO_TRADE_COLLECT_SECONDS=10
AUTO_TRADE_CYCLE_INTERVAL_SECONDS=20
KIS_HTS_ID=<KIS_HTS_LOGIN_ID>
ORDER_SUPERVISOR_INTERVAL_SECONDS=5
OPENAI_TIMEOUT_SECONDS=8
OPENAI_MAX_RETRIES=1
OPENAI_MAX_RESPONSE_AGE_SECONDS=20
AUTO_TRADE_DECISION_DEADLINE_SECONDS=25
DATA_DIR=./data
```

### 주문 게이트

저장소 기본값은 `LIVE_TRADING_ENABLED=false`이며, 이 값이 주문 제출을 여는 유일한 배포 게이트입니다.

```dotenv
LIVE_TRADING_ENABLED=true
```

첫 배포에서는 `false`로 두고 smoke·상태 점검부터 진행합니다. `false`이면 broker 주문 제출이 차단됩니다. `true`여도 서비스가 paused 상태이면 주문하지 않으며, Telegram `/resume`, KRX 거래일·장중 조건, risk 및 불일치 검사를 모두 통과해야 주문할 수 있습니다.

기타 설정 의미:

- `KIS_ENV=demo`는 smoke·단발 CLI의 기본 대상입니다. 장기 실행 `trading-service`의 대상은 SQLite에 보존되며, 신규 DB에서는 `demo`로 시작합니다. paused 상태에서 Telegram `/env demo` 또는 `/env real`로 선택합니다. `KIS_REAL_*` 값은 모의 BUY/SELL 검증 전 입력하지 않는 것을 권장합니다.
- `OPENAI_API_KEY`는 `AUTO_TRADE_AI=openai`일 때 필요합니다. `AUTO_TRADE_AI=rule`은 규칙 기반 dry-run용입니다.
- `OPENAI_ADMIN_KEY` 또는 `OPENAI_USAGE_API_KEY`는 `/cost`와 비용 줄을 조회할 때만 선택적으로 사용합니다. 비용 키가 없다고 주문 판단 기능이 자동으로 중단되지는 않습니다.
- `AUTO_TRADE_COLLECT_SECONDS=10`, `AUTO_TRADE_CYCLE_INTERVAL_SECONDS=20`은 서비스 기본값입니다. 계좌·risk snapshot은 수집이 끝난 뒤 새로 읽습니다.
- `KIS_HTS_ID`는 KIS 앱 키·시크릿·계좌번호가 아니라 HTS 사용자/로그인 ID입니다. 실시간 체결통보 worker를 event-driven으로 사용하려면 입력해야 하며, 누락 시 REST 주문 supervisor가 5초 주기로 fallback합니다.
- AI는 deterministic 후보가 있을 때만 호출하고, 같은 bar에서는 유료 호출을 중복하지 않습니다. 판단 deadline은 25초, API timeout은 8초, 일시적 오류 재시도는 1회이며 응답 뒤 KIS 현재가를 다시 검증합니다.
- `BUY_ORDER_TTL_SECONDS=60`, `SELL_ORDER_TTL_SECONDS=30`은 미체결 취소 기준입니다.
- `TELEGRAM_ALLOWED_CHAT_ID`가 없으면 Telegram 제어와 알림을 사용할 수 없습니다. 허용되지 않은 chat id의 명령은 처리하지 않습니다.

자세한 변수 목록의 기준은 저장소의 `.env.example`입니다. `.env`는 Git에 추가하지 않습니다.

## Docker 실행

이미지 빌드와 기본 운영 서비스 시작:

```powershell
docker compose build trading-service
docker compose up -d trading-service
docker compose ps
docker compose logs -f trading-service
```

`trading-service`는 시작 알림을 Telegram으로 보내고 `runtime: paused`로 대기합니다. 로그와 Telegram `/status`에서 다음을 확인한 뒤에만 resume합니다.

```text
/control
/env demo
/positions
/orders
/fills
/resume
```

watchlist를 DB에 등록하지 않고 `--symbols`를 지정하지 않으면 서비스는 빈 watchlist로 동작합니다.

```powershell
python -m kis_ai_scalper.cli watchlist-add --db data/kis_ai_scalper.sqlite3 --symbols 005930
python -m kis_ai_scalper.cli watchlist-list --db data/kis_ai_scalper.sqlite3
```

단일 bounded cycle을 직접 실행할 수도 있지만, 장기 운영에서는 `trading-service`와 중복 주문이 생기지 않도록 동시에 실행하지 않습니다.

```powershell
docker compose --profile trade run --rm auto-trade-cycle
```

체결통보 구독 ACK만 주문 없이 확인하려면 다음 smoke를 사용합니다.

```powershell
docker compose --profile smoke run --rm smoke-fill-notice
```

## Telegram 운영

Telegram에서는 `/start` 또는 `/menu`를 누르면 메인 메뉴가 표시됩니다. 상태·거래·제어·환경·AI 하위 메뉴의 버튼을 따라가면 주요 조회와 제어 기능에 접근할 수 있으며, 각 하위 메뉴에는 메인 메뉴로 돌아가는 버튼이 있습니다. 버튼으로 이동할 때는 현재 메뉴 메시지를 교체하므로 메뉴 메시지가 계속 쌓이지 않습니다. 기존 명령과 `control:`/`approval:` 콜백도 계속 호환됩니다.

주요 명령:

```text
/menu             메인 버튼 메뉴
/control          현재 상태와 제어 버튼
/status           paused, environment, heartbeat, 불일치 플래그
/pause [reason]   즉시 일시정지
/resume [reason]  재개. real은 별도 arm 필요
/env              현재 환경 확인
/env demo         paused 상태에서 demo 선택
/env real         real challenge 발급, 아직 환경 변경 안 함
/confirm-real 123456
/positions        local/live 또는 broker snapshot 포지션
/orders           최근 broker 주문
/fills            최근 broker 체결
/approvals        대기 중인 고위험 승인
/approve <id>
/reject <id>
/report           paper 또는 live 리포트
/live-report      최근 broker snapshot
/cost             OpenAI 비용 조회
/emergency-stop   pause와 emergency stop 활성화
/cancel-open-buys 알려진 미체결 BUY 취소 요청 후 terminal 확정 대기
/clear-emergency  paused 상태에서 emergency stop 해제
```

`운용 제어` 메뉴의 **미체결 매수 취소** 버튼도 같은 동작을 합니다. `/emergency-stop`과 `/cancel-open-buys`는 runtime을 paused로 만들고, local ledger에 알려진 open BUY에 대해서만 취소를 요청합니다. KIS가 terminal 상태를 확인할 때까지 주문은 `CANCEL_PENDING`이며, 보유 주식을 자동 매도하거나 자동 청산하지 않습니다.

real 전환 순서:

1. `/pause` 상태인지 확인합니다. local open position 또는 active/unknown broker order가 있으면 환경 전환이 거부됩니다.
2. `/env real`을 입력합니다. 6자리 숫자 challenge가 발급되고 5분 동안 유효합니다.
3. `/confirm-real <challenge>`를 paused 상태에서 입력합니다. 성공하면 real 환경이 선택되고 15분 동안 1회 resume arm이 생깁니다.
4. KIS real 키·계정과 `LIVE_TRADING_ENABLED=true`를 확인한 뒤 `/resume`을 한 번만 입력합니다.

real에서 pause하거나 emergency stop을 사용하면 다시 resume하기 전에 새 challenge 확인이 필요합니다. 이는 환경을 바꾸는 `/env`만이 아니라 real resume 자체에 적용됩니다.

## 주문과 잔고 불일치 처리

장중 cycle은 대략 다음 순서로 동작합니다.

1. 시세 수집을 완료합니다.
2. 그 직후 KIS 주문 상태와 계좌 snapshot을 새로 읽습니다.
3. local 주문에 대응하는 KIS 주문만 ledger에 반영합니다. broker-only 체결·잔고는 자동으로 local position으로 만들지 않습니다.
4. local position과 broker position의 수량이 다르거나 한쪽에만 있으면 `operator_review=true`, `block_new_entries=true`를 기록합니다.
5. 이 상태에서는 신규 BUY를 차단하고 Telegram에 원인을 보냅니다. runtime을 자동 pause로 바꾸지는 않지만, 운영자는 원인 확인 전 `/pause`를 유지해야 합니다.
6. local position을 불일치 해소용으로 자동 청산하지 않습니다. 외부 수동 주문, 재시작 중 체결, 계좌·주문 조회 장애를 확인하고 ledger와 KIS 상태를 운영자가 대조합니다.

미체결 주문 취소도 같은 원칙을 따릅니다. BUY 60초, SELL 30초가 지나면 취소 요청을 한 번 보내고, KIS가 `CANCELLED`, `FILLED`, `REJECTED` 중 하나를 보고할 때까지 신규진입을 다시 평가하지 않습니다. 취소 요청이 애매하거나 주문 식별자가 맞지 않으면 `UNKNOWN`으로 남기고 신규진입을 차단합니다.

## 월요일 모의계좌 1주 검증

이 절차는 실전 전환 승인이 아니라 **demo 장중 동작을 확인하는 1주 운영 절차**입니다. 첫 주문 수량은 `AUTO_TRADE_MAX_QUANTITY=1`로 고정합니다.

월요일 장 시작 전:

1. `.env`에 demo 키·계좌와 Telegram 허용 chat id를 입력하고 `KIS_ENV=demo`, `LIVE_TRADING_ENABLED=false`를 확인합니다. 신규 DB의 runtime 대상은 demo이며, 기존 DB는 마지막 Telegram 선택을 유지합니다.
2. `data/`를 백업하고 기존 local position, broker 잔고, active order가 없는지 확인합니다.
3. `docker compose up -d trading-service`로 시작합니다. 서비스가 paused로 시작하는지 로그와 `/status`로 확인합니다.
4. KIS REST/WebSocket smoke와 watchlist를 확인합니다. KRX 휴장일이면 주문이 없는 것이 정상입니다.
5. demo 주문을 검증할 시점에만 `LIVE_TRADING_ENABLED=true`로 설정하고 서비스를 재시작합니다. 재시작 후에도 서비스는 다시 paused로 시작합니다.
6. `/control`, `/env demo`, `/positions`, `/orders`, `/fills`를 확인합니다. `/status`에서 체결통보 worker 또는 REST supervisor의 상태·heartbeat를 확인한 뒤 `/resume`합니다.

월요일부터 금요일까지 장중:

- 매일 시작 시 paused·environment·heartbeat·`operator_review`·`block_new_entries`를 확인합니다.
- 후보, HOLD, risk reject는 주문 없음으로 기록합니다. 고위험 BUY는 `/approvals`에서 조건과 만료 시각을 확인하고 필요한 경우에만 승인합니다.
- KIS 실시간 체결통보 worker의 접수·체결·부분 체결 heartbeat와 REST supervisor fallback을 확인합니다.
- BUY 미체결 60초, SELL 미체결 30초 취소가 KIS 상태 재조회로 확정되는지 확인합니다.
- 실제 demo 장중 BUY, 부분 체결, 취소, SELL을 각각 검증하기 전에는 real로 전환하지 않습니다.
- broker-only, local-only, 수량 불일치가 발생하면 신규진입을 허용하지 않고 원인을 기록합니다. 자동 채택·자동 청산으로 덮어쓰지 않습니다.
- 장 마감 후 `/report`, `/live-report`, `/orders`, `/fills`를 저장하고 data 백업을 갱신합니다.

금요일 검토:

- 실제 KIS demo 접수·체결·취소·부분 체결 응답과 local ledger가 일치하는지 확인합니다.
- Telegram pause/resume, approval 만료, 불일치 알림, service restart 후 pause 동작을 확인합니다.
- 실패·미확인 항목이 있으면 real로 전환하지 않습니다. 실제 장중 demo BUY·부분 체결·취소·SELL 검증은 실전 전환의 잔여 필수 조건입니다.

## 데이터 영속화와 백업

Compose는 `${DATA_DIR:-./data}`를 컨테이너 `/app/data`에 연결합니다. SQLite, `auth/` token cache, runtime metadata, 주문·체결 ledger가 이 경로에 저장되므로 운영 호스트의 영속 디스크를 사용해야 합니다.

SQLite 파일을 복사할 때는 일관성을 위해 먼저 서비스를 멈춥니다.

```powershell
docker compose stop trading-service
Copy-Item -Recurse -Force .\data .\backup\data-$(Get-Date -Format yyyyMMdd-HHmmss)
docker compose up -d trading-service
```

백업에는 token cache가 포함될 수 있으므로 접근권한을 제한하고 외부 저장소에 평문으로 공개하지 않습니다. 복원도 서비스를 멈춘 상태에서 수행하고, 복원 후 `/status`와 broker 잔고를 다시 대조합니다.

## 로컬 테스트와 smoke

```powershell
python -m pip install -e ".[dev]"
python -m pytest --basetemp .tmp_pytest_monday
python -m kis_ai_scalper.cli smoke-kis --config config/settings.yaml --env demo --symbol 005930
python -m kis_ai_scalper.cli smoke-ws --config config/settings.yaml --env demo --symbol 005930 --seconds 10
```

테스트 결과 수치는 커밋·환경에 따라 달라질 수 있으므로 이 문서에 고정된 과거 통과 개수는 기록하지 않습니다. smoke와 테스트 통과만으로 장중 주문 검증이나 실전 검증을 대신할 수 없습니다.

## 참고 문서

- [Portainer Git Stack 운영 절차](docs/PORTAINER.md)
- KIS Open API 공식 샘플: https://github.com/koreainvestment/open-trading-api
- `exchange_calendars` XKRX: https://github.com/gerrymanoim/exchange_calendars
- OpenAI Usage API: https://platform.openai.com/docs/api-reference/usage
