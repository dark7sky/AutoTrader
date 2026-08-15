# KIS AI Scalper

한국투자증권 KIS Open API 기반의 한국 주식 단타 자동매매 프로젝트입니다.

현재 목표는 **모의계좌에서 장중 검증 가능한 AI 자동매매 1-cycle 시스템**입니다. watchlist에 등록한 종목을 대상으로 시세를 수집하고, AI가 진입/보류를 판단하며, deterministic risk engine이 수량과 위험을 다시 제한합니다. 일반 진입/청산은 자동으로 처리하고, 고위험 판단만 Telegram 승인 대기로 보냅니다.

## 현재 구현 상태

구현된 기능:

- KIS REST 인증 및 현재가 smoke test
- KIS WebSocket 실시간 체결가 smoke test
- 제한 시간 market collector
- SQLite 기반 tick, 1분봉, 후보 신호 저장
- 시장 데이터 health gate
- deterministic 후보 스캐너와 risk engine
- paper ledger 및 paper report
- Telegram `/pause`, `/resume`, `/status`, `/report`, `/cost`, `/control`
- KIS 국내주식 현금주문 BUY/SELL 어댑터
- watchlist 관리
- OpenAI 기반 AI 매매 판단 클라이언트
- rule 기반 dry-run AI 클라이언트
- 자동 BUY, 자동 손절/익절/타임스탑 SELL
- 고위험 AI 판단 시 주문 차단 및 승인 요청 기록
- OpenAI 비용 조회 리포트
- Docker/Portainer 실행용 compose 서비스

아직 반드시 모의계좌에서 검증해야 하는 것:

- 실제 KIS 모의계좌 BUY 주문 정상 접수
- 실제 KIS 모의계좌 SELL 주문 정상 접수
- 체결/미체결 상황에서의 응답 형태
- Telegram 사후보고 확인
- `/pause` 즉시 차단 확인
- OpenAI 비용 조회 권한 확인

실계좌 전환은 위 항목이 장중에 검증된 뒤에만 진행해야 합니다.

## 안전 원칙

- 기본 런타임 상태는 `paused=true`입니다.
- 새 SQLite control DB는 `environment=demo`와 함께 paused로 시작합니다.
- Docker `trading-service`는 시작할 때 기존 DB 상태와 무관하게 먼저 `paused=true`로 되돌립니다.
- 자동매매는 `control-resume` 후에만 동작합니다.
- Telegram에서 environment를 바꿀 때는 반드시 paused 상태여야 합니다.
- Docker 서비스는 Telegram에 저장된 runtime environment로 demo/real을 선택합니다.
- 주문은 KRX `XKRX` 캘린더 기준 장중에만 시도합니다.
- `trading-service`는 장중 cycle 전에 KIS 잔고조회와 local DB live position을 대조합니다.
- 브로커 잔고와 local live position이 불일치하면 상황별로 처리합니다.
- local DB에만 남고 브로커에 없는 포지션은 이미 청산/미체결된 것으로 보고 local position을 `broker_position_missing`으로 종료 처리합니다.
- 브로커에만 있거나 수량이 다른 포지션은 이상 상황으로 보고 해당 cycle의 주문을 보류하고 Telegram으로 운영자 판단을 요청합니다. runtime pause 플래그는 자동으로 바꾸지 않습니다.
- local DB에 남은 미청산 live position과 브로커 잔고가 일치하면 다음 장중 cycle에서 신규 진입보다 먼저 손절/익절/타임스탑/전일잔고 청산 대상으로 처리합니다.
- pause 중 수동 주문, 외부 체결, 기존 실계좌 잔고처럼 DB 밖에서 생긴 잔고는 자동 매매하지 않고 operator 확인을 요구합니다.
- `auto-trade-cycle`은 `--confirm AUTO_TRADE`가 없으면 KIS/OpenAI 호출 전에 차단됩니다.
- 실전/모의 주문은 `LIVE_TRADING_ENABLED=true`와 `live_trading_enabled: true`가 모두 필요합니다.
- demo 주문은 `TRADING_MODE=micro_live` 또는 `TRADING_MODE=live`가 필요합니다.
- real 주문은 `TRADING_MODE=live`가 필요합니다.
- `.env`, `data/`, token cache, SQLite DB는 git에 올리지 않습니다.
- 첫 모의계좌 테스트는 반드시 `--max-quantity 1`로 시작합니다.

## 필요한 키와 환경변수

프로젝트 루트의 `.env`에 넣습니다.

경로:

```text
C:\Users\ysyoo\Documents\Visual Studio 2019\Python\260815_AutoTrade\.env
```

필수:

```text
TRADING_MODE=micro_live
LIVE_TRADING_ENABLED=true
KIS_ENV=demo
KIS_DEMO_APP_KEY=...
KIS_DEMO_APP_SECRET=...
KIS_DEMO_ACCOUNT_NO=...
KIS_DEMO_ACCOUNT_PRODUCT_CODE=01
OPENAI_API_KEY=...
TELEGRAM_BOT_TOKEN=...
TELEGRAM_ALLOWED_CHAT_ID=...
```

선택:

```text
OPENAI_MODEL=gpt-4o-mini
OPENAI_ADMIN_KEY=...
# 또는
OPENAI_USAGE_API_KEY=...
AUTO_TRADE_AI=openai
AUTO_TRADE_MAX_QUANTITY=1
AUTO_TRADE_COLLECT_SECONDS=10
AUTO_TRADE_CYCLE_INTERVAL_SECONDS=60
KIS_REAL_APP_KEY=...
KIS_REAL_APP_SECRET=...
KIS_REAL_ACCOUNT_NO=...
KIS_REAL_ACCOUNT_PRODUCT_CODE=01
```

### KIS 키

필요한 값:

- 모의투자: `KIS_DEMO_APP_KEY`, `KIS_DEMO_APP_SECRET`
- 모의계좌: `KIS_DEMO_ACCOUNT_NO`, `KIS_DEMO_ACCOUNT_PRODUCT_CODE`
- 실전투자: `KIS_REAL_APP_KEY`, `KIS_REAL_APP_SECRET`
- 실전계좌: `KIS_REAL_ACCOUNT_NO`, `KIS_REAL_ACCOUNT_PRODUCT_CODE`

KIS 개발자 포털에서 모의투자 앱을 먼저 만들고 demo 환경으로 테스트합니다. 실전 앱/실계좌 값은 모의계좌 BUY/SELL이 검증된 뒤에만 사용합니다.

중요:

- `KIS_HTS_ID`는 app key가 아닙니다. 현재 주문/시세 경로에서는 사용하지 않습니다.
- `KIS_DEMO_ACCOUNT_NO` 또는 `KIS_REAL_ACCOUNT_NO`에는 secret을 넣으면 안 됩니다. 계좌번호만 넣습니다.
- `.env`에서 `#KIS_REAL_APP_KEY=...`처럼 줄이 `#`로 시작하면 주석이라 프로그램이 읽지 않습니다.
- 실전 주문은 Telegram `/env real`, `TRADING_MODE=live`, `LIVE_TRADING_ENABLED=true`, YAML `live_trading_enabled: true`가 모두 맞아야 시도됩니다.

### OpenAI 키

`OPENAI_API_KEY`는 AI 매매 판단에 사용합니다.

생성 위치:

- OpenAI Platform Dashboard
- API Keys 또는 Project API Keys

주의:

- 일반 `OPENAI_API_KEY`만 있어도 AI 판단은 동작합니다.
- 이 키는 비용 조회용 organization costs API 권한이 없을 수 있습니다.

### OpenAI 비용 조회 키

비용 리포트는 OpenAI Usage/Costs API의 `/v1/organization/costs`를 호출합니다.

공식 문서 예시도 `Authorization: Bearer $OPENAI_ADMIN_KEY`를 사용합니다. 즉 비용 조회는 보통 일반 project key가 아니라 **organization admin key**가 필요합니다.

`OPENAI_ADMIN_KEY` 발급 조건:

- OpenAI organization owner 권한이 필요합니다.
- Organization owner만 Admin API key를 만들 수 있습니다.

발급 위치:

```text
https://platform.openai.com/settings/organization/admin-keys
```

발급 후 `.env`에 넣습니다.

```text
OPENAI_ADMIN_KEY=sk-admin-...
```

`OPENAI_USAGE_API_KEY`는 OpenAI의 별도 공식 키 타입이 아닙니다. 이 프로젝트에서 비용 조회 전용 키를 구분해서 넣고 싶을 때 쓰는 alias입니다. 값은 admin key를 넣으면 됩니다.

```text
OPENAI_USAGE_API_KEY=sk-admin-...
```

권장:

- 가능하면 비용 조회 전용 admin key를 따로 만들고 이름을 `KIS AI Scalper Usage`처럼 구분합니다.
- 비용 조회가 필요 없으면 넣지 않아도 됩니다.
- 없으면 리포트에 `openai cost: unavailable (OPENAI_ADMIN_KEY missing)`으로 표시됩니다.

공식 문서:

- OpenAI Usage/Costs API: https://platform.openai.com/docs/api-reference/usage
- OpenAI Admin API Keys: https://platform.openai.com/docs/api-reference/admin-api-keys

### Telegram 키

필요한 값:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_ALLOWED_CHAT_ID`

Telegram BotFather에서 bot token을 만들고, 본인 chat id만 `TELEGRAM_ALLOWED_CHAT_ID`에 넣습니다. 허용되지 않은 chat id의 명령은 무시됩니다.

## 설치와 테스트

로컬:

```powershell
python -m pip install -e ".[dev]"
python -m pytest --basetemp .tmp_pytest_monday
```

Docker:

```powershell
docker compose build app
docker compose run --rm app
```

현재 검증 기준:

```text
147 passed
```

## 월요일 장 시작 전 체크리스트

먼저 테스트:

```powershell
python -m pytest --basetemp .tmp_pytest_monday
```

KIS 인증/시세 smoke:

```powershell
python -m kis_ai_scalper.cli smoke-kis --config config/settings.yaml --env demo --symbol 005930
python -m kis_ai_scalper.cli smoke-ws --config config/settings.yaml --env demo --symbol 005930 --seconds 10
```

watchlist 등록:

```powershell
python -m kis_ai_scalper.cli watchlist-add --db data/kis_ai_scalper.sqlite3 --symbols 005930
python -m kis_ai_scalper.cli watchlist-list --db data/kis_ai_scalper.sqlite3
```

Telegram 확인:

```powershell
python -m kis_ai_scalper.cli telegram-poll --db data/kis_ai_scalper.sqlite3 --limit 10 --timeout-seconds 0
```

런타임 resume:

```powershell
python -m kis_ai_scalper.cli control-resume --db data/kis_ai_scalper.sqlite3 --reason monday_demo
```

Telegram에서 `/control`을 열면 현재 environment와 local 미청산 포지션을 확인할 수 있습니다. `/env`는 현재 environment를 보여주고, `/env demo` 또는 `/env real`은 paused 상태에서만 local control DB의 다음 실행 environment를 바꿉니다.

모의계좌 1회 bounded 자동매매:

```powershell
python -m kis_ai_scalper.cli auto-trade-cycle `
  --config config/settings.yaml `
  --env demo `
  --db data/kis_ai_scalper.sqlite3 `
  --symbols 005930 `
  --collect-seconds 10 `
  --ai openai `
  --max-quantity 1 `
  --notify-telegram `
  --confirm AUTO_TRADE
```

예상 동작:

- 조건이 안 맞으면 주문 없음
- KRX 캘린더상 장이 닫혀 있으면 주문 없음
- AI가 HOLD면 주문 없음
- AI가 HIGH risk로 판단하면 주문 없이 승인 요청 기록
- 정상 BUY 판단 + risk 통과 시 1주 BUY 제출
- 기존 포지션이 손절/익절/타임스탑 조건이면 SELL 제출
- 전일에 열린 local live position이 남아 있으면 신규 진입보다 먼저 SELL 제출

문제가 있으면 Telegram에서 즉시:

```text
/pause
```

## 주요 CLI

상태:

```powershell
python -m kis_ai_scalper.cli control-status --db data/kis_ai_scalper.sqlite3
```

일시정지:

```powershell
python -m kis_ai_scalper.cli control-pause --db data/kis_ai_scalper.sqlite3 --reason operator_pause
```

재개:

```powershell
python -m kis_ai_scalper.cli control-resume --db data/kis_ai_scalper.sqlite3 --reason operator_ready
```

watchlist:

```powershell
python -m kis_ai_scalper.cli watchlist-add --db data/kis_ai_scalper.sqlite3 --symbols 005930,000660
python -m kis_ai_scalper.cli watchlist-remove --db data/kis_ai_scalper.sqlite3 --symbols 000660
python -m kis_ai_scalper.cli watchlist-list --db data/kis_ai_scalper.sqlite3
```

리포트:

```powershell
python -m kis_ai_scalper.cli paper-report --db data/kis_ai_scalper.sqlite3
```

Telegram polling:

```powershell
python -m kis_ai_scalper.cli telegram-poll --db data/kis_ai_scalper.sqlite3 --limit 10 --timeout-seconds 5
```

자동매매 dry-run AI:

```powershell
python -m kis_ai_scalper.cli auto-trade-cycle `
  --config config/settings.yaml `
  --env demo `
  --db data/kis_ai_scalper.sqlite3 `
  --symbols 005930 `
  --ai rule `
  --max-quantity 1 `
  --confirm AUTO_TRADE
```

자동매매 OpenAI:

```powershell
python -m kis_ai_scalper.cli auto-trade-cycle `
  --config config/settings.yaml `
  --env demo `
  --db data/kis_ai_scalper.sqlite3 `
  --symbols 005930 `
  --collect-seconds 10 `
  --ai openai `
  --max-quantity 1 `
  --notify-telegram `
  --confirm AUTO_TRADE
```

## Telegram 명령

지원 명령:

```text
/pause
/resume
/status
/report
/cost
/control
/env
/env demo
/env real
/positions
```

`/control`은 inline 버튼을 표시합니다.

버튼:

- Pause
- Resume
- Status
- Paper report
- OpenAI cost
- Demo env / Real env (paused only)
- Positions

`/report`에는 OpenAI 비용 줄도 포함됩니다. 비용 조회 키가 없으면 unavailable로 표시됩니다.

`/status`와 `/positions`는 local SQLite에 기록된 미청산 포지션을 보여줍니다. `trading-service`는 resume 뒤 장중 cycle 전에 KIS 브로커 잔고와 local DB를 대조합니다. 브로커에만 있거나 수량이 다르면 주문을 보류하고 Telegram으로 묻습니다.

키 누락이나 설정 오류:

- `trading-service`는 resume 상태에서 KIS/OpenAI 필수 키가 없거나 live gate가 맞지 않으면 자동으로 pause로 되돌립니다.
- KIS 잔고조회가 실패하면 service error로 pause합니다.
- 브로커에만 있거나 수량이 다른 포지션은 pause 대신 주문 cycle을 보류하고 Telegram으로 묻습니다.
- Telegram이 설정되어 있으면 어떤 값이 빠졌는지 메시지로 보냅니다.
- Telegram 자체 키가 없으면 콘솔 로그에만 남습니다.

## Portainer 배포

Portainer Stack으로 `docker-compose.yml`을 배포합니다.

Stack 환경변수 예시:

```text
TRADING_MODE=micro_live
LIVE_TRADING_ENABLED=true
KIS_ENV=demo
AUTO_TRADE_AI=openai
AUTO_TRADE_MAX_QUANTITY=1
AUTO_TRADE_COLLECT_SECONDS=10
AUTO_TRADE_CYCLE_INTERVAL_SECONDS=60
```

프로젝트 루트의 `.env`를 compose 파일 옆에 둡니다. compose는 `.env`를 `/app/.env`로 read-only mount하고, `./data`를 `/app/data`로 mount합니다.

서비스:

- `app`: 전체 테스트 실행
- `smoke-kis`: KIS REST/auth smoke
- `smoke-ws`: KIS WebSocket smoke
- `collector`: 제한 시간 read-only 시세 수집
- `telegram-poll`: Telegram 명령 polling, profile `ops`
- `auto-trade-cycle`: 1회 bounded AI 자동매매 cycle, profile `trade`
- `trading-service`: 장기 실행 운영 서비스, profile `trade`

첫 모의계좌 장중 테스트는 `trading-service`를 켠 뒤 Telegram `/control`에서 상태를 보고 `/resume`으로 시작합니다. 서비스는 시작 시 항상 paused로 들어갑니다.

`trading-service`는 같은 `./data:/app/data` 볼륨에 runtime state와 포지션 기록을 남깁니다. Docker 작업 시작 후 Telegram `/status`에서 `paused`, `environment`, 포지션 수를 확인하고, 포지션 대조가 끝난 뒤 `/resume`합니다.

## Docker Compose 사용

테스트:

```powershell
docker compose run --rm app
```

KIS smoke:

```powershell
docker compose run --rm smoke-kis
docker compose run --rm smoke-ws
```

collector:

```powershell
docker compose run --rm collector
```

Telegram poll:

```powershell
docker compose --profile ops run --rm telegram-poll
```

auto trade one-shot:

```powershell
docker compose --profile trade run --rm auto-trade-cycle
```

장기 실행 서비스:

```powershell
docker compose --profile trade up -d trading-service
```

운영 시작:

```text
/control
/env demo
/positions
/resume
```

## GitHub

현재 작업 브랜치:

```text
codex/ai-auto-trading-readiness
```

```powershell
git push -u origin codex/ai-auto-trading-readiness
```

## 참고

- KIS 공식 샘플: https://github.com/koreainvestment/open-trading-api
- exchange_calendars XKRX: https://github.com/gerrymanoim/exchange_calendars
- OpenAI Usage/Costs API: https://platform.openai.com/docs/api-reference/usage
- OpenAI Admin API Keys: https://platform.openai.com/docs/api-reference/admin-api-keys
