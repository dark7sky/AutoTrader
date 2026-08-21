# Portainer Git Stack 운영자 절차

이 문서는 Portainer에서 저장소의 `docker-compose.yml`을 Git Stack으로 배포해 KIS AI Scalper를 운영하는 절차입니다. 대상은 장기 실행 `trading-service` 하나이며, 실전 검증 완료를 뜻하지 않습니다. 먼저 KIS 모의계좌로 장중 1주를 검증하고, 실제 주문 게이트와 실계좌 전환은 별도 승인 후 진행합니다.

## 1. 배포 전 준비

Portainer 호스트에서 다음을 준비합니다.

- Docker와 Portainer가 정상 동작하는지 확인합니다.
- Git 저장소에 접근할 수 있는 URL, 브랜치, compose 파일 경로를 준비합니다.
- `DATA_DIR`로 사용할 호스트의 영속 디렉터리를 만들고 컨테이너 사용자 UID `10001`이 읽고 쓸 수 있게 합니다.
- KIS demo 앱·계좌·HTS 사용자/로그인 ID, Telegram bot·허용 chat id, OpenAI API 키를 준비합니다.
- Git 저장소와 Stack 환경변수에 비밀값을 커밋하지 않습니다. 아래의 `<...>`는 실제 값으로 바꿔 Portainer UI에만 입력합니다.

`trading-service`는 Compose에서 profile이 없는 기본 서비스입니다. Stack 배포 시 이 서비스가 기본으로 올라오며, `app`, smoke, collector, `telegram-poll`, `auto-trade-cycle`은 보조 프로파일입니다. 운영 중 `auto-trade-cycle`을 별도로 실행하면 주문 경로가 중복될 수 있으므로 함께 실행하지 않습니다.

## 2. Git Stack 생성

1. Portainer에서 **Stacks > Add stack**을 엽니다.
2. 이름을 예를 들어 `kis-ai-scalper`로 지정합니다.
3. **Build method > Git Repository**를 선택합니다.
4. 저장소 URL과 운영할 브랜치를 입력합니다.
5. Compose path는 저장소 루트의 `docker-compose.yml`로 둡니다.
6. **Environment variables**에 아래 값을 입력합니다.
7. 첫 배포는 주문 게이트를 `false`로 두고 **Deploy the stack**을 실행합니다.

### 권장 Stack 환경변수

아래 값은 첫 demo 점검용 기준입니다. 비밀값은 자리표시자이므로 Portainer에서 실제 값으로 교체합니다.

```dotenv
LIVE_TRADING_ENABLED=false
KIS_ENV=demo

KIS_DEMO_APP_KEY=<KIS_DEMO_APP_KEY>
KIS_DEMO_APP_SECRET=<KIS_DEMO_APP_SECRET>
KIS_DEMO_ACCOUNT_NO=<KIS_DEMO_ACCOUNT_NO>
KIS_DEMO_ACCOUNT_PRODUCT_CODE=01

TELEGRAM_BOT_TOKEN=<TELEGRAM_BOT_TOKEN>
TELEGRAM_ALLOWED_CHAT_ID=<TELEGRAM_ALLOWED_CHAT_ID>
TELEGRAM_ALLOWED_USER_ID=<TELEGRAM_ALLOWED_USER_ID>

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
DATA_DIR=/srv/kis-ai-scalper/data
BUY_ORDER_TTL_SECONDS=60
SELL_ORDER_TTL_SECONDS=30
SERVICE_HEARTBEAT_MAX_AGE_SECONDS=180
```

`OPENAI_ADMIN_KEY` 또는 `OPENAI_USAGE_API_KEY`는 OpenAI `/cost` 조회가 필요할 때만 선택적으로 추가합니다.

```dotenv
OPENAI_ADMIN_KEY=<OPENAI_ADMIN_KEY>
# 또는
OPENAI_USAGE_API_KEY=<OPENAI_USAGE_API_KEY>
```

### 주문 기능 게이트

저장소 기본과 Compose 기본은 `LIVE_TRADING_ENABLED=false`이며 주문이 차단됩니다. 이 값이 주문 제출을 여는 유일한 배포 게이트입니다.

```dotenv
LIVE_TRADING_ENABLED=false
```

KIS demo 주문을 실제로 접수시키는 검증 시점에만 Stack 환경변수를 다음처럼 바꾸고 **Update the stack**으로 재배포합니다.

```dotenv
LIVE_TRADING_ENABLED=true
```

`LIVE_TRADING_ENABLED=true`여도 서비스가 paused 상태이면 주문하지 않습니다. Telegram `/resume`, KRX 거래일·장중 조건, risk 및 불일치 검사를 모두 통과해야 주문할 수 있습니다.

`KIS_ENV`는 smoke·단발 CLI의 기본 대상입니다. 장기 실행 `trading-service`의 runtime environment는 SQLite에 보존되고 신규 DB에서는 `demo`로 시작합니다. 이후 paused 상태에서 Telegram `/env demo` 또는 `/env real`로 선택합니다. real 전환에는 KIS real 키·계정과 challenge 확인이 필요합니다.

서비스 기본값은 수집 10초, cycle 간격 20초입니다. 수집이 끝난 직후 계좌·risk snapshot을 새로 읽습니다. AI 판단은 deterministic 후보가 있을 때만, 같은 bar에서는 한 번만 수행하며 25초 deadline·8초 timeout·1회 재시도를 사용하고 KIS 현재가를 응답 뒤 재검증합니다.

`KIS_HTS_ID`는 앱 키·시크릿·계좌번호가 아니라 KIS HTS 사용자/로그인 ID입니다. 실시간 체결통보 event-driven 처리를 위해 강하게 권장되며, 운영 주문에서는 입력을 필수로 취급합니다. 실전은 `H0STCNI0`, 모의는 `H0STCNI9`를 사용합니다. 체결통보 worker를 사용할 수 없는 동안에도 REST 주문 supervisor가 `ORDER_SUPERVISOR_INTERVAL_SECONDS=5`로 fallback합니다.

배포 호스트에서는 주문 없이 구독 ACK를 확인할 수 있습니다.

```powershell
docker compose --profile smoke run --rm smoke-fill-notice
```

## 3. 첫 기동 확인

배포 직후 Portainer에서 `kis-ai-scalper_trading-service`의 상태와 로그를 확인합니다.

- 컨테이너가 `running`인지 확인합니다.
- Healthcheck가 시작 유예 후 정상인지 확인합니다. heartbeat가 180초를 넘으면 healthcheck가 비정상으로 판단합니다.
- 로그에 `runtime: paused`와 서비스 시작 알림이 있는지 확인합니다.
- Stack을 재배포하거나 컨테이너가 재시작되어도 runtime은 항상 paused로 시작합니다.
- paused 상태에서도 preflight와 게이트가 유효하면 supervisor가 잔고·주문을 대조하고 heartbeat를 갱신합니다. 이 상태에서는 신규 주문을 제출하지 않습니다.
- `DATA_DIR`가 `/app/data`에 연결되었는지 확인합니다. SQLite와 token cache는 컨테이너 내부 임시 영역에 두지 않습니다.

Telegram에서 다음 순서로 확인합니다.

```text
/control
/status
/env
/positions
/orders
/fills
/cancel-open-buys
/env demo
```

`/env demo`도 paused 상태에서만 처리됩니다. local open position 또는 active/unknown broker order가 있으면 환경 변경이 거부됩니다.

## 4. demo 장중 시작

첫 주에는 반드시 demo로 진행하고 `AUTO_TRADE_MAX_QUANTITY=1`을 유지합니다.

1. 주문 게이트를 `false`로 둔 상태에서 Stack 기동, heartbeat, Telegram 연결, watchlist, KIS read-only 조회를 확인합니다.
2. smoke가 필요한 경우 Portainer에서 보조 프로파일을 별도로 실행하거나 호스트에서 read-only 명령을 실행합니다. 이 도구와 `trading-service`를 중복 운영하지 않습니다.
3. KRX 거래일과 장중 시각을 확인합니다. 서비스는 `XKRX` 캘린더를 사용하며, 휴장일·장외에는 주문하지 않습니다. 캘린더를 사용할 수 없으면 broker preflight가 fail-closed입니다.
4. 실제 demo 주문 접수 검증을 시작할 때 `LIVE_TRADING_ENABLED=true`로 바꾸고 Update the stack을 실행합니다.
5. 재기동 후에도 paused인지 확인합니다. Telegram의 `모의매매 준비 점검` 버튼 또는 `/readiness`에서 주문 게이트, demo KIS 키·계좌, AI 키, 관심종목, worker heartbeat와 불일치 플래그를 확인합니다.
6. `blockers: none`인지 확인하고 `/resume` 또는 `거래 재개` 버튼을 입력합니다. `krx_market_open=false`만으로는 Resume이 거부되지 않으며 장이 열릴 때까지 주문만 발생하지 않습니다.
7. 장중에는 `최근 AI 판단` 또는 `/decisions`에서 cycle의 action/reason/submitted와 최근 AI audit을 확인하고, `/status`, `/live-report`, `/orders`, `/fills`, `/approvals`를 관찰합니다.
8. 장 마감 후 보고서와 데이터를 백업합니다.

조건이 맞지 않아 주문이 없는 것은 오류가 아닙니다. HOLD, risk reject, KRX 휴장, 신규진입 시간 종료, AI 비용 제한, 잔고 대조 실패는 각각 주문이 없거나 신규진입이 차단되는 정상적인 안전 경로일 수 있습니다.

## 5. Telegram 제어와 real challenge

Telegram의 `/start` 또는 `/menu`에서 메인 메뉴를 열고 버튼으로 준비 점검·상태·거래·제어·환경·AI 메뉴에 접근할 수 있습니다. 거래 메뉴에서 관심종목과 최근 AI 판단을 조회할 수 있습니다. 버튼 탐색 중에는 현재 메뉴 메시지가 교체되어 메시지가 누적되지 않습니다. 기존 명령과 승인 버튼도 계속 사용할 수 있습니다.

지원하는 운영 명령은 다음과 같습니다.

```text
/menu
/pause [reason]
/resume [reason]
/status
/control
/env
/env demo
/env real
/confirm-real <6자리 challenge>
/positions
/orders
/fills
/approvals
/approve <request_id>
/reject <request_id>
/report
/live-report
/cost
/emergency-stop
/cancel-open-buys
/clear-emergency
```

`운용 제어` 메뉴의 **미체결 매수 취소** 버튼도 `/cancel-open-buys`와 같습니다. `/emergency-stop`과 `/cancel-open-buys`는 paused로 전환하고 local ledger에 알려진 open BUY만 취소 요청합니다. broker가 `CANCELLED` 등 terminal 상태를 확인할 때까지 `CANCEL_PENDING`으로 유지하며, 보유 주식을 자동 매도하거나 자동 청산하지 않습니다.

real 전환은 다음의 다단계 확인입니다.

1. runtime을 paused로 둡니다.
2. `/env real`을 입력하면 6자리 challenge가 발급됩니다. challenge는 5분 후 만료되며, 이 단계만으로 real 환경으로 바뀌지 않습니다.
3. `/confirm-real <challenge>`를 paused 상태에서 입력합니다.
4. 확인이 성공하면 runtime environment가 real로 선택되고, 15분 동안 1회만 사용할 수 있는 resume arm이 생깁니다.
5. `LIVE_TRADING_ENABLED=true`, KIS real 키·계정과 runtime 대상 real 선택을 확인한 뒤 `/resume`을 입력합니다.

real runtime에서 pause하거나 `/emergency-stop`을 사용하면 resume arm이 사라집니다. `/clear-emergency`도 paused 상태에서만 가능하며, 이후에는 새 real challenge가 필요합니다. 실계좌 전환은 demo 1주 검증 결과와 운영자 승인이 없는 상태에서 수행하지 않습니다.

## 6. 불일치·미체결·승인 운영

### 잔고와 주문 불일치

service cycle은 시세 수집 후 새로 읽은 KIS 주문 상태·계좌/risk snapshot과 local ledger를 대조합니다.

KIS 체결통보 worker는 실전 `H0STCNI0`, 모의 `H0STCNI9`를 사용해 접수·체결·부분 체결을 ledger에 반영합니다. `/status`에서 worker 상태와 heartbeat를 확인합니다. worker가 `KIS_HTS_ID` 누락 또는 연결 문제로 unavailable이면 REST order supervisor가 5초 주기로 보완합니다.

- broker에만 있는 기존 잔고는 local position으로 자동 채택하지 않습니다.
- local에만 있거나 수량이 다른 position은 자동 청산하지 않습니다.
- local 주문에 대응하지 않는 broker-only 주문·체결도 자동으로 local 거래로 만들지 않습니다.
- 불일치가 있으면 `operator_review=true`, `block_new_entries=true`가 기록되고 신규 BUY가 차단됩니다.
- 정상 `reconciled`는 Telegram에 반복 전송하지 않습니다. 이상 원인은 `/status`와 `/readiness`에서 확인하며, 3회 연속 정상 대조가 확인되면 복구 알림이 한 번 전송됩니다.
- runtime을 자동으로 pause하지는 않으므로 운영자는 Telegram으로 알림을 확인하고 원인 확인 전 수동으로 `/pause`를 유지합니다.

재시작 중 체결, 외부 수동 주문, 계좌 조회 실패, local DB 복원 시점을 확인한 뒤 KIS와 local ledger를 운영자가 대조합니다. 불일치를 자동 채택·자동 청산으로 덮어쓰지 않습니다.

### 미체결 주문

- BUY 미체결은 기본 60초 후 취소 요청입니다.
- SELL 미체결은 기본 30초 후 취소 요청입니다.
- 취소 요청의 HTTP 성공은 취소 확정이 아닙니다. KIS terminal 상태를 재조회할 때까지 `CANCEL_PENDING`으로 둡니다.
- 주문 식별자가 없거나, 취소가 애매하거나, 상태 조회가 실패하면 `UNKNOWN`과 운영자 검토를 기록하고 신규진입을 차단합니다.

### 고위험 승인

고위험 AI BUY만 Telegram 승인 요청으로 기록됩니다. 요청은 2분 후 만료되고, 승인 전에는 주문이 제출되지 않습니다. `/approvals`에서 request id·수량·가격·만료 시각을 확인하고 `/approve <id>` 또는 `/reject <id>`로 처리합니다. 승인 후에도 당시 신호·가격·수량·risk 조건이 바뀌면 주문하지 않습니다.

## 7. 월요일부터 1주 demo 검증

이 절차는 코드 준비 상태를 장중 운영 증거로 확인하는 과정입니다. 실전 검증 완료 선언이 아닙니다.

월요일 장 시작 전:

1. `DATA_DIR`를 백업하고 KIS demo 계좌와 local DB의 기존 position·active order를 대조합니다.
2. Stack을 `paused`, `KIS_ENV=demo`, `LIVE_TRADING_ENABLED=false`, `AUTO_TRADE_MAX_QUANTITY=1`로 기동합니다.
3. `/status`, `/positions`, `/orders`, `/fills`와 heartbeat를 확인합니다.
4. read-only smoke와 watchlist를 확인합니다.
5. demo 주문을 검증할 때만 `LIVE_TRADING_ENABLED=true`로 바꾸고 재배포한 뒤, 다시 `/status`를 확인하고 `/resume`합니다.
6. `/status`에서 체결통보 worker 또는 REST supervisor의 상태·heartbeat를 확인합니다.

화요일부터 금요일까지:

- 매일 장 시작 시 pause·environment·heartbeat·operator review·신규진입 차단 플래그를 기록합니다.
- BUY/SELL 접수, 체결, 부분 체결, 미체결, BUY 60초·SELL 30초 취소와 terminal 확정을 기록합니다.
- 실제 demo 장중 BUY, 부분 체결, 취소, SELL을 모두 검증합니다. 이 검증이 끝나기 전에는 real로 전환하지 않습니다.
- 고위험 승인 2분 만료와 승인 후 조건 재검사를 확인합니다.
- broker-only·local-only·수량 불일치가 한 번이라도 생기면 신규진입을 차단한 채 원인을 조사합니다.
- 장 마감 후 `/report`, `/live-report`, `/orders`, `/fills`와 관련 로그를 백업합니다.

금요일에는 KIS demo 응답과 local ledger의 일치 여부, service 재시작 후 항상 paused가 되는지, Telegram 제어와 알림이 동작하는지를 검토합니다. 실제 장중 demo BUY·부분 체결·취소·SELL 검증이 남아 있거나 미확인 항목이 하나라도 있으면 real로 전환하지 않습니다.

## 8. 영속화와 백업

Compose의 `DATA_DIR`는 호스트 경로를 컨테이너 `/app/data`로 마운트합니다. 다음 자료가 여기에 있습니다.

- `kis_ai_scalper.sqlite3`: runtime control, tick·bar, AI audit, 주문·체결·position ledger
- `auth/`: KIS token cache
- runtime metadata, heartbeat, Telegram update offset, live report snapshot

Portainer에서 백업할 때는 다음 순서를 지킵니다.

1. Stack의 `trading-service`를 중지합니다.
2. 호스트의 `DATA_DIR` 전체를 날짜가 있는 별도 백업 위치로 복사합니다.
3. 백업에 token cache가 포함될 수 있으므로 파일 권한과 접근자를 제한합니다.
4. Stack을 다시 시작하고 `/status`, KIS 잔고, local position을 대조합니다.

Linux Docker 호스트의 예:

```bash
sudo mkdir -p /srv/kis-ai-scalper/data
sudo chown -R 10001:10001 /srv/kis-ai-scalper/data
docker compose stop trading-service
tar -czf /srv/kis-ai-scalper-backup-$(date +%Y%m%d-%H%M%S).tgz -C /srv/kis-ai-scalper data
docker compose up -d trading-service
```

실제 Stack 경로가 다르면 `DATA_DIR`에 입력한 경로를 사용합니다. 복원도 서비스 중지 후 수행하며, 복원된 잔고·주문 상태를 운영자가 다시 확인하기 전에는 `/resume`하지 않습니다.

## 9. 장애 시 조치

- 즉시 멈춤: `/emergency-stop` 또는 `/pause`
- 상태 확인: `/status`, `/live-report`, `/positions`, `/orders`, `/fills`
- 불일치: 신규진입을 허용하지 말고 KIS·local ledger·외부 수동 주문을 대조
- Telegram 장애: Portainer 로그와 heartbeat를 확인하고, 필요하면 Stack을 paused 상태로 재시작
- KIS/API 오류: 서비스가 preflight 또는 cycle을 fail-closed로 처리했는지 확인하고 오류 원인을 기록
- 승인 오작동 우려: `/reject <request_id>` 또는 `/emergency-stop` 후 원인 확인

장애를 해결하기 위해 DB의 position이나 broker order를 임의로 삭제하지 않습니다. 상태를 보존하고, 운영 기록과 KIS 원장에 근거해 조정합니다.
