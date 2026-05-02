# 수출입 통계 대시보드 — 프로젝트 문서

## 개요
한국 수출 품목별 월간 통계를 모니터링하는 Streamlit 대시보드.
Excel 파일(주요품목별수출정리 N월.xlsx) → SQLite → 웹 대시보드 구조.

---

## 디렉터리 구조

```
C:\수출입 통계\               ← Excel 원본 보관 (git 비추적)
│   주요품목별수출정리 4월 잠정.xlsx
│   주요품목별수출정리 5월.xlsx  ...
│
└── dashboard\                ← git 저장소 루트
    ├── streamlit_app.py      ← 배포 메인 (Railway/Streamlit Cloud)
    ├── app.py                ← 로컬 실행용 (streamlit_app.py와 항상 동일)
    ├── parser.py             ← Excel → SQLite ETL
    ├── scheduler.py          ← 매일 09:00 자동 파싱 (로컬 전용)
    ├── export_data.db        ← SQLite DB (git 추적, 배포 시 포함)
    ├── visitors.db           ← 방문자 카운터 DB (git 비추적, 서버 자체 생성)
    ├── requirements.txt      ← Python 패키지
    ├── runtime.txt           ← python-3.11 지정
    ├── Procfile              ← Railway 배포 명령
    ├── .streamlit/
    │   └── config.toml       ← 다크 테마 설정
    ├── start.bat             ← 로컬 대시보드 시작
    ├── update.bat            ← 변경 파일만 재파싱
    ├── force_update.bat      ← 전체 강제 재파싱
    └── push_update.bat       ← 파싱 + git commit + push (데이터 업데이트용)
```

---

## 데이터 소스 — Excel 구조

- **파일명 패턴**: `주요품목별수출정리*.xlsx`
- **위치**: `C:\수출입 통계\`
- **시트 구성**: 첫 번째 시트(목록·설정) 제외, 나머지 261개 시트가 각 품목
- **매월 사용자가 직접 파일 추가**

### 시트 내부 구조 (0-indexed 컬럼)
| 컬럼 | 인덱스 | 내용 |
|---|---|---|
| 날짜 | 0 | `2024년04월` 형태 |
| 일평균 수출액($) | 5 | export_avg |
| MoM | 6 | 전월 대비 |
| YoY | 7 | 전년 동월 대비 |
| 단가($/kg) | 8 | unit_price |
| 단가 YoY | 9 | price_yoy |
| 월간 총 수출액($) | 16 | export_dollar (표준 시트) |
| 월간 총 수출액(₩) | 17 | export_won |
| 중량(kg) | 18 | weight_kg (표준 시트) |

### 비표준 시트 (미국·중국 등 국가별)
- 오른쪽 보조 테이블 시작 위치가 다름
- `parser.py`의 `detect_dollar_weight_cols()` 함수가 헤더 텍스트('달러', '중량')를 스캔해 자동 탐지
- 탐지 실패 시 crosscheck(`dollar/weight ≈ unit_price`) 로직으로 폴백

### 메타정보 위치
- **기업명**: row 1, col 10 이후 첫 번째 문자열
- **HS코드**: row 2, col 10 이후 첫 번째 숫자
- **데이터 시작행**: row 1~11 스캔, `2024년04월` 형태 날짜 패턴(`\d{4}`) 탐지

---

## SQLite DB 스키마

```sql
items        (id, sheet_name UNIQUE, company, hs_code, source_file, updated_at)
export_data  (id, sheet_name, period TEXT "YYYY-MM", export_avg, mom, yoy,
              unit_price, price_yoy, export_dollar, export_won, weight_kg,
              UNIQUE(sheet_name, period))
parse_log    (id, file_path, parsed_at, sheets_count, records_count)
```

- `period` 형식: `"2024-04"` (YYYY-MM)
- ON CONFLICT UPSERT 방식으로 증분 업데이트
- `needs_reparse()`: 파일 mtime vs parse_log 비교로 변경 감지

---

## 파싱 실행 방법

```bash
# 변경된 파일만
python parser.py

# 전체 강제 재파싱
python parser.py --force
```

**파싱 결과 (2025-05 기준)**: 246개 품목, 12,661건  
(15개 시트는 데이터 없음으로 스킵 — 빈 시트 또는 다른 구조)

---

## 대시보드 기능

### 📋 전체 현황
1. KPI (총 품목 수, MoM 상승/하락, 평균 YoY)
2. 전체 품목 합산 월별 수출액 + 12M 이동평균 + 평균 MoM
3. MoM 히트맵 (수출액 상위 40개 품목 × 최근 18개월)
4. 최신 수출액 Top/Bottom 15 (MoM 색상 표시)
5. MoM/YoY 상위·하위 15개 가로 막대
6. 필터·정렬 가능 전체 테이블

### 🔍 종목별 상세
- 사이드바: selectbox 클릭 → 타이핑 → Enter (1단계 검색)
- KPI 카드 (월간수출액, 일평균, MoM, YoY, 단가)
- Tab 1: 수출액 막대 + 일평균 선 + 단가 2축 + 이동평균
- Tab 2: MoM/YoY 막대 (상승=초록, 하락=빨강) + 최근 12개월 가로 막대
- Tab 3: 연도별 비교 (연도별 선, x축=월) + YoY 비교 + 단가 비교
- Tab 4: 단가 추이 + 단가 YoY + 중량 추이 (3단 서브플롯)
- Tab 5: 원본 데이터 테이블 + CSV 다운로드

### 📈 4개월 성장 분석
- 비교 기준: 최신월 vs 4개월 전 `(latest - 4mo_ago) / 4mo_ago`
- KPI (성장/감소 품목 수, 최고 성장 품목, 평균 성장률)
- 성장률 순위 상위/하위 N 가로 막대 (슬라이더로 N 조정)
- 버블 차트: X=4M성장률, Y=YoY, 버블크기=수출액 (4분면 자동 레이블)
- 인덱스 추이: 상위 N개 품목의 7개월 추이 (4개월 전=100 정규화)
- 실제 수출액 그룹 막대
- 전체 성장률 테이블 + CSV 다운로드

### 사이드바 공통
- 누적 방문자 수 (`visitors.db` SQLite, 세션당 1회)
- 로컬 환경: 갱신/강제갱신 버튼 표시
- 클라우드 환경: "push_update.bat 실행" 안내 표시
- 최종 갱신 시각 표시

---

## 배포 환경

### Railway (현재 운영 중) ← 메인
- **GitHub 저장소**: `https://github.com/cygnusg10th-commits/export-dashboard`
- **배포 파일**: `streamlit_app.py`
- **시작 명령**: `Procfile` — `streamlit run streamlit_app.py --server.port $PORT --server.address 0.0.0.0`
- 뱃지·워터마크 없음

### Streamlit Community Cloud (백업)
- 동일 저장소 연결, `streamlit_app.py` 사용
- 하단 왕관 뱃지가 완전 제거 불가 (무료 플랜 제약)

### 로컬
- `start.bat` 실행 → `http://localhost:8501`
- `IS_LOCAL = Path(r"C:\수출입 통계").exists()` 로 환경 자동 감지

---

## 데이터 업데이트 워크플로우

```
① C:\수출입 통계\ 에 새 Excel 파일 추가
② push_update.bat 실행
   → python parser.py          (증분 파싱)
   → git add export_data.db
   → git commit -m "data: update YYYY-MM-DD"
   → git push
③ Railway 자동 재배포 (1~2분)
```

---

## 주요 설계 결정 및 주의사항

1. **한국어 인코딩**: `ws.cell().value == "날짜"` 비교가 Windows 터미널에서 실패함.
   → 날짜 행 탐지는 정규식 `\d{4}` 패턴만 사용 (한글 비교 금지)

2. **비표준 시트**: '미국', '중국' 등 국가별 시트는 오른쪽 보조 테이블 컬럼 위치가 다름.
   → `detect_dollar_weight_cols()` 함수가 헤더 텍스트 스캔으로 자동 처리

3. **app.py vs streamlit_app.py**: 두 파일은 항상 동일해야 함.
   → 수정 후 반드시 `cp streamlit_app.py app.py` 동기화

4. **visitors.db**: `.gitignore` 처리됨. Railway 서버에서 자체 생성·유지.
   재배포 시 카운터 초기화됨 (Railway 컨테이너 재시작 시)

5. **export_data.db**: git 추적됨. `push_update.bat`으로 함께 push.
   GitHub 파일 크기 제한(100MB) 주의.

6. **python-3.11**: `runtime.txt`로 고정. pandas 2.2.2가 Python 3.14 미지원.

---

## 패키지

```
streamlit==1.35.0
plotly==5.22.0
pandas==2.2.2
openpyxl
```

로컬 전용 (requirements.txt 미포함):
```
apscheduler==3.10.4  (scheduler.py용)
```
