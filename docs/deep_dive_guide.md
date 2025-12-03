# 개인 금융 데이터 플랫폼 - 12살 탐험가를 위한 완벽 가이드 🚀

> 이 문서는 "왜?"라는 질문을 끝없이 던지는 당신을 위한 것입니다.
> 표면적인 설명이 아닌, 기술의 본질과 철학을 탐험합니다.

---

## 📖 목차

1. [프로젝트 전체 개요](#1-프로젝트-전체-개요)
2. [Apache Spark - 분산 처리의 마법사](#2-apache-spark)
3. [Delta Lake - 시간여행이 가능한 데이터 저장소](#3-delta-lake)
4. [Medallion Architecture - 데이터의 정제 과정](#4-medallion-architecture)
5. [Star Schema - 데이터를 별처럼 배치하는 이유](#5-star-schema)
6. [Faker - 가짜 데이터를 만드는 마법](#6-faker)
7. [전체 시스템이 동작하는 과정](#7-전체-시스템-동작-과정)

---

## 1. 프로젝트 전체 개요

### 🎯 우리가 만든 것: 개인 금융 분석 로봇

상상해보세요. 당신이 1년 동안 카드로 결제한 모든 내역이 있다고 해봅시다.
- 스타벅스에서 커피 100번
- 이마트에서 장보기 50번
- CGV에서 영화 20번
- 총 10,000건의 거래

**질문들**:
- 이번 달에 얼마나 썼지?
- 어디서 가장 많이 썼지?
- 주말에 더 많이 쓰나, 평일에 더 많이 쓰나?
- 작년 같은 달과 비교하면 어떻지?

이런 질문에 **자동으로 답해주는 로봇**을 만든 겁니다!

### 🏗️ 시스템의 3단계 여정

```
📝 가짜 데이터 생성
    ↓
🔶 Bronze (청동) - 원본 그대로 보관
    ↓
🔷 Silver (은) - 깨끗하게 정리
    ↓
🌟 Gold (금) - 분석하기 쉽게 재배치
    ↓
📊 답변: "이번 달 식비는 50만원입니다!"
```

---

## 2. Apache Spark

### 2.1 설계 이념: "데이터가 너무 커서 한 컴퓨터로는 안 돼!"

#### 🤔 풀고자 했던 문제

**상황**: 2000년대 초반, 구글과 페이스북의 고민
- 하루에 수십억 개의 검색 기록
- 수억 명의 사용자 데이터
- 한 대의 컴퓨터로는 **절대 처리 불가능**

**전통적인 방법의 한계**:
```python
# 일반적인 Python 코드
data = []
for i in range(1_000_000_000):  # 10억 건
    data.append(process(i))

# 문제:
# 1. 메모리 부족 (RAM이 터짐)
# 2. 시간이 너무 오래 걸림 (며칠~몇 주)
# 3. 중간에 실패하면 처음부터 다시
```

**Spark의 해결책**: "여러 컴퓨터를 동시에 사용하자!"
```
컴퓨터 1: 데이터 1~100만 처리
컴퓨터 2: 데이터 100만~200만 처리
컴퓨터 3: 데이터 200만~300만 처리
...
컴퓨터 100: 데이터 9900만~1억 처리

→ 100배 빠르게 완료!
```

### 2.2 비슷한 기술들과의 비교

#### Option 1: Hadoop MapReduce (Spark의 할아버지)
```
장점: 안정적, 오래됨
단점: 느림 (디스크에 계속 저장)

비유: 
- MapReduce = 우편 배달 (편지를 쓰고, 우체통에 넣고, 배달되고...)
- Spark = 전화 통화 (바로바로 대화)
```

#### Option 2: Pandas (Python 라이브러리)
```
장점: 쉬움, 빠름 (작은 데이터)
단점: 큰 데이터 불가능 (한 컴퓨터의 메모리 한계)

비유:
- Pandas = 혼자 청소 (빠르지만 큰 집은 무리)
- Spark = 청소 팀 (느리지만 큰 건물도 가능)
```

#### Option 3: Dask (Pandas의 분산 버전)
```
장점: Pandas와 비슷한 문법
단점: 생태계가 작음, 안정성 낮음

비유:
- Dask = 신생 회사 (빠르게 성장 중이지만 아직 작음)
- Spark = 대기업 (안정적이고 검증됨)
```

### 2.3 왜 Spark를 선택했는가?

**우리 프로젝트의 요구사항**:
1. ✅ 학습 목적 → 현업에서 가장 많이 사용하는 기술
2. ✅ 확장 가능 → 10만 건 → 100만 건 → 1억 건으로 늘릴 수 있어야 함
3. ✅ 포트폴리오 → 면접관이 알아보는 기술

**트레이드오프**:
| 얻은 것 | 잃은 것 |
|--------|--------|
| 대용량 처리 능력 | 설정이 복잡함 |
| 분산 처리 | 작은 데이터에는 오버헤드 |
| 현업 표준 기술 | 학습 곡선 가파름 |

### 2.4 Spark의 장단점

**장점** ✅:
1. **속도**: 메모리에서 처리 (Hadoop보다 100배 빠름)
2. **확장성**: 컴퓨터 1대 → 1000대로 확장 가능
3. **다양한 작업**: 배치, 스트리밍, ML, 그래프 처리 모두 가능
4. **언어 지원**: Python, Scala, Java, R, SQL

**단점** ❌:
1. **복잡성**: 설정과 튜닝이 어려움
2. **메모리 소비**: RAM을 많이 사용
3. **작은 데이터**: 1만 건 이하는 Pandas가 더 빠름
4. **디버깅**: 에러 메시지가 복잡함

### 2.5 Spark 내부 동작 과정

#### Step 1: 작업 분해 (Job → Stage → Task)

```python
# 당신이 쓴 코드
df = spark.read.parquet("data.parquet")
df.filter(col("amount") > 10000).count()
```

**Spark가 내부적으로 하는 일**:
```
1. Job 생성: "데이터를 읽고, 필터링하고, 세어라"
2. Stage 분해:
   - Stage 1: 파일 읽기
   - Stage 2: 필터링 및 카운트
3. Task 생성:
   - Task 1-1: 파일 1~100 읽기
   - Task 1-2: 파일 101~200 읽기
   - ...
   - Task 2-1: 필터링 및 카운트
```

#### Step 2: 데이터 분산 (Partitioning)

```
원본 파일 (1GB)
    ↓
[Partition 1: 100MB] → 컴퓨터 1
[Partition 2: 100MB] → 컴퓨터 2
[Partition 3: 100MB] → 컴퓨터 3
...
[Partition 10: 100MB] → 컴퓨터 10

각 컴퓨터가 동시에 처리!
```

#### Step 3: Lazy Evaluation (게으른 실행)

**중요한 개념**: Spark는 **진짜 필요할 때까지 실행 안 함**

```python
# 이 코드들은 실행 안 됨 (계획만 세움)
df = spark.read.parquet("data")
df2 = df.filter(col("amount") > 10000)
df3 = df2.select("merchant_name")

# 여기서 처음 실행됨!
df3.show()  # ← Action (액션)
```

**왜 이렇게 할까?**
```
나쁜 방법 (즉시 실행):
1. 파일 읽기 → 디스크에 저장
2. 필터링 → 디스크에 저장
3. 선택 → 디스크에 저장
(디스크 I/O가 느림!)

좋은 방법 (Lazy):
1. 전체 계획을 먼저 봄
2. 최적화 (불필요한 단계 제거)
3. 한 번에 실행 (메모리에서)
```

#### Step 4: DAG (Directed Acyclic Graph) 최적화

```
당신의 코드:
read → filter → select → join → groupBy → count

Spark의 최적화:
read → (filter + select) → join → (groupBy + count)
       └─ 합침 ─┘              └─── 합침 ───┘
```

---

## 3. Delta Lake

### 3.1 설계 이념: "데이터 레이크의 문제를 해결하자"

#### 🤔 풀고자 했던 문제

**Data Lake의 문제점**:
```
상황: 회사에 데이터가 쌓여있는 폴더

/data/
  ├── transactions_2024_01.parquet
  ├── transactions_2024_02.parquet
  ├── transactions_2024_02_fixed.parquet  ← 뭐가 최신?
  ├── transactions_2024_02_final.parquet  ← 이게 진짜?
  └── transactions_2024_02_REAL_FINAL.parquet ← ???

문제:
1. 어떤 파일이 최신인지 모름
2. 실수로 삭제하면 복구 불가능
3. 동시에 쓰면 데이터 깨짐
4. 스키마 변경하면 이전 데이터 못 읽음
```

**Delta Lake의 해결책**: "데이터베이스처럼 관리하자!"

### 3.2 비슷한 기술들과의 비교

#### Option 1: 일반 Parquet 파일
```
장점: 빠름, 압축 좋음
단점: ACID 없음, 버전 관리 없음

비유:
- Parquet = 종이 노트 (빠르지만 수정 어려움)
- Delta Lake = 워드 프로세서 (수정 이력 저장)
```

#### Option 2: Apache Iceberg
```
장점: Delta Lake와 비슷한 기능
단점: 생태계 작음, Spark 외 지원 약함

비유:
- Iceberg = 경쟁 제품 (비슷하지만 덜 유명)
- Delta Lake = 시장 점유율 1위
```

#### Option 3: Apache Hudi
```
장점: 실시간 업데이트 특화
단점: 복잡함, 학습 곡선 가파름

비유:
- Hudi = 전문가용 도구
- Delta Lake = 일반인도 쓸 수 있는 도구
```

### 3.3 왜 Delta Lake를 선택했는가?

**우리 프로젝트의 요구사항**:
1. ✅ ACID 트랜잭션 → 데이터 안정성
2. ✅ Time Travel → 실수해도 복구 가능
3. ✅ Spark 통합 → 쉬운 사용
4. ✅ 학습 자료 많음 → Databricks 공식 지원

**트레이드오프**:
| 얻은 것 | 잃은 것 |
|--------|--------|
| ACID 보장 | 약간의 성능 오버헤드 |
| 버전 관리 | 스토리지 공간 증가 |
| 스키마 진화 | 복잡도 증가 |

### 3.4 Delta Lake의 장단점

**장점** ✅:
1. **ACID 트랜잭션**: 데이터 일관성 보장
2. **Time Travel**: 과거 버전으로 돌아가기
3. **Schema Evolution**: 스키마 변경 가능
4. **Upsert (Merge)**: 업데이트와 삽입을 동시에
5. **최적화**: OPTIMIZE, VACUUM 명령어

**단점** ❌:
1. **스토리지 증가**: 버전 이력 저장으로 용량 증가
2. **복잡성**: 일반 Parquet보다 복잡
3. **Spark 의존성**: Spark 없이 사용 어려움

### 3.5 Delta Lake 내부 동작 과정

#### 핵심: Transaction Log (트랜잭션 로그)

```
Delta Lake 폴더 구조:

/data/gold/fact_transactions/
  ├── _delta_log/
  │   ├── 00000000000000000000.json  ← 버전 0
  │   ├── 00000000000000000001.json  ← 버전 1
  │   ├── 00000000000000000002.json  ← 버전 2
  │   └── 00000000000000000003.json  ← 버전 3 (최신)
  ├── part-00000.parquet
  ├── part-00001.parquet
  └── part-00002.parquet
```

**Transaction Log의 내용**:
```json
{
  "commitInfo": {
    "timestamp": 1701590400000,
    "operation": "WRITE",
    "operationMetrics": {
      "numFiles": 3,
      "numOutputRows": 10000
    }
  },
  "add": {
    "path": "part-00000.parquet",
    "size": 1024000,
    "modificationTime": 1701590400000
  }
}
```

#### Time Travel 동작 원리

```python
# 최신 데이터 읽기
df = spark.read.format("delta").load("/data/gold/fact_transactions")

# 버전 1로 돌아가기
df_v1 = spark.read.format("delta") \
    .option("versionAsOf", 1) \
    .load("/data/gold/fact_transactions")

# 내부 동작:
# 1. _delta_log/00000000000000000001.json 읽기
# 2. 그 시점에 존재했던 파일 목록 확인
# 3. 해당 파일들만 읽기
```

#### ACID 트랜잭션 동작

```python
# 예: 데이터 업데이트
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/data/gold/fact_transactions")

# Merge (Upsert) 실행
delta_table.alias("target").merge(
    new_data.alias("source"),
    "target.transaction_id = source.transaction_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# 내부 동작:
# 1. 새로운 transaction log 파일 생성 (임시)
# 2. 데이터 파일 쓰기
# 3. transaction log 파일 커밋 (원자적)
# 4. 성공하면 완료, 실패하면 롤백
```

---

## 4. Medallion Architecture

### 4.1 설계 이념: "데이터도 정제 과정이 필요하다"

#### 🤔 풀고자 했던 문제

**상황**: 원본 데이터는 지저분함
```
원본 데이터의 문제:
1. 중복: 같은 거래가 2번 기록됨
2. 오류: 금액이 -100원 (말이 안 됨)
3. 불일치: "스타벅스", "스타벅스 강남점", "Starbucks" (같은 곳)
4. 누락: 날짜가 없는 거래
```

**전통적인 방법의 문제**:
```python
# 한 번에 모든 처리
raw_data → 정제 → 분석

문제:
1. 원본 데이터 손실 (실수하면 복구 불가)
2. 디버깅 어려움 (어디서 문제?)
3. 재사용 불가 (다른 분석에 못 씀)
```

**Medallion의 해결책**: "단계별로 나누자!"

### 4.2 3단계 레이어 설명

#### 🔶 Bronze Layer (청동): 원본 보관소

```
역할: 원본 데이터를 그대로 저장
규칙: 절대 수정하지 않음 (Immutable)

비유: 박물관의 원본 유물
- 만지지 않음
- 보존만 함
- 필요하면 복사본 만듦
```

**예시**:
```python
# Bronze에 저장
df_bronze = spark.read.parquet("raw_data.parquet")
df_bronze.write.format("delta").mode("append").save("/bronze/transactions")

# 특징:
# - 중복 있어도 OK
# - 오류 있어도 OK
# - 그냥 저장만 함
```

#### 🔷 Silver Layer (은): 정제된 데이터

```
역할: 데이터 정제 및 검증
작업:
1. 중복 제거
2. 오류 데이터 필터링
3. 데이터 타입 변환
4. 정규화 (통일)

비유: 세탁소
- 더러운 옷 → 깨끗한 옷
- 찢어진 옷 → 버림
- 같은 옷 → 하나만 남김
```

**예시**:
```python
# Silver 변환
df_silver = df_bronze \
    .dropDuplicates(["transaction_id"]) \  # 중복 제거
    .filter(col("amount") > 0) \            # 오류 제거
    .withColumn("merchant_normalized",      # 정규화
                normalize_udf(col("merchant_name")))

df_silver.write.format("delta").save("/silver/transactions")
```

#### 🌟 Gold Layer (금): 분석 최적화

```
역할: 비즈니스 분석을 위한 재구성
작업:
1. Star Schema로 재배치
2. 집계 및 요약
3. 비즈니스 로직 적용

비유: 요리된 음식
- 재료 (Silver) → 완성된 요리 (Gold)
- 바로 먹을 수 있음
- 목적에 맞게 조리됨
```

**예시**:
```python
# Gold: Fact + Dimension 테이블
fact_transactions = silver_df.join(dim_date, ...).join(dim_merchant, ...)
fact_transactions.write.format("delta").save("/gold/fact_transactions")
```

### 4.3 비슷한 아키텍처들

#### Option 1: Lambda Architecture
```
구조: Batch Layer + Speed Layer + Serving Layer
장점: 실시간 + 배치 모두 처리
단점: 복잡함, 유지보수 어려움

비유:
- Lambda = 고속도로 + 일반도로 (복잡하지만 빠름)
- Medallion = 일반도로만 (단순하지만 충분)
```

#### Option 2: Kappa Architecture
```
구조: Stream Processing만 사용
장점: 단순함
단점: 배치 처리 어려움

비유:
- Kappa = 스트리밍만 (실시간 특화)
- Medallion = 배치 특화
```

### 4.4 왜 Medallion을 선택했는가?

**우리 프로젝트의 요구사항**:
1. ✅ 배치 처리 → Medallion이 최적
2. ✅ 단순함 → 학습하기 쉬움
3. ✅ 업계 표준 → Databricks 공식 권장

**트레이드오프**:
| 얻은 것 | 잃은 것 |
|--------|--------|
| 명확한 단계 구분 | 스토리지 3배 사용 |
| 디버깅 용이 | 처리 시간 증가 |
| 재사용 가능 | 복잡도 증가 |

### 4.5 Medallion 내부 동작 과정

```
전체 흐름:

1. Raw Data (CSV/Parquet)
   ↓
2. Bronze Ingestion
   - 원본 그대로 Delta Lake에 저장
   - 메타데이터 추가 (언제, 어디서 왔는지)
   ↓
3. Silver Transformation
   - 중복 제거: dropDuplicates()
   - 검증: filter(amount > 0)
   - 정규화: UDF 적용
   - Delta Lake Merge (Upsert)
   ↓
4. Gold Star Schema
   - Dimension 테이블 생성
   - Fact 테이블 생성 (Surrogate Key 조인)
   - Partitioning 적용
   ↓
5. Analysis
   - SQL 쿼리
   - 집계 및 리포트
```

---

## 5. Star Schema

### 5.1 설계 이념: "데이터를 별처럼 배치하면 빠르다"

#### 🤔 풀고자 했던 문제

**상황**: 일반적인 테이블 구조의 문제

```sql
-- 일반 테이블 (Normalized)
transactions:
  transaction_id, date, merchant_id, category_id, amount

merchants:
  merchant_id, name, category, location

categories:
  category_id, name, parent_category

dates:
  date, year, month, quarter, is_weekend

-- 분석 쿼리
SELECT 
    d.year, d.month,
    c.name,
    SUM(t.amount)
FROM transactions t
JOIN merchants m ON t.merchant_id = m.merchant_id
JOIN categories c ON m.category_id = c.category_id  
JOIN dates d ON t.date = d.date
GROUP BY d.year, d.month, c.name

-- 문제:
-- 1. JOIN이 너무 많음 (느림)
-- 2. 쿼리가 복잡함
-- 3. 인덱스 관리 어려움
```

**Star Schema의 해결책**: "중심에 Fact, 주변에 Dimension"

```
        dim_date
           │
           ├── dim_category
           │        │
    fact_transactions (중심)
           │        │
           ├── dim_merchant

별 모양! ⭐
```

### 5.2 Fact vs Dimension 이해하기

#### Fact Table (사실 테이블): 측정 가능한 것

```
특징:
- 숫자 데이터 (amount, quantity)
- 많은 행 (수백만~수억 건)
- Foreign Key로 Dimension 연결

비유: 영수증
- 날짜: 2024-01-15
- 상점: 스타벅스
- 금액: 4,500원
```

#### Dimension Table (차원 테이블): 설명하는 것

```
특징:
- 텍스트 데이터 (이름, 카테고리)
- 적은 행 (수백~수천 건)
- 계층 구조 (year → month → day)

비유: 사전
- 날짜 정보: 2024년 1월 15일 월요일 주말아님
- 상점 정보: 스타벅스 강남점 식비-카페
```

### 5.3 비슷한 스키마들

#### Option 1: Snowflake Schema (눈송이)
```
구조: Dimension이 또 정규화됨

dim_category
    ↓
dim_subcategory
    ↓
dim_category_group

장점: 스토리지 절약
단점: JOIN 더 많음 (느림)

비유:
- Star = 평평한 지도 (빠름)
- Snowflake = 입체 지도 (정확하지만 복잡)
```

#### Option 2: Normalized Schema (정규화)
```
구조: 3NF 정규화

장점: 중복 없음, 업데이트 쉬움
단점: 분석 쿼리 느림

비유:
- Normalized = 백과사전 (정확하지만 찾기 어려움)
- Star = 요약본 (빠르게 찾기)
```

### 5.4 왜 Star Schema를 선택했는가?

**우리 프로젝트의 요구사항**:
1. ✅ 분석 쿼리 최적화 → Star가 최고
2. ✅ BI 도구 호환 → Star가 표준
3. ✅ 이해하기 쉬움 → 별 모양 직관적

**트레이드오프**:
| 얻은 것 | 잃은 것 |
|--------|--------|
| 쿼리 속도 빠름 | 스토리지 증가 (중복) |
| JOIN 최소화 | 업데이트 복잡 |
| BI 도구 최적화 | 유연성 감소 |

### 5.5 Star Schema 내부 동작 과정

#### Step 1: Dimension 테이블 생성

```python
# dim_date 생성 (731일)
dates = []
for day in range(731):
    dates.append({
        'date_key': 20230101 + day,  # Surrogate Key
        'full_date': date(2023, 1, 1) + timedelta(days=day),
        'year': 2023,
        'month': 1,
        'is_weekend': day % 7 in [5, 6]
    })

dim_date = spark.createDataFrame(dates)
dim_date.write.format("delta").save("/gold/dim_date")
```

#### Step 2: Fact 테이블 생성 (Surrogate Key 조인)

```python
# Silver 데이터
silver_df:
  transaction_id, date, merchant_name, amount

# Dimension 테이블들
dim_date:
  date_key, full_date, year, month

dim_merchant:
  merchant_key, merchant_name

# Fact 테이블 생성
fact_df = silver_df \
    .join(dim_date, silver_df.date == dim_date.full_date) \
    .join(dim_merchant, silver_df.merchant_name == dim_merchant.merchant_name) \
    .select(
        monotonically_increasing_id().alias("transaction_key"),
        dim_date.date_key,        # ← Surrogate Key
        dim_merchant.merchant_key, # ← Surrogate Key
        silver_df.amount
    )

# 결과:
# transaction_key | date_key | merchant_key | amount
# 1               | 20230101 | 5            | 4500
# 2               | 20230101 | 12           | 8900
```

#### Step 3: 분석 쿼리 실행

```sql
-- 월별 카테고리 지출
SELECT 
    d.year,
    d.month_name,
    c.category_name,
    SUM(f.amount) as total
FROM fact_transactions f
JOIN dim_date d ON f.date_key = d.date_key        -- 1번 JOIN
JOIN dim_category c ON f.category_key = c.category_key  -- 2번 JOIN
GROUP BY d.year, d.month_name, c.category_name

-- 장점:
-- 1. JOIN 2번만 (Normalized는 5번+)
-- 2. 인덱스 최적화 쉬움 (Surrogate Key)
-- 3. 쿼리 이해하기 쉬움
```

---

## 6. Faker

### 6.1 설계 이념: "진짜 같은 가짜 데이터"

#### 🤔 풀고자 했던 문제

**상황**: 테스트 데이터가 필요함

```python
# 나쁜 방법: 수동으로 만들기
data = [
    {"name": "김철수", "date": "2024-01-01", "amount": 5000},
    {"name": "이영희", "date": "2024-01-02", "amount": 3000},
    # ... 10,000건을 손으로?? 😱
]

문제:
1. 시간이 너무 오래 걸림
2. 현실적이지 않음 (패턴이 없음)
3. 재현 불가능 (매번 다름)
```

**Faker의 해결책**: "자동으로 현실적인 데이터 생성"

### 6.2 Faker의 장단점

**장점** ✅:
1. **다양한 로케일**: 한국어, 영어, 일본어 등
2. **현실적**: 실제 이름, 주소, 전화번호 패턴
3. **재현 가능**: Seed로 동일한 데이터 생성
4. **쉬움**: 간단한 API

**단점** ❌:
1. **완벽하지 않음**: 가끔 이상한 데이터
2. **느림**: 대용량 생성 시 시간 걸림
3. **메모리**: 많은 데이터 생성 시 메모리 부족

### 6.3 Faker 내부 동작 과정

#### Seed의 마법

```python
# Seed 없이
fake1 = Faker()
print(fake1.name())  # "김철수"
fake2 = Faker()
print(fake2.name())  # "이영희" (다름!)

# Seed 사용
Faker.seed(42)
fake1 = Faker()
print(fake1.name())  # "김철수"

Faker.seed(42)
fake2 = Faker()
print(fake2.name())  # "김철수" (같음!)

# 내부 동작:
# 1. Seed = 난수 생성기의 시작점
# 2. 같은 시작점 = 같은 순서의 난수
# 3. 같은 난수 = 같은 데이터
```

#### 로케일의 작동 원리

```python
# 한국어
fake_kr = Faker('ko_KR')
fake_kr.name()      # "김철수"
fake_kr.address()   # "서울특별시 강남구..."

# 영어
fake_en = Faker('en_US')
fake_en.name()      # "John Smith"
fake_en.address()   # "123 Main St, New York..."

# 내부 동작:
# 1. 로케일별 데이터 파일 로드
#    - ko_KR: 한국 성씨 목록, 한국 지명
#    - en_US: 영어 이름 목록, 미국 주소
# 2. 랜덤하게 조합
#    - 성 + 이름
#    - 도시 + 구 + 동
```

---

## 7. 전체 시스템 동작 과정

### 7.1 전체 흐름 (처음부터 끝까지)

```
┌─────────────────────────────────────────────────────────┐
│ Step 1: 데이터 생성 (Faker)                              │
│ - 10,000건의 가짜 거래 데이터                             │
│ - Parquet 파일로 저장                                    │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│ Step 2: Bronze Layer (Spark + Delta Lake)               │
│ - Parquet 읽기                                          │
│ - 메타데이터 추가 (언제, 어디서)                          │
│ - Delta Lake에 저장 (원본 보존)                          │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│ Step 3: Silver Layer (Spark Transformation)             │
│ - 중복 제거: dropDuplicates()                           │
│ - 검증: filter(amount > 0)                              │
│ - 정규화: normalize_merchant_udf()                      │
│ - Delta Lake Merge (Upsert)                             │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│ Step 4: Gold Layer (Star Schema)                        │
│ - Dimension 테이블 생성 (dim_date, dim_merchant, ...)   │
│ - Fact 테이블 생성 (Surrogate Key 조인)                 │
│ - Partitioning (date_key)                               │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│ Step 5: Analysis (Spark SQL)                            │
│ - 월별 지출 분석                                         │
│ - 카테고리별 분석                                        │
│ - Top 상점 분석                                          │
│ - 인사이트 도출                                          │
└─────────────────────────────────────────────────────────┘
```

### 7.2 실제 코드 실행 흐름

```bash
# 1. 데이터 생성
$ uv run python data_generator/generate_all.py --records 10000

내부 동작:
1. TransactionGenerator 초기화 (seed=42)
2. Faker('ko_KR') 생성
3. 10,000번 반복:
   - 랜덤 상점 선택
   - 랜덤 날짜 생성
   - 랜덤 금액 생성
4. Pandas DataFrame 생성
5. Parquet 파일 저장

# 2. 파이프라인 실행
$ uv run python jobs/run_pipeline.py

내부 동작:
1. Spark Session 생성
   - Delta Lake 설정
   - 메모리 설정
2. Bronze Ingestion
   - Parquet 읽기
   - Delta Lake 쓰기
3. Silver Transformation
   - Delta Lake 읽기
   - 중복 제거, 검증
   - Delta Lake Merge
4. Gold Dimension 생성
   - dim_date (731일)
   - dim_merchant (41개)
   - dim_category (18개)
5. Gold Fact 생성
   - Surrogate Key 조인
   - Partitioning
6. Spark Session 종료

# 3. 분석 실행
$ uv run python analytics/quick_analysis.py

내부 동작:
1. Spark Session 생성
2. Delta Lake 테이블 로드
3. Temp View 등록
4. SQL 쿼리 실행
   - 월별 집계
   - 카테고리별 집계
   - Top 상점 분석
5. 결과 출력
6. Spark Session 종료
```

### 7.3 데이터의 여정 (한 거래의 일생)

```
1. 탄생 (Faker)
   transaction_id: "abc-123"
   date: 2024-01-15
   merchant: "스타벅스 강남점"
   amount: 4500

2. Bronze 입성
   + ingestion_timestamp: 2024-12-03 16:00:00
   + source_file: "card_transactions.parquet"

3. Silver 정제
   - 중복 체크: OK (유일함)
   - 검증: OK (amount > 0)
   + normalized_merchant: "스타벅스"
   + transaction_type: "EXPENSE"

4. Gold 변신
   transaction_key: 1
   date_key: 20240115 (Surrogate Key)
   merchant_key: 5 (Surrogate Key)
   category_key: 2 (Surrogate Key)
   amount: 4500

5. 분석에 사용됨
   "2024년 1월 식비-카페: 4,500원"
```

---

## 8. 핵심 개념 정리

### 8.1 왜 이렇게 복잡하게?

**질문**: "그냥 Excel로 하면 안 돼?"

**답변**:
```
Excel의 한계:
- 최대 100만 행 (우리는 1억 건 목표)
- 한 컴퓨터만 사용 (Spark는 100대 사용 가능)
- 버전 관리 없음 (Delta Lake는 Time Travel)
- 분석 느림 (Star Schema는 최적화)

결론: 작은 데이터는 Excel, 큰 데이터는 Spark!
```

### 8.2 각 기술의 역할 요약

| 기술 | 역할 | 비유 |
|-----|------|------|
| **Spark** | 대용량 데이터 처리 | 청소 팀 (여러 명이 동시에) |
| **Delta Lake** | 안전한 저장 | 은행 금고 (ACID, 버전 관리) |
| **Medallion** | 단계별 정제 | 세탁소 (더러움 → 깨끗함) |
| **Star Schema** | 빠른 분석 | 별 모양 지도 (빠른 검색) |
| **Faker** | 테스트 데이터 | 가짜 돈 (연습용) |

### 8.3 트레이드오프 총정리

```
우리가 선택한 것:
✅ 확장성 (1억 건 처리 가능)
✅ 안정성 (ACID, 버전 관리)
✅ 학습 가치 (현업 기술)

우리가 포기한 것:
❌ 단순함 (설정 복잡)
❌ 속도 (작은 데이터는 느림)
❌ 스토리지 (3배 사용)

결론: 큰 데이터, 안정성이 중요하면 이 방법이 최고!
```

---

## 9. 더 깊이 파고들기

### 9.1 추천 학습 경로

```
1단계: 기본 이해
- Spark 공식 문서 읽기
- Delta Lake 튜토리얼
- Star Schema 예제

2단계: 실습
- 데이터 크기 늘리기 (10만 → 100만 → 1000만)
- 쿼리 최적화 실험
- Time Travel 사용해보기

3단계: 심화
- Spark Internals 공부
- Delta Lake 소스코드 읽기
- 성능 튜닝
```

### 9.2 자주 묻는 질문

**Q1: Spark 없이 Delta Lake 쓸 수 있나요?**
```
A: 기술적으로는 가능하지만 어려움
- Delta Lake는 Spark API에 최적화됨
- 다른 도구 (Presto, Trino)도 지원하지만 제한적
```

**Q2: Bronze/Silver/Gold 꼭 써야 하나요?**
```
A: 아니요, 선택사항
- 작은 프로젝트: Bronze → Gold 바로
- 큰 프로젝트: 3단계 모두 권장
```

**Q3: Star Schema vs Snowflake Schema?**
```
A: 목적에 따라 다름
- 분석 많음 → Star (빠름)
- 업데이트 많음 → Snowflake (정규화)
```

---

## 10. 마무리

### 당신이 배운 것

1. **Spark**: 여러 컴퓨터로 빠르게 처리
2. **Delta Lake**: 안전하게 저장하고 시간여행
3. **Medallion**: 단계별로 깨끗하게 정제
4. **Star Schema**: 별처럼 배치해서 빠르게 분석
5. **Faker**: 진짜 같은 가짜 데이터 생성

### 다음 단계

```
🎯 도전 과제:
1. 데이터를 100만 건으로 늘려보기
2. 새로운 분석 쿼리 만들어보기
3. 시각화 추가하기 (matplotlib)
4. 실시간 데이터 처리 (Spark Streaming)
```

### 기억할 것

> "모든 기술은 문제를 해결하기 위해 만들어졌다.
> 문제를 이해하면, 기술도 이해할 수 있다."

---

**축하합니다! 🎉**
당신은 이제 현대 데이터 엔지니어링의 핵심 개념을 이해했습니다.
계속 탐험하고, 질문하고, 실험하세요!
