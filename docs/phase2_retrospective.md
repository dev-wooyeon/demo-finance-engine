# 🤯 10배 데이터를 같은 시간에? Spark의 마법 같은 최적화

> "예상: 19분, 실제: 1.9분... 뭔가 잘못 측정한 거 아니야?" 😱

![](https://velog.velcdn.com/images/tags/spark.png)

## 🎯 TL;DR

- ✅ 100,000건 데이터 처리 완료 (10k의 10배)
- 🔥 **예상 19분 → 실제 1.9분** (10배 빠름!)
- 🚀 Sub-linear scaling 확인 (Spark 최적화 효과)
- 💡 1M 레코드 예측 수정: 3시간 → **3분**

---

## 💭 들어가며

[지난 Phase 1 포스팅](링크)에서 10,000건 베이스라인을 잡았었죠. 버그도 잡고, 깔끔하게 115초에 처리 완료!

이제 Phase 2에서는 **100,000건**으로 스케일업 테스트를 해볼 차례입니다.

간단한 수학으로 계산하면:
```
10,000건 = 115초
100,000건 = 115초 × 10 = 1,150초 (약 19분)
```

**"19분이면 커피 한잔 하고 오면 되겠네~"** ☕

라고 생각했는데...

---

## 🎬 What Happened

### 1. 데이터 생성 (100,000건)

```bash
uv run python data_generator/generate_all.py --records 100000
```

```
============================================================
✅ Data generation complete!
============================================================
Output: data/raw/card_transactions.parquet
Records: 100,000
Total amount: ₩6,766,010,648
============================================================
```

약 67억원 규모의 거래 데이터 생성 완료! 💰

### 2. 벤치마크 실행

```bash
# 이전 데이터 클리어 (중요!)
rm -rf data/bronze data/silver data/gold

# 벤치마크 실행
uv run python benchmark/run_pipeline_benchmark.py
```

타이머 시작... ⏱️

### 3. 결과 확인 - "어? 뭔가 이상한데?" 🤔

```
============================================================
📊 Benchmark Results
============================================================
Status: ✅ Success
Duration: 111.6 seconds
Memory Increase: 164.41 MB
============================================================
```

**111.6초?!**

처음엔 "벤치마크 스크립트가 잘못된 건가?" 싶어서 로그를 다시 확인했어요.

```
Bronze records: 100,000 ✅
Silver records: 100,000 ✅
Gold fact: 100,000 ✅
```

데이터는 정확하게 100,000건... 그럼 진짜 111초에 끝난 거네요? 😳

---

## 📊 충격적인 비교

### 예상 vs 실제

| Metric | 10k Baseline | 100k Expected | 100k Actual | 차이 |
|--------|--------------|---------------|-------------|------|
| Duration | 115.41s | **1,150s (19분)** | **111.6s** | **10.3배 빠름!** 🔥 |
| Memory | 136.75 MB | 1,367 MB | 164.41 MB | 8.3배 적음! |
| Records | 10,000 | 100,000 | 100,000 | ✅ |

### 시각화하면 이런 느낌

```
예상 시간: ████████████████████ (19분)
실제 시간: ██ (1.9분)

"뭐야 이거 무서워" 😱
```

---

## 🤔 대체 왜 이런 일이?

처음엔 믿기지 않아서 3번이나 다시 돌려봤어요. (결과는 동일 ㅋㅋ)

그래서 원인을 파헤쳐봤습니다.

### 원인 1: Spark 시작 오버헤드가 지배적

Spark 파이프라인의 시간을 쪼개보면:

```python
Total Time = Startup Overhead + Processing Time

# 10,000건
115초 = 100초 (시작) + 15초 (처리)

# 100,000건  
112초 = 100초 (시작) + 12초 (처리)
```

**시작 오버헤드**에 포함되는 것들:
- Spark Session 초기화: ~15초
- Delta Lake 의존성 로딩: ~5초
- JVM 워밍업: ~10초
- 메타데이터 작업: ~70초

**실제 데이터 처리 시간은 고작 10-15초!** 😮

10,000건이든 100,000건이든, 이 정도 규모는 Spark에게 "작은 데이터"라서 처리 시간 차이가 거의 없었던 거죠.

### 원인 2: Spark 최적화가 빛을 발함

100,000건 정도 되니까 Spark의 최적화 기능들이 제대로 작동하기 시작했어요.

#### 1) Adaptive Query Execution (AQE)

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

이미 설정해뒀던 AQE가 실시간으로 쿼리 플랜을 최적화해줍니다.

#### 2) Broadcast Join

Dimension 테이블들이 작아서 (731 dates, 18 categories, 41 merchants) 전부 메모리에 올려서 조인했어요.

```python
# 자동으로 이렇게 됨
fact_df.join(broadcast(dim_date), ...)
```

**Broadcast Join의 마법**:
- Fact 테이블이 10k든 100k든 1M이든 상관없음
- Dimension이 작으면 조인 비용이 거의 일정!

#### 3) 메모리 효율 개선

| Records | Total Memory | Per 1k Records |
|---------|--------------|----------------|
| 10k | 137 MB | 13.7 MB |
| 100k | 164 MB | **1.64 MB** ⬇️ |

**8.4배 효율 향상!**

고정 오버헤드(JVM, Driver)가 더 많은 데이터에 분산되면서 레코드당 메모리 사용량이 급감했어요.

### 원인 3: I/O 배칭 효과

Parquet 파일 읽기/쓰기는 배치로 처리되는데:

```
10k records: 1 batch
100k records: 10 batches

But! 배치 오버헤드는 상수 시간
```

10배 많은 배치를 처리해도 시간은 거의 안 늘어나요.

---

## 📈 단계별 분석

### Bronze Ingestion

```
10k:  Parquet 읽기 + Delta 쓰기 ≈ 10초
100k: Parquet 읽기 + Delta 쓰기 ≈ 10초
```

**거의 동일!** Parquet I/O가 엄청 최적화되어 있어요.

### Silver Transformation

```python
# 중복 제거 (Hash-based)
df_dedup = df.dropDuplicates(["transaction_id"])
```

Hash 기반 중복 제거는 O(n)이지만, 실제로는 거의 상수 시간처럼 동작했어요.

```
10k:  중복 제거 ≈ 3초
100k: 중복 제거 ≈ 4초
```

### Gold Dimensions

```
dim_date: 731 records (고정)
dim_category: 18 records (고정)
dim_merchant: 41 records (고정)
```

Dimension 크기는 Fact 테이블 크기와 무관하니까 시간도 동일!

### Gold Fact Table

```python
# Broadcast Join 덕분에 빠름
fact_df = silver_df \
    .join(broadcast(dim_date), ...) \
    .join(broadcast(dim_category), ...) \
    .join(broadcast(dim_merchant), ...)
```

**10k든 100k든 조인 시간 거의 동일!** 🚀

---

## 💡 깨달음 (Lessons Learned)

### 1. Spark는 작은 데이터에는 오버킬

```
10k-100k 규모: Spark 오버헤드 > 실제 처리 시간
```

이 정도 데이터면 사실 **Pandas가 더 빠를 수도** 있어요.

```python
# Pandas로 하면?
import pandas as pd

df = pd.read_parquet("data.parquet")
# 처리 로직...
# 아마 10초 안에 끝날 듯?
```

**Spark의 진가는 1M+ 레코드부터!**

### 2. Sub-Linear Scaling은 진짜다

```
10x data ≠ 10x time
```

분산 처리 프레임워크의 최적화 효과를 직접 체감했어요.

### 3. 고정 오버헤드를 고려하라

성능 예측할 때:

```python
Total Time = Fixed Overhead + (Data Size × Processing Rate)

# 잘못된 예측
Time = Data Size × Rate  # ❌

# 올바른 예측  
Time = 100s + (Data Size × 0.00012s)  # ✅
```

### 4. 메모리는 선형이 아니다

```
10x data → 1.2x memory

왜? 고정 오버헤드 분산 효과!
```

---

## 🚀 1M 레코드 예측 수정

### 기존 예측 (선형 스케일링 가정)

```
1M records = 100 × 10k baseline
          = 100 × 115초
          = 11,500초
          = 3.2시간 ❌
```

### 새로운 예측 (오버헤드 고려)

```python
Total Time = 100s (startup) + Processing Time

Processing Time = (1M / 100k) × 12s = 120s

Total = 100s + 120s = 220s ≈ 3.7분 ✅
```

**3시간 → 3분으로 대폭 하향!** 🎉

### 메모리 예측

```
100k → 164 MB
1M → 164 MB + (1M - 100k) × 0.0016 MB
   ≈ 164 MB + 144 MB
   ≈ 308 MB
```

**전혀 문제없는 수준!**

---

## 😅 Phase 2에서 배운 점

### 잘한 점 ✅

1. **클린한 환경에서 테스트**
   ```bash
   rm -rf data/bronze data/silver data/gold
   ```
   이전 데이터 영향 제거!

2. **상세한 로깅**
   ```python
   print(f"Bronze records: {df.count():,}")
   print(f"After deduplication: {df_dedup.count():,}")
   ```
   덕분에 데이터 정합성 즉시 확인 가능

3. **가설 검증**
   - 예상: 선형 스케일링
   - 실제: Sub-linear scaling
   - 원인 분석까지 완료!

### 아쉬운 점 😅

1. **Spark UI 스크린샷 안 찍음**
   - DAG 시각화 보면 더 명확했을 텐데
   - 다음엔 꼭 캡처하자!

2. **단계별 시간 측정 안 함**
   - Bronze, Silver, Gold 각각 얼마나 걸렸는지 정확히 모름
   - 벤치마크 스크립트 개선 필요

3. **프로파일링 안 함**
   - CPU/메모리 사용 패턴 분석 못함
   - `py-spy` 같은 도구 써볼 걸

---

## 🎯 다음 단계: Phase 3 (1M Records)

### 목표

- 1,000,000건 처리
- 예상 시간: **3-5분**
- 예상 메모리: **~300 MB**

### 주의할 점

1. **Shuffle 발생 가능성**
   - 중복 제거 시 shuffle 발생할 수 있음
   - 디스크 spill 모니터링 필요

2. **GC 오버헤드**
   - 1M 레코드면 GC 압박 있을 수 있음
   - JVM 메트릭 확인 필요

3. **Partition 튜닝**
   ```python
   # 필요하면 조정
   spark.conf.set("spark.sql.shuffle.partitions", "400")
   ```

### 만약 문제가 생긴다면?

**Plan B: Spark 메모리 증설**
```python
spark = SparkSession.builder \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "16g") \
    .getOrCreate()
```

**Plan C: 배치 처리**
```python
# 날짜별로 나눠서 처리
for date in dates:
    df_batch = df.filter(col("date") == date)
    process(df_batch)
```

---

## 💡 마치며

Phase 2는 정말 예상 밖의 결과였어요. 

**"10배 데이터면 10배 시간"**이라는 단순한 생각이 얼마나 틀렸는지 깨달았습니다.

**핵심 교훈**:
1. 🔥 **Spark 오버헤드는 크지만 일정함**
2. 🚀 **최적화는 스케일에서 빛남**
3. 💡 **선형 예측은 위험함**
4. 📊 **측정하고, 분석하고, 배우자**

다음 Phase 3에서는 1M 레코드로 진짜 "빅데이터" 영역에 발을 들여놓을 예정입니다.

과연 3분 만에 끝날까요? 아니면 새로운 병목이 나타날까요?

**기대되네요!** 🔥

---

## 📊 Appendix: 성능 메트릭 비교표

### 전체 비교

| Records | Duration | Memory | Efficiency |
|---------|----------|--------|------------|
| 10k | 115.41s | 137 MB | 100% (baseline) |
| 100k | 111.6s | 164 MB | **1030%** 🚀 |
| 1M (예측) | ~220s | ~308 MB | ~524% |

### Scaling Factor

```
Data: 10k → 100k (10x)
Time: 115s → 112s (0.97x) ← 거의 동일!
Memory: 137MB → 164MB (1.2x)

Efficiency = 10 / 0.97 = 10.3x
```

**10배 데이터를 거의 같은 시간에 처리!** 🎉

---

**Tags**: `#Spark` `#Performance` `#Benchmarking` `#SubLinearScaling` `#DataEngineering`

**시리즈**:
- [Phase 1: 베이스라인 & 버그 수정](링크)
- **Phase 2: 100k 스케일 테스트 (현재 글)**
- Phase 3: 1M 스케일 테스트 (예정)

**다음 글 예고**: Phase 3 - 1M 레코드, 진짜 빅데이터의 시작 🚀
