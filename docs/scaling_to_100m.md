# MacBook M2 Pro 32GB에서 1억 건 처리 분석

## 🖥️ 하드웨어 스펙

```
MacBook M2 Pro
- CPU: 10-12 코어 (Performance + Efficiency)
- RAM: 32GB 통합 메모리
- SSD: 512GB ~ 2TB (읽기: ~5GB/s, 쓰기: ~4GB/s)
- 아키텍처: ARM64 (Apple Silicon)
```

---

## 📊 데이터 크기 추정

### 현재 (10,000건)
```
Raw Parquet: ~2MB
Bronze Delta: ~5MB
Silver Delta: ~5MB
Gold Delta: ~10MB
총 스토리지: ~22MB
```

### 1억 건 (10,000배)
```
Raw Parquet: ~2GB
Bronze Delta: ~5GB
Silver Delta: ~5GB
Gold Delta (Fact + Dimensions): ~10GB
Transaction Log: ~500MB
총 스토리지: ~22.5GB

✅ 스토리지: 문제없음 (충분한 공간)
```

---

## ⚡ 예상 실행 시간

### 1. 데이터 생성 (Faker)

```bash
uv run python data_generator/generate_all.py --records 100000000
```

**예상 시간: 30분 ~ 1시간**

```python
# 병목: Python 단일 스레드
# 초당 생성: ~50,000건
# 100,000,000건 / 50,000 = 2,000초 = 33분

실제 요인:
- Faker 호출 오버헤드
- Pandas DataFrame 생성
- Parquet 쓰기 (압축)

최적화 방법:
- 멀티프로세싱 사용
- 배치 단위로 저장 (10만 건씩)
```

### 2. Bronze Ingestion

```bash
uv run python jobs/run_pipeline.py
```

**예상 시간: 2-3분**

```
작업:
1. Parquet 읽기: ~30초
   - 2GB / 5GB/s = 0.4초 (이론값)
   - 실제: 압축 해제, 파싱 포함 ~30초

2. Delta Lake 쓰기: ~2분
   - 메타데이터 추가
   - Parquet 파일 생성
   - Transaction Log 작성

메모리 사용: ~8GB
- Spark Driver: 2GB
- Executor: 4GB
- 버퍼: 2GB
```

### 3. Silver Transformation

**예상 시간: 5-10분**

```
작업:
1. Delta Lake 읽기: ~1분
2. 중복 제거 (dropDuplicates): ~3분
   - 전체 데이터 스캔 필요
   - 해시 테이블 생성
3. 검증 (filter): ~30초
4. UDF 적용 (normalize): ~2분
5. Delta Lake Merge: ~3분

메모리 사용: ~15GB
- 중복 제거 시 해시 테이블: ~8GB
- Shuffle 버퍼: ~4GB
- 기타: ~3GB

⚠️ 주의: 메모리 부족 가능성 있음
```

### 4. Gold Layer (Dimension + Fact)

**예상 시간: 10-15분**

```
작업:
1. Dimension 테이블 생성: ~1분
   - dim_date: 731건 (빠름)
   - dim_merchant: 41건 (빠름)
   - dim_category: 18건 (빠름)

2. Fact 테이블 생성: ~10분
   - Silver 읽기: ~1분
   - Dimension 조인 (3개): ~5분
     * Broadcast Join 사용 (Dimension 작음)
   - Surrogate Key 생성: ~2분
   - Partitioning 쓰기: ~2분

메모리 사용: ~20GB
- Silver 데이터: ~10GB
- Join 버퍼: ~6GB
- 출력 버퍼: ~4GB

⚠️ 주의: 메모리 압박 심함
```

### 5. Analysis

**예상 시간: 1-2분**

```
작업:
- 테이블 로드: ~30초
- 월별 집계: ~30초
- 카테고리 집계: ~20초
- Top 상점: ~20초

메모리 사용: ~5GB
✅ 문제없음
```

---

## 🎯 총 예상 시간

```
┌─────────────────────────────────────────┐
│ 단계              │ 시간      │ 메모리   │
├─────────────────────────────────────────┤
│ 1. 데이터 생성    │ 30-60분   │ 4GB     │
│ 2. Bronze         │ 2-3분     │ 8GB     │
│ 3. Silver         │ 5-10분    │ 15GB    │
│ 4. Gold           │ 10-15분   │ 20GB    │
│ 5. Analysis       │ 1-2분     │ 5GB     │
├─────────────────────────────────────────┤
│ 총 시간           │ 50-90분   │ 최대 20GB│
└─────────────────────────────────────────┘

✅ 실행 가능: 예
⚠️ 메모리 압박: 있음 (최적화 필요)
```

---

## 🚨 예상 문제점

### 1. 메모리 부족 (OOM - Out of Memory)

**증상**:
```
25/12/03 17:00:00 ERROR Executor: Exception in task
java.lang.OutOfMemoryError: Java heap space
```

**발생 시점**:
- Silver 중복 제거 (dropDuplicates)
- Gold Fact 테이블 조인

**해결 방법**:
```python
# 1. Spark 메모리 설정 증가
spark = SparkSession.builder \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "16g") \
    .config("spark.memory.fraction", "0.8") \
    .getOrCreate()

# 2. Partition 수 증가 (메모리 분산)
df.repartition(200)  # 기본 200 → 400

# 3. 배치 처리
for i in range(10):
    df_batch = df.filter(col("id") % 10 == i)
    process(df_batch)
```

### 2. Shuffle 병목

**증상**:
```
Stage 진행률이 99%에서 멈춤
Shuffle Read/Write가 느림
```

**원인**:
- 중복 제거, 조인 시 Shuffle 발생
- 디스크 I/O 병목

**해결 방법**:
```python
# 1. Broadcast Join 사용 (작은 테이블)
df.join(broadcast(dim_table), ...)

# 2. Shuffle Partition 조정
spark.conf.set("spark.sql.shuffle.partitions", "400")

# 3. AQE (Adaptive Query Execution) 활용
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

### 3. 디스크 공간 부족

**증상**:
```
No space left on device
```

**원인**:
- Delta Lake 버전 이력 누적
- Shuffle 임시 파일

**해결 방법**:
```python
# 1. VACUUM (오래된 파일 삭제)
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, "data/silver/transactions")
delta_table.vacuum(0)  # 즉시 삭제 (주의!)

# 2. Shuffle 디렉토리 정리
spark.conf.set("spark.local.dir", "/path/to/large/disk")
```

---

## 💡 최적화 전략

### 1. 데이터 생성 최적화

```python
# 멀티프로세싱 사용
from multiprocessing import Pool

def generate_batch(batch_id):
    gen = TransactionGenerator(seed=42 + batch_id)
    return gen.generate_card_transactions(1000000)

with Pool(8) as p:  # 8개 프로세스
    results = p.map(generate_batch, range(100))
    
# 시간: 60분 → 15분
```

### 2. Spark 설정 최적화

```python
# utils/spark_session.py
def create_spark_session_optimized():
    return SparkSession.builder \
        .appName("FinanceDataPlatform") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "16g") \
        .config("spark.sql.shuffle.partitions", "400") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .master("local[*]") \
        .getOrCreate()
```

### 3. 배치 처리

```python
# jobs/silver_transformation.py
def transform_to_silver_batched(spark, bronze_path, silver_path):
    """배치 단위로 처리"""
    
    # 날짜별로 분할 처리
    dates = spark.read.format("delta").load(bronze_path) \
        .select("transaction_date").distinct().collect()
    
    for date_row in dates:
        date = date_row.transaction_date
        
        # 하루치 데이터만 처리
        df_batch = spark.read.format("delta").load(bronze_path) \
            .filter(col("transaction_date") == date)
        
        # 변환
        df_transformed = transform(df_batch)
        
        # Append
        df_transformed.write.format("delta") \
            .mode("append").save(silver_path)
```

### 4. Partitioning 전략

```python
# 날짜별 파티셔닝
df.write.format("delta") \
    .partitionBy("year", "month") \
    .save("data/silver/transactions")

# 장점:
# - 날짜 필터 쿼리 빠름
# - 메모리 사용 감소
# - 병렬 처리 효율 증가
```

---

## 📈 성능 비교

### 현재 (10,000건)
```
총 시간: 3분
메모리: 2GB
스토리지: 22MB
```

### 최적화 없이 (1억 건)
```
총 시간: 50-90분
메모리: 20GB (압박)
스토리지: 22.5GB
실패 가능성: 30%
```

### 최적화 후 (1억 건)
```
총 시간: 30-40분
메모리: 16GB (안정)
스토리지: 22.5GB
실패 가능성: 5%
```

---

## 🎯 실전 권장사항

### 단계별 접근

```bash
# 1단계: 10만 건 (10배)
uv run python data_generator/generate_all.py --records 100000
uv run python jobs/run_pipeline.py
# 예상: 5분, 메모리 3GB

# 2단계: 100만 건 (100배)
uv run python data_generator/generate_all.py --records 1000000
uv run python jobs/run_pipeline.py
# 예상: 10분, 메모리 6GB

# 3단계: 1000만 건 (1000배)
uv run python data_generator/generate_all.py --records 10000000
uv run python jobs/run_pipeline.py
# 예상: 20분, 메모리 12GB

# 4단계: 1억 건 (10000배)
# 최적화 적용 후 실행
```

### 모니터링

```bash
# 메모리 사용량 확인
watch -n 1 'ps aux | grep spark'

# 디스크 사용량 확인
du -sh data/*

# Spark UI 확인
# http://localhost:4040
```

---

## 🔮 예상 결과

### 성공 시나리오 (70% 확률)

```
✅ 데이터 생성: 완료 (30분)
✅ Bronze: 완료 (3분)
✅ Silver: 완료 (8분, 메모리 압박)
✅ Gold: 완료 (12분, 메모리 압박)
✅ Analysis: 완료 (2분)

총 시간: ~55분
최종 스토리지: 22.5GB
```

### 실패 시나리오 (30% 확률)

```
✅ 데이터 생성: 완료
✅ Bronze: 완료
❌ Silver: OOM 에러 (중복 제거 중)

해결책:
1. Spark 메모리 증가
2. 배치 처리로 전환
3. Partition 수 증가
```

---

## 💪 최종 결론

### MacBook M2 Pro 32GB에서 1억 건 처리

**가능 여부**: ✅ **가능함**

**조건**:
1. Spark 메모리 설정 최적화 필수
2. 배치 처리 권장 (안정성)
3. 충분한 디스크 공간 (50GB 이상)
4. 다른 앱 종료 (메모리 확보)

**예상 시간**: 30-60분

**권장 전략**:
```
1. 10만 건부터 시작 (테스트)
2. 점진적으로 증가 (100만 → 1000만)
3. 최적화 적용
4. 1억 건 도전
```

**대안**:
- 클라우드 사용 (AWS EMR, Databricks)
- 더 강력한 하드웨어 (64GB RAM)
- 데이터 샘플링 (1000만 건으로 제한)

---

## 🚀 바로 시도해보기

```bash
# 1. 최적화된 Spark 설정 적용
# utils/spark_session.py 수정

# 2. 10만 건으로 테스트
uv run python data_generator/generate_all.py --records 100000
uv run python jobs/run_pipeline.py

# 3. 성공하면 100만 건
uv run python data_generator/generate_all.py --records 1000000
uv run python jobs/run_pipeline.py

# 4. 최종 1억 건 도전!
uv run python data_generator/generate_all.py --records 100000000
uv run python jobs/run_pipeline.py
```

**행운을 빕니다!** 🍀
