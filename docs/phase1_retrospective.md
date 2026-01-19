# 🔥 Spark 파이프라인 성능 벤치마크 Phase 1 회고

> "10,000건이 10,000,000건이 되는 마법을 목격했습니다 🪄"

![](https://velog.velcdn.com/images/tags/spark.png)

## 🎯 TL;DR

- ✅ 10,000건 데이터로 베이스라인 성능 측정 완료
- 🐛 Bronze 레이어 데이터 중복 버그 발견 및 해결 (1000배 증폭 ㄷㄷ)
- 🚀 성능 2.2배 개선 (253초 → 115초)
- 📊 100만건 처리 가능성 확인

---

## 💭 시작하며

데이터 엔지니어링 공부하면서 "실제로 대용량 데이터 처리하면 어떨까?" 궁금했던 적 있으신가요? 저도 그랬습니다. 그래서 이번에 **MacBook M2 Pro 32GB**로 Spark 파이프라인 성능 테스트를 진행했는데요, 예상치 못한 버그를 만나면서 많은 걸 배웠습니다.

이번 Phase 1에서는 10,000건 데이터로 베이스라인을 잡는 게 목표였는데... 결과는? 🤔

---

## 🎬 What Happened

### 1. 벤치마크 스크립트 작성

먼저 성능 측정을 위한 벤치마크 도구가 필요했어요.

```python
class SimpleBenchmark:
    def run(self):
        start_time = time.time()
        start_mem = process.memory_info().rss / 1024 / 1024
        
        run_full_pipeline()  # 파이프라인 실행
        
        end_time = time.time()
        end_mem = process.memory_info().rss / 1024 / 1024
```

`psutil`로 메모리 사용량을 측정하고, 시간은 `time` 모듈로 간단하게 체크했습니다. 

**배운 점**: 벤치마크 도구는 심플하게! 복잡하게 만들면 오히려 오버헤드가 생깁니다.

### 2. 첫 번째 실행 - 뭔가 이상한데? 🤔

```bash
uv run python benchmark/run_pipeline_benchmark.py
```

결과:
- ⏱️ Duration: **253.38초** (~4.2분)
- 💾 Memory: **155.53 MB**
- 📊 Records: **10,000,000건** ← 어?

**10,000건 넣었는데 10,000,000건이 나왔습니다.** 

처음엔 "어? 내가 잘못 봤나?" 싶었는데, 로그를 자세히 보니...

```
Bronze records: 10,020,000
After deduplication: 10,000,000 (removed 20,000)
```

아... 이거 완전 **데이터 증식 버그**잖아요? 😱

### 3. 버그 추적 - 범인을 찾아라 🕵️

Bronze 레이어 코드를 열어봤습니다.

```python
# jobs/bronze_ingestion.py
df_with_meta.write \
    .format("delta") \
    .mode("append") \  # 👈 범인 발견!
    .save(bronze_path)
```

**문제**: `append` 모드로 계속 데이터를 쌓고 있었던 거죠. 파이프라인을 여러 번 돌릴 때마다 데이터가 계속 추가되는 구조였습니다.

**왜 1000배?**: 이전에 테스트하면서 파이프라인을 1000번 정도 돌렸나봐요... (정확히는 1002번 ㅋㅋ)

### 4. 해결 - 간단하지만 임팩트 큼 💪

```python
df_with_meta.write \
    .format("delta") \
    .mode("overwrite") \  # 👈 이렇게 바꿈
    .save(bronze_path)
```

개발 환경에서는 `overwrite` 모드가 맞죠. 프로덕션에서는 다르게 처리해야겠지만요.

### 5. 재측정 - 이제 진짜 베이스라인 📊

데이터 클리어하고 다시 실행:

```bash
rm -rf data/bronze data/silver data/gold
uv run python benchmark/run_pipeline_benchmark.py
```

결과:
- ⏱️ Duration: **115.41초** (~1.9분) ← 2.2배 빨라짐!
- 💾 Memory: **136.75 MB**
- 📊 Records: **10,000건** ← 정상!

```
Bronze records: 10,000 ✅
After deduplication: 10,000 (removed 0) ✅
Silver transformation: 10,000 records ✅
fact_transactions: 10,000 records ✅
```

드디어 제대로 된 베이스라인을 얻었습니다! 🎉

---

## 📚 배운 점 (Lessons Learned)

### 1. 데이터 검증은 필수 중의 필수

> "믿지 말고 검증하라" - 데이터 엔지니어의 철칙

처음부터 각 레이어별 레코드 수를 로깅했다면 더 빨리 발견했을 거예요. 

**개선 아이디어**:
```python
def validate_record_count(layer_name, expected, actual):
    if expected != actual:
        raise ValueError(f"{layer_name}: Expected {expected}, got {actual}")
```

이런 식으로 assertion을 추가하면 좋을 것 같아요.

### 2. Append vs Overwrite - 상황에 맞게

| 모드 | 사용 시기 | 장점 | 단점 |
|------|----------|------|------|
| `append` | 프로덕션, 증분 로드 | 기존 데이터 보존 | 중복 가능성 |
| `overwrite` | 개발, 전체 재처리 | 깔끔함 | 기존 데이터 손실 |

개발 환경에서는 `overwrite`가 더 안전합니다. 특히 테스트 반복할 때요!

### 3. 벤치마크는 클린한 상태에서

이전 실행의 잔여 데이터가 있으면 결과가 왜곡됩니다. 

**Best Practice**:
```bash
# 벤치마크 전 항상 클린업
rm -rf data/bronze data/silver data/gold
```

### 4. 로그는 상세하게

```python
print(f"Bronze records: {df.count():,}")
print(f"After deduplication: {df_dedup.count():,} (removed {removed:,})")
```

이런 로그 덕분에 버그를 빠르게 찾을 수 있었어요. 귀찮아도 로깅은 꼼꼼하게!

---

## 😅 아쉬운 점

### 1. 초기 설계 단계에서 놓친 것들

Bronze 레이어 설계할 때 "append vs overwrite"를 충분히 고민하지 않았어요. 

**교훈**: 설계 단계에서 "이 코드가 여러 번 실행되면 어떻게 될까?"를 항상 생각해야 합니다.

### 2. 테스트 자동화 부족

수동으로 벤치마크 돌리고, 수동으로 결과 확인하고... 

**개선 방향**: 
- CI/CD에 성능 테스트 추가
- 자동으로 이전 결과와 비교
- 성능 저하 시 알림

### 3. 모니터링 도구 없음

Spark UI는 봤지만, 실시간 메모리/CPU 모니터링은 안 했어요.

**다음에는**:
- `htop`으로 실시간 모니터링
- Spark UI 스크린샷 저장
- 병목 구간 프로파일링

---

## 🚀 앞으로의 계획

### Phase 2: 100,000건 테스트 (예상 19분)

```bash
uv run python data_generator/generate_all.py --records 100000
uv run python benchmark/run_pipeline_benchmark.py
```

**목표**:
- 선형 스케일링 확인 (10배 데이터 = 10배 시간?)
- 메모리 사용 패턴 분석
- 병목 구간 식별

### Phase 3: 1,000,000건 테스트 (예상 3.2시간)

**예상 이슈**:
- 메모리 부족 가능성 (14GB 예상)
- Shuffle 병목
- GC 오버헤드

**대비책**:
```python
spark = SparkSession.builder \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "16g") \
    .config("spark.sql.shuffle.partitions", "400")
```

### Phase 4: 최적화 적용

1. **Partitioning 전략**
   ```python
   df.write.partitionBy("year", "month").save(gold_path)
   ```

2. **Broadcast Join**
   ```python
   fact_df.join(broadcast(dim_date), ...)
   ```

3. **배치 처리**
   - 날짜별로 분할 처리
   - 메모리 압박 완화

---

## 💡 마치며

처음에는 "그냥 10,000건 돌려보고 끝내자"였는데, 예상치 못한 버그를 만나면서 오히려 더 많이 배웠습니다.

**핵심 교훈**:
1. 🔍 **검증, 검증, 검증** - 데이터는 항상 의심하라
2. 📝 **로깅은 상세하게** - 미래의 나를 위해
3. 🧹 **클린한 환경** - 벤치마크는 깨끗한 상태에서
4. 🤔 **설계 단계부터 고민** - "여러 번 실행되면?"

다음 Phase 2에서는 100,000건으로 스케일링 테스트를 진행할 예정입니다. 과연 선형적으로 스케일될까요? 아니면 새로운 병목이 나타날까요? 

기대되네요! 🔥

---

## 📊 Appendix: 성능 메트릭 요약

### Before Fix (버그 있을 때)
```
Records: 10,000 → 10,000,000 (1000x 증폭)
Duration: 253.38초
Memory: 155.53 MB
```

### After Fix (정상)
```
Records: 10,000 → 10,000 (정상)
Duration: 115.41초 (2.2x 개선)
Memory: 136.75 MB
```

### 성능 예측
| Records | Time | Memory |
|---------|------|--------|
| 10k | 1.9분 | 137 MB |
| 100k | 19분 | 1.4 GB |
| 1M | 3.2시간 | 14 GB |

---

**Tags**: `#Spark` `#DataEngineering` `#Performance` `#Benchmarking` `#회고`

**다음 글 예고**: Phase 2 - 100,000건 스케일 테스트 🚀
