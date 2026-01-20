# [Tech Blog] Delta Lake로 구축하는 고성능 데이터 마트: SCD Type 2와 증분 로딩 적용기

데이터 엔지니어링 프로젝트에서 가장 중요한 단계 중 하나는 분석가들이 사용하기 편하면서도 데이터 정합성이 보장된 **Gold 레이어(Data Mart)**를 구축하는 것입니다. 이번 포스팅에서는 Apache Spark와 Delta Lake를 활용하여 스타 스키마 기반의 Gold 레이어를 고도화한 경험을 공유합니다.

---

## 1. 차원 테이블의 변화 관리: SCD Type 2 적용

데이터 마트에서 차원(Dimension) 데이터는 시간이 지남에 따라 변합니다. 예를 들어, 특정 상점의 카테고리가 '식비-카페'에서 '문화-영화'로 변경될 수 있습니다. 이때 단순히 현재 값으로 덮어쓰면(SCD Type 1), 과거의 지출 통계까지 모두 현재 카테고리로 집계되는 오류가 발생합니다.

이를 해결하기 위해 **Slowly Changing Dimension (SCD) Type 2**를 적용했습니다.

### 핵심 구현 사항:
- `is_current`, `effective_date`, `expiration_date` 컬럼 추가
- Delta Lake의 `MERGE` 연산을 활용하여 기존 레코드는 만료(Expire) 처리하고, 새로운 레코드를 삽입(Insert)
- 이를 통해 특정 시점의 거래가 당시 어떤 카테고리였는지 정확하게 추적할 수 있습니다.

```python
# Delta MERGE를 이용한 SCD Type 2 구현 예시
dt_target.alias("target").merge(
    staged_updates.alias("staged"),
    "target.merchant_name = staged.mergeKey"
).whenMatchedUpdate(
    condition="target.is_current = true AND target.merchant_category <> staged.merchant_category",
    set={"is_current": "false", "expiration_date": "staged.effective_date"}
).whenNotMatchedInsert(
    values={...}
).execute()
```

---

## 2. 대용량 데이터 대응: Fact 테이블 증분 로딩 (Incremental Loading)

거래 내역(Fact Table)은 데이터가 매우 빠르게 쌓이는 곳입니다. 매번 전체 데이터를 읽어서 다시 쓰는 방식(Overwrite)은 데이터가 커질수록 비효율적입니다.

우리는 Delta Lake의 성능을 극대화하기 위해 **증분 로딩(Incremental Loading)** 방식으로 전환했습니다.

### 주요 이점:
- **중복 방지**: `transaction_id`를 기준으로 매칭하여 동일한 거래가 중복 적재되는 것을 원천 차단했습니다.
- **성능 최적화**: 변경되거나 새로 추가된 데이터만 적재하므로 파이프라인 실행 시간이 대폭 단축되었습니다.

---

## 3. 안정적인 파이프라인을 위한 팁

작업 과정에서 발생한 몇 가지 기술적 난관과 해결책을 공유합니다.

### Deterministic Key Generation
Delta `MERGE` 연산 시 `rand()`와 같은 비결정론적(Non-deterministic) 함수를 사용하면 오류가 발생합니다. 대신 `hash()` 함수를 사용하여 데이터 기반의 안정적인 **Surrogate Key**를 생성했습니다.

### Lineage Breaking with localCheckpoint
복잡한 DAG(Directed Acyclic Graph)를 가진 데이터프레임을 `MERGE`의 소스로 사용할 때 성능 저하나 오류가 발생할 수 있습니다. `localCheckpoint()`를 적용하여 중간 상태를 실재화(Materialize)함으로써 파이프라인의 안정성을 높였습니다.

---

## 마무리하며

이번 스타 스키마 고도화를 통해 데이터의 역사성을 관리할 수 있게 되었고, 적재 성능 또한 크게 향상되었습니다. Delta Lake의 강력한 기능을 활용하면 복잡한 데이터 분석 환경에서도 높은 신뢰성을 유질 수 있습니다.

다음 단계로는 Airflow를 활용한 전체 파이프라인 자동화와 데이터 품질 체크 자동화를 계획하고 있습니다. 읽어주셔서 감사합니다!

---
**Tags**: #DataEngineering #ApacheSpark #DeltaLake #StarSchema #SCD #ETL
