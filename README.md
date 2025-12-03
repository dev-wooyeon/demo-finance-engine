# Personal Finance Data Platform

Apache Spark 기반 개인 금융 데이터 분석 플랫폼 (Medallion Architecture + Star Schema)

## 🎯 프로젝트 목표

- Apache Spark 배치 처리 학습
- Medallion Architecture (Bronze/Silver/Gold) 구현
- Star Schema 기반 데이터 마트 설계
- Delta Lake를 활용한 데이터 레이크하우스 구축

## 🏗️ 아키텍처

```
Raw Data (Parquet)
    ↓
🔶 Bronze Layer (Delta Lake) - 원본 데이터
    ↓
🔷 Silver Layer (Delta Lake) - 정제된 데이터
    ↓
🌟 Gold Layer (Star Schema) - 분석용 데이터 마트
    ├── dim_date
    ├── dim_category
    ├── dim_merchant
    └── fact_transactions
```

## 🚀 빠른 시작

### 1. 환경 설정

```bash
# uv 설치 (macOS)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 프로젝트 초기화
uv venv
source .venv/bin/activate

# 패키지 설치
uv pip install pyspark==3.5.0 delta-spark==3.0.0 faker==22.0.0 pandas pyarrow
```

### 2. 데이터 생성

```bash
# 10만건의 카드 거래 데이터 생성
uv run python data_generator/generate_all.py --records 100000
```

### 3. 파이프라인 실행

```bash
# 전체 ETL 파이프라인 실행
uv run python jobs/run_pipeline.py
```

### 4. 데이터 분석

```bash
# 빠른 분석 실행
uv run python analytics/quick_analysis.py
```

## 📁 프로젝트 구조

```
finance-spark-platform/
├── data/
│   ├── raw/                    # Parquet 원본 데이터
│   ├── bronze/                 # Bronze Layer (Delta)
│   ├── silver/                 # Silver Layer (Delta)
│   └── gold/                   # Gold Layer (Star Schema)
├── data_generator/             # 데이터 생성 스크립트
├── jobs/                       # Spark Job 스크립트
├── analytics/                  # 분석 스크립트
├── schemas/                    # 스키마 정의
├── utils/                      # 유틸리티
└── docs/                       # 문서
```

## 📊 데이터 모델

### Star Schema

```
        dim_date
           │
           ├── dim_category
           │        │
    fact_transactions (중심)
           │        │
           ├── dim_merchant
```

## 🔍 분석 쿼리 예시

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Analysis").getOrCreate()

# 월별 카테고리 지출 분석
spark.sql("""
    SELECT 
        d.year, d.month_name,
        c.level_1 as category,
        SUM(f.amount) as total_amount
    FROM fact_transactions f
    JOIN dim_date d ON f.date_key = d.date_key
    JOIN dim_category c ON f.category_key = c.category_key
    GROUP BY d.year, d.month_name, c.level_1
    ORDER BY d.year, d.month, total_amount DESC
""").show()
```

## 📈 분석 결과

10,000건 거래 데이터 분석 결과:
- **총 지출**: ₩646,895,601
- **평균 거래액**: ₩64,690
- **Top 카테고리**: 주거(42%), 쇼핑(20%), 식비(13%)
- **주말 vs 평일**: 평균 거래액 거의 동일 (차이 0.2%)

자세한 분석 결과는 [docs/analysis_results.md](docs/analysis_results.md)를 참고하세요.

## 📚 학습 포인트

- ✅ Spark DataFrame API
- ✅ Delta Lake (ACID, Time Travel)
- ✅ Medallion Architecture
- ✅ Star Schema 설계
- ✅ Partitioning 전략
- ✅ UDF (User Defined Functions)

## 📖 상세 문서

- [로드맵](docs/roadmap.md) - 전체 프로젝트 로드맵
- [분석 결과](docs/analysis_results.md) - 데이터 분석 인사이트
- [실행 가이드](docs/walkthrough.md) - 파이프라인 실행 과정
