# 🚨 [용량주의] 1억 건 데이터 생성하다가 서버(?) 터뜨린 썰 (OOM 탈출기)

> "내 맥북이 비명을 지르기도 전에 프로세스가 먼저 '나 죽소' 하고 드러누웠습니다." 💀

![](https://velog.velcdn.com/images/tags/troubleshooting.png)

## 🎯 TL;DR

- ✅ **문제**: 1억 건 생성 시도 중 메모리 부족(OOM)으로 프로세스 강제 종료
- 🔍 **원인**: "일단 만들고 한꺼번에 저장하자"는 안일한 생각 (Pandas의 한계)
- 🚀 **해결**: PyArrow를 활용한 **배치 스트리밍(Batch Streaming)** 도입
- 💡 **결과**: 메모리 사용량 80% 감소 & 무한 확장 가능한 데이터 생성기 완성

---

## 💭 1억 건? 그게 뭐야? 그냥 돌리면 되는 거 아냐?

Phase 4에서 1,000만 건을 4분 만에 끝낸 뒤, 근거 없는 자신감이 하늘을 찔렀습니다. 

**"야, 1,000만 건이 4분이면 1억 건은 40분이면 되겠네! 가즈아!"** 🔥

하고 호기롭게 `--records 100000000` 옵션을 주고 퇴근(?)했습니다.

---

## 🎬 사건의 전말 (The Incident)

다음 날 아침, 기대에 가득 차서 파일을 확인해보니...

```bash
ls -lh data/raw/card_transactions.parquet
# 425MB...? 어? 왜 이렇게 작아?
```

분명 1억 건이면 기가바이트 단위여야 하는데, 파일 크기가 1,000만 건 때랑 비슷했습니다. 파이썬 프로세스는 이미 흔적도 없이 사라져 있었고요.

범인은 바로... **OOM (Out Of Memory)** 이었습니다. 😱

---

## 🔍 원인 분석: "메모리 빌런" Pandas

기존 코드는 이랬습니다:

```python
def generate_card_transactions(self, num_records):
    data = []
    for i in range(num_records):
        data.append({ ... }) # 👈 1억 번 리스트에 추가 (메모리 폭발!)
    
    return pd.DataFrame(data) # 👈 한 번에 DataFrame 변환 (여기서 사망)
```

### 왜 망했나?
1. **Python Dictionary의 사치**: 파이썬 딕셔너리는 메모리를 생각보다 많이 먹습니다. 1억 개의 딕셔너리를 리스트에 담는 순간, 맥북의 32GB 램은 이미 비명을 지르고 있었던 거죠.
2. **Pandas의 데이터 로드**: Pandas는 데이터를 메모리에 **전부(In-memory)** 올려서 처리하는 게 기본입니다. 1억 건의 거래 데이터를 한 줄로 변환하려니 메모리가 부족해서 OS가 프로세스를 강제로 죽여버린 겁니다 (`SIGKILL`).

---

## 🚀 해결책: "나눠서 던지기" (Batch Streaming)

문제를 해결하려면 **"전부 메모리에 올리지 않고, 조금씩 만들어서 바로바로 디스크에 적기"** 전략이 필요했습니다.

이때 구원투수로 등장한 것이 바로 `PyArrow`의 `ParquetWriter`입니다.

### 개선된 설계: Batch Generation
- 전체 작업을 **100만 건 단위의 배치(Batch)** 100개로 나눕니다.
- 각 배치(100만 건)가 완성될 때마다 Parquet 파일에 **Append(추가)** 합니다.
- 파일 작성이 끝나면 메모리에서 해당 데이터는 미련 없이 지웁니다.

```python
def generate_and_save_in_batches(self, output_path, num_records, batch_size):
    # ParquetWriter 준비 (스트리밍 시작!)
    writer = pq.ParquetWriter(output_path, schema, ...)

    for b in range(num_batches):
        data = generate_batch(batch_size) # 100만 건만 생성
        table = pa.Table.from_pandas(pd.DataFrame(data)) # 잠깐 DataFrame 거쳐가고
        writer.write_table(table) # 디스크로 골인! ⚽️
        # 여기서 data, table은 자동으로 GC(Garbage Collector)의 먹이가 됨
```

---

## 📊 결과: 평온해진 메모리

### Before (Pandas 전량 처리)
- 📉 메모리 사용량: 2GB -> 8GB -> 16GB -> 32GB -> **Boom! (Crashed)**
- ⏱️ 성공 확률: 0%

### After (Batch Streaming)
- 📈 메모리 사용량: **2GB 선에서 일정하게 유지** (Steady State)
- ✅ 성공 확률: 100%
- 🚀 확장성: 1억 건이 아니라 10억 건도 디스크 용량만 있으면 가능!

---

## 💡 이번 실전 트러블슈팅으로 배운 점

### 1. "작은 데이터"와 "큰 데이터"는 설계부터 다르다
1,000만 건까지는 운 좋게 메모리 안에서 해결됐지만, **0 하나가 더 붙는 순간 게임의 룰이 바뀝니다.** 대용량 처리를 할 때는 항상 "이게 메모리에 다 들어갈까?"를 의심해야 합니다.

### 2. 스트리밍(Streaming)은 필수
데이터 엔지니어링에서 **Batching**과 **Streaming**은 선택이 아닌 필수입니다. 데이터를 흐르게 만들어야 시스템이 터지지 않습니다.

### 3. 디버깅 로그의 중요성
```
📦 Processing Batch 1/100...
✅ Batch 1 written.
📦 Processing Batch 2/100...
```
로그를 상세히 찍어두니, 어느 지점에서 속도가 느려지는지 혹은 터지는지 한눈에 보였습니다.

---

## 🎯 다음 단계

이제 안정적인 공급망(?)을 확보했으니, 곧 1억 건 생성이 완료됩니다.

진짜 **"느림의 미학"**을 보여줄 **Unoptimized 100M 벤치마크**로 돌아오겠습니다. 과연 얼마나 느릴지, 그리고 최적화 후에는 얼마나 드라마틱하게 빨라질지 기대해주세요! 🔥

---

**Tags**: `#Python` `#Pandas` `#PyArrow` `#BigData` `#OOM` `#Troubleshooting` `#MZ개발자`

**다음 글 예고**: Phase 5 - 1억 건, Spark도 비명을 지를 것인가? 🚀
