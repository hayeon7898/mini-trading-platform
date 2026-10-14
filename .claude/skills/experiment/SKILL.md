---
name: experiment
description: 실험 결과 기록 파일의 뼈대를 만든다. 사용법 /experiment E1
argument-hint: <실험ID>
disable-model-invocation: true
---

실험 ID: $ARGUMENTS

1. `docs/specs/04-reliability-experiments.md` 에서 $ARGUMENTS 절을 찾아 가설과 방법을 읽는다.
   다른 스펙의 실험이면 해당 스펙에서 찾는다.
2. 오늘 날짜로 `experiments/results/YYYYMMDD-$ARGUMENTS.md` 를 만든다. 같은 날 같은 실험이 있으면 `-2` 를 붙인다.
3. 아래 형식으로 작성한다. 가설·조건은 스펙에서 채우고, 결과·해석·조치는 비워 둔다.

```markdown
# $ARGUMENTS. <실험 제목> — 장애 재현 실험

## 가설

## 조건
- 설정값:
- 데이터량 / TPS:
- 환경: 로컬 Docker (CPU, 메모리 할당)
- 관련 커밋:

## 방법

## 결과
| 조합 | 지표1 | 지표2 |
|---|---|---|

(그래프 캡처: `experiments/results/img/`)

## 해석

## 조치와 재측정
```

4. 측정에 필요한 명령어(부하 생성, 장애 주입, 검증 쿼리)를 정리해 제안한다.
