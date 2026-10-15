---
name: issues
description: 스펙 문서의 완료 기준이나 자유 입력 설명으로 GitHub 이슈를 생성한다. 사용법 /issues 02 또는 /issues docker-compose 인프라 구성 이슈 하나
argument-hint: <스펙번호> | <이슈 설명>
disable-model-invocation: true
---

입력: $ARGUMENTS

## 제목 형식 (공통)

`[TYPE] 제목` — TYPE은 대문자: `FEAT`, `FIX`, `PERF`, `EXP`, `REFACTOR`, `TEST`, `DOCS`, `CHORE`
예: `[FEAT] 주문 접수 API 구현`, `[CHORE] docker-compose 인프라 구성`

라벨은 소문자 type(`feat`, `chore` 등)과 단계(`phase-1`/`phase-2`), 스펙 이슈일 때만 `spec-NN`을 붙인다.

## 입력 해석

- **입력이 스펙 번호만**(예: `02`)이고 `docs/specs/02-*.md`가 있으면 → 아래 "스펙 기반"으로 진행한다.
- **설명이 함께 있거나 스펙 번호가 아니면**(예: `03: docker-compose 이슈 하나`, `CI 설정 이슈`) → 앞의 숫자는 무시하고 "단건 이슈"로 진행한다.
  설명이 스펙 내용과 관련 있어 보이면 그 스펙 라벨을 붙일지 사용자에게 묻는다.

## 스펙 기반

1. 스펙 문서를 읽는다.
2. `gh issue list --label spec-NN --state all` 로 이미 만들어진 이슈를 확인해 중복을 피한다.
3. "완료 기준"과 본문을 바탕으로 이슈 목록을 설계한다.
   - 체크박스 하나가 너무 작으면 묶고, 너무 크면 나눈다. 이슈 하나는 반나절~하루 작업이 적당하다.
   - 작업 순서대로 정렬한다.
4. 제목, 라벨, 요약을 표로 보여주고 **사용자 확인을 받는다.**
5. 확인 후 생성한다. 스펙 06(2단계 백로그)이면 `phase-2` 라벨, 마일스톤 생략.

## 단건 이슈

1. 설명과 `docs/ARCHITECTURE.md`, 관련 스펙을 참고해 목적·할 일·완료 기준을 구체적으로 작성한다.
   완료 기준은 명령어로 확인 가능한 형태로 쓴다.
2. `gh issue list --state open` 으로 비슷한 이슈가 있는지 확인한다.
3. 제목, 라벨, 본문을 보여주고 **사용자 확인을 받는다.**

## 생성 명령

본문은 `.github/ISSUE_TEMPLATE/task.md` (실험이면 `experiment.md`) 형식을 따른다.

```bash
gh issue create \
  --title "[TYPE] <제목>" \
  --label "<type>" \
  --milestone "M1-Setup" \
  --body-file - <<'BODY'
<본문>
BODY
```

생성 후 이슈 번호와 제목을 출력한다.