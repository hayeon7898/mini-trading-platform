---
name: issues
description: 스펙 문서의 완료 기준을 읽어 GitHub 이슈를 생성한다. 사용법 /issues 02
argument-hint: <스펙번호>
disable-model-invocation: true
---

스펙 번호: $ARGUMENTS

1. `docs/specs/$ARGUMENTS-*.md` 를 읽는다. 없으면 사용 가능한 스펙 목록을 보여주고 멈춘다.
2. `gh issue list --label spec-$ARGUMENTS --state all` 로 이미 만들어진 이슈를 확인해 중복을 피한다.
3. "완료 기준"과 본문을 바탕으로 이슈 목록을 설계한다.
   - 체크박스 하나가 너무 작으면 묶고, 너무 크면 나눈다. 이슈 하나는 반나절~하루 작업이 적당하다.
   - 작업 순서대로 정렬한다.
4. 각 이슈의 제목, type 라벨(feat/exp/docs/chore), 요약을 표로 보여주고 **사용자 확인을 받는다.**
5. 확인 후 `.github/ISSUE_TEMPLATE/task.md` 형식으로 본문을 작성해 생성한다.

```bash
gh issue create \
  --title "[<스펙번호>] <제목>" \
  --label "spec-<스펙번호>,phase-1,<type>" \
  --milestone "1단계 (~10/25)" \
  --body "<본문>"
```

   - 스펙 06(2단계 백로그)이면 라벨은 `phase-2`, 마일스톤은 생략한다.
6. 생성된 이슈 번호와 제목 목록을 출력한다.
