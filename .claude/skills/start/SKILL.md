---
name: start
description: GitHub 이슈를 읽고 작업 브랜치를 만든 뒤 작업 계획을 제안한다. 사용법 /start 12
argument-hint: <이슈번호>
disable-model-invocation: true
---

이슈 번호: $ARGUMENTS

1. `git status` 로 커밋되지 않은 변경이 있는지 확인한다. 있으면 멈추고 사용자에게 어떻게 할지 묻는다.
2. `gh issue view $ARGUMENTS` 로 이슈를 읽는다.
3. 브랜치 이름을 정한다: `<type>/$ARGUMENTS-<영문-소문자-짧은-설명>`
   - type은 이슈 라벨(feat/exp/docs/chore)을 따른다.
4. 실행한다:

```bash
git switch main
git pull --ff-only
git switch -c <브랜치이름>
gh issue edit $ARGUMENTS --add-assignee @me
```

5. 이슈가 가리키는 스펙 문서 해당 부분과 `docs/ARCHITECTURE.md` 의 관련 절을 읽는다.
6. 작업 계획을 3~6단계로 제안한다. 각 단계는 커밋 하나 정도의 크기로, 단계마다 무엇을 확인하면 완료인지 적는다.
   설계 결정이 필요한 지점은 선택지와 트레이드오프를 함께 제시한다.
