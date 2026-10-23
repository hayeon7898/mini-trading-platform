---
name: spec
description: 스펙 문서 대비 현재 구현 진행 상황을 점검하고 다음 작업을 제안한다. 사용법 /spec 02
argument-hint: <스펙번호>
---

스펙 번호: $ARGUMENTS

1. `docs/specs/$ARGUMENTS-*.md` 를 읽고, 관련 이슈의 마일스톤 마감일을 `gh api repos/{owner}/{repo}/milestones` 로 확인한다.
2. 레포의 실제 파일(설정, 코드, SQL, 실험 결과)을 살펴 완료 기준 항목별로 상태를 판단한다.
   - 완료 / 진행 중 / 미착수, 판단 근거(파일 경로)를 함께 표시한다.
   - 실행해야만 확인 가능한 항목은 "검증 필요"로 두고 확인 명령을 알려준다.
3. `gh issue list --label spec-$ARGUMENTS --state all` 로 이슈 상태와 대조한다.
4. 마일스톤 마감일 대비 늦어졌다면 범위를 줄일 방법을 제안한다 (스펙 안의 우선순위 참고).
5. 다음에 할 작업 1~2개를 제안한다.
