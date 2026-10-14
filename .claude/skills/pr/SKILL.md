---
name: pr
description: 현재 브랜치를 푸시하고 이슈와 연결된 PR을 생성한다.
disable-model-invocation: true
---

1. `git status` 로 커밋되지 않은 변경이 없는지 확인한다. 있으면 `/commit` 을 먼저 하라고 안내한다.
2. 브랜치 이름에서 이슈 번호를 추출하고 `gh issue view <번호>` 로 이슈를 읽는다.
3. `git log main..HEAD --oneline` 과 `git diff main...HEAD --stat` 으로 변경 범위를 파악한다.
4. `.github/pull_request_template.md` 형식으로 PR 본문을 작성한다.
   - `experiments/results/` 에 추가·변경된 파일이 있으면 핵심 수치를 "측정 결과"에 옮긴다.
   - 이슈의 완료 기준 중 이 PR로 달성된 것을 체크한다.
   - 본문 마지막에 `Closes #<번호>` 를 넣는다.
5. PR 제목은 `<type>(<scope>): <요약> (#<번호>)` 형식. 본문과 함께 보여주고 **사용자 확인을 받는다.**
6. 확인 후 실행한다:

```bash
git push -u origin HEAD
gh pr create --base main --title "<제목>" --body "<본문>"
```

7. PR 링크를 출력하고 `gh pr checks --watch` 로 CI 결과를 기다린다.
   - 실패한 체크가 있으면 `gh run view --log-failed` 로 원인을 확인해 설명하고, 머지하지 않는다.
8. CI가 모두 통과하면 머지할지 묻는다. 머지하기로 하면:

```bash
gh pr merge --merge --delete-branch
git switch main
git pull --ff-only
```

   - 머지 커밋 방식(`--merge`)을 쓰는 이유: 브랜치별 세부 커밋 이력을 main에서도 볼 수 있게 하기 위해.