---
name: pr
description: 현재 브랜치를 푸시하고 이슈와 연결된 PR을 생성한다.
disable-model-invocation: true
---

1. `git status` 로 커밋되지 않은 변경이 없는지 확인한다. 있으면 `/commit` 을 먼저 하라고 안내한다.
2. 브랜치 이름에서 이슈 번호를 추출하고 `gh issue view <번호> --json title,body,milestone,state` 로 이슈를 읽는다.
3. `git log main..HEAD --oneline` 과 `git diff main...HEAD --stat` 으로 변경 범위를 파악한다.
4. 이 PR로 이슈가 **완료되는지** 판단한다.
   - 이슈의 완료 기준이 모두 충족되면 → 마지막 PR. "Issue 진행 상황"에 `- Closes #<번호>`
   - 이슈를 여러 PR로 나눠 진행 중이면 → 중간 PR. `- #<번호>` 로 언급만 한다 (close, fix, resolve 같은 단어를 `#<번호>` 앞에 쓰지 않는다)
   - 판단이 애매하면 사용자에게 묻는다.
   - 같은 이슈의 이전 PR이 있으면(`gh pr list --state merged --search "<번호>"`) 그 진행 상황 체크리스트를 이어받아, 이번에 끝낸 항목만 추가로 체크한다.
5. `.github/pull_request_template.md` 형식으로 본문을 작성한다.
   - **섹션 제목은 템플릿 그대로(영어 포함), 내용은 한국어로** 쓴다.
   - 해당 없는 섹션은 삭제한다.
   - 작업 중 겪은 문제는 `## Troubleshooting` 표(증상 / 원인 / 조치)로 정리한다. 대화에서 해결한 문제도 빠짐없이 옮긴다.
   - `## Verification` 에는 **실제로 실행한 명령**(`### 명령어`)과 **그 결과**(`### 결과`)를 짝지어 쓴다. 실행하지 않은 명령은 넣지 않는다.
   - `experiments/results/` 에 추가·변경된 파일이 있으면 핵심 수치를 `### 결과`에 옮긴다.
6. PR 제목은 `<type>(<scope>): <요약> (#<번호>)` 형식.
   마일스톤은 이슈의 마일스톤을 그대로 쓴다 (없으면 생략).
   제목·마일스톤·본문을 보여주고 **사용자 확인을 받는다.**
7. 확인 후 실행한다:

```bash
git push -u origin HEAD
gh pr create --base main \
  --title "<제목>" \
  --milestone "<이슈의 마일스톤>" \
  --body-file - <<'BODY'
<본문>
BODY
```

8. PR 링크를 출력하고 `gh pr checks --watch` 로 CI 결과를 기다린다.
   - 실패한 체크가 있으면 `gh run view --log-failed` 로 원인을 확인해 설명하고, 머지하지 않는다.
9. CI가 모두 통과하면 머지할지 묻는다. 머지하기로 하면:

```bash
gh pr merge --merge --delete-branch
git switch main
git pull --ff-only
```

   - 머지 커밋 방식(`--merge`)을 쓰는 이유: 브랜치별 세부 커밋 이력을 main에서도 볼 수 있게 하기 위해.