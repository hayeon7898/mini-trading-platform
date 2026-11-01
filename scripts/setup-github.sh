#!/usr/bin/env bash
# 최초 1회 실행: GitHub 라벨과 마일스톤 생성
# 사전 준비: gh auth login
set -euo pipefail

label() { gh label create "$1" --color "$2" --description "$3" --force; }

# 단계
label phase-1 "0E8A16" "핵심 파이프라인"
label phase-2 "C5DEF5" "확장 과제"

# 스펙
for n in 01 02 03 04 05 06; do
  label "spec-$n" "BFD4F2" "docs/specs/$n"
done

# 작업 유형
label feat  "1D76DB" "기능 추가"
label fix   "D73A4A" "버그 수정"
label perf  "FBCA04" "성능 개선"
label exp   "5319E7" "장애·부하 재현 실험"
label docs  "0075CA" "문서"
label chore "EDEDED" "빌드·설정"

# 마일스톤 (이미 있으면 건너뜀)
repo=$(gh repo view --json nameWithOwner -q .nameWithOwner)
title="M1-Setup"
if ! gh api "repos/$repo/milestones?state=all" -q '.[].title' | grep -qx "$title"; then
  gh api "repos/$repo/milestones" -f title="$title" >/dev/null
  echo "마일스톤 생성: $title"
fi

echo "✅ 라벨·마일스톤 준비 완료"
