# syntax=docker/dockerfile:1
# order-server 이미지 (#46)
# 빌드 단계에서 bootJar를 만들고, 실행 단계에는 JRE와 jar만 남긴다.
# CI의 GHCR 배포는 소스만 체크아웃하므로, jar 빌드까지 이 파일 안에서 끝나야 한다.

# ---- 1. 빌드 --------------------------------------------------------------
FROM eclipse-temurin:21.0.12.1_1-jdk-noble AS build
WORKDIR /src

# 의존성 해석에 필요한 파일만 먼저 복사 → 소스만 바뀌면 이 레이어는 캐시를 재사용
COPY gradlew settings.gradle build.gradle ./
COPY gradle ./gradle
RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew dependencies --no-daemon -q > /dev/null

COPY src ./src
# 테스트는 CI(Build & Test)에서 돌린다. Testcontainers는 이미지 빌드 안에서 동작하지 않음
RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew bootJar --no-daemon -q \
 && cp build/libs/*-SNAPSHOT.jar /app.jar

# ---- 2. 실행 --------------------------------------------------------------
FROM eclipse-temurin:21.0.12.1_1-jre-noble
WORKDIR /app

# root가 아닌 전용 사용자로 실행
RUN useradd --system --uid 10001 --no-create-home app
COPY --from=build --chown=app /app.jar app.jar
USER app

EXPOSE 8080
# 힙을 컨테이너 메모리 상한(compose mem_limit)의 75%로 맞춘다. 나머지는 메타스페이스·스레드 스택 등
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "app.jar"]
