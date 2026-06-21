# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM maven:3.9.6-eclipse-temurin-17 AS build

WORKDIR /workspace

# Copy parent POM first so Maven can resolve the reactor
COPY pom.xml ./pom.xml
COPY common/pom.xml   ./common/pom.xml
COPY producer/pom.xml ./producer/pom.xml
COPY consumer/pom.xml ./consumer/pom.xml

# Pre-fetch dependencies (layer is cached as long as POMs don't change)
RUN mvn dependency:go-offline -pl common,producer --also-make -q

# Copy source
COPY common/src   ./common/src
COPY producer/src ./producer/src

# Build only the modules needed for the producer
RUN mvn package -pl common,producer --also-make -DskipTests -q

# ── Stage 2: runtime ───────────────────────────────────────────────────────────
FROM eclipse-temurin:17-jre-alpine

# Non-root user for security
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

WORKDIR /app

COPY --from=build /workspace/producer/target/producer.jar producer.jar

ENV BROKER_URL=tcp://broker:61616
ENV PRODUCE_INTERVAL_MS=2000
ENV MAX_ROUNDS=-1

ENTRYPOINT ["java", "-jar", "producer.jar"]
