# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM maven:3.9.6-eclipse-temurin-17 AS build

WORKDIR /workspace

# Copy parent POM first so Maven can resolve the reactor
COPY pom.xml ./pom.xml
COPY jms-common/pom.xml ./jms-common/pom.xml
COPY jms-publisher/pom.xml ./jms-publisher/pom.xml
COPY jms-subscriber/pom.xml ./jms-subscriber/pom.xml

# Pre-fetch dependencies (layer is cached as long as POMs don't change)
RUN mvn dependency:go-offline -pl jms-common,jms-publisher --also-make -q

# Copy source
COPY jms-common/src ./jms-common/src
COPY jms-publisher/src ./jms-publisher/src

# Build only the modules needed for the publisher
RUN mvn package -pl jms-common,jms-publisher --also-make -DskipTests -q

# ── Stage 2: runtime ───────────────────────────────────────────────────────────
FROM eclipse-temurin:17-jre-alpine

# Non-root user for security
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

WORKDIR /app

COPY --from=build /workspace/jms-publisher/target/publisher.jar publisher.jar

# BROKER_URL is injected at runtime (e.g. via docker-compose environment)
ENV BROKER_URL=tcp://activemq:61616
ENV PUBLISH_INTERVAL_MS=3000
ENV MAX_ROUNDS=-1

ENTRYPOINT ["java", "-jar", "publisher.jar"]
