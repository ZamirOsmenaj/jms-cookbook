# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM maven:3.9.6-eclipse-temurin-17 AS build

WORKDIR /workspace

# Copy parent POM first so Maven can resolve the reactor
COPY pom.xml ./pom.xml
COPY jms-common/pom.xml ./jms-common/pom.xml
COPY jms-publisher/pom.xml ./jms-publisher/pom.xml
COPY jms-subscriber/pom.xml ./jms-subscriber/pom.xml

# Pre-fetch dependencies (layer is cached as long as POMs don't change)
RUN mvn dependency:go-offline -pl jms-common,jms-subscriber --also-make -q

# Copy source
COPY jms-common/src ./jms-common/src
COPY jms-subscriber/src ./jms-subscriber/src

# Build only the modules needed for the subscriber
RUN mvn package -pl jms-common,jms-subscriber --also-make -DskipTests -q

# ── Stage 2: runtime ───────────────────────────────────────────────────────────
FROM eclipse-temurin:17-jre-alpine

# Non-root user for security
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

WORKDIR /app

COPY --from=build /workspace/jms-subscriber/target/subscriber.jar subscriber.jar

# BROKER_URL is injected at runtime (e.g. via docker-compose environment)
ENV BROKER_URL=tcp://activemq:61616

ENTRYPOINT ["java", "-jar", "subscriber.jar"]
