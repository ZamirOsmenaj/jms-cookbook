FROM maven:3.8.6-openjdk-8

WORKDIR /app
COPY pom.xml .
COPY src ./src

RUN mvn package -q -DskipTests

CMD ["java", "-jar", "target/producer-1.0-SNAPSHOT-jar-with-dependencies.jar"]
