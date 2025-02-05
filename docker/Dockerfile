FROM maven:3.9-openjdk-17 as build

USER root

WORKDIR /app

COPY pom.xml .

RUN mvn dependency:go-offline

COPY src ./src

RUN mvn package -DskipTests

FROM openjdk:17-jre-slim

RUN useradd -m -s /bin/bash -u 10063 appuser

RUN chmod -R 777 /tmp

USER appuser

WORKDIR /app

COPY --from=build /app/target/kafka-to-pubsub-1.0-SNAPSHOT.jar /app/kafka-to-pubsub-1.0-SNAPSHOT.jar

ENV SPRING_PROFILES_ACTIVE=default
ENV JAVA_OPTS="-server -Xms1g -Xmx1g -XX:-UseContainerSupport -XX:+UseG1GC -XX:GCTimeRatio=2 -XX:ParallelGCThreads=10 -XX:MaxGCPauseMillis=50 -XX:+DisableExplicitGG -XX:-UsePerfData"

ENTRYPOINT [ "sh", "-c", "java $JAVA_OPTS -jar /app/kafka-to-pubsub-1.0-SNAPSHOT.jar --config=${CONFIG_LOCATION}"]