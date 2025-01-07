# Build image
FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /app
COPY pom.xml .
COPY src ./src

RUN mvn clean package

# Runtime image
FROM eclipse-temurin:21-jdk-noble

WORKDIR /app
COPY --from=build /app/target/fainder-demo-1.0-SNAPSHOT.jar .

ENV FAINDER_DEMO_HOME=/app
EXPOSE 8001

CMD ["java", "--enable-native-access=ALL-UNNAMED", "--add-modules", "jdk.incubator.vector", "-jar", "fainder-demo-1.0-SNAPSHOT.jar"]
