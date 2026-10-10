FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml ./
COPY src ./src
RUN mvn -B -ntp verify

FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /build/target/app.jar app.jar
COPY LICENSE /app/LICENSE
LABEL org.opencontainers.image.licenses="Apache-2.0"
RUN mkdir -p /app/state && chown -R 10001:10001 /app/state
USER 10001:10001
EXPOSE 8000
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=65", "-jar", "/app/app.jar"]
