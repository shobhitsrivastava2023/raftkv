FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY gradlew settings.gradle build.gradle ./
COPY gradle gradle
RUN chmod +x gradlew && ./gradlew --no-daemon -q help
COPY src src
RUN ./gradlew --no-daemon -q installDist

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/build/install/raftkv /app
VOLUME /data
EXPOSE 9000
ENTRYPOINT ["/app/bin/raftkv"]
