# 1. JDK 이미지 선택 (예: OpenJDK 17)
FROM openjdk:17-jdk-alpine

# 2. JAR 파일 복사
ARG JAR_FILE=build/libs/*.jar
COPY ${JAR_FILE} app.jar

# 3. JAR 파일 실행
ENTRYPOINT ["java", "-jar", "/app.jar"]
