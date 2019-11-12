FROM openjdk:11

EXPOSE 8080
EXPOSE 8081

ADD /target/*.jar /app/application.jar

ENTRYPOINT java $JAVA_OPTS -jar /app/application.jar
