FROM openjdk:8-jdk-alpine
VOLUME /tmp
COPY kafka-rest-springboot*.jar /app/lib/
#COPY kafka-rest-springboot-1.1.x-release-executable.jar /app/lib/kafka-rest-executable.jar
COPY classes/config/* /app/config/
COPY classes/jmxtemplates/* /app/config/jmxtemplates/
COPY classes/security.yml /app/config/security.yml
ENTRYPOINT ["java","-cp", "/app:/app/config/*:/app/lib/*","-Dspring.config.location=/app/config/", "-Dspring.main.allow-bean-definition-overriding=true","-jar","/app/lib/kafka-rest-springboot-1.1.x-release-executable.jar"]