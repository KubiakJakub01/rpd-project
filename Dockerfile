# Starting with a base image containing Java runtime 
FROM eclipse-temurin:17-jre

# Adding Maintainer Info
LABEL maintainer="maciej.szubiczuk1@gmail.com"

# Adding a volume pointing to /tmp
VOLUME /tmp

# Making port 8080 available to the world outside this container
EXPOSE 8081

# The application's jar file
ARG JAR_FILE=target/demo-0.0.1-SNAPSHOT.jar

# Adding the application's jar to the container
ADD ${JAR_FILE} app.jar

# Running the jar file
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/app.jar"]
