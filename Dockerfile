#Base Image Used for the service is defined below

FROM openjdk:16-jdk-alpine
#Changing the working directory

RUN mkdir -p /prd/ecp/wa-dispatcher/
# Copy source code

COPY ./target/wa-dispatcher-*.jar /prd/ecp/wa-dispatcher/

WORKDIR /prd/ecp/wa-dispatcher/

#Startup command for the application/service

# CMD /usr/bin/java -Xmx600m -Xms100m -Xmn200m -Xss1m -XX:PermSize=100m -XX:MaxPermSize=100m -XX:+DisableExplicitGC -XX:+UseParallelGC -XX:ParallelGCThreads=4 com.comviva.ngage.microfab.AppMain -c sms -i 1 -x smsChannel

CMD java -jar wa-dispatcher-*.jar
