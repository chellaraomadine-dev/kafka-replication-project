FROM apache/kafka:latest

USER root

# Remove existing connect-mirror jars
RUN find /opt/kafka/libs -name "connect-mirror-*.jar" -delete
RUN find /opt/kafka/libs -name "connect-mirror-client-*.jar" -delete

# Copy our pre-compiled patched JARs (built from 4.3.0-SNAPSHOT source)
COPY connect-mirror-4.3.0-SNAPSHOT.jar /opt/kafka/libs/
COPY connect-mirror-client-4.3.0-SNAPSHOT.jar /opt/kafka/libs/

USER appuser

LABEL description="Kafka with enhanced MirrorMaker 2 fault tolerance"
LABEL version="4.3.0-SNAPSHOT-enhanced"
