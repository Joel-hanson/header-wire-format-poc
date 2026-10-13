# Lightweight: base CP Connect + prebuilt SMT jar (built on host via mvn).
# Avoids a second Maven image pull when disk is tight.
FROM confluentinc/cp-kafka-connect:7.7.1

USER root
RUN mkdir -p /kafka/connect/apicurio-header-smt
COPY smt/target/apicurio-header-to-confluent-smt-1.0.0.jar \
     /kafka/connect/apicurio-header-smt/
ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components,/kafka/connect"
USER appuser
