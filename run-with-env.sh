export KAFKA_BOOTSTRAP_SERVERS="${KAFKA1}:29092,${KAFKA2}:29093,${KAFKA3}:29094,${KAFKA4}:29095,${KAFKA5}:29096,${KAFKA6}:29097"
export KAFKA_SECURITY_PROTOCOL="SSL"
export KAFKA_SSL_TRUSTSTORE_LOCATION="/home/greg/work/kafka-heartbeat/secrets/kafka_truststore.jks"
export KAFKA_SSL_TRUSTSTORE_PASSWORD="confluent"
export KAFKA_SSL_KEYSTORE_LOCATION="/home/greg/work/kafka-heartbeat/secrets/kafka_keystore.jks"
export KAFKA_SSL_KEYSTORE_PASSWORD="confluent"
export KAFKA_SSL_KEY_PASSWORD="confluent"
export KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM=" "
export REBALANCE_CONFIG="{\"dryrun\":false,\"delay\":5,\"topics\":[\"greg-test1\",\"greg-test2\"]}"

java -jar target/kafka-rebalance-0.1.jar
