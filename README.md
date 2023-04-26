## Requirements
* java 17
* maven

## How to build and run

1. Run a kafka broker on localhost:9092
   ```bash
   Download and extract an apache kafka binary distribution
   ./bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
   ./bin/kafka-server-start.sh -daemon config/server.properties
   ```
2. Build the kstreams app with `mvn package`
3. Run `java -jar target/kafka-streams-examples-1.0-SNAPSHOT-jar-with-dependencies.jar`
4. Use an HTTP client to see the output at http://localhost:8000/test/a and http://localhost:8000/test/b
5. Push another message with a `b` prefix:
   ```bash
   ./bin/kafka-console-producer.sh --topic ktable --property "parse.key=true" --property "key.separator=:" --bootstrap-server localhost:9092
   >b:magic
   >
   ```
6. Use an HTTP client to check the output again at http://localhost:8000/test/b