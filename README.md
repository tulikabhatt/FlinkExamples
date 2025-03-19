If you enciunter the following error:
```
java.lang.reflect.InaccessibleObjectException: Unable to make field private final java.lang.Object[] java.util.Arrays$ArrayList.a accessible: module java.base does not "opens java.util" to unnamed module @1ad282e0
```
Then you need to add the following VM option to your run configuration:
```bash
--add-opens java.base/java.util=ALL-UNNAMED
```
You can do this in IntelliJ by going to the following menu:

Run—>EditConfigurations—>Modify options—>JAVA Add VM options—>VM options

For running StateExamples.java
1. Install latest Kafka version from https://kafka.apache.org/downloads
2. Start Zookeeper and Kafka server using the following commands:
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```
3. Create a topic named "crypto-prices" using the following command:
```bash
bin/kafka-topics.sh --create --topic crypto-prices --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
4. Run OnlineSource.java
5. Run StateExamples.java
 
For running SenimentAnalysis.java
1. Run netcat using the following command:
```bash
nc -l 12345
```
To check if port are open, you can use the following command:
```bash
nc -zv localhost 12345
```
You can test the program by typing some text in the netcat terminal and pressing enter.
2. Run SentimentAnalysis.java
