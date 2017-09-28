import java.util.Properties;
import java.util.Arrays;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.connect.json.JsonDeserializer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import UserDefinedConfig;

public class StreamSensorConsumer {
   
   public static void main(String[] args) {

      String topicName = "Sensors1";
      final Serde<String> stringSerde = Serdes.String();
      final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
      final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
      final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
      Properties props = getStreamProperties();

      Session session = startCassandra();

      final KStreamBuilder builder = new KStreamBuilder();
      final KStream<String, JsonNode> Sensors1_Stream = builder.stream(stringSerde, jsonSerde, topicName);
      System.out.println("Subscribed to topic " + topicName);
      Sensors1_Stream.foreach((k, v) -> printStream2Cassandra(k, v,session));

      final KafkaStreams streams = new KafkaStreams(builder, props);
      streams.cleanUp(); // This part is not needed in production 
      streams.start();

      Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
   }

    private static Session startCassandra(){
	String node = UserDefinedConfig.getCassandraServer();
	Cluster cluster = Cluster.builder().addContactPoint(node).build();
	Session session = cluster.connect();

   	session.execute("CREATE KEYSPACE IF NOT EXISTS CarTaker WITH replication "
                       + "= {'class':'SimpleStrategy', 'replication_factor':3};");
	session.execute("CREATE TABLE IF NOT EXISTS CarTaker.S1_Table (" 
        		+ "VID text,"
                        + "SID text,"
        		+ "DateTime timestamp,"
        		+ "Lat float,"
        		+ "Lng float,"
        		+ "LatV float,"
        		+ "LngV float,"
        		+ "Ladar float,"
        		+ "Radar float,"
        		+ "UltraSound float,"
        		+ "PRIMARY KEY ((VID),DateTime)"
    			+ ")WITH CLUSTERING ORDER BY (DateTime DESC);");

	return session;
    }

    private static void printStream2Cassandra(String key, JsonNode value, Session session){
	    String statement = "INSERT INTO CarTaker.S1_Table (VID,SID,DateTime,Lat,Lng,LatV,LngV,Ladar,Radar,Ultrasound)"
                + "VALUES ("
                + "\'"+value.findValue("VID").textValue()+"\',"
                + "\'"+ value.findValue("SID").textValue()+"\',"
                + "\'"+ value.findValue("Date-Time").textValue()+"\',"
                + value.findValue("lat").numberValue().toString()+","
                + value.findValue("lng").numberValue().toString()+","
                + value.findValue("latv").numberValue().toString()+","
                + value.findValue("lngv").numberValue().toString()+","
                + value.findValue("Ld").numberValue().toString()+","
                + value.findValue("Rd").numberValue().toString()+","
                + value.findValue("US").numberValue().toString()+");";
    	    session.execute(statement);
    }

    private static Properties getStreamProperties() {

      Properties props = new Properties();
      props.put(StreamsConfig.CLIENT_ID_CONFIG, "streams-sensors1");
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-CarTaker");
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,UserDefinedConfig.getKafkaServers());
      props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 2);
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      return props;
   }
}
