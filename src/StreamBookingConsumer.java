import java.util.Properties;
import java.util.Arrays;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.TimestampExtractor;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.lang.reflect.Method;
import org.apache.kafka.streams.kstream.Windowed;
import java.lang.StringBuffer;

import UserDefinedConfig;

public class StreamBookingConsumer {
   
   public static void main(String[] args) {

      String topicName = "BookingEnd";
      final Serde<String> stringSerde = Serdes.String();
      final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
      final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
      final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
      Properties props = getStreamProperties();

      Session session = startCassandra();

      final KStreamBuilder builder = new KStreamBuilder();

      final KStream<String, JsonNode> BookingEnd_Stream = builder.stream(stringSerde, jsonSerde, topicName);
      System.out.println("Subscribed to topic " + topicName);
      BookingEnd_Stream.map((k,v)->map2Block(k,v))
		       .groupByKey(stringSerde, jsonSerde)
		       .count(TimeWindows.of((long) 60*60*1000),"Hour-Window")
                       .toStream()
                       .foreach((k, v) -> printStream(k, v));


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
	session.execute("CREATE TABLE IF NOT EXISTS CarTaker.Booking_End_Table ("
        		+ "DateTime timestamp,"
        		+ "Block text,"
        		+ "Count int,"
        		+ "PRIMARY KEY ((Block),DateTime)"
    			+ ")WITH CLUSTERING ORDER BY (DateTime DESC);");

	return session;
    }

    private static KeyValue map2Block(String key, JsonNode value){
	ObjectMapper mapper = new ObjectMapper();
	ObjectNode o = mapper.createObjectNode();
	o.put("Date-Time", value.findValue("Date-Time").textValue());
	double lat = value.findValue("StLat").asDouble();
	double lng = value.findValue("StLng").asDouble();
	String blk = findBlock(lat,lng);
	JsonNode newvalue = (JsonNode) o; 
	return KeyValue.pair(blk,newvalue);
    }

    private static String findBlock(double lat, double lng){
	// lat approx range(27,65), lng approx range(21,37)
	if (lat<=30){
	    if (lng<=24){
		return "B11";
	    }else if (lng<=34){
		return "B12";
	    }else{
		return "B13";
	    }
	}else if(lat<=40){
	    if (lng<=24){
		return "B21";
	    }else if (lng<=34){
		return "B22";
	    }else{
		return "B23";
	    }
	}else if(lat<=50){
	    if (lng<=24){
		return "B31";
	    }else if (lng<=34){
		return "B32";
	    }else{
		return "B33";
	    }
	}else if(lat<=60){
	    if (lng<=24){
		return "B41";
	    }else if (lng<=34){
		return "B42";
	    }else{
		return "B43";
	    }
	}else{
	    if (lng<=26){
		return "B51";
	    }else if (lng<=31){
		return "B52";
	    }else{
		return "B53";
	    }
	}
    }

    private static void printStream2Cassandra(String key, Object value, Session session){
	    Windowed wkey = (Windowed) key;
	    String dt = getDate(wkey.window().start());
	    System.out.println("Check This: Start="+wkey.key()+"@"+dt+", value="+value.toString());
	    String statement = "INSERT INTO CarTaker.Booking_End_Table (Block,DateTime,Count)"
                + "VALUES ("
                + "\'"+ wkey.key() +"\',"
                + "\'"+ dt +"\',"
                + value.toString()) +");";
    	    session.execute(statement);
    }

    private static void printStream(Object key, Object value){
	    Windowed wkey = (Windowed) key;
	    String dt = getDate(wkey.window().start());
	    System.out.println("Check This: Start="+wkey.key()+"@"+dt+", value="+value.toString());
    }

    private static Properties getStreamProperties() {

      Properties props = new Properties();
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-CarTaker");
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,UserDefinedConfig.getKafkaServers());
      props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 2);
      props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyTimestampExtractor.class);
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      return props;
   }

    public static class MyTimestampExtractor implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> consumerRecord) {
            Object val = consumerRecord.value();
	    JsonNode jnode = (JsonNode) val;
            if (val == null)
                return 0;
            else
                return getMillis(jnode.findValue("Date-Time").textValue());
        }
    }

    public static String getDate(long ms){
       try{
	  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	  Date date = new Date(ms);
          String SB = sdf.format(date).toString();
          return SB;
       }catch(Exception e){
	  e.printStackTrace();
	  return "None";
       }
    }

    public static long getMillis(String DT){
       try{
	  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	  Date date = sdf.parse(DT);
          return date.getTime();
       }catch(Exception e){
	  e.printStackTrace();
	  return (long) 1;
       }
    }
}
