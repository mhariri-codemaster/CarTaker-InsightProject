\**
* Copyright 2017 Mohamed Hariri Nokob 
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
* This piece of code is used to run the Kafka booking consumer. It reads
* from the booking topic into a Kafka stream and then processes the stream
* to obtain aggregate values and then writes the result into Cassandra. 
**/

import java.util.Properties;
import java.util.Date;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.lang.reflect.Method;
import java.lang.StringBuffer;
import java.text.SimpleDateFormat;

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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class StreamBookingConsumer 
{

   public static void main(String[] args) 
   {
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

      BookingEnd_Stream.map((k,v) -> map2Block(k,v))
		       .groupByKey(stringSerde, jsonSerde)
		       .count(TimeWindows.of((long) 60*60*1000),"Hour-Window")
                       .toStream()
                       .foreach((k, v) -> printStream2Cassandra(k, v, session));

      final KafkaStreams streams = new KafkaStreams(builder, props);
      streams.cleanUp(); // This part is not needed in production 
      streams.start();

      Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
   }

   public static Session startCassandra()
   {
      String node = getCassandraMainServer();
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

   public static KeyValue map2Block(String key, JsonNode value)
   {
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode o = mapper.createObjectNode();
      o.put("Date-Time", value.findValue("Date-Time").textValue());
      double lat = value.findValue("StLat").asDouble();
      double lng = value.findValue("StLng").asDouble();
      String blk = findBlock(lat,lng);
      JsonNode newvalue = (JsonNode) o; 
      return KeyValue.pair(blk,newvalue);
   }

   public static String findBlock(double lat, double lng)
   {
      // lat approx range(27,65), lng approx range(21,37)
      double[] Lats = {30,40,50,60};
      double[] Lngs = {24,34};
      int i=0, j=0;
      while (i<4 && lat > Lats[i])
         i = i+1;
      while (j<2 && lng > Lngs[j])
         j = j+1;
      return "B"+(i+1)+(j+1);
   }

   public static void printStream2Cassandra(Object key, Object value, Session session)
   {
      Windowed wkey = (Windowed) key;
      String dt = getDate(wkey.window().start());
      String statement = "INSERT INTO CarTaker.Booking_End_Table (Block,DateTime,Count)"
                          + "VALUES ("
                          + "\'"+ wkey.key() +"\',"
                          + "\'"+ dt +"\',"
                          + value.toString() +");";
      session.execute(statement);
   }

   public static Properties getStreamProperties() 
   {
      Properties props = new Properties();
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-CarTaker");
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,getKafKaServers());
      props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 2);
      props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyTimestampExtractor.class);
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      props.put("group.id", "Bookings_Group");
      return props;
   }

   public static class MyTimestampExtractor implements TimestampExtractor 
   {
      @Override
      public long extract(ConsumerRecord<Object, Object> consumerRecord) 
      {
         Object val = consumerRecord.value();
         JsonNode jnode = (JsonNode) val;
         if (val == null)
            return 0;
         else
            return getMillis(jnode.findValue("Date-Time").textValue());
      }
   }

   public static String getDate(long ms)
   {
      try
      {
	 SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	 Date date = new Date(ms);
         String SB = sdf.format(date).toString();
         return SB;
      }
      catch(Exception e)
      {
	 e.printStackTrace();
	 return "None";
      }
   }

   public static long getMillis(String DT)
   {
      try
      {
	 SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	 Date date = sdf.parse(DT);
         return date.getTime();
      }
      catch(Exception e)
      {
	 e.printStackTrace();
	 return (long) 1;
      }
   }

   public static String getKafKaServers()
   {
      try
      {
         File file = new File("Config.txt");
	 FileReader fileReader = new FileReader(file);
	 BufferedReader bufferedReader = new BufferedReader(fileReader);
	 ObjectMapper mapper = new ObjectMapper();	

	 String line = bufferedReader.readLine();
	 if (line != null) 
         {
            JsonNode jnode = mapper.readTree(line);
            return jnode.findValue("KafkaServers").textValue();
	 } 
         else
         {
	    System.out.println("File Config.txt is empty. Returning localhost");
            return "localhost:9092";
	 }
      }
      catch(FileNotFoundException fe)
      {
         fe.printStackTrace();
         return null;
      }
      catch(Exception e)
      {
         e.printStackTrace();
         return null;
      }
   }

   public static String getCassandraMainServer() 
   {
      try
      {
         File file = new File("Config.txt");
	 FileReader fileReader = new FileReader(file);
	 BufferedReader bufferedReader = new BufferedReader(fileReader);
	 ObjectMapper mapper = new ObjectMapper();	

         String line = bufferedReader.readLine();
	 if (line != null) 
         {
	    JsonNode jnode = mapper.readTree(line);
            return jnode.findValue("CassandraMainServer").textValue();
	 } 
         else
         {
	    System.out.println("File Config.txt is empty. Returning localhost");
            return "localhost:9092";
	 }
      }
      catch(FileNotFoundException fe)
      {
         fe.printStackTrace();
         return null;
      }
      catch(Exception e)
      {
         e.printStackTrace();
         return null;
      }
   }
}
