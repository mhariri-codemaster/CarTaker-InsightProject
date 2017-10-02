/**
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
* This piece of code is used to run the Kafka producer. It separates
* the different messages receieved into different topics. 
**/

import java.util.Properties;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class CarTakerProducer 
{
   
   public static void main(String[] args) throws Exception
   {
      String topicName1 = "Sensors1"; 
      String topicName2 = "Sensors2";  
      String topicName3 = "BookingStart";
      String topicName4 = "BookingEnd";
      String topicName5 = "Movies";
      String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
      String valueSerializer = "org.apache.kafka.connect.json.JsonSerializer";
      Properties props = new Properties();
      props.put("bootstrap.servers", getKafKaServers());
      props.put("acks", "1");
      props.put("retries", 0);
      props.put("batch.size", 32000);
      props.put("linger.ms", 3);
      props.put("buffer.memory", 33554432);
      props.put("key.serializer", keySerializer);
      props.put("value.serializer", valueSerializer);
      
      Producer<String, JsonNode> producer = new KafkaProducer<String, JsonNode>(props);
                                                
      try
      {
         File file = new File("data0.txt");
	 FileReader fileReader = new FileReader(file);
	 BufferedReader bufferedReader = new BufferedReader(fileReader);
	 ObjectMapper mapper = new ObjectMapper();	

	 String line;
         int count = 1;
	 while ((line = bufferedReader.readLine()) != null) 
         {
	    JsonNode jnode = mapper.readTree(line);
	    if (jnode.has("Rd"))
            {
	       producer.send(new ProducerRecord<String, JsonNode>(topicName1, 
                  Integer.toString(count), jnode));
	    }
	    else if (jnode.has("temp"))
            {
	       producer.send(new ProducerRecord<String, JsonNode>(topicName2, 
                  Integer.toString(count), jnode));
	    }
	    else if (jnode.has("MovieID"))
            {
               producer.send(new ProducerRecord<String, JsonNode>(topicName5, 
                  Integer.toString(count), jnode));
	    }
	    else if (jnode.has("BID"))
            {
               String BID = jnode.findValue("BID").textValue();
	       if (BID.charAt(1)=="E".charAt(0))
               {
	          producer.send(new ProducerRecord<String, JsonNode>(topicName4, 
                     Integer.toString(count), jnode));
		  System.out.println(jnode.toString());
	       } 
               else if (BID.charAt(1)=="S".charAt(0))
               {
	          producer.send(new ProducerRecord<String, JsonNode>(topicName3, 
                     Integer.toString(count), jnode));
	       }
	    }
	    else 
            {
               System.out.println("Unrecognized Message");
	    }
            count = count+1;
	 }
         System.out.println("Message sent successfully");
         producer.close();
      }
      catch(FileNotFoundException fe)
      {
         fe.printStackTrace();
      }
      catch(Exception e)
      {
         e.printStackTrace();
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
}
