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

import UserDefinedConfig;

public class Producer {
   
   public static void main(String[] args) throws Exception{
      
      //Assign properties
      String topicName1 = "Sensors1"; 
      String topicName2 = "Sensors2";  
      String topicName3 = "BookingStart";
      String topicName4 = "BookingEnd";
      String topicName5 = "Movies";
      String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
      String valueSerializer = "org.apache.kafka.connect.json.JsonSerializer";
      Properties props = new Properties();
      props.put("bootstrap.servers", UserDefinedConfig.getKafkaServers());
      props.put("acks", "all");
      props.put("retries", 0);
      props.put("batch.size", 16384);
      props.put("linger.ms", 1);
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
	    while ((line = bufferedReader.readLine()) != null) {

		JsonNode jnode = mapper.readTree(line);
		if (jnode.has("Rd")){
	            producer.send(new ProducerRecord<String, JsonNode>(topicName1, 
                        Integer.toString(count), jnode));
		}
		else if (jnode.has("temp")){
	            producer.send(new ProducerRecord<String, JsonNode>(topicName2, 
                        Integer.toString(count), jnode));
		}
		else if (jnode.has("MovieID")){
	            producer.send(new ProducerRecord<String, JsonNode>(topicName5, 
                        Integer.toString(count), jnode));
		}
		else if (jnode.has("EndLat")){
	            producer.send(new ProducerRecord<String, JsonNode>(topicName4, 
                        Integer.toString(count), jnode));
		}
                else{
	            producer.send(new ProducerRecord<String, JsonNode>(topicName3, 
                        Integer.toString(count), jnode));
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
}
