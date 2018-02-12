import com.fasterxml.jackson.databind.ObjectMapper;
import com.sample.common.kafkaConsumerUtil;
import com.sample.common.kafkaProducerUtil;
import com.sample.model.kafkaMessage;
import org.testng.annotations.Test;

/**
 * In this method we will test kafka consumer and producer
 */
public class sampleTest {

    @Test(groups = "a")
    public void kafkaProducerTest(){

        kafkaProducerUtil  kafkaProd = new kafkaProducerUtil();

        kafkaMessage  kafkaMessg = new kafkaMessage();

        kafkaMessg.setCity("new york");
        kafkaMessg.setName("banana republic");

        ObjectMapper obj = new ObjectMapper();

        try{
            kafkaProd.sendMesgToTopic((obj.writeValueAsString(kafkaMessg)),"qa.testtopic1");
        }catch(Exception e){
            System.out.println(" Exception in Test class  for kafka producer");
        }


    }

    @Test(dependsOnGroups = "a")
    public void kafkaConsumerTest(){
        kafkaConsumerUtil kafkaConsumerUtil = new kafkaConsumerUtil();

        try {
            Thread.sleep(5000);

            kafkaConsumerUtil.runConsumer();
        }catch( Exception e){
            System.out.println("exception in consumer");
        }
    }
}
