import org.json.simple.parser.ParseException;
import services.Twitter_data_ingest;

import java.io.IOException;

class MainClassConsumer {
    public static void main(String[] args) throws IOException, ParseException {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-113-application";
        String topic = "tweet_26_topic";

        Twitter_data_ingest consumerDemo = new Twitter_data_ingest(bootstrapServers,groupId,topic);
        consumerDemo.retrieveDataFromProducer();
    }
}
