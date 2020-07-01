import com.google.common.collect.Lists;
import org.json.simple.parser.ParseException;
import services.Twitter_tweets_api;

import java.io.IOException;
import java.util.List;

public class MainClassProducer {
    public static void main(String[] args) throws IOException, ParseException {
        String consumerKey = "";
        String consumerSecret = "";
        String token = "";
        String secret = "";
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "tweet_26_topic";
        List<String> keywords = Lists.newArrayList("corona","Covi19","Virus");

        Twitter_tweets_api twitterProducer = new Twitter_tweets_api(consumerKey,consumerSecret,token,secret,bootstrapServers,topic,keywords);
        twitterProducer.run();
    }

}



