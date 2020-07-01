package services;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Twitter_tweets_api {

    private final String consumerKey;
    private final String consumerSecret;
    private final String token;
    private final String secret ;
    private final String bootstrapServers;
    private final String topic;
    List<String> keywords;
    Logger logger = LoggerFactory.getLogger(Twitter_tweets_api.class.getName());


    public Twitter_tweets_api(String consumerKey, String consumerSecret, String token, String secret, String bootstrapServers, String topic, List<String> keywords) {
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.token = token;
        this.secret = secret;
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.keywords = keywords;
    }

    public void run() {

        logger.info("Set up");
        LinkedBlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        Client client = createTwitterClient(msgQueue);
        client.connect();

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5,TimeUnit.SECONDS);
            } catch (InterruptedException e){
                e.printStackTrace();
                client.stop();
            }

            if(msg != null){
                Twitter_data_producer producerDemo = new Twitter_data_producer(bootstrapServers,topic);
                producerDemo.sendValue(msg);
                System.out.println(msg);
            }
        }
    }


    public Client createTwitterClient(BlockingQueue<String> msgQueue){
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(keywords);
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        return builder.build();
    }
}
