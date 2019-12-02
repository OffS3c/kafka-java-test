package com.github.OffS3c.kafka.java.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private String consumerKey = "6sSDsk1kDtl05Oh5k6oQiYZTK";
    private String consumerSecret = "NakCIC8ntYk0iPCsl7qErFuNMsbU0LEmmdcNVhrr90giv2R92K";
    private String token = "2793027104-AiQ4nBQaydifbrTaJm46dLNVThg6NdXr3Q7H6fu";
    private String secret = "49l4sVDinAnRWFNfeFe5TR3m20AI5MJcFIWpruDsJEd8X";

    List<String> mySearchTerms = Lists.newArrayList("kafka job");

    public void run() {
        logger.info("Hi!");

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

        // create twitter client
        Client client = createTwitterClient(msgQueue, eventQueue);
        client.connect();

        // create kafka producer

        // loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg != null) {
                logger.info(msg);
            }
        }

        logger.info("Bye!");
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue, BlockingQueue<Event> eventQueue) {


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        // List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = this.mySearchTerms;
        // hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("random-key-test")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();

        return hosebirdClient;
    }
}
