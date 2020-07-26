package com.learn.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private static final String CONSUMER_KEY = "E5x2uZkdZ9dnRvU1zp9wdFzrA";
    private static final String CONSUMER_SECRET = "fd5hk1utKfdgt7EemPg3fGgOoERUz2eFoZY8oYSfZTCGqM1ZiE";
    private static final String TOKEN = "1370312820-171hCjnMvekSYmkPo3DYZlX8Y1izOG0YbJ0MYSO";
    private static final String SECRET = "E3KGPvIMxzxFbPiSp9l5Vbh7sZDHiqcTH6ad2h4WlTKGQ";


    private TwitterProducer(){

    }

    private void run(){
        logger.info("Setup");

        // create a twitter client
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        Client client= createTwitterClient(msgQueue);
        client.connect();

        // create a Kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("shutting down client from twitter...");
            client.stop();
            logger.info("closing producer...");
            producer.close();
            logger.info("done!");
        }));

        // loop to send tweets to Kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);

            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
                producer.flush();
                producer.close();
            }
            if (msg != null) {
                logger.info(msg);
                ProducerRecord<String, String> record =
                        new ProducerRecord<>("twitter_tweets", msg);

                producer.send(record, ((recordMetadata, e) -> {
                    if (e != null) {
                        logger.error("Error while producing message to Kafka!");
                    }
                }));
            }
        }
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        String serverAddress = "localhost:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddress);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<String, String>(properties);

    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue){
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("Preeti");
        endpoint.trackTerms(terms);
        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET ,TOKEN, SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Twitter-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client client = builder.build();
        return client;

    }
    public static void main(String[] args) {
        new TwitterProducer().run();
    }
}
