package demos;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.pushtechnology.diffusion.api.APIException;
import com.pushtechnology.diffusion.api.config.ServerConfig;
import com.pushtechnology.diffusion.api.data.TopicDataFactory;
import com.pushtechnology.diffusion.api.data.metadata.MDataType;
import com.pushtechnology.diffusion.api.data.single.SingleValueTopicData;
import com.pushtechnology.diffusion.api.message.MessageException;
import com.pushtechnology.diffusion.api.message.TopicMessage;
import com.pushtechnology.diffusion.api.publisher.Client;
import com.pushtechnology.diffusion.api.publisher.Publisher;
import com.pushtechnology.diffusion.api.topic.Topic;
import com.pushtechnology.diffusion.client.Diffusion;
import com.pushtechnology.diffusion.client.callbacks.ErrorReason;
import com.pushtechnology.diffusion.client.content.Content;
import com.pushtechnology.diffusion.client.features.Topics;
import com.pushtechnology.diffusion.client.session.Session;
import com.pushtechnology.diffusion.client.topics.details.TopicDetails;
import com.pushtechnology.diffusion.client.types.UpdateContext;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Publisher
 */
public class DiffusionKafkaProducer extends Publisher {

    private static Logger log = LoggerFactory.getLogger(DiffusionKafkaProducer.class);

    private KafkaProducer kp;
    private String bootstrap = "localhost:9092";
    private String zookeeper = "localhost:2181";
    private volatile boolean stopped = false;
    private Session session;
    private final ServerConfig config;
    private final String serverTopicPartition;

    private ConsumerConnector consumer;
    private Map<String, List<KafkaStream<byte[], byte[]>>> topicToStreamMap = new HashMap<>();

    /**
     * The kafka topic to which all Diffusion information will be published.
     * The topic messages are further partitioned by Diffusion topic path.
     */
    private static final String KAFKA_DIFFUSION_TOPIC = "Diffusion";

    /**
     * The root Diffusion topic on which to publish native kafka information
     */
    public static final String DIFFUSION_KAFKA_TOPIC = "Kafka";

    private Set<Client> clientMap = new HashSet<>();
    private Topic rootTopic;


    public DiffusionKafkaProducer(ServerConfig config) {
        this.config = config;

        serverTopicPartition = KAFKA_DIFFUSION_TOPIC + "-" + config.getServerName();
    }

    private Topics.CompletionCallback callback = new Topics.CompletionCallback() {

        @Override
        public void onComplete() {
            log.info("onComplete()");
        }

        @Override
        public void onDiscard() {
            log.info("onDiscard()");
        }
    };

    private Topics.TopicStream topicStream = new Topics.TopicStream() {
        @Override
        public void onSubscription(String s, TopicDetails topicDetails) {
            log.info("onSubscription({})", s);
        }

        @Override
        public void onUnsubscription(String s, Topics.UnsubscribeReason unsubscribeReason) {
            log.info("onUnsubscription({})", s);
        }

        @Override
        public void onTopicUpdate(String topicPath, Content content, UpdateContext updateContext) {
            log.info("Publishing \"{}\" from \"{}\" to kafka", content.asString(), topicPath);
            ProducerRecord<String, String> record
                    = new ProducerRecord<>(serverTopicPartition, topicPath, content.asString());
            kp.send(record);
        }

        @Override
        public void onClose() {
            log.info("onClose()");
        }

        @Override
        public void onError(ErrorReason errorReason) {
            log.warn("onError({})", errorReason.toString());
        }
    };

    /**
     * The only way to get information about topics.
     *
     * @return
     */
    protected List<String> getKafkaTopics() {
        ZkClient zkClient = new ZkClient(zookeeper, 30000, 30000, ZKStringSerializer$.MODULE$);
        try {
            return zkClient.getChildren(ZkUtils.BrokerTopicsPath());
        }
        finally {
            zkClient.close();
        }
    }

    private Session createSession() throws Exception {
        String host = config.getConnector("Client Connector").getHost();
        if (host == null) host = "localhost";

        final Session session =
                Diffusion.sessions()
                        .connectionTimeout(10000)
                        .errorHandler(new Session.ErrorHandler.Default())
                        .open("dpt://" + host + ":" + config.getConnector("Client Connector").getPort());

        final Topics topics = session.feature(Topics.class);
        topics.addFallbackTopicStream(topicStream);
        // Get everything except the Diffusion publisher and the synthetic kafka topic
        topics.subscribe("?(?!(Diffusion|" + DIFFUSION_KAFKA_TOPIC + ")).*//", callback);

        return session;
    }

    @Override
    protected void initialLoad() throws APIException {

        ClassLoader ccl = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            // Expensive, so we will only do this at startup.
            Set<String> topics = new HashSet<>();
            topics.addAll(getKafkaTopics());
            // Don't monitor the synthetic kafka topic
            topics.remove(serverTopicPartition);

            // Find the root topic
            log.info("initialLoad({})", topics.toString());

            rootTopic = getTopic(DIFFUSION_KAFKA_TOPIC);
            if (rootTopic == null) {
                rootTopic = addTopic(DIFFUSION_KAFKA_TOPIC, TopicDataFactory.newSingleValueData(MDataType.STRING));
            }

            // Add all the kafka topics to Diffusion
            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

            for (String topic : topics) {
                if (rootTopic.getTopic(topic) == null) {
                    rootTopic.addTopic(topic, TopicDataFactory.newSingleValueData(MDataType.STRING));
                }
                topicCountMap.put(topic, new Integer(1));
            }

            Properties kprops = new Properties();

            kprops.put("acks", "1");
            kprops.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kprops.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kprops.put("bootstrap.servers", bootstrap);

            kp = new KafkaProducer(kprops);

            try {
                session = createSession();
            }
            catch (Exception e) {
                log.error("{}", e.getMessage(), e);
            }

            String clientId = "" + System.currentTimeMillis();
            Properties cprops = new Properties();
            cprops.put("zookeeper.connect", zookeeper);
            cprops.put("group.id", "mygroup1"); // committed offsets are per-consumer group
            cprops.put("zookeeper.session.timeout.ms", "400");
            cprops.put("zookeeper.sync.time.ms", "200");
            cprops.put("auto.commit.interval.ms", "1000");
            cprops.put("auto.offset.reset", "smallest"); // rewind to the smallest committed offset
            cprops.put("consumer.timeout.ms", "10"); // don't block consumer iterator
            cprops.put("consumer.id", clientId);  // consumed offsets are per consumer id which is store persistently
            cprops.put("auto.commit.enable", "false"); // we'll decide when we want to commit the offsets
            ConsumerConfig cc = new ConsumerConfig(cprops);
            consumer = Consumer.createJavaConsumerConnector(cc);

            topicToStreamMap = consumer.createMessageStreams(topicCountMap);

            // Schedule a thread to pull off messages and publish to clients.
            ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(5);

            for (String topic : topics) {
                final String theTopic = topic;
                ScheduledFuture sf = executorService.scheduleAtFixedRate(
                    new Runnable() {
                        private int cycles = 0;

                        @Override
                        public void run() {
                            if (stopped) {
                                throw new RejectedExecutionException();
                            }
                            ClassLoader ccl = Thread.currentThread().getContextClassLoader();
                            try {
                                Thread.currentThread().setContextClassLoader(DiffusionKafkaProducer.class.getClassLoader());
                                // Only output every 10s
                                if ((cycles++ % 10) == 0) {
                                    log.info("Checking {}", theTopic);
                                }

                                final Topic difftopic = rootTopic.getTopic(theTopic);
                                if (difftopic == null) {
                                    log.error("No topic matching {}", theTopic);
                                    return;
                                }
                                for (KafkaStream<byte[], byte[]> k : topicToStreamMap.get(theTopic)) {
                                    try {
                                        for (MessageAndMetadata<byte[], byte[]> msgAndMetadata : k) {
                                            byte[] kmessage = msgAndMetadata.message();
                                            log.info("Sending {} from topic {} to clients", new String(kmessage), theTopic);
                                            ((SingleValueTopicData) difftopic.getData()).updateAndPublish(new String(kmessage));
                                        }
                                    }
                                    catch (ConsumerTimeoutException cte) {
                                        // Didn't get anymore data
                                    }
                                    catch (APIException apie) {
                                        log.error(apie.getMessage(), apie);
                                    }
                                }
                                // Register that we are done reading the data.
                                consumer.commitOffsets();
                            }
                            catch (Throwable thr) {
                                log.error("{}", thr.getMessage(), thr);
                            }
                            finally {
                                Thread.currentThread().setContextClassLoader(ccl);
                            }
                        }
                    }, 0, 1, TimeUnit.SECONDS);
            }
        } finally {
            Thread.currentThread().setContextClassLoader(ccl);
        }
    }

    @Override
    /**
     * Called when a message is received from a client...
     */
    protected void messageFromClient(final TopicMessage message,
                                     final Client client) {
        log.info("messageFromClient({}, {})", message.getTopicName(), client.toString());

        try {
            log.info("Sending {} from {} to {}", new String(message.asString()), client.getClientID(), message.getTopicName());
            ProducerRecord record = new ProducerRecord(message.getTopicName(), message.asString());
            kp.send(record);
        }
        catch (MessageException me) {
            log.error(me.getMessage(), me);
        }
    }

    @Override
    /**
     * Returns the ITL upon a client subscription
     */
    protected void subscription(final Client client, final Topic topic,
                                final boolean loaded) throws APIException {
        log.info("subscription({})", topic.getName());
        super.subscription(client, topic, loaded);
    }

    @Override
    protected void unsubscription(Client client, Topic topic) {
        log.info("unsubscription({})", topic.getName());
        super.unsubscription(client, topic);
    }

    /**
     * Start the Kafka producer
     */
    @Override
    protected void publisherStarted() throws APIException {
        log.info("DiffusionKafkaProducer publisher started");
        stopped = false;
    }

    /**
     * Stop the Kafka producer
     */
    @Override
    protected void publisherStopping() throws APIException {
        kp.close();
        consumer.shutdown();
        log.info("DiffusionKafkaProducer publisher stopped");
        stopped = true;
    }

    @Override
    /**
     * States that this publisher is stoppable.
     */
    protected boolean isStoppable() {
        return true;
    }

}

