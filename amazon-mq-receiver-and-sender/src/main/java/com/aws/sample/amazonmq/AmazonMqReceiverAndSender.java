package com.aws.sample.amazonmq;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.*;

import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class AmazonMqReceiverAndSender implements Callable<Void> {

    private static final DateFormat df = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss.S");

    private CommandLine cmd;
    private AtomicInteger count;
    private Connection conn;
    private Session session;
    private Destination sourceDestination;
    private Destination targetDestination;
    private MessageConsumer consumer;
    private MessageProducer producer;

    public AmazonMqReceiverAndSender(ActiveMQSslConnectionFactory connFact, CommandLine cmd, AtomicInteger count) throws JMSException {
        this.cmd = cmd;
        this.count = count;

        conn = connFact.createConnection(cmd.getOptionValue("user"), cmd.getOptionValue("password"));
        conn.setClientID("AmazonMQWorkshop-" + UUID.randomUUID().toString());
        conn.start();

        session = conn.createSession(true, Session.SESSION_TRANSACTED);

        if (cmd.getOptionValue("type").contentEquals("queue")) {
            sourceDestination = session.createQueue(cmd.getOptionValue("sourceDestination"));
            targetDestination = session.createQueue(cmd.getOptionValue("targetDestination"));
        } else {
            sourceDestination = session.createTopic(cmd.getOptionValue("sourceDestination"));
            targetDestination = session.createTopic(cmd.getOptionValue("targetDestination"));
        }

        consumer = session.createConsumer(sourceDestination);
        producer = session.createProducer(targetDestination);
    }

    public Void call() throws JMSException {
        final int delay = Integer.parseInt(cmd.getOptionValue("delay", "0"));
        final long ttl = Integer.parseInt(cmd.getOptionValue("ttl", "-1"));
        final int deliveryMode = Integer.parseInt(cmd.getOptionValue("persistentMode", String.valueOf(DeliveryMode.PERSISTENT)));

        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                try {
                    if (delay > 0) {
                        Thread.sleep(delay);
                    }

                    producer.send(message, deliveryMode, 0, ttl);
                    session.commit();

                    count.incrementAndGet();
                } catch (JMSException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }});

        return null;
    }

    public static void main(String[] args) throws JMSException, ParseException {
        CommandLine cmd = parseAndValidateCommandLineArguments(args);
        final AtomicInteger count = new AtomicInteger();

        int concurrency = Integer.parseInt(cmd.getOptionValue("concurrency", "1"));

        try {
            ActiveMQSslConnectionFactory connFact = new ActiveMQSslConnectionFactory(cmd.getOptionValue("url"));
            connFact.setConnectResponseTimeout(10000);

            Collection<AmazonMqReceiverAndSender> processors = new ArrayList<>();

            for (int c = 0; c < concurrency; c++) {
                processors.add(new AmazonMqReceiverAndSender(connFact, cmd, count));
            }

            ExecutorService executor = Executors.newFixedThreadPool(concurrency);
            final long ds = System.currentTimeMillis();

            // print the throughput of all receivers at the end of our test
            registerShutdownHook(count, ds);

            List<Future<Void>> futures = executor.invokeAll(processors);
        } catch (Exception ex) {
            System.out.println(String.format("Error: %s", ex.getMessage()));
            System.exit(1);
        }
    }

    private static CommandLine parseAndValidateCommandLineArguments(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("help", false, "Print the help message.");
        options.addOption("url", true, "The broker connection url.");
        options.addOption("user", true, "The user to connect to the broker.");
        options.addOption("password", true, "The password for the user.");
        options.addOption("mode", true, "Whether to act as 'sender' or 'receiver'");
        options.addOption("type", true, "Whether to use a queue or a topic.");
        options.addOption("sourceDestination", true, "The name of the queue or topic where we are listening.");
        options.addOption("targetDestination", true, "The name of the queue or topic where we we are sending the messages to.");
        options.addOption("persistentMode", true, "Send messages in persistent or non-persistent mode");
        options.addOption("ttl", true, "The time to live value for the message.");
        options.addOption("concurrency", true, "The number of concurrent receivers.");
        options.addOption("delay", true, "The simulated processing delay in ms.");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("help")) {
            printUsage(options);
        }

        if (!(cmd.hasOption("url")
                && cmd.hasOption("user")
                && cmd.hasOption("password")
                && cmd.hasOption("type")
                && cmd.hasOption("sourceDestination")
                && cmd.hasOption("targetDestination"))) {
            printUsage(options);
        }

        return cmd;
    }

    private static void printUsage(Options options) throws ParseException {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "java -jar amazon-mq-receiver-and-sender.jar -url <url> -user <user> -password <password> -type <queue|topic> -sourceDestination <destination> -targetDestination <destination> [-concurrency <concurrency> -delay <delay in ms> -ttl <ttl>]", options);
        System.exit(1);
    }

    private static void registerShutdownHook(final AtomicInteger count, final long ds) {
        Thread shutdown = new Thread(new Runnable(){
            long d = ds;

            public void run() {
                long delta = System.currentTimeMillis() - d;
                int currentCount = count.get();
                System.err.print(
                        String.format("\nMessages: %d \nSeconds: %f \nRate: %f/sec",
                                currentCount,
                                delta / 1000.0,
                                currentCount / (delta / 1000.0)));
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdown);
    }
}