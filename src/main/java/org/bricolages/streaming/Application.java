package org.bricolages.streaming;

import org.bricolages.streaming.filter.ObjectFilterFactory;
import org.bricolages.streaming.filter.OpBuilder;
import org.bricolages.streaming.receiver.LocationParser;
import org.bricolages.streaming.stream.DataSinkHandle;
import org.bricolages.streaming.stream.Transformer;
import org.bricolages.streaming.event.EventQueue;
import org.bricolages.streaming.event.LogQueue;
import org.bricolages.streaming.event.SQSQueue;
import org.bricolages.streaming.s3.S3Agent;
import org.bricolages.streaming.s3.S3ObjectLocation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQSClient;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.Objects;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@EnableJpaRepositories
@EnableTransactionManagement
@Slf4j
@EnableConfigurationProperties(Config.class)
public class Application {
    static public void main(String[] args) throws Exception {
        try (val ctx = SpringApplication.run(Application.class, args)) {
            ctx.getBean(Application.class).run(args);
        }
    }

    public void run(String[] args) throws Exception {
        boolean oneshot = false;
        S3ObjectLocation mapUrl = null;
        S3ObjectLocation procUrl = null;

        for (int i = 0; i < args.length; i++) {
            if (Objects.equals(args[i], "--oneshot")) {
                oneshot = true;
            }
            else if (args[i].startsWith("--map-url=")) {
                val kv = args[i].split("=", 2);
                if (kv.length != 2) {
                    System.err.println("missing argument for --map-url");
                    System.exit(1);
                }
                mapUrl = S3ObjectLocation.forUrl(kv[1]);
            }
            else if (args[i].startsWith("--process-url=")) {
                val kv = args[i].split("=", 2);
                if (kv.length != 2) {
                    System.err.println("missing argument for --process-url");
                    System.exit(1);
                }
                procUrl = S3ObjectLocation.forUrl(kv[1]);
            }
            else if (Objects.equals(args[i], "--help")) {
                printUsage(System.out);
                System.exit(0);
            }
            else if (args[i].startsWith("-")) {
                System.err.println("unknown option: " + args[i]);
                System.exit(1);
            }
            else {
                int argc = args.length - 1;
                if (argc > 1) {
                    System.err.println("too many arguments");
                    System.exit(1);
                }
                break;
            }
        }

        if (mapUrl != null) {
            val metadata = locationParser().parse(mapUrl);
            System.out.println(metadata.destLocation());
            System.exit(0);
        }

        if (procUrl != null) {
            val metadata = locationParser().parse(procUrl);
            val packet = new S3Packet(s3(), metadata.streamName, procUrl);
            val sinkHandle = new StdoutSinkHandle();
            transformer().processOnly(packet, sinkHandle);
        }
        else if (oneshot) {
            queueListener().runOnce();
        }
        else {
            queueListener().run();
        }
    }

    class StdoutSinkHandle implements DataSinkHandle {
        public DataSink open() {
            return new StdoutSink(new BufferedWriter(new OutputStreamWriter(System.out)));
        }

        public String getLocation() {
            return "stdout";
        }
    }
    
    @RequiredArgsConstructor
    class StdoutSink implements DataSinkHandle.DataSink {
        private final BufferedWriter out;

        public BufferedWriter getBufferedWriter() {
            return out;
        }

        public void close() {
            try {
                out.flush();
                out.close();
            } catch(IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    void printUsage(PrintStream s) {
        s.println("Usage: bricolage-streaming-preprocessor [options]");
        s.println("Options:");
        s.println("\t--oneshot             Process one ReceiveMessage and quit.");
        s.println("\t--map-url=S3URL       Prints destination S3 URL for S3URL and quit.");
        s.println("\t--process-url=S3URL   Process the data file S3URL as configured and print to stdout.");
        s.println("\t--help                Prints this message and quit.");
    }

    @Autowired
    Config config;

    @Bean
    public QueueListener queueListener() {
        return new QueueListener(eventQueue(), s3(), transformer(), locationParser(), logQueue());
    }

    @Bean
    public Transformer transformer() {
        return new Transformer(filterFactory());
    }

    @Bean
    public EventQueue eventQueue() {
        val config = this.config.getEventQueue();
        val sqs = new SQSQueue(new AmazonSQSClient(), config.url);
        if (config.visibilityTimeout > 0) sqs.setVisibilityTimeout(config.visibilityTimeout);
        if (config.maxNumberOfMessages > 0) sqs.setMaxNumberOfMessages(config.maxNumberOfMessages);
        if (config.waitTimeSeconds > 0) sqs.setWaitTimeSeconds(config.waitTimeSeconds);
        return new EventQueue(sqs);
    }

    @Bean
    public LogQueue logQueue() {
        val config = this.config.getLogQueue();
        val sqs = new SQSQueue(new AmazonSQSClient(), config.url);
        return new LogQueue(sqs);
    }

    @Bean
    public S3Agent s3() {
        return new S3Agent(new AmazonS3Client());
    }

    @Bean
    public LocationParser locationParser() {
        val mappings = this.config.getMappings();
        return new LocationParser(mappings);
    }

    @Bean
    public ObjectFilterFactory filterFactory() {
        return new ObjectFilterFactory();
    }

    @Bean
    public OpBuilder opBuilder() {
        return new OpBuilder();
    }
}
