package dataflow;

import java.util.*;

import fn.*;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaPrint {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaPrint.class);

    public interface Options extends PipelineOptions {
        @Description("Kafka Bootstrap Servers")
        @Default.String("localhost:9092")
        String getKafkaServer();

        void setKafkaServer(String value);

        @Description("Kafka Topic Name")
        @Default.String("stream-heartbeat")
        String getInputTopic();

        void setInputTopic(String value);

        @Description("Pipeline duration to wait until finish in seconds")
        @Default.Long(-1)
        Long getDuration();

        void setDuration(Long duration);
    }

    public static void main(String[] args) throws Exception {
        LOG.info("DATAFLOW STARTING....");
        Options options = PipelineOptionsFactory.fromArgs(args)
                .withValidation().as(Options.class);
        LOG.info(options.toString());
        Pipeline pipeline = Pipeline.create(options);

        PCollection data = pipeline
                .apply("ReadFromKafka", KafkaIO.<Long, String>read()
                        .withTopics(Collections.singletonList(options.getInputTopic()))
                        .withKeyDeserializer(LongDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withBootstrapServers(options.getKafkaServer())
                        .withoutMetadata()
                )
                .apply("ExtractPayload", Values.<String>create())
                .apply("ParseJson&Timestamp", ParDo.of(new ParseJson()))
                .apply("CreateWindow", Window.into(FixedWindows.of(Duration.standardSeconds(60))));

        PCollection<KV<String, Iterable<String>>> output;
        output = (PCollection<KV<String, Iterable<String>>>) data.apply(GroupByKey.create());
        PDone latestPositionInfos = output
                .apply("CollectLatestPositions", ParDo.of(new LatestPositions()))
                .apply(TextIO.write().to("main/output/latest_position_logs").withWindowedWrites().withNumShards(1));

        PipelineResult run = pipeline.run();
        run.waitUntilFinish(Duration.standardSeconds(options.getDuration()));
    }

}
