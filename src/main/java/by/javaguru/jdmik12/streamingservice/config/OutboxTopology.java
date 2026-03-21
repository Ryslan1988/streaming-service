package by.javaguru.jdmik12.streamingservice.config;

import by.javaguru.jdmik12.common.streaming.message.command.StreamingCommand;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.charset.StandardCharsets;

import static by.javaguru.jdmik12.common.base.ServiceType.*;
import static org.springframework.kafka.support.KafkaHeaders.REPLY_TOPIC;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class OutboxTopology {
    private final KafkaConfig kafkaConfig;

    @Bean
    KStream<String, StreamingCommand> outboxStream(StreamsBuilder streamsBuilder,
                                                   ObjectMapper outboxObjectMapper,
                                                   @Value("${app.kafka-topics.outbox-topic.name}") String outboxTopic,
                                                   @Value("${app.kafka-topics.cv-topic.name}") String cvTopic,
                                                   @Value("${app.kafka-topics.accounting-topic.name}") String accountingTopic,
                                                   @Value("${app.kafka-topics.security-topic.name}") String securityTopic,
                                                   @Value("${app.kafka-topics.resources-topic.name}") String resourcesTopic,
                                                   @Value("${app.kafka-topics.dlt-topic.name}") String dltTopic) {

        var keySerde = Serdes.String();
        var processedStreamMap = streamsBuilder
                .stream(outboxTopic, Consumed.with(keySerde, kafkaConfig.outboxDtoJsonSerde()))
                .processValues(() -> new ContextualFixedKeyProcessor<String, StreamingCommand, ProcessedMessage>() {
                    @Override
                    public void process(FixedKeyRecord<String, StreamingCommand> record) {
                        try {
                            String serviceType = getHeader(record, "service_type");
                            String replyTopic = getHeader(record, "reply_topic");
                            String typeId = getHeader(record, "payload_type");
                            putHeader(record.headers(), REPLY_TOPIC, "reply_topic", replyTopic);
                            putHeader(record.headers(), "__TypeId__", "payload_type", typeId);
                            record.headers().remove("service_type");

                            ProcessedMessage result = new ProcessedMessage(record.value().payload(), serviceType, null);

                            context().forward(record.withValue(result));
                        } catch (Exception e) {
                            context().forward(record.withValue(new ProcessedMessage(record.value(), "DLT", e)));
                        }
                    }

                    private String getHeader(FixedKeyRecord<String, StreamingCommand> record, String headerName) {
                        var header = record.headers().lastHeader(headerName);
                        return (header != null) ? new String(header.value(), StandardCharsets.UTF_8) : "UNKNOWN";
                    }
                })
                .split(Named.as("branch-"))
                .branch((key, msg) -> msg.error() != null, Branched.as("dlt"))
                .branch((key, msg) -> PROFILER.getValue().equals(msg.type()), Branched.as("cv"))
                .branch((key, msg) -> ACCOUNTING.getValue().equals(msg.type()), Branched.as("accounting"))
                .branch((key, msg) -> SECURITY.getValue().equals(msg.type()), Branched.as("security"))
                .branch((key, msg) -> RESOURCES.getValue().equals(msg.type()), Branched.as("resources"))
                .defaultBranch(Branched.as("unknown"));

        processedStreamMap.get("branch-dlt").mapValues(ProcessedMessage::value).to(dltTopic);
        processedStreamMap.get("branch-cv").mapValues(ProcessedMessage::value).to(cvTopic);
        processedStreamMap.get("branch-accounting").mapValues(ProcessedMessage::value).to(accountingTopic);
        processedStreamMap.get("branch-security").mapValues(ProcessedMessage::value).to(securityTopic);
        processedStreamMap.get("branch-resources").mapValues(ProcessedMessage::value).to(resourcesTopic);
        processedStreamMap.get("branch-unknown").mapValues(ProcessedMessage::value).to(dltTopic);

        return null;
    }

    private static void putHeader(Headers headers, String headerName, String deleteHeader, String value) {
        headers.remove(deleteHeader);
        headers.add(headerName, value.getBytes(StandardCharsets.UTF_8));
    }

    private record ProcessedMessage(Object value, String type, Exception error) {
    }

}




