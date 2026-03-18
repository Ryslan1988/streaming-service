package by.javaguru.jdmik12.streamingservice.config;

import by.javaguru.jdmik12.common.accounting.message.command.AllocateBudgetCommand;
import by.javaguru.jdmik12.common.profiler.message.command.CurriculumVitaeCreateCommand;
import by.javaguru.jdmik12.common.resources.message.command.CheckResourcesCommand;
import by.javaguru.jdmik12.common.security.message.command.CheckSecurityCommand;
import by.javaguru.jdmik12.common.streaming.message.command.StreamingCommand;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.charset.StandardCharsets;

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
                                                   @Value("${app.kafka-topics.dlt-topic.name}") String dltTopic) {

        var keySerde = Serdes.String();
        var outboxStream = streamsBuilder
                .stream(outboxTopic, Consumed.with(keySerde, kafkaConfig.outboxDtoJsonSerde()));
        var processedStreamMap = outboxStream
                .peek((key, dto) ->
                        log.info("Outbox received with key {}, value {}", key, dto))

                .transformValues(() -> new ValueTransformerWithKey<String, StreamingCommand, ProcessedMessage>() {
                    private ProcessorContext context;

                    @Override
                    public void init(ProcessorContext context) {
                        this.context = context;
                    }

                    @Override
                    public ProcessedMessage transform(String key, StreamingCommand value) {
                        try {
                            String serviceType = getHeader("service_type");
                            String replyTopic = getHeader("reply_topic");
                            putHeader(context.headers(), REPLY_TOPIC, "reply_topic", replyTopic);

                            return parseOutboxDto(outboxObjectMapper, value, serviceType);
                        } catch (Exception e) {
                            log.error("Error while parsing outbox message", e);
                            return new ProcessedMessage(value, "dlt", e);
                        }
                    }

                    private String getHeader(String headerName) {
                        var header = context.headers().lastHeader(headerName);
                        String type = (header != null)
                                ? new String(header.value())
                                : dltTopic;
                        return type;
                    }

                    @Override
                    public void close() {
                    }
                })

                .split(Named.as("branch-"))
                .branch((key, message) -> message.error() != null, Branched.as("dlt"))
                .branch((key, message) -> message.value() instanceof CurriculumVitaeCreateCommand, Branched.as("cv"))
                .branch((key, message) -> message.value() instanceof AllocateBudgetCommand, Branched.as("accounting"))
                .branch((key, message) -> message.value() instanceof CheckSecurityCommand, Branched.as("security"))
                .defaultBranch(Branched.as("unknown"));

        processedStreamMap
                .get("branch-dlt").
                merge(processedStreamMap.get("branch-unknown")).
                to(dltTopic);

        processedStreamMap
                .get("branch-cv").
                mapValues(ProcessedMessage::value).
                peek((String key, Object createCvCommand) -> log.info("Streaming to CV topic with key {}, value {}", key, createCvCommand)).
                to(cvTopic);

        processedStreamMap
                .get("branch-accounting").
                mapValues(ProcessedMessage::value).
                peek((String key, Object createCvCommand) -> log.info("Streaming to accounting topic with key {}, value {}", key, createCvCommand)).
                to(accountingTopic);

        processedStreamMap
                .get("branch-security").
                mapValues(ProcessedMessage::value).
                peek((String key, Object createCvCommand) -> log.info("Streaming to security topic with key {}, value {}", key, createCvCommand)).
                to(securityTopic);

        return outboxStream;
    }

    private ProcessedMessage parseOutboxDto(ObjectMapper outboxObjectMapper, StreamingCommand outboxDto, String serviceType) {
        if (outboxDto == null || outboxDto.payload() == null) {
            return new ProcessedMessage(null, serviceType, new RuntimeException("Empty payload"));
        }
        JsonNode jsonNode = outboxObjectMapper.valueToTree(outboxDto.payload());

        try {
            return switch (serviceType) {
                case "ACCOUNTING" -> new ProcessedMessage(
                        outboxObjectMapper.treeToValue(jsonNode, AllocateBudgetCommand.class),
                        serviceType,
                        null
                );
                case "RESOURCES" -> new ProcessedMessage(
                        outboxObjectMapper.treeToValue(jsonNode, CheckResourcesCommand.class),
                        serviceType,
                        null
                );
                case "SECURITY" -> new ProcessedMessage(
                        outboxObjectMapper.treeToValue(jsonNode, CheckSecurityCommand.class),
                        serviceType,
                        null
                );
                case "PROFILER" -> new ProcessedMessage(
                        outboxObjectMapper.treeToValue(jsonNode, CurriculumVitaeCreateCommand.class),
                        serviceType,
                        null
                );


                default -> new ProcessedMessage(
                        outboxDto,
                        serviceType,
                        new RuntimeException("Unsupported type in header: " + serviceType)
                );
            };
        } catch (JsonProcessingException e) {
            log.error("Failed to parse payload for type: {}", serviceType, e);
            return new ProcessedMessage(outboxDto, serviceType, e);
        }
    }

    private static void putHeader(Headers headers, String headerName, String deleteHeader, String value) {
        headers.remove(deleteHeader);
        headers.add(headerName, value.getBytes(StandardCharsets.UTF_8));
    }

    private record ProcessedMessage(Object value, String type, Exception error) {
    }

}




