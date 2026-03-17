package by.javaguru.jdmik12.streamingservice.config;

import by.javaguru.jdmik12.common.accounting.message.command.AllocateBudgetCommand;
import by.javaguru.jdmik12.common.notification.message.command.NotificationCommand;
import by.javaguru.jdmik12.common.profiler.message.command.CurriculumVitaeCreateCommand;
import by.javaguru.jdmik12.common.resources.message.command.CheckResourcesCommand;
import by.javaguru.jdmik12.common.security.message.command.CheckSecurityCommand;
import by.javaguru.jdmik12.common.streaming.message.command.StreamingCommand;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
                                                   @Value("${app.kafka-topics.notification-topic.name}") String notificationTopic,
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
                            var header = context.headers().lastHeader("service_type");
                            String type = (header != null)
                                    ? new String(header.value())
                                    : "UNKNOWN";
                            return parseOutboxDto(outboxObjectMapper, value, type);
                        } catch (Exception e) {
                            log.error("Error while parsing outbox message", e);
                            throw new RuntimeException(e);
                        }
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
                .branch((key, message) -> message.value() instanceof NotificationCommand, Branched.as("notification"))
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

        processedStreamMap
                .get("branch-notification").
                mapValues(ProcessedMessage::value).
                peek((String key, Object createCvCommand) -> log.info("Streaming to notification topic with key {}, value {}", key, createCvCommand)).
                to(notificationTopic);

        return outboxStream;
    }

    private ProcessedMessage parseOutboxDto(ObjectMapper outboxObjectMapper, StreamingCommand outboxDto, String type) {
        if (outboxDto == null || outboxDto.payload() == null) {
            return new ProcessedMessage(null, type, new RuntimeException("Empty payload"));
        }
        JsonNode jsonNode = outboxObjectMapper.valueToTree(outboxDto.payload());

        try {
            return switch (type) {
                case "ACCOUNTING" -> new ProcessedMessage(
                        outboxObjectMapper.treeToValue(jsonNode, AllocateBudgetCommand.class),
                        type,
                        null
                );
                case "RESOURCES" -> new ProcessedMessage(
                        outboxObjectMapper.treeToValue(jsonNode, CheckResourcesCommand.class),
                        type,
                        null
                );
                case "SECURITY" -> new ProcessedMessage(
                        outboxObjectMapper.treeToValue(jsonNode, CheckSecurityCommand.class),
                        type,
                        null
                );
                case "PROFILER" -> new ProcessedMessage(
                        outboxObjectMapper.treeToValue(jsonNode, CurriculumVitaeCreateCommand.class),
                        type,
                        null
                );

                case "NOTIFICATION" -> new ProcessedMessage(
                        outboxObjectMapper.treeToValue(jsonNode, NotificationCommand.class),
                        type,
                        null
                );


                default -> new ProcessedMessage(
                        outboxDto,
                        type,
                        new RuntimeException("Unsupported type in header: " + type)
                );
            };
        } catch (JsonProcessingException e) {
            log.error("Failed to parse payload for type: {}", type, e);
            return new ProcessedMessage(outboxDto, type, e);
        }
    }

    private record ProcessedMessage(Object value, String type, Exception error) {
    }

}




