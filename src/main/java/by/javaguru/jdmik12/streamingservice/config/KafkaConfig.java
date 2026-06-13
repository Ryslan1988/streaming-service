package by.javaguru.jdmik12.streamingservice.config;

import by.javaguru.jdmik12.common.streaming.message.command.StreamingCommand;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.HashMap;
import java.util.Map;

@EnableKafkaStreams
@Configuration
@Slf4j
@EnableConfigurationProperties(StreamingCommandTopicProperties.class)
public class KafkaConfig {
    private static final String DLT_SUFFIX = ".dlt";

    @Bean
    public KafkaStreamsConfiguration defaultKafkaStreamsConfig(KafkaProperties kafkaProperties, SslBundles sslBundles) {

        var props = new HashMap<>(kafkaProperties.buildStreamsProperties(sslBundles));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);

        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public JsonSerde<StreamingCommand> outboxDtoJsonSerde() {
        JsonSerde<StreamingCommand> outboxSerde = new JsonSerde<>();
        outboxSerde.deserializer().configure(
                Map.of(
                        JsonDeserializer.TRUSTED_PACKAGES, "by.javaguru.jdmik12.common.*",
                        JsonDeserializer.VALUE_DEFAULT_TYPE, StreamingCommand.class
                ),
                false
        );

        return outboxSerde;
    }

    @Bean
    NewTopic streamingCommandTopic(StreamingCommandTopicProperties properties) {
        return buildTopic(properties.getName(), properties);
    }

    @Bean
    NewTopic streamingCommandDltTopic(StreamingCommandTopicProperties properties) {
        return buildTopic(properties.getName() + DLT_SUFFIX, properties);
    }

    private NewTopic buildTopic(String name, StreamingCommandTopicProperties properties) {
        return TopicBuilder
                .name(name)
                .partitions(properties.getPartitions())
                .replicas(properties.getReplicas())
                .configs(Map.of(
                        TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
                        properties.getMinInSyncReplicas()))
                .build();
    }

}
