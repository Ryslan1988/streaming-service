package by.javaguru.jdmik12.streamingservice.config;

import by.javaguru.jdmik12.common.streaming.message.command.StreamingCommand;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.HashMap;
import java.util.Map;

@EnableKafkaStreams
@Configuration
@Slf4j
public class KafkaConfig {

    @Bean
    public KafkaStreamsConfiguration defaultKafkaStreamsConfig(KafkaProperties kafkaProperties, SslBundles sslBundles) {

        var props = new HashMap<>(kafkaProperties.buildStreamsProperties(sslBundles));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class); // TODO: send to DLT topic?

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
    public DefaultErrorHandler errorHandler(KafkaTemplate<byte[], byte[]> bytesKafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(bytesKafkaTemplate);
        return new DefaultErrorHandler(recoverer);
    }

}
