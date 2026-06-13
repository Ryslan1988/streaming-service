package by.javaguru.jdmik12.streamingservice.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Getter
@Setter
@Validated
@ConfigurationProperties("app.kafka-topics.outbox-topic")
public class StreamingCommandTopicProperties {

    @NotBlank
    private String name;

    @Positive
    private Integer partitions;

    @Positive
    private Integer replicas;

    @NotBlank
    private String minInSyncReplicas;
}
