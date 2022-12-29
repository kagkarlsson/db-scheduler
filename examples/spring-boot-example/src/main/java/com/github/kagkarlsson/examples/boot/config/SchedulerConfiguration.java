package com.github.kagkarlsson.examples.boot.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.boot.config.DbSchedulerCustomizer;
import com.github.kagkarlsson.scheduler.serializer.JacksonSerializer;
import com.github.kagkarlsson.scheduler.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Optional;

@Configuration
public class SchedulerConfiguration {

    /**
     * Bean defined when a configuration-property in DbSchedulerCustomizer needs to be overridden.
     */
    @Bean
    DbSchedulerCustomizer customizer(ObjectMapper objectMapper) {
        return new DbSchedulerCustomizer() {
            @Override
            public Optional<SchedulerName> schedulerName() {
                return Optional.of(new SchedulerName.Fixed("spring-boot-scheduler-1"));
            }

            @Override
            public Optional<Serializer> serializer() {
                return Optional.of(new JacksonSerializer(objectMapper));
            }
        };
    }

}
