/**
 * Copyright (C) Gustav Karlsson
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.examples.boot;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.Task;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class App {
  private static final Logger log = LoggerFactory.getLogger(App.class);

  public static void main(String... args) {

    ConfigurableApplicationContext ctx = SpringApplication.run(App.class, args);
  }

  /** Example hack: use a {@link CommandLineRunner} to trigger scheduling of a one-time task. */
  @Bean
  CommandLineRunner executeOnStartup(Scheduler scheduler, Task<Void> sampleOneTimeTask) {
    log.info("Scheduling one time task to now!");

    return ignored ->
        scheduler.schedule(sampleOneTimeTask.instance("command-line-runner"), Instant.now());
  }
}
