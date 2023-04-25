/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.examples.boot.config;

import com.github.kagkarlsson.scheduler.task.*;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SendUserEmailConfiguration {

    public static final TaskWithDataDescriptor<NewUser> EMAIL_NEW_USER_TASK =
        new TaskWithDataDescriptor<>("email-new-user", NewUser.class);


    @Bean
    public Task<NewUser> sendEmailTask() {
        return Tasks.oneTime(EMAIL_NEW_USER_TASK)
            .execute((TaskInstance<NewUser> inst, ExecutionContext ctx) -> {

                NewUser newUser = inst.getData();
                sendWelcomeEmail(newUser); // dummy implementation

            });
    }

    public void sendWelcomeEmail(NewUser newUser) {
        // simulation
    }

}
