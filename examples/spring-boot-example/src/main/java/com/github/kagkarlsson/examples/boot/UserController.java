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
package com.github.kagkarlsson.examples.boot;

import com.github.kagkarlsson.examples.boot.config.NewUser;
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.UUID;

import static com.github.kagkarlsson.examples.boot.config.SendUserEmailConfiguration.EMAIL_NEW_USER_TASK;

@RestController
@RequestMapping("/user")
public class UserController {
    private static final Logger LOG = LoggerFactory.getLogger(UserController.class);

    private final UserDao userDao = new UserDao();
    private final SchedulerClient schedulerClient;
    private final TransactionTemplate tx;

public UserController(SchedulerClient schedulerClient, TransactionTemplate tx) {
    this.schedulerClient = schedulerClient;
    this.tx = tx;

}

@PostMapping
public void registerNewUser(String username, String emailAddress) {

    // begin transaction (tx)
    tx.executeWithoutResult((TransactionStatus status) -> {
        userDao.createUser(username, emailAddress);

        TaskInstance<NewUser> newEmailInstance =
            EMAIL_NEW_USER_TASK.instance(
                UUID.randomUUID().toString(),
                new NewUser(username, emailAddress));

        // Schedule the INTENT of sending an email
        // This will insert a new job in the db-table backing db-scheduler
        schedulerClient.schedule(newEmailInstance, Instant.now());

        doSomeOtherStuffThatMightFail();
    });
}

    private void doSomeOtherStuffThatMightFail() {

    }

    public static class UserDao {
        public long createUser(String username, String emailAddress) {
            return 1;
        }
    }
}
