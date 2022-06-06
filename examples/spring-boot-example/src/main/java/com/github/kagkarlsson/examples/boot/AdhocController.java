package com.github.kagkarlsson.examples.boot;

import java.time.Instant;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.RequestContextHolder;

import com.github.kagkarlsson.examples.boot.config.TaskConfiguration;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;

@RestController
public class AdhocController {

	private static Scheduler scheduler;

	@Autowired
	public void setScheduler(Scheduler scheduler) {
		AdhocController.scheduler = scheduler;
	}

	/*
	 * Add a one-time task by sending a HTTP POST request to /adhoc/<seconds>
	 */
	@PostMapping("/adhoc/{expiry}")
    public String addTask(@PathVariable Integer expiry) {
		String instanceid = RequestContextHolder.currentRequestAttributes().getSessionId();
		TaskInstance<Void> instance = TaskConfiguration.sampleOneTimeTask().instance(instanceid);
		scheduler.schedule(instance, Instant.now().plusSeconds(expiry));

		Instant taskexecution = scheduler.getScheduledExecution(instance).get().getExecutionTime();

		return("Added new task - ID: " + instanceid + " - Execution time: " + taskexecution);
    }

}
