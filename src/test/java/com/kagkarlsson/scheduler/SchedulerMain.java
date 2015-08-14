package com.kagkarlsson.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.function.Consumer;

public class SchedulerMain {
	private static final Logger LOG = LoggerFactory.getLogger(SchedulerMain.class);

	private static Consumer<TaskInstance> execute = taskInstance -> {

		LOG.info("Executing");
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		LOG.info("Done executing");

	};

//	public static void main(String[] args) {
//		try {
//			Scheduler scheduler = new Scheduler(new SystemClock(), new InMemoryTaskRespository(), 1, new Scheduler.FixedName("scheduler1"));
//
//			RecurringTask recurring1 = new RecurringTask("recurring1", Duration.ofSeconds(1), execute);
//			RecurringTask recurring2 = new RecurringTask("recurring2", Duration.ofSeconds(1), execute);
//			OneTimeTask onetime = new OneTimeTask("onetime", execute);
//
//			scheduler.schedule(LocalDateTime.now(), recurring1.instance("single"));
//			scheduler.schedule(LocalDateTime.now(), recurring2.instance("single"));
////			scheduler.schedule(LocalDateTime.now(), onetime.instance("1"));
//
//			Runtime.getRuntime().addShutdownHook(new Thread() {
//				@Override
//				public void run() {
//					LOG.info("Received shutdown signal.");
//					scheduler.stop();
//				}
//			});
//
//			scheduler.start();
//		} catch (Exception e) {
//			LOG.error("Error", e);
//		}
//
//	}
}
