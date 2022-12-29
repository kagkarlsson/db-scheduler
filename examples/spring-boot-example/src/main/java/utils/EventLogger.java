package utils;

import com.github.kagkarlsson.scheduler.task.HasTaskName;
import com.github.kagkarlsson.scheduler.task.HasTaskName.SimpleTaskName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventLogger {

    public static final Logger EVENT_LOG = LoggerFactory.getLogger("SchedulerEvents");

    public static void logTask(String taskName, String message) {
        logTask(new SimpleTaskName(taskName), message);
    }
    public static void logTask(HasTaskName taskName, String message) {
        EVENT_LOG.info(taskName.getTaskName() + ": " + message);
    }
}
