package com.github.kagkarlsson.scheduler;

import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.github.kagkarlsson.scheduler.exceptions.TaskInstanceCurrentlyRunningException;
import com.github.kagkarlsson.scheduler.exceptions.TaskInstanceNotFoundException;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.TaskInstance;

import static com.github.kagkarlsson.scheduler.task.TaskInstanceId.StandardTaskInstanceId;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SchedulerClientExceptionsTest {

    @InjectMocks
    SchedulerClient.StandardSchedulerClient schedulerClient;

    @Mock
    TaskRepository taskRepository;

    @Test
    public void failsToRescheduleWhenTaskIsNotFound() {
        StandardTaskInstanceId taskInstance = new StandardTaskInstanceId(randomAlphanumeric(10), randomAlphanumeric(10));
        when(taskRepository.getExecution(taskInstance.getTaskName(), taskInstance.getId())).thenReturn(Optional.empty());

        TaskInstanceNotFoundException actualException = assertThrows(TaskInstanceNotFoundException.class, () -> {
            schedulerClient.reschedule(taskInstance, Instant.now(), null);
        });
        assertEquals("Failed to perform action on task because it was not found.(task name: " + taskInstance.getTaskName() + ", instance id: " + taskInstance.getId() + ")",
            actualException.getMessage());
    }

    @Test
    public void failsToRescheduleWhenATaskIsPickedAndExecuting() {
        StandardTaskInstanceId taskInstance = new StandardTaskInstanceId(randomAlphanumeric(10), randomAlphanumeric(10));
        Execution expectedExecution = new Execution(
            Instant.now(),
            new TaskInstance(taskInstance.getTaskName(), taskInstance.getId()),
            true,
            randomAlphanumeric(5),
            null,
            null,
            0,
            null,
            1
        );

        when(taskRepository.getExecution(taskInstance.getTaskName(), taskInstance.getId())).thenReturn(Optional.of(expectedExecution));

        TaskInstanceCurrentlyRunningException actualException = assertThrows(TaskInstanceCurrentlyRunningException.class, () -> {
            schedulerClient.reschedule(taskInstance, Instant.now(), null);
        });
        assertEquals("Failed to perform action on task since it's currently running.(task name: " + taskInstance.getTaskName() + ", instance id: " + taskInstance.getId() + ")",
            actualException.getMessage());
    }

    @Test
    public void failsToCancelWhenTaskIsNotFound() {
        StandardTaskInstanceId taskInstance = new StandardTaskInstanceId(randomAlphanumeric(10), randomAlphanumeric(10));
        when(taskRepository.getExecution(taskInstance.getTaskName(), taskInstance.getId())).thenReturn(Optional.empty());

        TaskInstanceNotFoundException actualException = assertThrows(TaskInstanceNotFoundException.class, () -> {
            schedulerClient.cancel(taskInstance);
        });
        assertEquals("Failed to perform action on task because it was not found.(task name: " + taskInstance.getTaskName() + ", instance id: " + taskInstance.getId() + ")",
            actualException.getMessage());
    }

    @Test
    public void failsToCancelWhenATaskIsPickedAndExecuting() {
        StandardTaskInstanceId taskInstance = new StandardTaskInstanceId(randomAlphanumeric(10), randomAlphanumeric(10));
        Execution expectedExecution = new Execution(
            Instant.now(),
            new TaskInstance(taskInstance.getTaskName(), taskInstance.getId()),
            true,
            randomAlphanumeric(5),
            null,
            null,
            0,
            null,
            1
        );

        when(taskRepository.getExecution(taskInstance.getTaskName(), taskInstance.getId())).thenReturn(Optional.of(expectedExecution));

        TaskInstanceCurrentlyRunningException actualException = assertThrows(TaskInstanceCurrentlyRunningException.class, () -> {
            schedulerClient.cancel(taskInstance);
        });
        assertEquals("Failed to perform action on task since it's currently running.(task name: " + taskInstance.getTaskName() + ", instance id: " + taskInstance.getId() + ")",
            actualException.getMessage());
    }
}
