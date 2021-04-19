package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.exceptions.TaskInstanceCurrentlyRunningException;
import com.github.kagkarlsson.scheduler.exceptions.TaskInstanceNotFoundException;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Optional;

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
        StandardTaskInstanceId taskId = new StandardTaskInstanceId(randomAlphanumeric(10), randomAlphanumeric(10));
        when(taskRepository.getExecution(taskId.getTaskName(), taskId.getId())).thenReturn(Optional.empty());

        TaskInstanceNotFoundException actualException = assertThrows(TaskInstanceNotFoundException.class, () -> {
            schedulerClient.reschedule(taskId, Instant.now(), null);
        });
        assertEquals("Could not reschedule - no task with name '" + taskId.getTaskName() + "' and id '" + taskId.getId() + "' was found.", actualException.getMessage());
    }

    @Test
    public void failsToRescheduleWhenATaskIsPickedAndExecuting() {
        StandardTaskInstanceId taskId = new StandardTaskInstanceId(randomAlphanumeric(10), randomAlphanumeric(10));
        Execution expectedExecution = new Execution(
            Instant.now(),
            new TaskInstance(taskId.getTaskName(), taskId.getId()),
            true,
            randomAlphanumeric(5),
            null,
            null,
            0,
            null,
            1
        );

        when(taskRepository.getExecution(taskId.getTaskName(), taskId.getId())).thenReturn(Optional.of(expectedExecution));

        TaskInstanceCurrentlyRunningException actualException = assertThrows(TaskInstanceCurrentlyRunningException.class, () -> {
            schedulerClient.reschedule(taskId, Instant.now(), null);
        });
        assertEquals("Could not reschedule, the execution with name '" + taskId.getTaskName() + "' and id '" + taskId.getId() + "' is currently executing", actualException.getMessage());
    }

    @Test
    public void failsToCancelWhenTaskIsNotFound() {
        StandardTaskInstanceId taskId = new StandardTaskInstanceId(randomAlphanumeric(10), randomAlphanumeric(10));
        when(taskRepository.getExecution(taskId.getTaskName(), taskId.getId())).thenReturn(Optional.empty());

        TaskInstanceNotFoundException actualException = assertThrows(TaskInstanceNotFoundException.class, () -> {
            schedulerClient.cancel(taskId);
        });
        assertEquals("Could not cancel schedule - no task with name '" + taskId.getTaskName() + "' and id '" + taskId.getId() + "' was found.", actualException.getMessage());
    }

    @Test
    public void failsToCancelWhenATaskIsPickedAndExecuting() {
        StandardTaskInstanceId taskId = new StandardTaskInstanceId(randomAlphanumeric(10), randomAlphanumeric(10));
        Execution expectedExecution = new Execution(
            Instant.now(),
            new TaskInstance(taskId.getTaskName(), taskId.getId()),
            true,
            randomAlphanumeric(5),
            null,
            null,
            0,
            null,
            1
        );

        when(taskRepository.getExecution(taskId.getTaskName(), taskId.getId())).thenReturn(Optional.of(expectedExecution));

        TaskInstanceCurrentlyRunningException actualException = assertThrows(TaskInstanceCurrentlyRunningException.class, () -> {
            schedulerClient.cancel(taskId);
        });
        assertEquals("Could not cancel schedule, the execution with name '" + taskId.getTaskName() + "' and id '" + taskId.getId() + "' is currently executing", actualException.getMessage());
    }
}
