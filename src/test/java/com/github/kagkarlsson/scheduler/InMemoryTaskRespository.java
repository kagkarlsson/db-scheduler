package com.github.kagkarlsson.scheduler;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.Task;

public class InMemoryTaskRespository implements TaskRepository {
	private static final Logger LOG = LoggerFactory.getLogger(InMemoryTaskRespository.class);
	private final SchedulerName schedulerName;

	private final Set<Execution> futureExecutions = new HashSet<>();

	public InMemoryTaskRespository(final SchedulerName schedulerName) {
		this.schedulerName = schedulerName;
	}

	@Override
	public synchronized boolean createIfNotExists(final Execution execution) {
		LOG.debug("Adding execution {} if it does not exist.", execution);
		final String nameAndInstance = execution.taskInstance.getTaskAndInstance();
		final Optional<Execution> existing = futureExecutions.stream()
				.filter(e -> e.taskInstance.getTaskAndInstance().equals(nameAndInstance))
				.findAny();

		if (existing.isPresent()) {
			LOG.info("Recurring task with id {} already exists. Not scheduling duplicate.", nameAndInstance);
			return false;

		} else {
			futureExecutions.add(execution);
			LOG.debug("Added execution {}.", execution);
			return true;
		}
	}

	@Override
	public synchronized void remove(final Execution execution) {
		futureExecutions.remove(execution);
	}

	@Override
	public void markComplete(final Execution execution) {
		remove(execution);
	}

	@Override
	public synchronized void reschedule(final Execution execution, final LocalDateTime nextExecutionTime,
			final LocalDateTime lastSuccess, final LocalDateTime lastFailure) {
		futureExecutions.remove(execution);
		futureExecutions.add(new Execution(nextExecutionTime, execution.taskInstance));
	}

	@Override
	public Optional<Execution> pick(final Execution e, final LocalDateTime timePicked) {
		for (final Execution futureExecution : futureExecutions) {
			if (futureExecution.equals(e)) {
				futureExecution.setPicked(schedulerName.getName(), timePicked);
				return Optional.of(futureExecution);
			}
		}
		return Optional.empty();
	}

	@Override
	public List<Execution> getOldExecutions(final LocalDateTime olderThan) {
		final List<Execution> due = futureExecutions.stream()
				.filter(e -> e.executionTime.isBefore(olderThan) || e.executionTime.isEqual(olderThan))
				.filter(e -> e.picked)
				.collect(Collectors.toList());
		Collections.sort(due, Comparator.comparing(Execution::getExecutionTime));
		return due;
	}

	@Override
	public void updateHeartbeat(final Execution execution, final LocalDateTime heartbeatTime) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public List<Execution> getExecutionsFailingLongerThan(final Duration interval) {
		return new ArrayList<>();
	}

	@Override
	public synchronized List<Execution> getDue(final LocalDateTime now) {
		final List<Execution> due = futureExecutions.stream()
				.filter(e -> e.executionTime.isBefore(now) || e.executionTime.isEqual(now))
				.filter(e -> !e.picked)
				.collect(Collectors.toList());
		Collections.sort(due, Comparator.comparing(Execution::getExecutionTime));
		return due;
	}

	@Override
	public void registerTask(final Task task) {
		// NOOP -- not necessary
	}

}
