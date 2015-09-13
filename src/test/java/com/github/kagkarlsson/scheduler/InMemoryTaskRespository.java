package com.github.kagkarlsson.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class InMemoryTaskRespository implements TaskRepository {
	private static final Logger LOG = LoggerFactory.getLogger(InMemoryTaskRespository.class);
	private final SchedulerName schedulerName;

	private final Set<Execution> futureExecutions = new HashSet<>();

	public InMemoryTaskRespository(SchedulerName schedulerName) {
		this.schedulerName = schedulerName;
	}

	@Override
	public synchronized boolean createIfNotExists(Execution execution) {
		LOG.debug("Adding execution {} if it does not exist.", execution);
		String nameAndInstance = execution.taskInstance.getTaskAndInstance();
		Optional<Execution> existing = futureExecutions.stream()
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
	public synchronized void remove(Execution execution) {
		futureExecutions.remove(execution);
	}

	@Override
	public synchronized void reschedule(Execution execution, LocalDateTime nextExecutionTime) {
		futureExecutions.remove(execution);
		futureExecutions.add(new Execution(nextExecutionTime, execution.taskInstance));
	}

	@Override
	public Optional<Execution> pick(Execution e, LocalDateTime timePicked) {
		for (Execution futureExecution : futureExecutions) {
			if (futureExecution.equals(e)) {
				futureExecution.setPicked(schedulerName.getName(), timePicked);
				return Optional.of(futureExecution);
			}
		}
		return Optional.empty();
	}

	@Override
	public List<Execution> getOldExecutions(LocalDateTime olderThan) {
		List<Execution> due = futureExecutions.stream()
				.filter(e -> e.executionTime.isBefore(olderThan) || e.executionTime.isEqual(olderThan))
				.filter(e -> e.picked)
				.collect(Collectors.toList());
		Collections.sort(due, Comparator.comparing(Execution::getExecutionTime));
		return due;
	}

	@Override
	public void updateHeartbeat(Execution execution, LocalDateTime heartbeatTime) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public synchronized List<Execution> getDue(LocalDateTime now) {
		List<Execution> due = futureExecutions.stream()
				.filter(e -> e.executionTime.isBefore(now) || e.executionTime.isEqual(now))
				.filter(e -> !e.picked)
				.collect(Collectors.toList());
		Collections.sort(due, Comparator.comparing(Execution::getExecutionTime));
		return due;
	}
}
