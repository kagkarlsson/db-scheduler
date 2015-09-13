package com.github.kagkarlsson.scheduler;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface TaskRepository {

	boolean createIfNotExists(Execution execution);
	List<Execution> getDue(LocalDateTime now);

	void remove(Execution execution);
	void reschedule(Execution execution, LocalDateTime nextExecutionTime);

	Optional<Execution> pick(Execution e, LocalDateTime timePicked);

	List<Execution> getOldExecutions(LocalDateTime olderThan);

	void updateHeartbeat(Execution execution, LocalDateTime heartbeatTime);
}
