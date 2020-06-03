package com.github.kagkarlsson.scheduler;

public enum PollingStrategy {
    FETCH_CANDIDATES_THEN_LOCK_USING_OPTIMISTIC_LOCKING,
    LOCK_CANDIDATE_USING_SELECT_FOR_UPDATE
}
