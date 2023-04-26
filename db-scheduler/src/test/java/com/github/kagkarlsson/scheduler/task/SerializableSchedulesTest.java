package com.github.kagkarlsson.scheduler.task;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.kagkarlsson.scheduler.task.schedule.CronSchedule;
import java.io.*;
import java.time.Instant;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;

public class SerializableSchedulesTest {

    private static final String CRON_PATTERN = "0 * * * * ?";
    private static final ZoneId NEW_YORK = ZoneId.of("America/New_York");

    @Test
    void cron_schedule() {
        CronSchedule s = new CronSchedule(CRON_PATTERN, NEW_YORK);
        CronSchedule deserialized = deserialize(serialize(s), CronSchedule.class);
        assertEquals(s.getPattern(), deserialized.getPattern());
        assertEquals(s.getZoneId(), deserialized.getZoneId());

        ExecutionComplete executionComplete = ExecutionComplete.simulatedSuccess(Instant.now());
        assertEquals(s.getNextExecutionTime(executionComplete), deserialized.getNextExecutionTime(executionComplete));
    }

    private static <T extends Serializable> byte[] serialize(T obj) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static <T extends Serializable> T deserialize(byte[] b, Class<T> cl) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(b);
                ObjectInputStream ois = new ObjectInputStream(bais)) {
            Object o = ois.readObject();
            return cl.cast(o);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
