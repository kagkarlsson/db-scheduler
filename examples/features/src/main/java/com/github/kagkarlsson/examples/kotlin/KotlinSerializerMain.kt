package com.github.kagkarlsson.examples.kotlin

import com.github.kagkarlsson.examples.helpers.HsqlDatasource
import com.github.kagkarlsson.scheduler.Scheduler
import com.github.kagkarlsson.scheduler.task.ExecutionContext
import com.github.kagkarlsson.scheduler.task.TaskInstance
import com.github.kagkarlsson.scheduler.task.VoidExecutionHandler
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask
import com.github.kagkarlsson.scheduler.task.helper.Tasks
import kotlinx.serialization.Serializable
import java.time.Duration
import java.time.Instant

fun main() {
    val dataSource = HsqlDatasource.initDatabase();

    val myAdhocTask: OneTimeTask<JsonData> = Tasks.oneTime("json-task", JsonData::class.java)
            .execute { inst: TaskInstance<JsonData>, _: ExecutionContext? ->
                println("Executed! Custom data: " + inst.data)
            }

    val scheduler = Scheduler
            .create(dataSource, myAdhocTask)
            .serializer(KotlinSerializer())
            .pollingInterval(Duration.ofSeconds(1))
            .registerShutdownHook()
            .build()
    scheduler.start()

    scheduler.schedule(myAdhocTask.instance("id1", JsonData(1001L, Instant.now().toEpochMilli())), Instant.now().plusSeconds(1))
}

@Serializable
data class JsonData(val id: Long, val timeEpochMillis: Long) {

    override fun toString(): String {
        return "JsonData{" +
                "id=" + id +
                ", timeEpochMillis=" + timeEpochMillis +
                '}'
    }

}
