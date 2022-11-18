package com.github.kagkarlsson.examples.kotlin

import com.github.kagkarlsson.scheduler.serializer.Serializer
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer
import java.nio.charset.StandardCharsets

class KotlinSerializer : Serializer {
    val CHARSET = StandardCharsets.UTF_8

    override fun serialize(data: Any): ByteArray {
        val serializer = serializer(data.javaClass)
        return Json.encodeToString(serializer, data).toByteArray(CHARSET);
    }

    override fun <T : Any?> deserialize(clazz: Class<T>, serializedData: ByteArray): T {
        // Hackish workaround?
        // https://github.com/Kotlin/kotlinx.serialization/issues/1134
        // https://stackoverflow.com/questions/64284767/replace-jackson-with-kotlinx-serialization-in-javalin-framework/64285478#64285478

        val deserializer = serializer(clazz) as KSerializer<T>
        return Json.decodeFromString(deserializer, String(serializedData, CHARSET))
    }
}
