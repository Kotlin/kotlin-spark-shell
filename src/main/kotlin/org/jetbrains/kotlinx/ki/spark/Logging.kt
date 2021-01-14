package org.jetbrains.kotlinx.ki.spark

import org.slf4j.Logger
import org.slf4j.LoggerFactory

open class Logging: org.apache.spark.internal.Logging {
    override fun `org$apache$spark$internal$Logging$$log_`(): Logger {
        return LoggerFactory.getLogger(logName())
    }

    override fun `org$apache$spark$internal$Logging$$log__$eq`(`x$1`: Logger?) {
        TODO("Not yet implemented")
    }

    fun logInfo(message: String) {
        log().info(message)
    }
}