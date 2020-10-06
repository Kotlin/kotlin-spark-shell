package org.jetbrains.kotlin.ki.spark

import org.jetbrains.kotlin.ki.shell.Shell
import org.jetbrains.kotlin.ki.shell.configuration.CachedInstance
import org.jetbrains.kotlin.ki.shell.configuration.ReplConfiguration
import org.jetbrains.kotlin.ki.shell.configuration.ReplConfigurationImpl
import org.jetbrains.kotlin.ki.shell.replJars
import kotlin.script.experimental.api.ScriptCompilationConfiguration
import kotlin.script.experimental.api.ScriptEvaluationConfiguration
import kotlin.script.experimental.api.baseClass
import kotlin.script.experimental.jvm.baseClassLoader
import kotlin.script.experimental.jvm.defaultJvmScriptingHostConfiguration
import kotlin.script.experimental.jvm.jvm
import kotlin.script.experimental.jvm.updateClasspath
import kotlin.script.experimental.jvm.util.scriptCompilationClasspathFromContext

abstract class SparkScriptBase : java.io.Serializable

object Main {
    @JvmStatic
    fun main(args: Array<String>) {
        val repl =
                Shell(
                        configuration(),
                        defaultJvmScriptingHostConfiguration,
                        ScriptCompilationConfiguration {
                            baseClass(SparkScriptBase::class)
                            jvm {
                                updateClasspath(replJars())
                                scriptCompilationClasspathFromContext(wholeClasspath = true)
                            }
                        },
                        ScriptEvaluationConfiguration {
                            jvm {
                                baseClassLoader(Shell::class.java.classLoader)
                            }
                        }
                )

        Runtime.getRuntime().addShutdownHook(Thread {
            println("\nBye!")
            repl.cleanUp()
        })

        repl.doRun()
    }

    fun configuration(): ReplConfiguration {
        val instance = CachedInstance<ReplConfiguration>()
        val klassName: String? = System.getProperty("config.class")

        return if (klassName != null) {
            instance.load(klassName, ReplConfiguration::class)
        } else {
            instance.get { ReplConfigurationImpl(extraPlugins = listOf(SparkPlugin::class.qualifiedName!!)) }
        }
    }
}