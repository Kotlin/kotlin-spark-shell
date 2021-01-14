package org.jetbrains.kotlinx.ki.spark

import org.jetbrains.kotlinx.ki.shell.Shell
import org.jetbrains.kotlinx.ki.shell.configuration.CachedInstance
import org.jetbrains.kotlinx.ki.shell.configuration.ReplConfiguration
import org.jetbrains.kotlinx.ki.shell.configuration.ReplConfigurationBase
import org.jetbrains.kotlinx.ki.shell.plugins.DependenciesPlugin
import org.jetbrains.kotlinx.ki.shell.replJars
import kotlin.script.experimental.api.ScriptCompilationConfiguration
import kotlin.script.experimental.api.ScriptEvaluationConfiguration
import kotlin.script.experimental.api.baseClass
import kotlin.script.experimental.jvm.baseClassLoader
import kotlin.script.experimental.jvm.defaultJvmScriptingHostConfiguration
import kotlin.script.experimental.jvm.jvm
import kotlin.script.experimental.jvm.updateClasspath
import kotlin.script.experimental.jvm.util.scriptCompilationClasspathFromContext

abstract class SparkScriptBase : java.io.Serializable

object SparkShell {
    @JvmStatic
    fun main(args: Array<String>) {
        val repl = Shell(
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

    private fun configuration(): ReplConfiguration {
        val instance = CachedInstance<ReplConfiguration>()
        val klassName: String? = System.getProperty("config.class")

        return if (klassName != null) {
            instance.load(klassName, ReplConfiguration::class)
        } else {
            val plugins = ReplConfigurationBase.DEFAULT_PLUGINS
                .filter { it != DependenciesPlugin::class.qualifiedName!! } +
                    listOf(SparkPlugin::class.qualifiedName!!)

            instance.get { object : ReplConfigurationBase(plugins) {} }
        }
    }
}