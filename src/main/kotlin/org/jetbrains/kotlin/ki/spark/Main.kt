package org.jetbrains.kotlin.ki.spark

import org.jetbrains.kotlin.ki.shell.Shell
import org.jetbrains.kotlin.ki.shell.configuration.CachedInstance
import org.jetbrains.kotlin.ki.shell.configuration.PropertyBasedReplConfiguration
import org.jetbrains.kotlin.ki.shell.configuration.ReplConfiguration
import org.jetbrains.kotlin.ki.shell.plugins.*
import org.jetbrains.kotlin.ki.shell.replJars
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.util.*
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

//        repl.addClasspathRoots(replJars())
        Runtime.getRuntime().addShutdownHook(Thread {
            println("\nBye!")
            repl.cleanUp()
        })

        repl.doRun()
    }

    fun configuration(): ReplConfiguration {
        class MyConfigurationImpl
            : PropertyBasedReplConfiguration(
                Properties(),
                listOf(LoadFilePlugin::class.qualifiedName!!,
                        RuntimePlugin::class.qualifiedName!!,
                        HelpPlugin::class.qualifiedName!!,
                        PastePlugin::class.qualifiedName!!,
                        SyntaxPlugin::class.qualifiedName!!,
                        PromptPlugin::class.qualifiedName!!,
                        ConfigPlugin::class.qualifiedName!!,
                        DependenciesPlugin::class.qualifiedName!!,
                        ExecutionEnvironmentPlugin::class.qualifiedName!!,
                        SparkPlugin::class.qualifiedName!!
                )
        ) {
            override fun load() {
                val path = configPath()

                if (File(path).exists()) {
                    props.load(BufferedReader(FileReader(path)))
                }

                super.load()
            }

            private fun configPath() =
                    System.getProperty("config.path") ?:
                    System.getenv("KI_CONFIG") ?:
                    (System.getProperty("user.home") ?: "") + File.separator + ".ki-shell"
        }

        val instance = CachedInstance<ReplConfiguration>()
        val klassName: String? = System.getProperty("config.class")

        return if (klassName != null) {
            instance.load(klassName, ReplConfiguration::class)
        } else {
            instance.get { MyConfigurationImpl() }
        }
    }
}