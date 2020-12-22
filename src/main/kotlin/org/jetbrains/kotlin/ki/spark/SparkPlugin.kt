package org.jetbrains.kotlin.ki.spark

import org.jetbrains.kotlin.ki.shell.*
import org.jetbrains.kotlin.ki.shell.configuration.ReplConfiguration

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.util.Utils
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.Serializable
import java.util.*
import kotlin.script.experimental.api.KotlinType
import kotlin.script.experimental.api.dependencies
import kotlin.script.experimental.api.implicitReceivers
import kotlin.script.experimental.jvm.JvmDependency
import kotlin.script.experimental.jvm.impl.KJvmCompiledModuleInMemory
import kotlin.script.experimental.jvm.impl.copyWithoutModule
import kotlin.script.experimental.jvm.impl.scriptMetadataPath
import kotlin.script.experimental.jvm.impl.toBytes
import kotlin.script.experimental.jvm.updateClasspath
import kotlin.script.experimental.jvm.util.scriptCompilationClasspathFromContext

class SparkPlugin : Logging(), Plugin {
    lateinit var version: String

    override fun init(repl: Shell, config: ReplConfiguration) {
        val jars = getAddedJars()
        val conf = SparkConf()
                .setMaster(getMaster())
                .setJars(jars.toTypedArray())
                .setAppName("Kotlin Spark Shell")

        val tmp = System.getProperty("java.io.tmpdir")
        val rootDir = conf.get("spark.repl.classdir", tmp)
        val outputDir = Utils.createTempDir(rootDir, "spark")
        Holder.sparkSessionDelegate = createSparkSession(conf, outputDir)
        Holder.sparkContextDelegate = JavaSparkContext.fromSparkContext(Holder.sparkSessionDelegate.sparkContext())
        val replJars = replJars(jars)

        repl.eventManager.registerEventHandler(OnCompile::class, object : EventHandler<OnCompile> {
            fun fullPath(path: String) = File(outputDir.absolutePath + File.separator + path)

            override fun handle(event: OnCompile) {

                with(event.data().get()) {
                    writeClass(fullPath(scriptMetadataPath(scriptClassFQName)), copyWithoutModule().toBytes())

                    val module = (getCompiledModule() as? KJvmCompiledModuleInMemory)
                            ?: throw IllegalArgumentException("Unsupported module type ${getCompiledModule()}")

                    for ((path, bytes) in module.compilerOutputFiles) {
                        writeClass(fullPath(path), bytes)
                    }
                }
            }
        })


        val dependenciesClasspath = JvmDependency(
                scriptCompilationClasspathFromContext(
                        wholeClasspath = true // DependsOn and Repository annotations are taken from it
                )
        )

        repl.updateCompilationConfiguration {
            implicitReceivers.append(KotlinType(Holder::class))
            dependencies.append(dependenciesClasspath)
            updateClasspath(replJars)
        }

        repl.updateEvaluationConfiguration {
            implicitReceivers.append(Holder)
        }
    }

    object Holder : Serializable {
        val spark: SparkSession by lazy { sparkSessionDelegate }
        val sc: JavaSparkContext by lazy { sparkContextDelegate }

        lateinit var sparkSessionDelegate: SparkSession
        lateinit var sparkContextDelegate: JavaSparkContext
    }

    private fun createSparkSession(conf: SparkConf, outputDir: File): SparkSession {
        val execUri = System.getenv("SPARK_EXECUTOR_URI")
        var sparkSession: SparkSession
        conf.setIfMissing("spark.app.name", "Spark shell")
        // SparkContext will detect this configuration and register it with the RpcEnv's
        // file server, setting spark.repl.class.uri to the actual URI for executors to
        // use. This is sort of ugly but since executors are started as part of SparkContext
        // initialization in certain cases, there's an initialization order issue that prevents
        // this from being set after SparkContext is instantiated.
        conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath())
        if (execUri != null) {
            conf.set("spark.executor.uri", execUri)
        }
        if (System.getenv("SPARK_HOME") != null) {
            conf.setSparkHome(System.getenv("SPARK_HOME"))
        }

        val builder = SparkSession.builder().config(conf)

        if (conf.get(CATALOG_IMPLEMENTATION().key(), "hive").toLowerCase(Locale.ROOT) == "hive") {
            if (hiveClassesArePresent()) {
                // In the case that the property is not set at all, builder's config
                // does not have this value set to 'hive' yet. The original default
                // behavior is that when there are hive classes, we use hive catalog.
                sparkSession = builder.enableHiveSupport().getOrCreate()
                logInfo("Created Spark session with Hive support")
            } else {
                // Need to change it back to 'in-memory' if no hive classes are found
                // in the case that the property is set to hive in spark-defaults.conf
                builder.config(CATALOG_IMPLEMENTATION().key(), "in-memory")
                sparkSession = builder.getOrCreate()
                logInfo("Created Spark session")
            }
        } else {
            // In the case that the property is set but not to 'hive', the internal
            // default is 'in-memory'. So the sparkSession will use in-memory catalog.
            sparkSession = builder.getOrCreate()
            logInfo("Created Spark session")
        }
        version = sparkSession.version()
        return sparkSession
    }

    override fun cleanUp() {}

    private fun getAddedJars(): List<String> {
        val jars = System.getProperty("spark.jars")
        return Utils.resolveURIs(jars).split(",").filter { s -> s.trim() != "" }
    }

    private fun getMaster(): String {
        val propMaster = System.getProperty("spark.master")
        return propMaster ?: System.getenv()["MASTER"] ?: "local[*]"
    }

    private fun writeClass(path: File, bytes: ByteArray) {
        if (!path.parentFile.exists()) path.parentFile.mkdirs()
        val out = BufferedOutputStream(FileOutputStream(path))
        out.write(bytes)
        out.flush()
        out.close()
    }

    private fun hiveClassesArePresent(): Boolean {
        return try {
            Utils.classForName<Class<*>>("org.apache.spark.sql.hive.HiveSessionStateBuilder", true, false)
            Utils.classForName<Class<*>>("org.apache.hadoop.hive.conf.HiveConf", true, false)
            true
        } catch (e: Exception) {
            when (e) {
                is ClassNotFoundException, is NoClassDefFoundError -> false
                else -> throw e
            }
        }
    }

    fun hadoopConfiguration() = Holder.sc.hadoopConfiguration()

    fun getSparkVersion(): String = version
}