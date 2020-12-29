# Kotlin Language Support for Apache Spark

The project is a Kotlin Shell for Apache Spark.
It's based on [ki-shell](https://github.com/Kotlin/kotlin-interactive-shell)
and provide the same user experience, including syntax highlighting, a type inference,
and some level of completion.

Kotlin API [extension](https://github.com/JetBrains/kotlin-spark-api) will also work
in the shell.

## Build From Source

To build from source use:
```bash
git clone https://github.com/Kotlin/kotlin-spark-shell
cd kotlin-spark-shell
mvn package
```
It may be useful to build the project with a specific version of Kotlin or Scala. 
To do so use:
```bash
mvn -Dkotlin.version=1.4.10 -Dscala.version=2.11
```

