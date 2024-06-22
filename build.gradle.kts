buildscript {
    repositories {
        mavenCentral()
    }
}

plugins {
    id("java-library")
    id("distribution")
}

repositories {
    mavenCentral()
}

subprojects {
    // https://docs.gradle.org/current/userguide/java_library_plugin.html
    apply(plugin = "java")

    // https://docs.gradle.org/current/userguide/checkstyle_plugin.html
    apply(plugin = "checkstyle")

    // https://docs.gradle.org/current/userguide/jacoco_plugin.html
    apply(plugin = "jacoco")

    // https://docs.gradle.org/current/userguide/distribution_plugin.html
    apply(plugin = "distribution")

    apply(plugin = "idea")

    apply(plugin = "maven-publish")

    repositories {
        mavenCentral()
    }

    configure<CheckstyleExtension> {
        toolVersion = "10.12.0"
        configDirectory = rootProject.file("checkstyle/")
    }

    tasks {
        withType<JavaCompile> {
            sourceCompatibility = "21"
            targetCompatibility = "21"
        }

        val sourcesJar by creating(Jar::class) {
            archiveClassifier.set("sources")
            from(sourceSets.main.get().allSource)
        }

        val javadocJar by creating(Jar::class) {
            dependsOn.add(javadoc)
            archiveClassifier.set("javadoc")
            from(javadoc)
        }

        javadoc {
            options {
                (this as CoreJavadocOptions).addStringOption("Xdoclint:all,-missing", "-quiet")
            }
        }


        artifacts {
            archives(sourcesJar)
            archives(javadocJar)
            archives(jar)
        }

        distributions {
            main {
                contents {
                    from(jar)
                    from(sourcesJar)
                    from(configurations.runtimeClasspath)
                }
            }
        }
    }
}
