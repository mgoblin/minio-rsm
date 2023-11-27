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

//    java.sourceCompatibility = JavaVersion.VERSION_21
//    java.targetCompatibility = JavaVersion.VERSION_21

    configure<CheckstyleExtension> {
        toolVersion = "10.12.0"
        configDirectory = rootProject.file("checkstyle/")
    }

//    val integrationTest = sourceSets.create("integrationTest")
//    integrationTest.compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
//    integrationTest.runtimeClasspath += integrationTest.output + integrationTest.compileClasspath
//
//    sourceSets {
//        integrationTest.java {
//            srcDirs("src/integration-test/java")
//        }
//        integrationTest.resources {
//            srcDirs("src/integration-test/resources")
//        }
//    }
//
//    }
//
//    configure<IdeaModel> {
//        module {
//            testSources.plus(integrationTest.java.srcDirs)
//            testSources.plus(integrationTest.resources.srcDirs)
//        }
//    }

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
