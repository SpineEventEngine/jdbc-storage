/*
 * Copyright 2021, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Redistribution and use in source and/or binary forms, with or without
 * modification, must retain the above copyright notice and the following
 * disclaimer.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

import io.spine.internal.dependency.ErrorProne
import io.spine.internal.dependency.JUnit
import io.spine.internal.gradle.VersionWriter
import io.spine.internal.gradle.applyStandard
import io.spine.internal.gradle.checkstyle.CheckStyleConfig
import io.spine.internal.gradle.forceVersions
import io.spine.internal.gradle.github.pages.updateGitHubPages
import io.spine.internal.gradle.javac.configureErrorProne
import io.spine.internal.gradle.javac.configureJavac
import io.spine.internal.gradle.javadoc.JavadocConfig
import io.spine.internal.gradle.kotlin.applyJvmToolchain
import io.spine.internal.gradle.kotlin.setFreeCompilerArgs
import io.spine.internal.gradle.publish.IncrementGuard
import io.spine.internal.gradle.publish.Publish.Companion.publishProtoArtifact
import io.spine.internal.gradle.publish.PublishingRepos
import io.spine.internal.gradle.publish.spinePublishing
import io.spine.internal.gradle.report.coverage.JacocoConfig
import io.spine.internal.gradle.report.license.LicenseReporter
import io.spine.internal.gradle.report.pom.PomGenerator
import io.spine.internal.gradle.testing.configureLogging
import io.spine.internal.gradle.testing.registerTestTasks
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile


@Suppress("RemoveRedundantQualifierName") // Cannot use imported things here.
buildscript {
    apply(from = "$rootDir/version.gradle.kts")
    io.spine.internal.gradle.doApplyStandard(repositories)
    io.spine.internal.gradle.doForceVersions(configurations)

    val mcJavaVersion: String by extra
    val spineBaseVersion: String by extra

    dependencies {
        classpath("io.spine.tools:spine-mc-java:$mcJavaVersion")
    }

    configurations.all {
        resolutionStrategy {
            force(
                io.spine.internal.dependency.Kotlin.stdLib,
                io.spine.internal.dependency.Kotlin.stdLibCommon,
                "io.spine:spine-base:$spineBaseVersion"
            )
        }
    }
}

plugins {
    `java-library`
    kotlin("jvm")
    idea
    id(io.spine.internal.dependency.Protobuf.GradlePlugin.id)
    id(io.spine.internal.dependency.ErrorProne.GradlePlugin.id)

}

val credentialsPropertyFile: String by extra("credentials.properties")
val projectsToPublish: List<String> by extra(listOf("rdbms"))

spinePublishing {
    targetRepositories.addAll(setOf(
        PublishingRepos.cloudRepo
    ))
    projectsToPublish.add("rdbms")
}

allprojects {
    apply(from = "$rootDir/version.gradle.kts")

    apply {
        plugin("jacoco")
        plugin("idea")
    }

    group = "io.spine.gcloud"
    version = extra["versionToPublish"]!!

    repositories.applyStandard()
}

subprojects {
    apply {
        plugin("java-library")
        plugin("com.google.protobuf")
        plugin("net.ltgt.errorprone")
        plugin("io.spine.mc-java")
        plugin("kotlin")
        plugin("pmd")
        plugin("maven-publish")
    }

    tasks.withType<JavaCompile> {
        configureJavac()
        configureErrorProne()
    }

    @Suppress("MagicNumber")
    val javaVersion = 11
    kotlin {
        applyJvmToolchain(javaVersion)
        explicitApi()
    }

    tasks.withType<KotlinCompile>().configureEach {
        kotlinOptions.jvmTarget = JavaVersion.VERSION_11.toString()
        setFreeCompilerArgs()
    }

    val spineBaseVersion: String by extra
    val spineCoreVersion: String by extra


    configurations.forceVersions()
    configurations {
        all {
            resolutionStrategy {
                force(
                    io.spine.internal.dependency.Grpc.stub,
                    io.spine.internal.dependency.Grpc.api,

                    "io.spine:spine-base:$spineBaseVersion"
                )
            }
        }
    }

    dependencies {

        ErrorProne.apply {
            errorprone(core)
        }

        implementation("io.spine:spine-server:$spineCoreVersion")

        testImplementation(JUnit.runner)
        testImplementation("io.spine.tools:spine-testutil-server:$spineCoreVersion")
        testImplementation(group = "io.spine",
            name = "spine-server",
            version = spineCoreVersion,
            classifier = "test")

        testImplementation("io.spine.tools:spine-testlib:$spineBaseVersion")
    }

    tasks {
        registerTestTasks()
        test {
            useJUnitPlatform {
                includeEngines("junit-jupiter")
            }
            configureLogging()
        }
    }

    tasks.register("sourceJar", Jar::class) {
        from(sourceSets.main.get().allJava)
        archiveClassifier.set("sources")
    }

    tasks.register("testOutputJar", Jar::class) {
        from(sourceSets.test.get().output)
        archiveClassifier.set("test")
    }

    tasks.register("javadocJar", Jar::class) {
        from("$projectDir/build/docs/javadoc")
        archiveClassifier.set("javadoc")
        dependsOn(tasks.javadoc)
    }

    val sourcesRootDir = "$projectDir/src"
    val generatedRootDir = "$projectDir/generated"
    val generatedJavaDir = "$generatedRootDir/main/java"
    val generatedTestJavaDir = "$generatedRootDir/test/java"
    val generatedGrpcDir = "$generatedRootDir/main/grpc"
    val generatedTestGrpcDir = "$generatedRootDir/test/grpc"
    val generatedSpineDir = "$generatedRootDir/main/spine"
    val generatedTestSpineDir = "$generatedRootDir/test/spine"

    sourceSets {
        main {
            //TODO:2022-01-26:alex.tymchenko: remove this eventually.
            // java.srcDirs(generatedJavaDir, "$sourcesRootDir/main/java", generatedSpineDir)
            resources.srcDir("$generatedRootDir/main/resources")
            proto.srcDirs("$sourcesRootDir/main/proto")
        }
        test {
            java.srcDirs(generatedTestJavaDir, "$sourcesRootDir/test/java", generatedTestSpineDir)
            resources.srcDir("$generatedRootDir/test/resources")
            proto.srcDir("$sourcesRootDir/test/proto")
        }
    }

    // Apply the same IDEA module configuration for each of sub-projects.
    idea {
        module {
            generatedSourceDirs.addAll(files(
                generatedJavaDir,
                generatedGrpcDir,
                generatedSpineDir,
                generatedTestJavaDir,
                generatedTestGrpcDir,
                generatedTestSpineDir
            ))
            testSourceDirs.add(file(generatedTestJavaDir))

            isDownloadJavadoc = true
            isDownloadSources = true
        }
    }

    apply<IncrementGuard>()
    apply<VersionWriter>()
    publishProtoArtifact(project)
    LicenseReporter.generateReportIn(project)
    JavadocConfig.applyTo(project)
    CheckStyleConfig.applyTo(project)

    updateGitHubPages(project.version.toString()) {
        allowInternalJavadoc.set(true)
        rootFolder.set(rootDir)
    }
    project.tasks["publish"].dependsOn("${project.path}:updateGitHubPages")
}


JacocoConfig.applyTo(project)
PomGenerator.applyTo(project)
LicenseReporter.mergeAllReports(project)
