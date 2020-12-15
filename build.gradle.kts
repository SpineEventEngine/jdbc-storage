/*
 * Copyright 2020, TeamDev. All rights reserved.
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

import io.spine.gradle.internal.DependencyResolution
import io.spine.gradle.internal.Deps

buildscript {

    apply(from = "$rootDir/version.gradle.kts")

    @Suppress("RemoveRedundantQualifierName") // Cannot use imports here.
    val dependencyResolution = io.spine.gradle.internal.DependencyResolution
    val spineBaseVersion: String by extra
    dependencyResolution.defaultRepositories(repositories)
    dependencies {
        classpath("io.spine.tools:spine-model-compiler:$spineBaseVersion")
    }
}

plugins {
    `java-library`
    idea
    jacoco
    @Suppress("RemoveRedundantQualifierName") // Cannot use imports here.
    id("com.google.protobuf").version(io.spine.gradle.internal.Deps.versions.protobufPlugin)
    @Suppress("RemoveRedundantQualifierName") // Cannot use imports here.
    id("net.ltgt.errorprone").version(io.spine.gradle.internal.Deps.versions.errorPronePlugin)
}

val credentialsPropertyFile: String by extra("credentials.properties")
val projectsToPublish: List<String> by extra(listOf("rdbms"))

allprojects {
    apply {
        from("$rootDir/version.gradle.kts")
        from("$rootDir/config/gradle/dependencies.gradle")
    }

    group = "io.spine"
    version = extra["versionToPublish"]!!
}

subprojects {
    apply {
        plugin("java-library")
        plugin("net.ltgt.errorprone")
        plugin("pmd")
        plugin("jacoco")
        plugin("maven-publish")
        from(Deps.scripts.javadocOptions(project))
        from(Deps.scripts.projectLicenseReport(project))
        from(Deps.scripts.testOutput(project))
        from(Deps.scripts.javacArgs(project))
        from(Deps.scripts.pmd(project))
    }

    java {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }

    DependencyResolution.defaultRepositories(repositories)
    DependencyResolution.forceConfiguration(configurations)
    DependencyResolution.excludeProtobufLite(configurations)

    val spineBaseVersion: String by extra
    val spineCoreVersion: String by extra

    dependencies {
        errorprone(Deps.build.errorProneCore)
        errorproneJavac(Deps.build.errorProneJavac)

        implementation("io.spine:spine-server:$spineCoreVersion")
        implementation(Deps.build.guava)
        compileOnlyApi(Deps.build.jsr305Annotations)
        compileOnlyApi(Deps.build.checkerAnnotations)
        Deps.build.errorProneAnnotations.forEach { compileOnlyApi(it) }

        testImplementation(Deps.test.guavaTestlib)
        Deps.test.junit5Api.forEach { testImplementation(it) }
        Deps.test.truth.forEach { testImplementation(it) }
        testImplementation("io.spine:spine-testutil-server:$spineCoreVersion")
        testImplementation(group = "io.spine",
                name = "spine-server",
                version = spineCoreVersion,
                classifier = "test")
        testImplementation("io.spine:spine-testlib:$spineBaseVersion")
        testImplementation("io.spine.tools:spine-mute-logging:$spineBaseVersion")
        testRuntimeOnly(Deps.test.junit5Runner)
    }

    tasks.test {
        useJUnitPlatform {
            includeEngines("junit-jupiter")
        }
    }

    tasks.jacocoTestReport {
        reports {
            xml.isEnabled = true
            html.isEnabled = true
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

    tasks.check {
        dependsOn(tasks.jacocoTestReport)
    }
}

apply {
    from(Deps.scripts.jacoco(project))
    from(Deps.scripts.publish(project))
    from(Deps.scripts.repoLicenseReport(project))
    from(Deps.scripts.generatePom(project))
}
