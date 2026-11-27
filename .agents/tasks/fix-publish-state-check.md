---
slug: fix-publish-state-check
branch: cursor/fix-publish-state-check-c41e
owner: cursor
status: in-progress
started: 2026-11-27
---

## Goal

The `Publish` workflow on `master` can complete its publication-state check
again. The run of 2026-10-29 failed before `:publicationState` reported
anything, because a cold `buildSrc` classpath resolution got `429` from
Maven Central.

## Context

- Run `40928054707`, job "Publish to Maven repositories". The Gradle task
  `:publicationState` never started. Configuration of `:buildSrc` failed
  resolving `kotlin-gradle-plugins-bom:2.4.10` with HTTP 429, and Gradle then
  disabled the Plugin Portal. The workflow's failure step prints the generic
  "publication state check failed" text for any failure of that step.
- Nothing was uploaded. `2.0.0-SNAPSHOT.131` is the last published version;
  `.132` was never published. Ubuntu CI on the same push was green because
  that job restores the Gradle cache.
- `publishing.yml` already passes longer retry properties to the upload
  invocation only. The state check is the cold resolution, and it does not.
- Sonatype's consumption-block guidance says not to poll through a long
  block. The limits below are the ones the upload step already uses: five
  attempts, then stop.
- Other Spine repositories on current `config` (`ca4febc0`, Gradle 9.9.2)
  publish green, including `core-jvm` on 2026-11-26. This repository was
  still pinned at `config@2f6f370`.

## Plan

- [x] `./config/pull` to `config@ca4febc0`. Local pins only moved up.
- [x] Give dependency resolution the same retry budget the upload step uses,
      via `gradle.properties`, so `:publicationState` survives a brief `429`.
- [x] Drop the Compiler CLI `.069` override. `config` now falls back to `.085`.
- [x] Bump `version.gradle.kts` once and refresh dependency reports.
- [x] Adapt tests to the instance `ServerEnvironment` that came with core-jvm `.781`.
- [x] Move to core-jvm `.801` (core-jvm#1742) and Base `.466`. The delivery smoke
      tests override `newStorageFactory()` with a new H2 factory per call, and the
      temporary `FactoryLeftOpen` wrapper is gone.
- [x] `./gradlew build dokkaGenerate` on JDK 17: 413 tests, 289 succeeded,
      0 failed, 124 skipped (no Docker). Gradle reports the retry properties
      as 5 attempts and a 5000 ms initial backoff.

## Log

- 2026-11-27 — the failed log is a Maven Central 429 during `:buildSrc`
  configuration, not a publication-state verdict.
