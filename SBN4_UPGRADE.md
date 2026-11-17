# Spring Boot 4 Migration — Decisions and Rationale

This document records **why** the Spring Boot 3 → 4 migration looks the way it does. It is not a
status report or a task list.

Almost every non-obvious choice below traces back to one decision: **Genie stayed on Jackson 2.**
That decision was correct for this migration and is explained in §1. It is also **temporary and on
a clock** — the Spring Boot shim that makes it possible shipped deprecated and will be removed in a
future 4.x release. §2 is the inventory of everything that has to be unwound when that happens.

> **Read §5 first if you are planning work.** The Jackson 3 migration is the single highest-priority
> follow-up to this upgrade, and it has two hard blockers that need lead time to resolve.

---

## 1. The core decision: stay on Jackson 2 for this migration

### 1.1 What Boot 4 changed

Spring Boot 4 moves to **Jackson 3** by default. Jackson 3 is not a version bump — it is a new
artifact group (`tools.jackson.*`) with a new package root, so Jackson 2 (`com.fasterxml.jackson.*`)
and Jackson 3 coexist on the classpath without conflicting. Boot 4 simply *prefers* Jackson 3 for
its HTTP message converters.

Boot 4 ships `spring-boot-jackson2` as a compatibility shim to let applications keep Jackson 2
while they migrate.

### 1.2 Why Genie could not just move to Jackson 3

Jackson 3 relocated its annotations. Jackson 2's `com.fasterxml.jackson.databind.annotation.*`
annotations are **silently ignored** by a Jackson 3 mapper — there is no error, the annotation is
simply not seen.

Genie depends on exactly that mechanism. **24 DTO classes** across `genie-common` and
`genie-common-external` are immutable and deserialize through Lombok-style builders declared with
Jackson 2's `@JsonDeserialize(builder = …)`:

```
AgentJobRequest, ApiJobRequest, Application, ApplicationMetadata, ApplicationRequest,
Cluster, ClusterMetadata, ClusterRequest, Command, CommandMetadata, CommandRequest,
ContainerImage, Criterion (×2), Image, Job, JobEnvironment, JobEnvironmentRequest,
JobExecution, JobMetadata (×2), JobRequest, Runtime, RuntimeResources
```

This was measured, not assumed. Pointing the HTTP converters at Jackson 3 makes **every** write
endpoint fail:

```
tools.jackson.databind.exc.InvalidDefinitionException: Cannot construct instance of
`com.netflix.genie.common.dto.Application` (no Creators, like default constructor, exist):
cannot deserialize from Object value (no delegate- or property-based Creator)
```

All 21 `ApplicationRestControllerIntegrationTest` tests fail this way. Migrating properly means
touching ~223 Jackson imports across 7 modules, re-testing the full serialization surface, and
clearing the blockers in §5.2 — a separate piece of work with its own risk profile, not something
to fold into a framework upgrade.

**Decision: keep Jackson 2 for the HTTP layer; make Jackson 3 its own migration.**

### 1.3 The bug this decision also fixed

`genie-common` and `genie-common-internal` put `@JsonFilter(GenieObjectMapper.EXCEPTIONS_FILTER_NAME)`
on three public exception types:

- `genie-common/…/exceptions/GenieException.java:30`
- `genie-common-internal/…/unchecked/GenieRuntimeException.java:29`
- `genie-common-internal/…/checked/GenieCheckedException.java:29`

`GenieExceptionMapper` returns those objects **as the HTTP response body**. A `@JsonFilter` requires
a matching `FilterProvider` on the serializing mapper; Genie registers one only on the Jackson 2
mapper (§2.3). Under a stock Jackson 3 mapper the same path throws:

```
tools.jackson.databind.exc.InvalidDefinitionException:
  Cannot resolve PropertyFilter with id 'GenieExceptionsJSONFilter'; no FilterProvider configured
```

That is **every Genie REST error response returning 500**. Letting Boot 4 default to Jackson 3
without migrating would have shipped that regression. Note the underlying design smell independent
of Jackson version: a library placing `@JsonFilter` on *public exception types* imposes an
unsatisfiable-by-default Jackson requirement on every consumer.

---

## 2. What was added or pinned to keep Jackson 2 working

Everything in this section exists **only** to support the §1 decision. All of it should be deleted
or reversed by the Jackson 3 migration. This is the unwind checklist.

### 2.1 Configuration

| Location | Setting | Why |
|---|---|---|
| `genie-web/src/main/resources/genie-web-defaults.yml:175` | `spring.http.converters.preferred-json-mapper: jackson2` | Boot 4 defaults the HTTP message converters to Jackson 3. This forces Jackson 2. |
| `genie-test-web/src/main/resources/application.yml:131` | same | **Must be kept in sync.** `genie-web-defaults.yml` is loaded by `GenieDefaultPropertiesPostProcessor` as *defaults* (lowest precedence); this file overrides it on the test classpath. Changing only one silently has no effect. |
| `genie-web-defaults.yml:178`, `application.yml:133` | `spring.jackson.*` → **`spring.jackson2.*`** | Not cosmetic and not reversible on its own. Jackson 3's autoconfiguration binds `spring.jackson.*` to Jackson 3 types whenever Jackson 3 is on the classpath — regardless of converter preference. Leaving the keys under `spring.jackson.*` produces a hard startup failure: `No enum constant tools.jackson.databind.SerializationFeature.write-dates-as-timestamps`. |

The relocated keys are `mapper.accept-case-insensitive-enums`, `serialization.write-dates-as-timestamps`
and `time-zone`.

### 2.2 Dependencies

| Coordinate | Where | Why |
|---|---|---|
| `org.springframework.boot:spring-boot-jackson2` | `genie-web/build.gradle:105` | The deprecated Boot 4 shim. Supplies the Jackson 2 `ObjectMapper` autoconfiguration and the `spring.jackson2.*` binding. **This is the artifact scheduled for removal** (§4). |
| **`org.springframework.hateoas:spring-hateoas:2.4.1`** | `build.gradle:165` | **A deliberate downgrade — see §3.** The Boot 4 BOM selects hateoas 3.x, which is Jackson 3–only. |

### 2.3 Code

| Location | What | Why |
|---|---|---|
| `genie-web/…/apis/ApisAutoConfiguration.java:188` | `Jackson2ObjectMapperBuilderCustomizer apisObjectMapperCustomizer()` | Registers `GenieObjectMapper.FILTER_PROVIDER` on the Jackson 2 mapper that serves HTTP. This is what keeps §1.3 from happening. It is **load-bearing**, not vestigial. Note the import is already the Boot 4 path: `org.springframework.boot.jackson2.autoconfigure.Jackson2ObjectMapperBuilderCustomizer`. |
| `genie-common-external/…/util/GenieObjectMapper.java` | The hand-built Jackson 2 `ObjectMapper` singleton | Used throughout the codebase independently of Spring. Unaffected by converter preference, so it also pins the internal code to Jackson 2. |

In Jackson 3 the customizer type becomes `JsonMapperBuilderCustomizer`
(`org.springframework.boot.jackson.autoconfigure`, taking a `JsonMapper.Builder`).

---

## 3. Dependencies pinned back because they are Jackson 3–only

This is the category the next migration most needs to know about: libraries whose current versions
have **dropped Jackson 2 entirely**, forcing Genie onto older releases.

### 3.1 spring-hateoas → pinned to 2.4.1 (BOM wants 3.1.3)

**spring-hateoas 3.x is Jackson 3–only.** Measured on 3.1.3: 925 references to `tools.jackson`
versus 9 to `com.fasterxml` — and all 9 of those are *annotations*, which Jackson 3 deliberately
keeps on the 2.x coordinates. There is no Jackson 2 code path in it at all:

```
HalJacksonModule extends tools.jackson.databind.module.SimpleModule
```

A Jackson 2 `ObjectMapper` cannot register a Jackson 3 module — incompatible type hierarchies. So
with `preferred-json-mapper: jackson2`, HATEOAS's HAL support is configured onto a mapper that never
serves HTTP.

**The failure mode is silent and severe.** Nothing errors. Responses simply serialize the raw
`RepresentationModel` / `CollectionModel` bean properties:

```jsonc
// what Boot 4 + hateoas 3.x + Jackson 2 converters produce
"links":   [ { "rel": "self", "href": "…" } ],
"content": [ … ]

// what the v3 API contract requires
"_links":    { "self": { "href": "…" } },
"_embedded": { "clusterList": [ … ] }
```

That is a breaking change to every HAL response in the public v3 API, and it only surfaced in
integration tests that assert on `_links`/`_embedded`.

**Resolution:** pin `spring-hateoas:2.4.1`, the Jackson 2 line (854 `com.fasterxml` references, zero
`tools.jackson`), which still ships `Jackson2HalModule`. Compatibility with its two Boot 4 consumers
was verified at the member level before adopting it:

- `spring-boot-hateoas:4.1.2` calls exactly **two** hateoas members — `HalConfiguration.<init>()`
  and `HalConfiguration.withMediaType(MediaType)`. Both present in 2.4.1, identical signatures.
- `springdoc 3.2.0` calls exactly **five** — `LinkRelation.value()`,
  `RepresentationModel.<init>(Iterable)` / `equals` / `hashCode`, and
  `LinkRelationProvider.getCollectionResourceRelFor(Class)`. All present in 2.4.1. (springdoc 3.2.0
  dropped the `Jackson2HalModule` reference that springdoc 2.8.x carried.)

**Accepted risk:** 2.4.1 is compiled against Spring Framework 6 and runs here on Spring 7. It is
green across the full suite, but it is an out-of-BOM pin that the Boot 4 BOM will keep trying to
drag forward. **Delete this pin as part of the Jackson 3 migration** — it is the single clearest
signal that the migration is overdue.

### 3.2 The reverse case: libraries that block Jackson 3

Two dependencies are Jackson 2–bound and must be dealt with *before* Jackson 3 is possible. These
are the hard blockers:

| Coordinate | Where | Problem |
|---|---|---|
| `com.github.fge:json-patch:1.9` | pinned `build.gradle:132` | Jackson 2–only and effectively unmaintained. Used in **production** REST controllers: `ApplicationRestController`, `ClusterRestController`, `CommandRestController`, and across `genie-client` (`ApplicationClient`, `ClusterClient`, `CommandClient` + their `*Service` interfaces). Needs a replacement or a fork. |
| `com.squareup.retrofit2:converter-jackson` | `build.gradle:137` | Jackson 2–bound; used by `genie-client`. |

---

## 4. Why this is urgent, not optional

`spring-boot-jackson2` **shipped deprecated**. Verified directly in `spring-boot-jackson2:4.1.2`:
26 `forRemoval` markers, and every one of the ~30 `spring.jackson2.*` properties carries:

```json
"deprecation": { "reason": "Deprecated in favor of Jackson 3", "since": "4.0.0" }
```

It exists solely to buy migration time. Spring's stated intent is removal in a future 4.x release;
the originating issue describes the deprecated code as aligned with Spring Framework's deprecated
Jackson 2 support "so that it can be removed after 12 months." Boot 4.0 shipped in November 2025,
which puts removal in roughly the **4.2 / 4.3** timeframe.

> **Accuracy note:** no Spring source I could find names a specific removal version. Treat 4.3 as a
> planning assumption, not a published commitment — but treat the ~12-month window as real.

When that module is removed:

- `spring.http.converters.preferred-json-mapper: jackson2` stops having a Jackson 2 option.
- The entire `spring.jackson2.*` namespace disappears.
- `Jackson2ObjectMapperBuilderCustomizer` disappears, taking the §1.3 `@JsonFilter` fix with it.
- Genie is pinned to a Boot 4 version that no longer receives fixes.

**The practical consequence: Genie cannot take a Boot 4 minor upgrade past that point without
having migrated to Jackson 3 first.** Every item in §2 and §3.1 is deferred work with a deadline
attached, and the §3.2 blockers — particularly replacing an unmaintained `json-patch` used in
production controllers — need lead time.

One option worth evaluating that is *not* the full migration: `spring.jackson.use-jackson2-defaults=true`
makes the auto-configured Jackson 3 `JsonMapper` mimic Jackson 2's defaults. It does **not** help
here on its own — it does nothing for the relocated `@JsonDeserialize` annotations (§1.2), the
hand-built `GenieObjectMapper`, or hateoas (§3.1) — but it meaningfully reduces the behavioural-change
surface in §5.1 once the annotations are ported.

---

## 5. What the Jackson 3 migration involves

### 5.1 Scope and the silent behaviour changes

~223 Jackson imports across 7 modules:

| Module | Imports | Annotation-only (no change needed) |
|---|---|---|
| genie-common-internal | 67 | 40 |
| genie-web | 63 | 1 |
| genie-common | 54 | 19 |
| genie-client | 15 | 1 |
| genie-common-external | 12 | 1 |
| genie-agent | 9 | 0 |
| genie-ui | 3 | 0 |

Jackson 3 keeps the *annotations* artifact on the 2.x coordinates (`tools.jackson.core:jackson-databind:3.x`
depends on `com.fasterxml.jackson.core:jackson-annotations:2.x`), so plain
`com.fasterxml.jackson.annotation.*` imports need no change. Annotations living in **other** modules
— `databind`, `dataformat-xml` — *do* move with their package. `@JsonDeserialize` is in `databind`,
which is precisely why §1.2 breaks.

The real risk is not the renames; it is the **defaults that changed silently**:

- `SORT_PROPERTIES_ALPHABETICALLY` → `true` (breaks byte-for-byte JSON assertions and golden files)
- **Enums read/written via `toString()` instead of `name()`** — high risk for persisted/transmitted values
- `FAIL_ON_TRAILING_TOKENS` → `true`; `FAIL_ON_NULL_FOR_PRIMITIVES` → `true`
- `DateTimeFeature.ONE_BASED_MONTHS` → `true`
- `MapperFeature.FIX_FIELD_NAME_UPPER_CASE_PREFIX` enabled — **affects Lombok-generated accessors**, which this repo relies on heavily
- `DEFAULT_VIEW_INCLUSION`, `USE_GETTERS_AS_SETTERS`, `ALLOW_FINAL_FIELDS_AS_MUTATORS` → `false`

API renames to expect: `@JsonComponent` → `@JacksonComponent`; `@JsonMixin` → `@JacksonMixin`;
`JsonSerializer`/`JsonDeserializer` → `ValueSerializer`/`ValueDeserializer`; `Module` → `JacksonModule`;
`JsonFactory` → `TokenStreamFactory`; `TextNode` → `StringNode`; `JsonLocation` → `TokenStreamLocation`.
Exceptions become unchecked (`JsonProcessingException` → `JacksonException extends RuntimeException`),
so many `throws` clauses can simply be deleted. Defining an `ObjectMapper` bean no longer overrides
autoconfiguration — define a `JsonMapper` bean instead.

`GenieObjectMapper` should shrink: Jackson 3 folds `Jdk8Module` and `JavaTimeModule` into databind
and makes `WRITE_DATES_AS_TIMESTAMPS=false` and `FAIL_ON_UNKNOWN_PROPERTIES=false` the defaults.
`ACCEPT_CASE_INSENSITIVE_ENUMS`, `defaultTimeZone` and `filterProvider` remain. Note `getMapper()`
returns `ObjectMapper`; in Jackson 3 the concrete type is `JsonMapper` and mappers are immutable.

Not migrated to 3.x at all: the jsonSchema and Hibernate datatype modules.

### 5.2 Suggested order

1. Replace or fork `com.github.fge:json-patch` and resolve `converter-jackson` (§3.2). These gate
   everything else and are the least predictable.
2. Port the 24 `@JsonDeserialize(builder = …)` DTOs and the `@JsonSerialize`/custom serializers in
   `JsonUtils`.
3. Port `GenieObjectMapper` and the `ApisAutoConfiguration` customizer
   (`Jackson2ObjectMapperBuilderCustomizer` → `JsonMapperBuilderCustomizer`).
4. Drop the `spring-hateoas:2.4.1` pin (§3.1) and let the BOM select 3.x.
5. Remove `spring-boot-jackson2`, `preferred-json-mapper`, and the `spring.jackson2.*` blocks in
   **both** config files (§2.1).
6. Add the regression test described in §6.

---

## 6. Recommended test coverage gap to close

The `@JsonFilter` breakage in §1.3 would have reached production: **no existing test covered error-response
serialization.** The integration tests assert on the `message` field of error payloads, so they catch
gross breakage, but only indirectly.

Add a focused regression test that serializes a `@JsonFilter`-annotated Genie exception through the
Spring-configured converter and asserts the body contains only `errorCode` and `message`. It is
cheap, and it is the one test that would fail loudly if the mapper wiring regresses — in either
direction.

---

## 7. Decisions unrelated to Jackson

Recorded here so the reasoning is not lost, but none of these are on a deadline.

### 7.1 Spring Cloud ↔ Boot 4.1 version gap

No Spring Cloud release train targets Boot 4.1.x: 2025.1.4 targets 4.0.x, and 2026.0.0-RC1 jumps to
4.2.0-RC1. With the project's constraints (Boot 4.1.x, Spring Cloud 2025.1.*), the compatibility
verifier is told the pairing is acceptable via
`spring.cloud.compatibility-verifier.compatible-boot-versions: [4.1.x]` in `genie-web-defaults.yml`.
Chosen over `enabled: false` because it states intent rather than silencing the check.

Risk is low: Genie's entire Spring Cloud surface is one `CuratorFramework` bean plus an
`@AutoConfigureAfter(ZookeeperAutoConfiguration.class)` in `genie-web/…/ZookeeperAutoConfiguration.java:49`.
There are zero other `org.springframework.cloud` imports in main source, and Zookeeper is off by
default. **Worthwhile follow-up:** define the `CuratorFramework` bean directly (Curator is already
on the classpath) and drop `spring-cloud-starter-zookeeper`, eliminating the gap permanently.

### 7.2 Tracing propagation forced to B3

`management.tracing.propagation.type: B3` in `genie-web-defaults.yml` is a **correction, not a
workaround**. Genie's propagation is B3 throughout — see `BraveTracePropagator` and
`EnvVarBraveTracePropagatorImpl` (`GENIE_AGENT_B3_*` / `GENIE_B3_*` keys).

Boot 4 defaults propagation to W3C, which installs micrometer's `W3CBaggagePropagator`. That asks
Brave's `GrpcServerRequest` for arbitrary header names, and Brave throws
`AssertionError: We currently don't support getting headers except propagation fields`. The effect
was that **every gRPC server call failed** with `StatusRuntimeException: UNKNOWN`, agents could never
complete HANDSHAKE, and no job could start. This was a production defect, not a test issue.

### 7.3 License tasks disabled

All `license*` tasks are disabled in root `build.gradle` with a `TODO`. The chain
`netflixoss → license-gradle-plugin:0.16.1 → com.mycila:license-maven-plugin:3.0` expands the
`${year}` header placeholder using Spring's `PropertyPlaceholderHelper`; Boot 4's Gradle plugin
shares the buildscript classpath and forces spring-core to 7.x, which removed the 4-arg constructor
mycila 3.0 compiled against.

Neither upgrade path works today: 0.16.1 is the newest release of the Gradle plugin (netflixoss
13.0.0 still bundles it), and mycila 4.6/5.1.2 drop the spring-core dependency but changed
`Document.<init>` and `HeaderSource$UrlHeaderSource.<init>` from `String encoding` to
`java.nio.charset.Charset`, which 0.16.1 invokes via `invokespecial`. Only the automated header
check is lost; the Apache 2.0 headers in source are unaffected.

### 7.4 Other notable Boot 4 modularization effects

Boot 4 split monolithic autoconfiguration into per-technology modules. Where an autoconfiguration
stopped applying, the fix was to add the module that now owns it — not to re-add excludes:

- `spring-boot-starter-flyway` — `FlywayAutoConfiguration` left `spring-boot-autoconfigure`. Only raw
  `flyway-core` was declared, so **migrations never ran** and `/admin/health` returned 503.
- `spring-boot-micrometer-tracing-brave` — `BraveAutoConfiguration` moved out of
  `spring-boot-actuator-autoconfigure`; nothing defined the `brave.Tracer` bean. (This is what
  exposed §7.2.)
- `spring-boot-starter-data-jpa-test` / `spring-boot-jpa-test` — `@DataJpaTest` and `TestEntityManager`.
- `TestRestTemplate` autoconfiguration is now **opt-in** via `@AutoConfigureTestRestTemplate`;
  `@SpringBootTest(RANDOM_PORT)` no longer implies it.

Conversely, a `spring.autoconfigure.exclude` entry naming a now-missing autoconfiguration class is
usually dead code and should be deleted rather than renamed.

### 7.5 springdoc 2.8.8 → 3.2.0

springdoc 2.x is compiled against Boot 3: `SpringDocHateoasConfiguration#hateoasHalProvider` takes an
`org.springframework.boot.autoconfigure.hateoas.HateoasProperties` parameter that no longer exists,
producing a `TypeNotPresentException`. springdoc 3.2.0's parent POM is spring-boot 4.1.2.

---

## 8. References

- [Spring Boot 4.0 Migration Guide — Upgrading Jackson](https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-4.0-Migration-Guide#upgrading-jackson)
- [Introducing Jackson 3 support in Spring](https://spring.io/blog/2025/10/07/introducing-jackson-3-support-in-spring/)
- [Spring Boot reference — JSON](https://docs.spring.io/spring-boot/reference/features/json.html)
- [spring-boot#47688 — Add Jackson 2 module to ease upgrade effort](https://github.com/spring-projects/spring-boot/issues/47688) (the "removed after 12 months" note)
- [Jackson 3.0 release notes — major changes](https://github.com/FasterXML/jackson/wiki/Jackson-Release-3.0#major-changesfeatures-in-30)
- [Jackson 3 migration guide (upstream, WIP)](https://github.com/FasterXML/jackson/blob/main/jackson3/MIGRATING_TO_JACKSON_3.md)
