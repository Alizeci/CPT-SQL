# Contexto del proyecto — SQL Load Testing System

## Información general

- **Proyecto:** Sistema de pruebas de carga continua para consultas SQL
- **Tesis:** "Diseño y Desarrollo de un Sistema de Pruebas de Carga Continua para Consultas SQL durante el Ciclo de Vida del Software"
- **Autora:** Laura Izquierdo
- **Director:** Andrés Gómez
- **Institución:** Escuela Colombiana de Ingeniería Julio Garavito
- **Repositorio:** https://github.com/Alizeci/sql_load_testing_system
- **Fork de referencia BenchmarkSQL:** https://github.com/Alizeci/BenchmarkSQL-4

---

## Arquitectura

**Tipo:** Monolito modular con principios de DDD en el dominio.

**Módulos Gradle:**

```
sql-load-testing-system/
├── sql-testing-domain         → Modelo de datos puro
├── sql-testing-processor      → Anotaciones + Annotation Processor (tiempo de compilación)
├── sql-testing-infrastructure → Wrapper JDBC, captura de métricas, motor de benchmark
└── sql-testing-api            → Spring Boot, endpoints REST, clases de ejemplo
```

**Dependencias entre módulos:**
```
sql-testing-api            → sql-testing-domain, sql-testing-infrastructure
sql-testing-infrastructure → sql-testing-domain
sql-testing-processor      → (sin dependencias — standalone)
```

**El dominio NO depende de nadie.**

---

## Stack tecnológico

- Java 17 (toolchain Gradle)
- Gradle 9.3 (multi-módulo)
- Spring Boot 3.4.3 (solo en sql-testing-api e sql-testing-infrastructure)
- Lombok (en sql-testing-domain e sql-testing-infrastructure)
- JUnit Jupiter 5.10.2
- PostgreSQL (BD espejo en Fase 3, via Docker)
- GitHub Actions (CI/CD, dos workflows)

---

## build.gradle raíz

```groovy
plugins {
    id 'java'
    id 'org.springframework.boot' version '3.4.3' apply false
    id 'io.spring.dependency-management' version '1.1.7' apply false
}

group = 'escuelaing.edu.co'
version = '0.0.1-SNAPSHOT'
description = 'System for continuous load testing of SQL queries during the software development lifecycle.'

subprojects {
    apply plugin: 'java'
    java {
        toolchain { languageVersion = JavaLanguageVersion.of(17) }
    }
    repositories { mavenCentral() }
    test { useJUnitPlatform() }
}
```

---

## Estado actual por módulo

### sql-testing-processor ✅ COMPLETO (Fase 1)

**Estructura:**
```
src/main/java/escuelaing/edu/co/processor/
├── annotation/
│   ├── SqlQuery.java
│   └── Req.java
└── SqlQueryProcessor.java

src/main/resources/META-INF/services/
└── javax.annotation.processing.Processor
```

**`@SqlQuery`** — `@Retention(SOURCE)`, `@Target(METHOD)`
- `queryId` (String, requerido)
- `description` (String, default "")

**`@Req`** — `@Retention(SOURCE)`, `@Target(METHOD, TYPE)`
- `maxResponseTimeMs` (long, default 1000)
- `priority` (Priority enum: HIGH/MEDIUM/LOW, default MEDIUM)
- `description` (String, default "")
- `allowPlanChange` (boolean, default true)

**`SqlQueryProcessor`:**
- `@SupportedAnnotationTypes` con los dos nombres completos
- `@SupportedSourceVersion(RELEASE_17)`
- Resuelve `@Req`: método tiene precedencia sobre clase
- Genera `queries.json` en `build/classes/java/main/loadtest/`
- Serialización JSON manual (sin dependencias externas)
- Registro automático via `META-INF/services`

---

### sql-testing-domain ✅ COMPLETO (Fases 1 y 2)

**Estructura:**
```
src/main/java/escuelaing/edu/co/domain/
├── model/
│   ├── QueryEntry.java          (@Data @Builder Lombok)
│   ├── TransactionRecord.java   (@Data @Builder Lombok)
│   └── LoadProfile.java         (@Data @Builder Lombok)
└── sample/
    └── OrderRepository.java     (clase de ejemplo — PENDIENTE mover a sql-testing-api)
```

**QueryEntry — campos:**
```java
String queryId, className, methodName, queryDescription, priority, reqDescription
boolean hasReq, allowPlanChange
long maxResponseTimeMs
```

**TransactionRecord — campos:**
```java
String queryId
String sql
long latencyMs
Instant timestamp
String executionPlan  // null en Fase 2; se llena en Fase 3 via EXPLAIN ANALYZE
```

**LoadProfile — campos:**
```java
Instant generatedAt
int totalSamples
Map<String, QueryStats> queries

// QueryStats campos:
String queryId
long sampleCount
double meanMs, medianMs, p95Ms, p99Ms
long minMs, maxMs
double callsPerMinute
```

**Pendiente Fase 3 en domain:**
```java
BenchmarkResult     // métricas por queryId + veredicto PASS/FAIL
DegradationReport   // detalle de qué consultas fallaron y por qué
```

---

### sql-testing-infrastructure ✅ COMPLETO (Fase 2)

**Clases implementadas:**
```
src/main/java/escuelaing/edu/co/infrastructure/
├── QueryRegistryLoader.java    → Lee queries.json de Fase 1 → mapa queryId → QueryEntry
├── CaptureContext.java         → try-with-resources + ThreadLocal
├── SamplingFilter.java         → Reglas: HIGH siempre, anomalía siempre, resto 10%
├── JdbcWrapper.java            → Proxy JDBC, mide latencia con System.nanoTime()
├── CaptureToggle.java          → Feature flag con JMX + REST (GET/PUT /loadtest/capture)
├── MetricsBuffer.java          → Cola en memoria, hilo flusher asíncrono cada 500ms en lotes
└── LoadProfileBuilder.java     → Agrega TransactionRecords → calcula estadísticas completas
```

**Regla de muestreo (SamplingFilter) — orden de precedencia:**
1. `priority == HIGH` → siempre capturar
2. `latencia > maxResponseTimeMs` → siempre capturar (anomalía)
3. Ninguna → capturar con probabilidad 0.10

**Pendiente Fase 3 en infrastructure:**
```
MirrorDatabaseProvisioner   → crea esquema TPC-C + conecta PostgreSQL vía Docker
SyntheticDataGenerator      → genera 90% sintético con privacidad diferencial
BenchmarkRunner             → orquesta ejecución con ramp-up, think time, ventana de medición
QueryExecutor               → ejecuta consultas del perfil, captura parámetros via toString() + fallback sintético
DegradationDetector         → compara métricas contra @Req y línea base
BaselineManager             → guarda/recupera línea base de referencia en JSON versionado
```

---

### sql-testing-api ⬜ PENDIENTE

Tiene código generado por Spring Initializr — ignorar por ahora.

---

## Artefactos del sistema

### Fase 1 → `queries.json`
```json
{
  "generatedAt": "2026-03-12T03:06:16.905871Z",
  "totalQueries": 3,
  "queries": [
    {
      "queryId": "getUserOrders",
      "className": "escuelaing.edu.co.domain.sample.OrderRepository",
      "methodName": "getUserOrders",
      "queryDescription": "Órdenes activas de un usuario",
      "hasReq": true,
      "maxResponseTimeMs": 200,
      "priority": "HIGH",
      "reqDescription": "Crítica del checkout",
      "allowPlanChange": false
    }
  ]
}
```

### Fase 2 → `transactions.json` + `metrics.json`
- `transactions.json` — registros individuales de ejecución
- `metrics.json` — perfil de carga con estadísticas agregadas

### Fase 3 → `benchmark-<perfil>-YYYYMMDD-HHMMSS.json`
- Métricas por queryId + veredicto PASS/FAIL
- Vincula métricas con versión del código y perfil

---

## Metodología del sistema (4 fases)

### Fase 1 ✅ — Instrumentación en tiempo de compilación
- Anotaciones `@SqlQuery` y `@Req` sobre métodos JDBC
- Annotation Processor extrae metadatos y genera `queries.json`
- Cero impacto en runtime

### Fase 2 ✅ — Extracción de métricas de producción
- Wrapper JDBC intercepta ejecuciones reales
- SamplingFilter con tres reglas de precedencia
- MetricsBuffer asíncrono con batching
- LoadProfileBuilder calcula media, mediana, min, max, p95, p99, cpm
- Artefactos: fichero transaccional + fichero de métricas
- EXPLAIN ANALYZE acotado a Fase 3 (no en producción)

### Fase 3 ⬜ — Motor de pruebas de carga
- **BD espejo:** PostgreSQL con Docker
- **Esquema:** TPC-C adaptado (referencia: https://github.com/Alizeci/BenchmarkSQL-4)
- **Datos:** 10% reales (Fase 2) + 90% sintéticos con privacidad diferencial
- **Motor:** BenchmarkRunner con ramp-up, think time, ventana de medición
- **Detección:** DegradationDetector compara contra línea base + umbrales @Req
- **Parámetros:** captura via `toString()` del PreparedStatement + fallback sintético
- **Costo por operación:** costo estimado del planificador via EXPLAIN ANALYZE
- **CI/CD:** GitHub Actions — dos workflows:
  - `pre-test.yml` → trigger: push/PR, diff de queries.json para detectar consultas cambiadas
  - `nightly-benchmark.yml` → trigger: cron medianoche, prueba completa

### Fase 4 ⬜ — Integración y publicación de resultados
- Artefactos JSON/CSV versionados con nombre `benchmark-<perfil>-YYYYMMDD-HHMMSS`
- Dashboard de tendencias históricas
- Alertas configurables por umbrales @Req

---

## Decisiones de diseño tomadas

| Decisión | Justificación |
|----------|---------------|
| `RetentionPolicy.SOURCE` en anotaciones | Cero bytecode, cero impacto runtime |
| `@Req` en método tiene precedencia sobre clase | Principio de especificidad Java |
| Serialización JSON manual en processor | El processor no debe tener dependencias externas |
| Registro via `META-INF/services` | Activación automática por javac sin config en build |
| Wrapper JDBC como mecanismo de captura | Portabilidad PostgreSQL/MySQL, no requiere permisos admin |
| Monolito modular (no hexagonal) | Menor overhead, más fácil de defender en tesis |
| `CaptureContext` via try-with-resources | Limpieza automática del ThreadLocal, evita memory leaks |
| Latencia con `System.nanoTime()` | Precisión sub-milisegundo, independiente del reloj del sistema |
| HIGH siempre capturado, resto 10% | Consultas críticas nunca se pierden en el muestreo |
| `SyntheticDataGenerator` en Fase 3 | Los datos sintéticos son efímeros y dependen del escenario elegido |
| Captura de parámetros via `toString()` + fallback sintético | Simplicidad + robustez |
| Costo por operación = EXPLAIN ANALYZE | Comparable entre versiones, nativo de PostgreSQL |
| Esquema TPC-C adaptado para BD espejo | Estándar reconocido, representa patrones transaccionales reales |
| GitHub Actions para CI/CD | Ya tiene el repo ahí; extensible a Jenkins/GitLab sin cambios en el núcleo |
| Diff de queries.json para pre-test | Solo re-ejecuta consultas que cambiaron — más preciso y eficiente |

---

## Flujo de ciclo de vida

```
COMPILACIÓN (Fase 1)
  Desarrollador anota métodos con @SqlQuery/@Req
  → SqlQueryProcessor genera queries.json

PRODUCCIÓN CONTINUA (Fase 2)
  App corre con CaptureToggle ON
  CaptureContext.begin("queryId") envuelve ejecuciones JDBC
  JdbcWrapper captura SQL + latencia
  SamplingFilter decide si registrar
  MetricsBuffer acumula y hace flush asíncrono
  LoadProfileBuilder construye perfil de carga
  → transactions.json + metrics.json versionados en repo

CADA COMMIT (Fase 3 — Nivel 1)
  Pipeline detecta diff en queries.json
  Ejecuta pre-test sobre consultas cambiadas + HIGH
  Compara contra línea base (@Req umbrales)
  → PASS: merge permitido / FAIL: merge bloqueado

CADA NOCHE (Fase 3 — Nivel 2)
  Pipeline ejecuta prueba completa con todos los perfiles
  Incluye escenarios de carga pico y mantenimiento
  → Resultados disponibles al inicio del día siguiente

PUBLICACIÓN (Fase 4)
  Artefactos JSON/CSV versionados
  Dashboard de tendencias históricas
  Alertas cuando se supera umbral de @Req
```

---

## Pendientes técnicos

1. Implementar Fase 3 en `sql-testing-infrastructure`:
   - `MirrorDatabaseProvisioner`
   - `SyntheticDataGenerator`
   - `BenchmarkRunner`
   - `QueryExecutor`
   - `DegradationDetector`
   - `BaselineManager`
2. Agregar a `sql-testing-domain`:
   - `BenchmarkResult`
   - `DegradationReport`
3. Crear workflows GitHub Actions:
   - `.github/workflows/pre-test.yml`
   - `.github/workflows/nightly-benchmark.yml`
4. Mover `OrderRepository` de `sql-testing-domain/src/main` a `sql-testing-api/src/main`
5. Implementar Fase 4

---

## Pendientes en documento de tesis

1. Aplicar correcciones pendientes de Cap2 (Tabla 1, párrafo de cierre, separación Kusterer/Jung)
2. Completar sección 1.5 al finalizar todos los capítulos
3. En sección 3.6 mencionar que la estrategia de validación es consistente con SynQB (Gruenheid et al., 2024)

---

## Comandos útiles

```bash
# Build completo
./gradlew build

# Compilar solo el dominio y ver output del processor
./gradlew :sql-testing-domain:compileJava --rerun-tasks 2>&1 | grep "LoadTest"

# Ver el JSON generado por Fase 1
cat sql-testing-domain/build/classes/java/main/loadtest/queries.json

# Ver estructura de archivos Java por módulo
find sql-testing-processor/src -name "*.java"
find sql-testing-domain/src -name "*.java"
find sql-testing-infrastructure/src -name "*.java"

# Subir cambios a GitHub
git add . && git commit -m "mensaje" && git push origin main
```
