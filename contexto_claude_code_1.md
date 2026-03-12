# Contexto del proyecto — SQL Load Testing System

## Información general

- **Proyecto:** Sistema de pruebas de carga continua para consultas SQL
- **Tesis:** "Diseño y Desarrollo de un Sistema de Pruebas de Carga Continua para Consultas SQL durante el Ciclo de Vida del Software"
- **Autora:** Laura Izquierdo
- **Director:** Andrés Gómez
- **Institución:** Escuela Colombiana de Ingeniería Julio Garavito
- **Repositorio local:** `~/Desktop/TG 2026/JAN/MVP/sql-load-testing-system`

---

## Arquitectura

**Tipo:** Monolito modular con principios de DDD en el dominio.

**Módulos Gradle:**

```
sql-load-testing-system/
├── sql-testing-domain        → Modelo de datos puro (QueryEntry, LoadProfile, BenchmarkResult)
├── sql-testing-processor     → Anotaciones + Annotation Processor (tiempo de compilación)
├── sql-testing-infrastructure → Wrapper JDBC, lectura/escritura JSON, conexión BD espejo
└── sql-testing-api           → Spring Boot, endpoints REST, clases de ejemplo
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
- Lombok (solo en sql-testing-domain)
- JUnit Jupiter 5.10.2

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

**build.gradle:**
```groovy
dependencies {
    // sin dependencias externas
}
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

**META-INF/services contiene:**
```
escuelaing.edu.co.processor.SqlQueryProcessor
```

---

### sql-testing-domain ✅ COMPLETO (Fase 1)

**Estructura:**
```
src/main/java/escuelaing/edu/co/domain/
├── model/
│   └── QueryEntry.java       (@Data @Builder Lombok)
└── sample/
    └── OrderRepository.java  (clase de ejemplo — PENDIENTE mover a sql-testing-api)
```

**build.gradle:**
```groovy
configurations {
    annotationProcessor
}

dependencies {
    compileOnly 'org.projectlombok:lombok:1.18.32'
    annotationProcessor 'org.projectlombok:lombok:1.18.32'
    compileOnly project(':sql-testing-processor')
    annotationProcessor project(':sql-testing-processor')
    testImplementation 'org.junit.jupiter:junit-jupiter:5.10.2'
    testRuntimeOnly 'org.junit.platform:junit-platform-launcher:1.10.2'
}

tasks.named('compileJava') {
    options.annotationProcessorPath = configurations.annotationProcessor
    options.compilerArgs += ['-Xlint:all']
}
```

**QueryEntry.java — campos:**
```java
String queryId
String className
String methodName
String queryDescription
boolean hasReq
long maxResponseTimeMs
String priority
String reqDescription
boolean allowPlanChange
```

---

### sql-testing-infrastructure ⬜ VACÍO

Solo tiene `src/` y `build.gradle` vacío.

---

### sql-testing-api ⬜ VACÍO (tiene algo generado por Spring Initializr — ignorar)

---

## Artefacto de salida de la Fase 1

**Ruta:** `sql-testing-domain/build/classes/java/main/loadtest/queries.json`

**Ejemplo de salida:**
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
    },
    {
      "queryId": "insertOrder",
      "className": "escuelaing.edu.co.domain.sample.OrderRepository",
      "methodName": "insertOrder",
      "queryDescription": "Inserta una nueva orden",
      "hasReq": true,
      "maxResponseTimeMs": 500,
      "priority": "MEDIUM",
      "reqDescription": "Default del repositorio de órdenes",
      "allowPlanChange": true
    },
    {
      "queryId": "getOrderHistory",
      "className": "escuelaing.edu.co.domain.sample.OrderRepository",
      "methodName": "getOrderHistory",
      "queryDescription": "Historial completo de órdenes",
      "hasReq": true,
      "maxResponseTimeMs": 500,
      "priority": "MEDIUM",
      "reqDescription": "Default del repositorio de órdenes",
      "allowPlanChange": true
    }
  ]
}
```

---

## Metodología del sistema (4 fases)

### Fase 1 ✅ — Instrumentación en tiempo de compilación
- Anotaciones `@SqlQuery` y `@Req` sobre métodos JDBC
- Annotation Processor extrae metadatos y genera `queries.json`
- Cero impacto en runtime

### Fase 2 ⬜ — Extracción de métricas de producción
- **Mecanismo principal:** Wrapper JDBC
- **Mecanismo complementario:** EXPLAIN ANALYZE (planes de ejecución)
- Intercepta ejecución real de consultas
- Captura: SQL real, latencia, throughput
- Control de activación y muestreo
- Construye perfil de carga (insumo para Fase 3)
- **Módulo:** `sql-testing-infrastructure`
- **Conexión con Fase 1:** usa `queryId` para correlacionar métricas con reglas de negocio

### Fase 3 ⬜ — Motor de pruebas de carga
- Base de datos espejo
- Ejecución del benchmark
- Estrategia CI/CD: pre-test ligero por commit + prueba completa nocturna
- Consultas HIGH siempre en pre-test; MEDIUM/LOW en prueba completa

### Fase 4 ⬜ — Integración y publicación de resultados
- Publicación en CI/CD
- Dashboard de resultados

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
| `@SupportedAnnotationTypes` con nombres específicos | Buena práctica — no usar wildcard "*" en producción |

---

## Pendientes técnicos

1. Mover `OrderRepository` de `sql-testing-domain/src/main` a `sql-testing-api/src/main`
2. Resolver warning de Java 21 IntelliJ (no bloquea, es cosmético)
3. Implementar Fase 2: wrapper JDBC en `sql-testing-infrastructure`

---

## Pendientes en documento de tesis

1. Ajustar sección 3.2.2: cambiar "extrae el texto de la consulta SQL" por referencia al wrapper JDBC de Fase 2
2. Aplicar correcciones pendientes de Cap2 (Tabla 1, párrafo de cierre, separación Kusterer/Jung)
3. Completar sección 1.5 al finalizar todos los capítulos

---

## Comandos útiles

```bash
# Build completo
./gradlew build

# Compilar solo el dominio y ver output del processor
./gradlew :sql-testing-domain:compileJava --rerun-tasks 2>&1 | grep "LoadTest"

# Ver el JSON generado
cat sql-testing-domain/build/classes/java/main/loadtest/queries.json

# Ver estructura de archivos Java
find sql-testing-processor/src -name "*.java"
find sql-testing-domain/src -name "*.java"
```
