# Sistema Distribuido de Análisis de Películas

Este proyecto implementa un sistema distribuido para el análisis de datos de películas utilizando microservicios en Docker y RabbitMQ como sistema de mensajería.

## 📋 Requisitos Previos

- Docker y Docker Compose instalados
- Python 3.x
- Bash shell

## 📁 Estructura del Dataset

Antes de ejecutar el sistema, asegúrate de tener los siguientes archivos CSV en el directorio `docs/`:

```
docs/
├── movies.csv      # Datos de películas
├── ratings.csv     # Calificaciones de usuarios
└── credits.csv     # Créditos y cast de películas
```

## 🚀 Scripts de Ejecución

### 1. Configuración Inicial

Antes de ejecutar el sistema, necesitas crear un archivo `.env` con las variables de configuración:

```bash
# Ejemplo de archivo .env
MOVIES_RECEIVER_AMOUNT=2
CREDITS_RECEIVER_AMOUNT=2
RATINGS_RECEIVER_AMOUNT=2
Q1_FILTER_AMOUNT=2
Q3_FILTER_AMOUNT=2
Q4_FILTER_AMOUNT=2
CREDITS_JOINER_AMOUNT=2
RATINGS_JOINER_AMOUNT=2
BUDGET_REDUCER_AMOUNT=2
SENTIMENT_REDUCER_AMOUNT=2
BUDGET_SINK_AMOUNT=1
Q1_SINK_AMOUNT=1
CREDITS_SINK_AMOUNT=1
SENTIMENT_SINK_AMOUNT=1
SENTIMENT_WORKER_AMOUNT=2
CLIENTS_AMOUNT=3
RESUSCITATOR_AMOUNT=3
```

### 2. Scripts Disponibles

#### `compose-generate.sh`

Genera el archivo `docker-compose.yml` basado en las variables de entorno del archivo `.env`.

```bash
./compose-generate.sh
```

#### `compose-build.sh`

Construye todas las imágenes Docker de los microservicios en paralelo.

```bash
./compose-build.sh
```

#### `compose-run.sh`

Ejecuta el sistema completo usando Docker Compose.

```bash
./compose-run.sh
```

**Funcionalidad:**

- Detiene y limpia contenedores previos (`docker compose down -v`)
- Inicia todos los servicios (`docker compose up`)

#### `compose-build-and-run.sh`

Combina la construcción y ejecución en un solo comando.

```bash
./compose-build-and-run.sh
```

**Equivale a:**

```bash
./compose-build.sh
./compose-run.sh
```

#### `absolute-chaos.sh`

Script de prueba que simula fallos masivos en el sistema.

```bash
./absolute-chaos.sh
```
