# Kafka Docker Project

## Overview
This project hosts a Kafka infrastructure using Docker and provides Java-based producer and consumer applications.

## Project Structure
- `docker/`: Contains Docker Compose file and related documentation.
- `src/`: Java source code for producer, consumer, and shared utilities.
- `config/`: Configuration files for Java applications.
- `build.gradle` or `pom.xml`: Dependency manager for Java.

## How to Use
1. Navigate to the `docker` folder.
2. Run the Kafka infrastructure:
   ```bash
   docker-compose up -d
