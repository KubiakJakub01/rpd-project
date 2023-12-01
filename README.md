# rpd-project
Projekt z rozproszonego przetwarzania danych / Distributed computing college project

## Getting Started

To get started with this project, follow the instructions below.

## Prerequisites

- Docker
- Docker Compose
- Git
- Maven
- Java JDK (Java 17)

## Running the app

### Step 1: Clone the Repository
```
git clone https://github.com/KubiakJakub01/rpd-project.git
cd rpd-project
```
### Step 2: Switch to producer branch
```
git checkout producer
```
### Step 3: Docker Hub Login
```
docker login
```
You will need a security token from https://hub.docker.com/
### Step 4: Set Up MinIO Entrypoint Script
Update the path to minio-entrypoint.sh in docker-compose.yml, you need to give the exact path to a directory on your local host where minio-entrypoint.sh is located,change this:
```
- /home/maciek/test_docker_compose/minio-entrypoint.sh:/usr/bin/minio-entrypoint.sh
```
to 
```
- /your_path/minio-entrypoint.sh:/usr/bin/minio-entrypoint.sh
```
### Step 5: Run Docker Compose
```
docker-compose up
```
## Verifying Data in MinIO using MinIO Client (mc)
To check if the data is being saved correctly in MinIO, you can use the MinIO Client (mc). If you run docker-compose in a regular way, you can run commands below in another terminal, or you can run docker-compose in background:
```
docker-compose up -d
```
and then run mc in the same terminal
### Prerequisites
spring boot app must be running in the background
### Step 1: Install MinIO Client
Install mc on your local machine:
```
wget https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
sudo mv mc /usr/local/bin/
```
### Step 2: Configure mc for MinIO
Set up an alias for your MinIO server:
```
mc alias set myminio http://localhost:9000 minioadmin minioadmin
```
### Step 3: List Buckets and Contents
List all buckets:
```
mc ls myminio
```

List the contents of a specific bucket:
```
mc ls myminio/your-bucket-name
```
Currently data is saved to two buckets 'csv-data' and 'realtime-data'

## Creating docker image
### Step 1: Clone the Repository
```
git clone https://github.com/KubiakJakub01/rpd-project.git
cd rpd-project
```
### Step 2: Switch to producer branch
```
git checkout producer
```
### Step 3: Build the Maven Package
```
mvn clean package -Dmaven.test.skip
```
### Step 4: Create a Docker Image
```
docker build -t rpd-spring-boot .
```
### Step 5: Tag created Docker Image
```
sudo docker tag rpd-spring-boot:latest szubidubi/rpd:latest
```
### Step 6: Push to Docker Hub
```
docker login
sudo docker push szubidubi/rpd:latest
```




