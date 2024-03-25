# Airflow with Docker Compose

Based on https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

## How to run it
```sh
docker-compose up airflow-init
```

Wait unit you get this response:
```sh
airflow-init_1       | Upgrades done
airflow-init_1       | Admin user airflow created
airflow-init_1       | 2.8.2
start_airflow-init_1 exited with code 0
```

Then, run:
```sh
docker-compose up
```

Login in a browser:
http://localhost:8080/login/

```
username: airflow
password: airflow
```