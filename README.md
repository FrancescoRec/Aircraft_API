## Aircraft API  :airplane:

![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi)
![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)
![postgressql](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)

### Overview

The Aircraft API provides access to an aircraft database containing information about various aircraft types, including fuel consumption rates and other relevant data. The API supports basic CRUD operations for managing aircraft data.

### Data Sources

The API utilizes several data sources to compile its database:

1. **ADS-B Exchange Readsb Hist**: Provides historical data on aircraft flights.
   - Source: [ADS-B Exchange Readsb Hist](https://samples.adsbexchange.com/readsb-hist/)

2. **Flight CO2 Analysis**: Contains aircraft type and fuel consumption rate data in JSON format.
   - Source: [Flight CO2 Analysis GitHub](https://github.com/martsec/flight_co2_analysis/blob/main/data/aircraft_type_fuel_consumption_rates.json)

3. **ADS-B Exchange Basic Aircraft Database**: Offers basic information about aircraft.
   - Source: [ADS-B Exchange Basic Aircraft Database](http://downloads.adsbexchange.com/downloads/basic-ac-db.json.gz)

### Exercises

The API is structured to support different levels of complexity, from a simple local setup to more advanced configurations. Each exercise (S1 to S8) represents a stage of development and deployment:

- **S1 (Local Setup)**: Basic local setup allowing CRUD operations with raw data.
  
- **S4 (S3 Upload)**: Uploading raw data to an S3 bucket and accessing it from there.
  
- **S7 (S3 to RDS/Postgres)**: Retrieving raw data from S3 and creating a database in RDS or locally with PostgreSQL.
  
- **S8 (Data Pipeline with DAGs)**: Creation of Directed Acyclic Graphs (DAGs) for dataset processing and management. Includes tasks such as uploading raw data to S3 and creating datasets.

### Implementation Details

- **Framework**: The API is built using FastAPI to handle POST and GET actions efficiently.
  
- **Deployment**: Can be deployed as a standalone service using Uvicorn or as a Docker image for containerized deployment.
  
- **Database**: Utilizes PostgreSQL for storing and querying aircraft data.
  
- **Querying**: SQL queries are employed to retrieve and manipulate data, including joining datasets for analysis.

### Conclusion

The Aircraft API provides a versatile platform for accessing and managing aircraft data, offering flexibility in deployment and scalability for handling large datasets. With its structured exercises, developers can gradually build upon the API, adding functionalities and integrating with various data sources as needed.


