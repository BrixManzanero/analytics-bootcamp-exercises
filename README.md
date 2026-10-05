# Analytics Solutions Bootcamp
Welcome to the Analytics Solutions Bootcamp!

This is the repository where the lecture slides, references, lab work, case study guide, and capstone project instructions are posted. 

Here are the main directories and files of this repository.
1. `setup`: This is the folder that contains the configurations and datasets used during the bootcamp. 
   1. `compose.yaml`: Docker compose file that manages all the services used during the bootcamp. 
   2. `requirements.txt`: Package dependencies of `setup_db.py`.
   3. `setup_db.py`: Python script that's used to ingest sample data to PostgreSQL.
2. `01_slides`: Contains the PDF version of the presentation used during live lectures. Note that I am not sharing the actual PowerPoint slides. 
3. `02_lab`: Contains the script and datasets used during live lab demo. You can use these codes as guide for your case study work and even capstone project.
4. `03_case_study`: Contains the instructions for the case study work that you can do. 
5. `airflow`: Contains scripts for running DAGs in Airflow. 

## Prerequisites
- To run the services from the `compose.yaml` file, you need to setup and download the following.
  - **Windows Subsystem for Linux (WSL)**. Run `wsl --install` via PowerShell
  - **Docker Desktop**. Download from [Docker](https://www.docker.com/).
  - **Git**. Download from [Git](https://git-scm.com/).
  - **GitHub**. Create your account [here](https://github.com/).
  - **Python**. This project uses Python 3.11.9. You may use newer versions, but you might encounter some issues. Download from [Python](https://www.python.org/).
  - **R**. Download from [R](https://www.r-project.org/).
  - **RStudio**. Download from [Posit](https://posit.co/).
  - **VS Code**. Download from [VS Code](https://code.visualstudio.com/).
  - **Google Colab**. In case that you can't do heavy setup on your machine, you may use Google Colab, a browser-based notebook by Google, for Python and R coding. You may check [here](https://colab.research.google.com/). 

- Create `.env` file by copying the contents from the `.env-example`. You may keep things as is. 

## Running services
The `compose.yaml` file contains the services that were used during this project. Most of these services were used during Module 3 - Fundamentals of Data Engineering. You may further refine this to minimize what you're using. 

1. Ensure that the Docker Engine is running. Run `docker info` on the terminal.
   - If running: It outputs system-wide information (e.g., number of containers, server version, kernel version).
   - If stopped: It fails with the same daemon connection error.
2. Run `docker compose up -d` to run all services. Alternatively, run `docker compose up -d [service-name]` to run individual services. Make sure that the dependent service/s are also running.
3. Wait for the services to run. You may access the services through the following ports.
   - PostgreSQL
     - Using DBeaver
         - POSTGRES_HOST=localhost or 0.0.0.0
         - POSTGRES_PORT=5432
         - POSTGRES_DB_OLTP=asb_oltp
         - POSTGRES_DB_OLAP=asb_olap
         - POSTGRES_USER=db_user
         - POSTGRES_PASSWORD=db_password
     - Using other Docker services
         - POSTGRES_HOST=asb-postgres-db
         - POSTGRES_PORT=5432
         - POSTGRES_DB_OLTP=asb_oltp
         - POSTGRES_DB_OLAP=asb_olap
         - POSTGRES_USER=db_user
         - POSTGRES_PASSWORD=db_password
   - SeaweedFS file browser: http://localhost:8889
   - Airflow Web Server: http://localhost:8082
   - Spark Jupyter server: http://localhost:8888
   - Metabase: http://localhost:3000
   - CloudBeaver: http://localhost:8978