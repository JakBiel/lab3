# Apache Airflow DAG with building permissions - project

## Table of content

- [Apache Airflow DAG with building permissions - project](#apache-airflow-dag-with-building-permissions---project)
  - [Table of content](#table-of-content)
  - [Why did I create this project?](#why-did-i-create-this-project)
  - [What does this project do?](#what-does-this-project-do)
  - [Docker containers runned in this project](#docker-containers-runned-in-this-project)
  - [How to run locally?](#how-to-run-locally)
  - [Opening Apache Airflow server](#opening-apache-airflow-server)
  - [Opening Jupyter Notebook server](#opening-jupyter-notebook-server)
  - [Schema of the project - directory tree](#schema-of-the-project---directory-tree)
  - [Example of email sent by DAG](#example-of-email-sent-by-dag)
  - [Example of email attachment with Great Expectations validation report](#example-of-email-attachment-with-great-expectations-validation-report)
  - [A data sample from the table "permissions\_results2022"](#a-data-sample-from-the-table-permissions_results2022)
  - [Table schema of "permissions\_results2022" in BigQuery](#table-schema-of-permissions_results2022-in-bigquery)
  - [Contributing](#contributing)
  - [Authors and acknowledgment](#authors-and-acknowledgment)
  - [License](#license)
  - [Project status](#project-status)

## Why did I create this project?

This project is a result of my learning and practicing about DAGs in Apache Airflow and visualisations made in Jupyter Notebook or Looker Studio.

## What does this project do?

This project uses Apache Airflow's directed acyclic graph (or, shortly - DAG) to organize work with data coming from polish governmental Main Building Supervision Office. The data is related to building permissions.

The DAG works with the following tasks (in order):

![DAG TASKS](img/dag_tasks.PNG)

1. **zip_data_downloader_task** - this task is responsible for downloading the building permissions data and unzipping it (to the original .csv format).
2. **validation task** - this task does the validation of the selected columns of the downloaded data using Great Expectations library. There are four goals of the validation which are: 
    - checking if the dates in 'data_wplywu_wniosku_do_urzedu' column have the correct format
    - checking if the values in 'kategoria' column are the expected numbers of the roman type
    - checking if the 'terc' column includes the correct type (7 digits) of TERC codes
    - checking if the 'rodzaj_zam_budowlanego' column includes all 4 expected types of building construction intention.
3. **unzipped_data_uploader_task** - this task uploades the validated data (a table named "permissions_results2022") to a BigQuery database.
4. **aggregates_creation_task** - this task uses the validated data to create a customized aggregates. The aggregates table consists of columns including information about how many building actions were reported to the polish Main Building Supervision Office in the last 3, 2 and 1 month (divided also by building categories types). At the end, the task sends the aggregates table to the BigQuery database.
5. **send_email_task** - this task sends a reporting email to a certain mail address. It has the validation report from "validation task" (in .html format).

This DAG is scheduled to be executed once per month, on a first day of each month. It does catchups if necessary.

Moreover, for this project, I created visualisations basing on the "permissions_results2022" table in Jupyter Notebook. The visualisations include various plots and GIF file showing the changes in the number of building permissions per last 3, 2 and 1 month (the GIF is below)

![VOIVOD_CHANGES321](img/voivodeships1.gif)

Also, as a part of the project, I created related visualisations in Looker Studio.

![LOOKER GIF](img/looker_gif.gif)

## Docker containers runned in this project

- **airflow-webserver-1** - to present a web page that is a GUI to use Apache Airflow
- **airflow-scheduler-1** - a scheduler of scheduled DAGs in Apache Airflow
- **airflow-init-1** - to initialize Apache Airflow server
- **flower-1** - a support container by initialization of Apache Airflow server
- **jupyter-1** - to code and present visualisations done in Jupyter Notebook
- **extending-ariflow-builder-1** - the parent container responsile for starting all the other Apache Airflow containers
- **postgres-1** - a container to keep Apache Airflow server's data (e.g. xcom)


## How to run locally?

To run this project, a few activities will be necessary:

- Clone the project
- Go to the project directory. Type in the terminal (all commands from this chapter will be for Windows OS):

```bash
gci -Name
```

- You should see this:

```bash
airflow_dockerfile
dags
zip_data_jupyter_notebook
.gitignore
docker-compose.yaml
README.md
```

- Now, we will need to download and install a few docker images from the internet. Please execute this command:


```bash
docker-compose pull
```

- Then, type the following command to initialize all the containers that the project includes:

```bash
docker-compose up
```

## Opening Apache Airflow server

If you want to go directly to Apache Airflow server, where the DAG is scheduled, please open your browser and type in a new tab:

```bash
localhost:8080
```

This command will open the web page that is running Apache Airflow GUI. You will need to insert a login and password to login into the server. 
  

## Opening Jupyter Notebook server

If you want to go directly to the Jupyter Notebook analyzes, please open your browser and type in a new tab:

```bash
localhost:8888
```

This command will open the container's content that includes csv_analyze.ipynb file with the building permissions table analyze. In order to check the results of the analyze in the browser tab, please click on the "Run" tab of the displayed page and choose the suboption of "Run All Cells"

![IPYNB1](img/jupyter_scr2.png)

The step above will generate plots with various analysis of the data.

## Schema of the project - directory tree

```bash
lab3
│   .gitignore # This file excludes from GIT all the files needed locally only like e.g. virtual environmen etc.
│   docker-compose.yaml # The file to simultaneous management of all the docker container used in the project
│   README.md # The file with project's documentation
│
├───airflow_dockerfile # The folder for the Dockerfile of all the Apache Airflow containers
│       Dockerfile # Dockerfile used for creation of all the Apache Airflow containers
│
├───dags # The folder for DAG's code pieces which are programmed in Python language
│       aggregates_python.py # The main Python file of the DAG presented in the project
│       aggregates_python_helpers.py # The Python file including the helpers of the main Python file "aggregates_python.py" 
│   
│
└───zip_data_jupyter_notebook # The folder for the JupyterNotebook docker container designed for JupyterNotebook analyzes
    │   requirements.txt # List of programs to be pre-installed in order to correctly execute the whole Jupyter application
    │   Dockerfile # Dockerfile used for the creation of the JupyterNotebook docker container
    │   config.yaml # File used for setting the configuration of the connection between JupyterNotebook container and BigQuery database
    │
    └───jpdata
            csv_analyze.ipynb # The file with analyzes of data informing about building permissions in Poland

```

## Example of email sent by DAG

![MAIL_EXMPL](img/email_example.png)

## Example of email attachment with Great Expectations validation report 

![GX_EXMPL](img/gx_pic1.png)

## A data sample from the table "permissions_results2022"

![DATA_EXMPL](img/bq_pic2.png)

## Table schema of "permissions_results2022" in BigQuery

![DATA_EXMPL2](img/schema_pic.png)

## Contributing

Here, I want to say thank you to my mentor Wojtek for his great contribution in code review and supporting me everytime when I got stucked somewhere in the project.

## Authors and acknowledgment

It is all my work, with a review and recommendations of my mentor Wojtek.

## License

No special license issued so far, I present it as a result of my work only.

## Project status

It is initaly finished (End of April 2024). Unknown if it is going to be developed in the future.
