# Streamlit Chatbot Greenplum

## Prerequisite

Before starting the project, please ensure your system meet the below prerequisite.

* Docker installed before running this app. Please run the below command to verify Docker is installed in your system
</br>
  `docker --version`

  If it is not installed, please refer to this [Docker installation guide](https://docs.docker.com/engine/install/).

* Greenplum (version 7) database is up and running

## How to Start

1. Download the source code from Github

  `git clone https://github.com/ahmedrachid/streamlit-chatbot-greenplum.git`

2. Go to the project repository

  `cd streamlit-chatbot-greenplum`

3. Open the app.py file and change the database credential if required

  `vim app.py`

Change this section: `postgres://<db-user>:<password>@<db-ip>:<db-port>/<databasename>`


4. Build the docker image. The below command is to build a docker image with name streamline-chatbot-greenplum with the latest version

  `docker build -t streamlit-chatbot-greenplum .`

5. Verify image `streamlit-chatbot-greenplum` is built successfully

  `docker images`

6. Run the docker image streamlit-chatbot-Greenplum

  `docker run -p 8501:8501 streamlit-chatbot-greenplum`

7. Go to the browser and open the web link. 
Note: the IP address is the IP that your VM / local env uses. For example, `http://35.189.1.42:8501`

## Use Case

Please refer to [this blog](https://medium.com/greenplum-data-clinics/building-large-scale-ai-powered-search-in-greenplum-using-pgvector-and-openai-4f5c5811f54a) for detailed explaination and use case.