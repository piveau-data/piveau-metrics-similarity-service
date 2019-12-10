# Dataset Similarity Service

Fingerprint datasets for later comparison and provides and endpoint to retrieve similar datasets for a given ID using a distance metric.


## Setup

1. Install all of the following software
        
* Java JDK >= 1.8
* Git >= 2.17
  
2. Clone the directory and enter it
    
        git@gitlab.com:european-data-portal/dataset-similarity-service.git
        
3. Edit the environment variables in the `Dockerfile` to your liking. Variables and their purpose are listed below:
   
| Key | Description | Default |
| :--- | :--- | :--- |
| PORT | Port this service will run on | 8086 |
| API_KEY | Authorization secret required for certain endpoints. Must be configured for service to run. | null |
| WORK_DIR | Directory into which fingerprint files are written | /tmp |
| SPARQL_URL | Address of the SPARQL endpoint | https://www.europeandataportal.eu/sparql |

        
## Run

### Production

Build the project by using the provided Maven wrapper. This ensures everyone this software is provided to can use the exact same version of the maven build tool.
The generated _fat-jar_ can then be found in the `target` directory.

* Linux
    
        ./mvnw clean package
        java -jar target/similarity-service-0.1-fat.jar

* Windows

        mvnw.cmd clean package
        java -jar target/similarity-service-0.1-fat.jar
      
* Docker

    1. Start your docker daemon 
    2. Build the application as described in Windows or Linux
    3. Adjust the port number (`EXPOSE` in the `Dockerfile`)
    4. Build the image: `docker build -t edp/similarity-service .`
    5. Run the image, adjusting the port number as set in step _iii_: `docker run -i -p 8086:8086 edp/mqa-metric-service`
    6. Configuration can be changed without rebuilding the image by overriding variables: `-e PORT=8087`

### Development

For use in development two scripts are provided in the project's root folder. These enable hot deployment (dynamic recompiling when changes are made to the source code).
Linux users should run the `redeploy.sh` and Windows users the `redeploy.bat` file.

## CI

The repository uses the gitlab in-build CI Framework. The .gitlab-ci.yaml file starts as soon a new push event occurs. After running the test cases the application is build, a new docker image is created and stored in the gitlab registry. 

## API

A formal OpenAPI 3 specification can be found in the `src/main/resources/webroot/openapi.yaml` file.
A visually more appealing version is available at `{url}:{port}` once the application has been started.
