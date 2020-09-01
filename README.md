# Geoscience Australia DEA Airflow DAGs repository

## Deployment Workflow

This repository contains two branches, `master` and `develop`.

The `master` branch requires Pull Requests and reviews to merge code into, and
is deployed automatically to the Production (Sandbox) Airflow deployment.

The `develop` branch can be committed to directly, or via Pull Request, and is
deployed automatically to the Development Airflow deployment.

We're not happy with this strategy, and are looking for an alternative that
doesn't have us deploying and inadvertently running code in multiple places by
accident, but haven't come up with anything yet.

## Development

## Using Docker
## Development

### Local Editing of DAG's
If you have Docker available, by far the easiest development setup is to use
Docker Compose.

``` bash
<insert-fernet-key-here>
python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())' 
docker-compose up

docker-compose exec webserver /bin/bash
. /entrypoint.sh
airflow connections --add --conn_id lpgs_gadi --conn_uri 'ssh://dra547@gadi.nci.org.au'

```

``` bash
docker-compose up
docker-compose exec 
docker-compose run --rm webserver airflow upgradedb
docker-compose run --rm webserver airflow connections --add --conn_id lpgs_gadi --conn_uri ssh://dra547@gadi.nci.org.au/
docker-compose exec webserver /entrypoint.sh airflow connections --add --conn_id dea_public_data_upload --conn_uri s3://foo:bar@dea-public-data-dev/
```

### Local Editing of DAG's

DAGs can be locally edited and validated. Development can be done in `conda` or `venv` according to developer preference. Grab everything airflow and write DAGs. Use `autopep8` and `pylint` to achieve import validation and consistent formatting as the CI pipeline for this repository matures.

```bash
pip install apache-airflow[aws,kubernetes,postgres,redis,ssh,celery]
pip install pylint pylint-airflow

pylint dags plugins
```
