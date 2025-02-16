FROM bitnami/spark:3.5.0


WORKDIR /opt/bitnami/spark

RUN pip install py4j

COPY ./src/spark_pgsql/spark_pgsql.py /opt/bitnami/spark/scripts/spark_pgsql.py
COPY ./src/constants.py /opt/bitnami/spark/scripts/constants.py

ENV POSTGRES_DOCKER_USER=host.docker.internal
ARG POSTGRES_PASSWORD
ENV POSTGRES_PASSWORD=$POSTGRES_PASSWORD
