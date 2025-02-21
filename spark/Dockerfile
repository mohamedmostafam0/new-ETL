FROM bitnami/spark:3.5.0

WORKDIR /opt/bitnami/spark

RUN pip install py4j
RUN pip install python-dotenv
# ✅ Ensure directory exists
RUN mkdir -p /opt/bitnami/spark/scripts

# ✅ Copy files to correct location
COPY ./src/spark_pgsql/spark_pgsql.py /opt/bitnami/spark/spark_pgsql.py
COPY ./src/constants.py /opt/bitnami/spark/constants.py

# Set environment variables
ARG POSTGRES_PASSWORD
ENV POSTGRES_PASSWORD=$POSTGRES_PASSWORD
