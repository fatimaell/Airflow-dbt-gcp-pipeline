#ROM quay.io/astronomer/astro-runtime:12.7.1
FROM astrocrpublic.azurecr.io/runtime:3.1-9

# Set environment variables
ENV VENV_PATH="/usr/local/airflow/dbt_venv"
ENV PATH="$VENV_PATH/bin:$PATH" 

# Install dbt into a virtual environment and upgrade pip + setuptools
RUN python -m venv $VENV_PATH && \
    source $VENV_PATH/bin/activate && \
    pip install --upgrade pip setuptools && \
    pip install --no-cache-dir dbt-bigquery==1.5.3 pandas Faker pyarrow numpy && \
   pip install --no-cache-dir apache-airflow-providers-google==10.6.0 google-cloud-storage &&\
   pip install psycopg2-binary &&\
   pip install asyncpg &&\

    deactivate


# Store environment activation script inside Airflow home
RUN echo "source $VENV_PATH/bin/activate" > /usr/local/airflow/dbt_env.sh
RUN chmod +x /usr/local/airflow/dbt_env.sh
