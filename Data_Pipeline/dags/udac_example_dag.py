from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                DataQualityOperator, CreateTablesOperator)
from helpers import SqlQueries
from sparkify_dend_dimesions_subdag import load_dimensional_tables_dag


start_date = datetime.utcnow()

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'end_date': datetime(2019, 11, 30),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('sparkify_dend_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=3
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=False,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_path="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    file_format="JSON",
    execution_date=start_date
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=False,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_path="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    data_format="JSON",
    execution_date=start_date
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql=songplay_table_insert
    append_only=False
)

load_user_dimension_table = LoadDimensionOperator(
        dag=dag,
        task_id='load_user_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        start_date= datetime(2019, 1, 12),
        table="users",
        sql="user_table_insert",
        append_only=False
)

load_song_dimension_table = LoadDimensionOperator(
        dag=dag,
        task_id='load_song_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        start_date= datetime(2019, 1, 12),
        table="song",
        sql="song_table_insert",
        append_only=False
)

load_artist_dimension_table = LoadDimensionOperator(
        dag=dag,
        task_id='load_artist_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="artist",
        start_date= datetime(2019, 1, 12),
        sql="artist_table_insert"
        append_only=False
)

load_time_dimension_table = LoadDimensionOperator(
        dag=dag
        task_id='load_time_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="time",
        start_date= datetime(2019, 1, 12),
        sql="time_table_insert",
        append_only=False
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    tables=["songplay", "users", "song", "artist", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Setting tasks dependencies

start_operator >> create_redshift_tables >> [stage_songs_to_redshift, stage_events_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,
                           load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator

