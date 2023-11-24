from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements 
from udacity.common import create_tables
from udacity.common.final_project_sql_statements import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past':False,
    'start_date': datetime(2023, 1, 1), 
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
}

dag = DAG('final_project_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup = False,
          start_date = datetime.now()
         )
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    create_stage_events_table = PostgresOperator(
        task_id = 'Create_stage_events',
        dag=dag,
        postgres_conn_id='redshift',
        sql=create_tables.create_staging_events_table_sql
    )
    create_stage_songs_table = PostgresOperator(
        task_id = 'Create_stage_songs',
        dag=dag,
        postgres_conn_id='redshift',
        sql=create_tables.create_staging_songs_table_sql
    )
    create_fact_songplays_table = PostgresOperator(
        task_id = 'Create_fact_songplays',
        dag=dag,
        postgres_conn_id='redshift',
        sql=create_tables.create_fact_songplays_table_sql
    )

    create_dim_users_table = PostgresOperator(
        task_id = 'Create_dim_users',
        dag=dag,
        postgres_conn_id='redshift',
        sql=create_tables.create_dim_users_table_sql
    )
    create_dim_songs_table = PostgresOperator(
        task_id = 'Create_dim_songs',
        dag=dag,
        postgres_conn_id='redshift',
        sql=create_tables.create_dim_songs_table_sql
    )
    create_dim_artists_table = PostgresOperator(
        task_id = 'Create_dim_artists',
        dag=dag,
        postgres_conn_id='redshift',
        sql=create_tables.create_dim_artists_table_sql
    )
    create_dim_time_table = PostgresOperator(
        task_id = 'Create_dim_time',
        dag=dag,
        postgres_conn_id='redshift',
        sql=create_tables.create_dim_time_table_sql
    )



    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket="udacity-dend",
        s3_key='log_data',
        json_path="s3://udacity-dend/log_json_path.json",
        
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
     #   s3_bucket="hy-awsbucket/song-data",
        s3_bucket="udacity-dend",
        s3_key="song_data",
        file_format="json",
     #   JSON="auto"
         json_path="s3://udacity-dend/song-data"
      #  json_path="s3://hy-awsbucket/song-data"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table='fact_songplays',
        truncate=False,
        sql=SqlQueries.fact_songplay_table_insert

    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table='dim_users',
        truncate=False,
        sql=SqlQueries.dim_users_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table='dim_songs',
        truncate=False,
        sql=SqlQueries.dim_songs_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag=dag,
        redshift_conn_id="redshift",
       # postgres_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table='dim_artists',
        truncate=False,
        sql=SqlQueries.dim_artists_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table='dim_time',
        truncate=False,
        sql=SqlQueries.dim_time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table_list=['fact_songplays','dim_songs', 'dim_artists', 'dim_time'],
        check_type="count_check"
    )
    end_operator = DummyOperator(task_id="Stop_execution")

    ###  task dependencies
    start_operator >> create_stage_songs_table
    start_operator >> create_stage_events_table
    start_operator >> create_fact_songplays_table >> load_songplays_table
    start_operator >> create_dim_users_table
    start_operator >> create_dim_songs_table
    start_operator >> create_dim_artists_table
    start_operator >> create_dim_time_table
    create_stage_songs_table >> stage_events_to_redshift
    create_stage_events_table >> stage_songs_to_redshift
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> create_dim_songs_table >> load_song_dimension_table >> run_quality_checks
    load_songplays_table >> create_dim_users_table >> load_user_dimension_table >> run_quality_checks
    load_songplays_table >> create_dim_artists_table >> load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> create_dim_time_table >> load_time_dimension_table >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()
