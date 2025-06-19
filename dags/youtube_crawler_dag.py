from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import os
import sys
import subprocess

# Default arguments for the DAG
default_args = {
    'owner': 'youtube-crawler',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'youtube_crawler_dag',
    default_args=default_args,
    description='YouTube Crawler Service DAG',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    tags=['youtube', 'crawler', 'data'],
)

def crawl_data_task(**context):
    """Task to run crawl-data service"""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        # Set environment variables
        os.environ['MONGODB_URI'] = 'mongodb://192.168.161.230:27011,192.168.161.230:27012,192.168.161.230:27013/?replicaSet=rs0'
        os.environ['MONGODB_DB'] = 'youtube_data'
        os.environ['PYTHONPATH'] = '/opt/airflow/youtube-crawler'
        
        # Add the youtube-crawler directory to Python path
        sys.path.insert(0, '/opt/airflow/youtube-crawler')
        
        # Import and run the main function
        from src.main import main
        
        # Set up command line arguments
        sys.argv = ['main.py', '--service', 'crawl-data', '--num-keywords', '1', '--max-workers', '5']
        
        # Run the main function
        main()
        
        logger.info("Crawl data task completed successfully")
        return "Success"
        
    except Exception as e:
        logger.error(f"Error in crawl_data_task: {str(e)}")
        raise

def reset_quota_task(**context):
    """Task to run reset-quota service"""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        # Set environment variables
        os.environ['MONGODB_URI'] = 'mongodb://192.168.161.230:27011,192.168.161.230:27012,192.168.161.230:27013/?replicaSet=rs0'
        os.environ['MONGODB_DB'] = 'youtube_data'
        os.environ['PYTHONPATH'] = '/opt/airflow/youtube-crawler'
        
        # Add the youtube-crawler directory to Python path
        sys.path.insert(0, '/opt/airflow/youtube-crawler')
        
        # Import and run the main function
        from src.main import main
        
        # Set up command line arguments
        sys.argv = ['main.py', '--service', 'reset-quota']
        
        # Run the main function
        main()
        
        logger.info("Reset quota task completed successfully")
        return "Success"
        
    except Exception as e:
        logger.error(f"Error in reset_quota_task: {str(e)}")
        raise

def crawl_video_task(**context):
    """Task to run crawl-video service"""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        # Set environment variables
        os.environ['MONGODB_URI'] = 'mongodb://192.168.161.230:27011,192.168.161.230:27012,192.168.161.230:27013/?replicaSet=rs0'
        os.environ['MONGODB_DB'] = 'youtube_data'
        os.environ['PYTHONPATH'] = '/opt/airflow/youtube-crawler'
        
        # Add the youtube-crawler directory to Python path
        sys.path.insert(0, '/opt/airflow/youtube-crawler')
        
        # Import and run the main function
        from src.main import main
        
        # Set up command line arguments
        sys.argv = ['main.py', '--service', 'crawl-video', '--max-workers', '5']
        
        # Run the main function
        main()
        
        logger.info("Crawl video task completed successfully")
        return "Success"
        
    except Exception as e:
        logger.error(f"Error in crawl_video_task: {str(e)}")
        raise

def crawl_comment_task(**context):
    """Task to run crawl-comment service"""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        # Set environment variables
        os.environ['MONGODB_URI'] = 'mongodb://192.168.161.230:27011,192.168.161.230:27012,192.168.161.230:27013/?replicaSet=rs0'
        os.environ['MONGODB_DB'] = 'youtube_data'
        os.environ['PYTHONPATH'] = '/opt/airflow/youtube-crawler'
        
        # Add the youtube-crawler directory to Python path
        sys.path.insert(0, '/opt/airflow/youtube-crawler')
        
        # Import and run the main function
        from src.main import main
        
        # Set up command line arguments
        sys.argv = ['main.py', '--service', 'crawl-comment', '--max-workers', '5']
        
        # Run the main function
        main()
        
        logger.info("Crawl comment task completed successfully")
        return "Success"
        
    except Exception as e:
        logger.error(f"Error in crawl_comment_task: {str(e)}")
        raise

# Create PythonOperator tasks
crawl_data_operator = PythonOperator(
    task_id='crawl_data',
    python_callable=crawl_data_task,
    dag=dag,
)

# reset_quota_operator = PythonOperator(
#     task_id='reset_quota',
#     python_callable=reset_quota_task,
#     dag=dag,
# )

# crawl_video_operator = PythonOperator(
#     task_id='crawl_video',
#     python_callable=crawl_video_task,
#     dag=dag,
# )

# crawl_comment_operator = PythonOperator(
#     task_id='crawl_comment',
#     python_callable=crawl_comment_task,
#     dag=dag,
# )

# Define task dependencies
# crawl_data_operator >> reset_quota_operator  # Uncomment if you want to reset quota after crawling
# crawl_data_operator >> create_indexes_operator  # Uncomment if you want to create indexes after crawling
# crawl_data_operator >> crawl_video_operator  # Uncomment if you want to crawl videos after data generation
# crawl_video_operator >> crawl_comment_operator  # Uncomment if you want to crawl comments after videos

# For now, just run the crawl_data_operator
crawl_data_operator 