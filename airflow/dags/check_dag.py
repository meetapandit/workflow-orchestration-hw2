from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    schedule="0 0 * * *",
    start_date=datetime(2025, 1, 1),
    description="A DAG to check file creation and reading",
    default_args={"owner": "airflow", "retries": 1},
    tags=["file_check"],
    max_consecutive_failed_dag_runs=3,  # Pause DAG after 3 consecutive
)
def check_dag():
    
    @task.bash
    def create_file():
        return 'echo "Hi there!" > /tmp/dummy'
    
    @task.bash
    def check_file():
        return 'test -f /tmp/dummy'

    @task
    def read_file():
        print(open('/tmp/dummy', 'rb').read())


    create_file() >> check_file() >> read_file()

check_dag()