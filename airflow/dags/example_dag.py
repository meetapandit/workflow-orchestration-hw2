from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    schedule="@daily",
    start_date=datetime(2026, 1, 27),
    description="An example DAG",
    default_args={"owner": "airflow", "retries": 1},
    tags=["example"],
    max_consecutive_failed_dag_runs=3,  # Pause DAG after 3 consecutive failures
)
def example_dag():
    
    @task
    def task_a():
        print("Hello from task A")

    @task
    def task_b():
        print("Hello from task B")

    @task
    def task_c():
        print("Hello from task C")
    
    @task
    def task_d():
        print("Hello from task D")

    task_a() >> task_b() >> [task_c(), task_d()]

example_dag()