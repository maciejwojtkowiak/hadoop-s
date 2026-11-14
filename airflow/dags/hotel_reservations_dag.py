"""
Airflow DAG: Hotel Reservations Pipeline
Compliant with criteria W01-W08:
- classic_or_streaming default parameter
- clean_output_mr_dir, clean_output_dir
- pick_classic_or_streaming (BranchPythonOperator)
- mapreduce_classic / hadoop_streaming (BashOperator)
- hive (BashOperator with beeline)
- get_output (BashOperator displaying JSON output)
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

dag = DAG(
    'hotel_reservations_pipeline',
    default_args=default_args,
    description='Hotel Reservations MapReduce + Hive Pipeline',
    schedule_interval=None,
    catchup=False,
    params={
        'classic_or_streaming': 'streaming',
        'dags_home': '/opt/airflow/dags',
        'input_dir': '/user/root/input/reservations',
        'hotels_input_dir': '/user/root/input/hotels',
        'output_mr_dir': '/user/root/output/mr_results',
        'output_dir': '/user/root/output/hive_results',
    },
)

# 1. Clean output directories
clean_output_mr_dir = BashOperator(
    task_id='clean_output_mr_dir',
    bash_command='hdfs dfs -rm -r -f {{ params.output_mr_dir }} 2>/dev/null || true',
    dag=dag,
)

clean_output_dir = BashOperator(
    task_id='clean_output_dir',
    bash_command='hdfs dfs -rm -r -f {{ params.output_dir }} 2>/dev/null || true',
    dag=dag,
)

# 2. Variant branch selector
def choose_variant(**kwargs):
    variant = kwargs['params'].get('classic_or_streaming', 'streaming')
    if variant == 'classic':
        return 'mapreduce_classic'
    return 'hadoop_streaming'

pick_classic_or_streaming = BranchPythonOperator(
    task_id='pick_classic_or_streaming',
    python_callable=choose_variant,
    dag=dag,
)

# 3. MapReduce Classic
mapreduce_classic = BashOperator(
    task_id='mapreduce_classic',
    bash_command="""
        hdfs dfs -rm -r -f {{ params.output_mr_dir }} 2>/dev/null || true
        hadoop jar {{ params.dags_home }}/hotel-reservations-1.0.jar \
            hotels.ReservationDriver \
            {{ params.input_dir }} \
            {{ params.output_mr_dir }}
    """,
    dag=dag,
)

# 3. Hadoop Streaming
hadoop_streaming = BashOperator(
    task_id='hadoop_streaming',
    bash_command="""
        hdfs dfs -rm -r -f {{ params.output_mr_dir }} 2>/dev/null || true
        mapred streaming \
            -files {{ params.dags_home }}/mapper.py,{{ params.dags_home }}/combiner.py,{{ params.dags_home }}/reducer.py \
            -input {{ params.input_dir }} \
            -output {{ params.output_mr_dir }} \
            -mapper "python3 mapper.py" \
            -combiner "python3 combiner.py" \
            -reducer "python3 reducer.py"
    """,
    dag=dag,
)

# 4. Hive Operator (using beeline, parameters: output_mr_dir, input_dir, output_dir)
hive = BashOperator(
    task_id='hive',
    bash_command="""
        hdfs dfs -rm -r -f {{ params.output_dir }} 2>/dev/null || true
        beeline -u "jdbc:hive2://localhost:10000/default" -n root \
            --hivevar input_dir3="{{ params.output_mr_dir }}" \
            --hivevar input_dir4="{{ params.hotels_input_dir }}" \
            --hivevar output_dir6="{{ params.output_dir }}" \
            -f {{ params.dags_home }}/hotel_ranking.hql || \
        beeline -u "jdbc:hive2://" \
            --hivevar input_dir3="{{ params.output_mr_dir }}" \
            --hivevar input_dir4="{{ params.hotels_input_dir }}" \
            --hivevar output_dir6="{{ params.output_dir }}" \
            -f {{ params.dags_home }}/hotel_ranking.hql
    """,
    trigger_rule='one_success',
    dag=dag,
)

# 5. Output display task (presents JSON records in log)
get_output = BashOperator(
    task_id='get_output',
    bash_command="""
        echo "=== MapReduce intermediate output (3) ==="
        hdfs dfs -cat {{ params.output_mr_dir }}/* 2>/dev/null || true
        echo ""
        echo "=== Hive final ranking output JSON (6) ==="
        hdfs dfs -cat {{ params.output_dir }}/* 2>/dev/null || true
    """,
    dag=dag,
)

# Pipeline dependency graph
[clean_output_mr_dir, clean_output_dir] >> pick_classic_or_streaming
pick_classic_or_streaming >> mapreduce_classic >> hive
pick_classic_or_streaming >> hadoop_streaming >> hive
hive >> get_output
