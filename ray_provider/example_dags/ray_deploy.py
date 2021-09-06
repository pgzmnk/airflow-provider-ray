
import json
import requests

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

import ray
from ray import serve

default_args = {
    'owner': 'airflow',
}


@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['example'])
def ray_deploy():
    """Serve and call Ray model.
    """
    @task()
    def serve_model():

        # This will connect to the running Ray cluster.
        ray.client('192.168.1.75:10001').namespace("airflow").connect()
        serve.start(detached=True)

        route_prefix = "regressor"

        @serve.deployment(route_prefix='/'+route_prefix)
        def my_func(request):
            name = request.query_params["name"]
            return f"Hello {name}!"

        my_func.deploy()

        return route_prefix

    @task(multiple_outputs=True)
    def call_model(route_prefix):

        name_to_pass = 'serve_poc'

        r = requests.get(
            f"http://host.docker.internal:8000/{route_prefix}?name={name_to_pass}")

        return {'return_value':  r.text}

    route_prefix = serve_model()
    order_summary = call_model(route_prefix)


ray_deploy_dag = ray_deploy()
