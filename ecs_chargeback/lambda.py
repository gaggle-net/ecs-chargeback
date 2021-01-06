import boto3
from datetime import timedelta
from lib.cluster import Cluster
from lib.cost_calculator import ClusterCostCalculator
from lib.datadog_handler import DataDogHandler
import os

dd = DataDogHandler(
    api_key=os.getenv("DATADOG_API_KEY"),
    metric_prefix=os.getenv("DATADOG_METRIC_PREFIX"),
)


def handler(event, context):
    ecs = boto3.client("ecs")
    clusterArns = ecs.list_clusters()["clusterArns"]
    for clusterArn in clusterArns:
        cluster_name = clusterArn.split("/")[1]
        cluster = Cluster(
            name=cluster_name,
            utilization_lookback=timedelta(
                minutes=int(os.getenv("UTILIZATION_LOOKBACK_MINS", "5"))
            ),
        )

        if len(cluster.services) == 0:
            continue

        cluster_calculator = ClusterCostCalculator(
            name=cluster_name,
            cluster_tag=os.getenv("CLUSTER_TAG", "cluster"),
            cost_lookback=timedelta(days=int(os.getenv("COST_LOOKBACK_DAYS", "3"))),
            cache_bucket=os.getenv("CACHE_BUCKET", None),
            cache_prefix=os.getenv("CACHE_PREFIX", None),
        )

        for service in cluster.services:
            cost = cluster_calculator.hourly_service_reservation_cost(service)
            waste = cost - cluster_calculator.hourly_service_utilization_cost(service)
            dd.handle_service(
                cluster=cluster_name,
                service=service.name,
                tags=service.tags,
                cpu_reservation=service.cpu_reservation,
                memory_reservation=service.memory_reservation,
                hourly_cost=cost,
                hourly_waste=waste,
            )


if __name__ == "__main__":
    handler({}, {})
