#!/usr/bin/env python3

from tabulate import tabulate
from lib.cluster import Cluster
from lib.cost_calculator import ClusterCostCalculator
from datetime import timedelta
import boto3


def main():
    ecs = boto3.client("ecs")
    clusterArns = ecs.list_clusters()["clusterArns"]
    for clusterArn in clusterArns:
        cluster_name = clusterArn.split("/")[1]
        cluster = Cluster(
            name=cluster_name,
            utilization_lookback=timedelta(minutes=5),
            utilization_period=60,
            utilization_stat="Average",
        )

        if len(cluster.services) == 0:
            continue

        cluster_calculator = ClusterCostCalculator(
            name=cluster_name,
            cluster_tag="cluster",
            cost_lookback=timedelta(days=3),
            cache_bucket=None,
            cache_prefix=None,
            cache_ttl=timedelta(days=1),
        )

        services_table = [
            [
                service.name,
                service.cpu_reservation,
                service.cpu_utilization,
                service.memory_reservation,
                service.memory_utilization,
                service.memory_per_vcpu,
                cluster_calculator.hourly_service_reservation_cost(service),
                cluster_calculator.hourly_service_reservation_cost(service)
                - cluster_calculator.hourly_service_utilization_cost(service),
            ]
            for service in cluster.services
        ]
        print(
            f"\n\nCLUSTER: {cluster.name} (cost/vcpu:{(cluster_calculator.hourly_vcpu_cost):.2f} mem/vcpu:{cluster_calculator.memory_per_vcpu:.0f}) \n"
        )
        print(
            tabulate(
                services_table,
                headers=[
                    "Service",
                    "CPU-R",
                    "CPU-U",
                    "MEM-R",
                    "MEM-U",
                    "MEM/VCPU",
                    "Cost (hourly)",
                    "Waste (hourly)",
                ],
            )
        )


if __name__ == "__main__":
    main()
