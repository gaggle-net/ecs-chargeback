from dataclasses import dataclass
from typing import List
from datetime import datetime, timezone, timedelta
import boto3
from .service import Service


@dataclass
class Cluster:
    name: str
    utilization_lookback: timedelta = timedelta(minutes=5)
    utilization_period: int = 60
    utilization_stat: str = "Average"
    _services: List[Service] = None

    @property
    def services(self):
        if self._services is None:
            self._load_services()
            self._load_service_utilization()
        return self._services

    def _load_services(self):
        self._services = []
        ecs = boto3.client("ecs")
        paginator = ecs.get_paginator("list_services")
        responses = paginator.paginate(cluster=self.name)
        for response in responses:
            if len(response["serviceArns"]) > 0:
                ecs_services = ecs.describe_services(
                    cluster=self.name,
                    services=response["serviceArns"],
                )["services"]
                for ecs_service in ecs_services:
                    task_definition = ecs.describe_task_definition(
                        taskDefinition=ecs_service["taskDefinition"]
                    )["taskDefinition"]

                    cpu, memory = 0, 0
                    for container in task_definition["containerDefinitions"]:
                        cpu += container["cpu"]
                        memory += container["memory"]

                    tags = []
                    try:
                        tags = ecs.list_tags_for_resource(
                            resourceArn=ecs_service["serviceArn"]
                        )["tags"]
                    except Exception as e:
                        print(f'{ecs_service["serviceArn"]} :: {e}')

                    self.services.append(
                        Service(
                            name=ecs_service["serviceName"],
                            task_count=ecs_service["runningCount"],
                            task_cpu_reservation=cpu,
                            task_memory_reservation=memory,
                            tags=tags,
                        )
                    )

    def _load_service_utilization(
        self,
    ):
        if len(self.services) == 0:
            return

        cw = boto3.client("cloudwatch")
        now = datetime.now(timezone.utc)
        paginator = cw.get_paginator("get_metric_data")
        queries = []
        for service in self.services:
            for metric in ["MemoryUtilization", "CPUUtilization"]:
                queries.append(
                    {
                        "Id": service.name.lower().replace("-", "") + "_" + metric,
                        "MetricStat": {
                            "Metric": {
                                "Namespace": "AWS/ECS",
                                "MetricName": metric,
                                "Dimensions": [
                                    {"Name": "ClusterName", "Value": self.name},
                                    {"Name": "ServiceName", "Value": service.name},
                                ],
                            },
                            "Period": self.utilization_period,
                            "Stat": self.utilization_stat,
                        },
                        "ReturnData": True,
                    }
                )

        response_iterator = paginator.paginate(
            MetricDataQueries=queries,
            StartTime=now - self.utilization_lookback,
            EndTime=now,
            ScanBy="TimestampDescending",
        )

        for response in response_iterator:
            for result in response["MetricDataResults"]:
                if result["StatusCode"] != "Complete":
                    print(result["StatusCode"])
                    print(result["Messages"])
                values = result["Values"]
                if len(values) > 0:

                    if len(self.services) == 1:
                        service = self.services[0]
                        if metric == "MemoryUtilization":
                            service.memory_utilization = max(values)
                        elif metric == "CPUUtilization":
                            service.cpu_utilization = max(values)
                    else:
                        service_name, metric = result["Label"].split(" ")
                        for service in self.services:
                            if service.name == service_name:
                                if metric == "MemoryUtilization":
                                    service.memory_utilization = max(values)
                                elif metric == "CPUUtilization":
                                    service.cpu_utilization = max(values)
