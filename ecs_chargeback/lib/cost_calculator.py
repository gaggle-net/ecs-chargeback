from dataclasses import dataclass
from typing import List
from .service import Service
from datetime import date, timezone, timedelta, datetime
import boto3
import botocore
import json
import dataclasses
from types import SimpleNamespace as Namespace


@dataclass
class ClusterInstanceType:
    instance_type_name: str
    cost: float = 0
    usage: float = 0
    vcpus: int = 0
    memory: int = 0


class DataclassJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


class ClusterCostCalculator:
    name: str
    _instance_types: List[ClusterInstanceType] = None

    def __init__(
        self,
        name: str,
        cluster_tag: str = "cluster",
        cost_lookback: timedelta = timedelta(days=3),
        cache_bucket: str = None,
        cache_prefix: str = None,
        cache_ttl: timedelta = timedelta(days=1),
    ):
        self.name = name

        s3 = boto3.resource("s3")
        cache_object = None
        if cache_bucket is not None:
            key = (
                f"{cache_prefix}/{self.name}.json"
                if cache_prefix is not None
                else f"{self.name}.json"
            )
            cache_object = s3.Object(cache_bucket, key)
            try:
                if cache_object.last_modified + cache_ttl > datetime.now(timezone.utc):
                    file_content = cache_object.get()["Body"].read().decode("utf-8")
                    self._instance_types = json.loads(
                        file_content, object_hook=lambda d: Namespace(**d)
                    )
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] != "404":
                    raise

        if self._instance_types is None:
            self._load_instance_types(
                cluster_tag=cluster_tag, cost_lookback=cost_lookback
            )
            self._load_instance_type_specs()

            if cache_object is not None:
                cache_object.put(
                    Body=json.dumps(self._instance_types, cls=DataclassJSONEncoder)
                )

    def _load_instance_types(
        self,
        cluster_tag: str,
        cost_lookback: timedelta,
    ):
        ce = boto3.client("ce")
        end = date.today()
        start = end - cost_lookback
        response = ce.get_cost_and_usage(
            TimePeriod={
                "Start": str(start),
                "End": str(end),
            },
            Granularity="DAILY",
            Filter={
                "And": [
                    {
                        "Tags": {
                            "Key": cluster_tag,
                            "Values": [self.name],
                        },
                    },
                    {
                        "Dimensions": {
                            "Key": "USAGE_TYPE_GROUP",
                            "Values": ["EC2: Running Hours"],
                        },
                    },
                ]
            },
            Metrics=["BlendedCost", "UsageQuantity"],
            GroupBy=[
                {
                    "Type": "DIMENSION",
                    "Key": "USAGE_TYPE",
                },
            ],
        )

        self._instance_types = []
        for result in response["ResultsByTime"]:
            for group in result["Groups"]:
                instance_type_name = group["Keys"][0].split(":")[1]

                instance_type = next(
                    (
                        it
                        for it in self._instance_types
                        if it.instance_type_name == instance_type_name
                    ),
                    None,
                )
                if instance_type is None:
                    instance_type = ClusterInstanceType(
                        instance_type_name=instance_type_name
                    )
                    self._instance_types.append(instance_type)

                instance_type.cost += float(group["Metrics"]["BlendedCost"]["Amount"])
                instance_type.usage += float(
                    group["Metrics"]["UsageQuantity"]["Amount"]
                )

    def _load_instance_type_specs(self):
        if self._instance_types is None or len(self._instance_types) == 0:
            return
        ec2 = boto3.client("ec2")
        response = ec2.describe_instance_types(
            InstanceTypes=[it.instance_type_name for it in self._instance_types],
        )

        for i in response["InstanceTypes"]:
            instance_type_name = i["InstanceType"]
            instance_type = next(
                (
                    it
                    for it in self._instance_types
                    if it.instance_type_name == instance_type_name
                ),
                None,
            )
            if instance_type is not None:
                instance_type.vcpus = i["VCpuInfo"]["DefaultVCpus"]
                instance_type.memory = i["MemoryInfo"]["SizeInMiB"]

    def hourly_service_reservation_cost(self, service: Service) -> float:
        if self.memory_per_vcpu == 0:
            return 0
        vcpus = service.cpu_reservation / 1024
        memory_multiplier = max(1, service.memory_per_vcpu / self.memory_per_vcpu)
        return self.hourly_vcpu_cost * vcpus * memory_multiplier

    def hourly_service_utilization_cost(self, service: Service) -> float:
        if self.memory_per_vcpu == 0:
            return 0
        vcpus = (service.cpu_utilization / 100) * (service.cpu_reservation / 1024)
        memory_multiplier = max(
            1,
            (service.memory_utilization / 100)
            * (service.memory_per_vcpu / self.memory_per_vcpu),
        )
        return self.hourly_vcpu_cost * vcpus * memory_multiplier

    @property
    def hourly_vcpu_cost(self) -> float:
        weighted_sum_of_cost = 0
        total_usage = 0

        for it in self._instance_types:
            total_usage += it.usage
            weighted_sum_of_cost += it.cost / it.vcpus

        if total_usage == 0:
            return 0
        return weighted_sum_of_cost / total_usage

    @property
    def memory_per_vcpu(self) -> float:
        weighted_sum_of_ratios = 0
        total_usage = 0

        for it in self._instance_types:
            total_usage += it.usage
            weighted_sum_of_ratios += it.usage * (it.memory / it.vcpus)

        if total_usage == 0:
            return 0
        return weighted_sum_of_ratios / total_usage
