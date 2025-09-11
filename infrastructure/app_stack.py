#!/usr/bin/env python3
import os

from aws_cdk import (
    aws_lambda,
    aws_events,
    aws_events_targets,
    aws_s3,
    aws_iam,
    BundlingOptions,
    Duration,
    SecretValue,
    Stack,
)
from os.path import join, dirname, abspath
from constructs import Construct
from gaggle_cdk.core import apply_permissions_boundary
from gaggle_cdk.core.tagging import GaggleTags
from gaggle_cdk.core.teams import GaggleTeam

base_path = dirname(dirname(abspath(__file__)))

app_name = "ecs-chargeback"


class ChargebackStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        cluster_tag: str,
        run_frequency_mins: int,
        cost_lookback_days: int,
        datadog_metric_prefix: str,
        dd_api_key_secret_id: str,
        dd_api_key_secret_field: str = "api_key",
        bucket_name: str = None,
        **kwargs
    ) -> None:
        super().__init__(scope, id, **kwargs)

        cache_bucket = aws_s3.Bucket(
            self,
            "ChargebackCacheBucket",
            bucket_name=bucket_name,
        )

        datadog_api_key = SecretValue.secrets_manager(
            secret_id=dd_api_key_secret_id,
            json_field=dd_api_key_secret_field,
        )

        iam_role = aws_iam.Role(
            self,
            "ChargebackLambdaRole",
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
            inline_policies={
                "LambdaPolicy": aws_iam.PolicyDocument(
                    statements=[
                        aws_iam.PolicyStatement(
                            effect=aws_iam.Effect.ALLOW,
                            actions=[
                                "ecs:ListClusters",
                                "ecs:ListServices",
                                "ecs:DescribeServices",
                                "ecs:DescribeTaskDefinition",
                                "ecs:ListTagsForResource",
                                "cloudwatch:GetMetricData",
                                "ec2:DescribeInstanceTypes",
                                "ce:GetCostAndUsage",
                            ],
                            resources=["*"],
                        )
                    ]
                )
            },
        )

        chargeback = aws_lambda.Function(
            self,
            "ChargebackHandler",
            runtime=aws_lambda.Runtime.PYTHON_3_13,
            function_name=app_name,
            code=aws_lambda.Code.from_asset(
                join(base_path, "ecs_chargeback"),
                bundling=BundlingOptions(
                    image=aws_lambda.Runtime.PYTHON_3_13.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        "pip install --no-cache -r requirements.txt -t /asset-output && cp -au . /asset-output",
                    ],
                ),
            ),
            handler="lambda.handler",
            environment={
                "CLUSTER_TAG": cluster_tag,
                "CACHE_BUCKET": cache_bucket.bucket_name,
                "UTILIZATION_LOOKBACK_MINS": str(run_frequency_mins),
                "COST_LOOKBACK_DAYS": str(cost_lookback_days),
                "DATADOG_API_KEY": datadog_api_key.to_string(),
                "DATADOG_METRIC_PREFIX": datadog_metric_prefix,
            },
            timeout=Duration.seconds(60),
            role=iam_role,
        )

        cache_bucket.grant_read_write(chargeback.role)

        rule = aws_events.Rule(
            self,
            "ChargebackHandlerRule",
            schedule=aws_events.Schedule.rate(Duration.minutes(run_frequency_mins)),
        )
        rule.add_target(aws_events_targets.LambdaFunction(chargeback))
