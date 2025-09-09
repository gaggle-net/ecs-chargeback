#!/usr/bin/env python3
import os

from aws_cdk import (
    aws_lambda,
    aws_events,
    aws_events_targets,
    aws_s3,
    aws_iam,
    App,
    Duration,
    Environment,
    SecretValue,
    Stack
)
from os.path import join, dirname, abspath
from constructs import Construct
from gaggle_cdk.core import apply_permissions_boundary
from gaggle_cdk.core.tagging import GaggleTags
from gaggle_cdk.core.teams import GaggleTeam

base_path = dirname(dirname(abspath(__file__)))

app_name = 'ecs-chargeback'

class ChargebackStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        environment: str,
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

        chargeback = aws_lambda.Function(
            self,
            "ChargebackHandler",
            runtime=aws_lambda.Runtime.PYTHON_3_13,
            function_name=app_name,
            code=aws_lambda.Code.from_asset('../ecs_chargeback/'),
            handler='lambda.handler',
            environment={
                "CLUSTER_TAG": cluster_tag,
                "CACHE_BUCKET": cache_bucket.bucket_name,
                "UTILIZATION_LOOKBACK_MINS": str(run_frequency_mins),
                "COST_LOOKBACK_DAYS": str(cost_lookback_days),
                "DATADOG_API_KEY": datadog_api_key.to_string(),
                "DATADOG_METRIC_PREFIX": datadog_metric_prefix,
            },
            timeout=Duration.seconds(60),
        )

        chargeback.add_to_role_policy(
            aws_iam.PolicyStatement(
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
        )

        cache_bucket.grant_read_write(chargeback.role)

        rule = aws_events.Rule(
            self,
            "ChargebackHandlerRule",
            schedule=aws_events.Schedule.rate(Duration.minutes(run_frequency_mins)),
        )
        rule.add_target(aws_events_targets.LambdaFunction(chargeback))

        GaggleTags(
            application=app_name,
            environment=environment,
            team=GaggleTeam.DEVOPS,
        )

app = App()

## Production
ChargebackStack(
    app,
    "ecs-chargeback",
    environment="production",
    cluster_tag=app.node.try_get_context("chargeback:cluster-tag"),
    bucket_name=app.node.try_get_context("chargetback:bucket-name"),
    run_frequency_mins=int(app.node.try_get_context("chargeback:run-frequency-mins")),
    cost_lookback_days=int(app.node.try_get_context("chargeback:cost-lookback-days")),
    dd_api_key_secret_id=app.node.try_get_context(
        "chargeback:datadog-api-key-secret-id"
    ),
    dd_api_key_secret_field=app.node.try_get_context(
        "chargeback:datadog-api-key-secret-field"
    ),
    datadog_metric_prefix=app.node.try_get_context("chargeback:datadog-metric-prefix"),
    env=Environment(
        account=os.environ["CDK_DEFAULT_ACCOUNT"],
        region=os.environ["CDK_DEFAULT_REGION"],
    ),
)

apply_permissions_boundary(app)

app.synth()
