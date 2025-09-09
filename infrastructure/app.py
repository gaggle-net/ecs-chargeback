#!/usr/bin/env python3
import os

from aws_cdk import App, Environment
from os.path import join, dirname, abspath
from gaggle_cdk.core import apply_permissions_boundary, GaggleTags

from app_stack import ChargebackStack

app = App()

base_path = dirname(dirname(abspath(__file__)))
app_name = 'ecs-chargeback'
environment = app.node.try_get_context("gaggle-cdk:environment")
account_id = app.node

tags = GaggleTags(
    application=app_name,
    environment=environment,
    team=GaggleTags.Team.DEVOPS
)

## Production
app_stack = ChargebackStack(
    app,
    "ecs-chargeback",
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
    )
)

apply_permissions_boundary(app_stack)
tags.apply(app_stack)

app.synth()
