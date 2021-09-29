#!/usr/bin/env python3
import os
import boto3
from aws_cdk import core as cdk

# For consistency with TypeScript code, `cdk` is the preferred import name for
# the CDK's core module.  The following line also imports it as `core` for use
# with examples from the CDK Developer's Guide, which are in the process of
# being updated to use `cdk`.  You may delete this import if you don't need it.
from aws_cdk import core

from flink_sql_demo.flink_sql_demo_stack import FlinkSqlDemoStack
from flink_sql_demo.download_connector import download_connector
from flink_sql_demo.utils import (
    check_glue_database,
    get_public_cidr,
    check_es_service_policy
)

# Download ES Connector
download_connector(
    url='https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7_2.11/1.11.2/flink-sql-connector-elasticsearch7_2.11-1.11.2.jar',
    filename='flink-sql-connector-elasticsearch7_2.11-1.11.2.jar'
)

app = core.App()
FlinkSqlDemoStack(app, "FlinkSqlDemoStack",
                  glueDBExists=check_glue_database('default'),
                  cidr=get_public_cidr(),
                  esServiceRoleExists=check_es_service_policy()
    # If you don't specify 'env', this stack will be environment-agnostic.
    # Account/Region-dependent features and context lookups will not work,
    # but a single synthesized template can be deployed anywhere.

    # Uncomment the next line to specialize this stack for the AWS Account
    # and Region that are implied by the current CLI configuration.

    #env=core.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), region=os.getenv('CDK_DEFAULT_REGION')),

    # Uncomment the next line if you know exactly what Account and Region you
    # want to deploy the stack to. */

    #env=core.Environment(account='123456789012', region='us-east-1'),

    # For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html
    )

app.synth()
