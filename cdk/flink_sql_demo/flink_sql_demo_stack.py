from aws_cdk import (
    core as cdk,
    aws_kinesis as kinesis,
    aws_ec2 as ec2,
    aws_elasticsearch as es,
    aws_kinesisanalytics as kda,
    aws_iam as iam,
    aws_ecs as ecs,
    aws_ecs_patterns as ecs_patterns,
    aws_databrew as databrew,
    aws_s3 as s3,
    custom_resources as custom_resources,
    aws_logs as logs,
    aws_s3_deployment as s3_deployment,
    aws_events as events,
    aws_events_targets as events_target,
    aws_glue as glue
)



class FlinkSqlDemoStack(cdk.Stack):


    def __init__(self, scope: cdk.Construct, construct_id: str,
                 glueDBExists: bool,
                 cidr: str,
                 esServiceRoleExists: bool,
                 **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        region = FlinkSqlDemoStack.of(self).region
        account_id = FlinkSqlDemoStack.of(self).account


        connectors_bucket = s3.Bucket(
            self, 'kdaConnectorsBucket',
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        deploy_connector = s3_deployment.BucketDeployment(
            self, 'flinkConnectorDeployment',
            destination_bucket=connectors_bucket,
            sources=[s3_deployment.Source.asset('../connectors')]
        )

        vpc = ec2.Vpc(
            self, 'streamingVPC',
            cidr='10.0.0.0/16'
        )

        data_stream = kinesis.Stream(
            self, 'dataStream',
            shard_count=2
        )

        es_cluster_sg = ec2.SecurityGroup(
            self, 'esSG',
            vpc=vpc
        )
        es_cluster_sg.add_ingress_rule(
            peer=ec2.Peer.ipv4('10.0.0.0/16'),
            connection=ec2.Port.tcp(80)
        )

        if not esServiceRoleExists:
            es_service_linked_role = iam.CfnServiceLinkedRole(
                self, 'esServiceLinkedRole',
                aws_service_name='es.amazonaws.com',
                description='Role for ES to access resources in my VPC'
            )

        es_cluster = es.Domain(
            self, 'esCluster',
            removal_policy=cdk.RemovalPolicy.DESTROY,
            version=es.ElasticsearchVersion.V7_10,
            capacity=es.CapacityConfig(
                data_nodes=2,
                data_node_instance_type='t3.medium.elasticsearch'
            ),
            vpc=vpc,
            vpc_subnets=[ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PUBLIC
            )],
            ebs=es.EbsOptions(
                volume_size=100,
                volume_type=ec2.EbsDeviceVolumeType.GP2
            ),
            node_to_node_encryption=True,
            encryption_at_rest=es.EncryptionAtRestOptions(
                enabled=True
            ),
            enforce_https=False,
            security_groups=[es_cluster_sg],
            zone_awareness=es.ZoneAwarenessConfig(
                availability_zone_count=2
            ),
            access_policies=[
                iam.PolicyStatement(
                    actions=['es:*'],
                    effect=iam.Effect.ALLOW,
                    principals=[iam.ArnPrincipal('*')]
                )
            ]
        )

        es_proxy_service = ecs_patterns.ApplicationLoadBalancedFargateService(
            self, "esProxyService",
            vpc=vpc,
            listener_port=80,
            task_image_options=ecs_patterns.ApplicationLoadBalancedTaskImageOptions(
                image=ecs.ContainerImage.from_asset(
                    directory='../elasticsearch_proxy/'
                ),
                environment={
                    'ES_ENDPOINT': es_cluster.domain_endpoint
                }
            ),
            public_load_balancer=True
        )

        es_proxy_service.load_balancer.connections.allow_from(
            ec2.Peer.ipv4(cidr),
            port_range=ec2.Port.tcp(80)
        )
        if not glueDBExists:
            glue_database = glue.Database(
                self, 'glueDefaultDatabase',
                database_name='default'
            )

        glue_vpce = ec2.InterfaceVpcEndpoint(
            self, 'glueVPCe',
            vpc=vpc,
            service=ec2.InterfaceVpcEndpointAwsService.GLUE,
            open=True,
            subnets=ec2.SubnetSelection(
                one_per_az=True,
                subnet_type=ec2.SubnetType.PUBLIC
            )
        )

        kinesis_vpce = ec2.InterfaceVpcEndpoint(
            self, 'kinesisVPCe',
            vpc=vpc,
            service=ec2.InterfaceVpcEndpointAwsService.KINESIS_STREAMS,
            open=True,
            subnets=ec2.SubnetSelection(
                one_per_az=True,
                subnet_type=ec2.SubnetType.PUBLIC
            )
        )

        kda_policy = iam.ManagedPolicy(
            self, 'kdaPolicy',
            statements=[
                iam.PolicyStatement(
                    actions=[
                        'glue:GetTable', 'glue:GetTables',
                        'glue:CreateTable', 'glue:UpdateTable',
                        'glue:GetUserDefinedFunction','glue:GetDatabase',
                        'glue:GetDatabases','glue:GetConnection', 'glue:GetPartitions',
                        'glue:DeleteTable'
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[
                        'arn:aws:glue:{}:{}:*'.format(region, account_id),

                    ]
                ),
            ]
        )

        kda_role = iam.Role(
            self, 'kdaRole',
            assumed_by=iam.ServicePrincipal('kinesisanalytics.amazonaws.com'),
            managed_policies=[kda_policy]
        )

        data_stream.grant_read(kda_role)
        kda_role.add_managed_policy(
            policy=iam.ManagedPolicy.from_aws_managed_policy_name('AmazonVPCFullAccess')
        )

        connectors_bucket.grant_read_write(kda_role)


        kda_studio = kda.CfnApplicationV2(
            self, 'kdaStudio',
            runtime_environment='ZEPPELIN-FLINK-1_0',
            application_mode='INTERACTIVE',
            service_execution_role=kda_role.role_arn,
        )

        kda_sg = ec2.SecurityGroup(
            self, 'kdaSG',
            vpc=vpc,
            description='Security Group for KDA Studio'
        )


        data_sources = [
            'yellow',
            'fhv',
            'fhvhv',
            'green'
        ]

        for src in data_sources:
            databrew.CfnDataset(
                self, f'nycDataSet{src}',
                input=databrew.CfnDataset.InputProperty(
                    s3_input_definition=databrew.CfnDataset.S3LocationProperty(
                        bucket='nyc-tlc',
                        key=f'trip data/{src}_tripdata_2019-05.csv'
                    )
                ),
                format='CSV',
                format_options=databrew.CfnDataset.FormatOptionsProperty(
                    csv=databrew.CfnDataset.CsvOptionsProperty(
                        delimiter=',',
                        header_row=True
                    )
                ),
                name=f'tlc-trip-record-data-2019-may-{src}'

            )

        data_bucket = s3.Bucket(
            self, 'dataBucket',
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        brew_recipe = databrew.CfnRecipe(
            self, 'nycDataSetRecipe',
            name='tlc-trip-record-data-2019-may-recipe',
            steps=[
                databrew.CfnRecipe.RecipeStepProperty(
                    action=databrew.CfnRecipe.ActionProperty(
                        operation='UNION',
                        parameters={
                            "datasetsColumns": "[[\"PULocationID\",\"DOLocationID\",\"tpep_pickup_datetime\",\"tpep_dropoff_datetime\"],[\"PULocationID\",\"DOLocationID\",\"pickup_datetime\",\"dropoff_datetime\"],[\"PULocationID\",\"DOLocationID\",\"lpep_pickup_datetime\",\"lpep_dropoff_datetime\"],[\"PULocationID\",\"DOLocationID\",\"pickup_datetime\",\"dropoff_datetime\"]]",
                            "secondaryDatasetNames": "[\"tlc-trip-record-data-2019-may-fhv\",\"tlc-trip-record-data-2019-may-green\",\"tlc-trip-record-data-2019-may-fhvhv\"]",
                            "secondaryInputs": "[{\"S3InputDefinition\":{\"Bucket\":\"nyc-tlc\",\"Key\":\"trip data/fhv_tripdata_2019-05.csv\"}},{\"S3InputDefinition\":{\"Bucket\":\"nyc-tlc\",\"Key\":\"trip data/green_tripdata_2019-05.csv\"}},{\"S3InputDefinition\":{\"Bucket\":\"nyc-tlc\",\"Key\":\"trip data/fhvhv_tripdata_2019-05.csv\"}}]",
                            "targetColumnNames": "[\"PULocationID\",\"DOLocationID\",\"tpep_pickup_datetime\",\"tpep_dropoff_datetime\"]"
                        }
                    )
                )
            ]
        )

        brew_role = iam.Role(
            self, 'tlc-trip-record-data-2019-may-role',
            assumed_by=iam.ServicePrincipal('databrew.amazonaws.com'),
            inline_policies={
                'getDataPolicy': iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=['s3:ListBucket', 's3:GetObject'],
                            resources=['arn:aws:s3:::nyc-tlc/*', 'arn:aws:s3:::nyc-tlc']
                        )
                    ]
                )
            }
        )
        data_bucket.grant_read_write(brew_role)

        publish_recipe = custom_resources.AwsCustomResource(
            self, 'publishRecipe',
            policy=custom_resources.AwsCustomResourcePolicy.from_statements(
                statements=[
                    iam.PolicyStatement(
                        actions=['databrew:*'],
                        effect=iam.Effect.ALLOW,
                        resources=['*']
                    )
                ]
            ),
            log_retention=logs.RetentionDays.ONE_DAY,
            on_update=custom_resources.AwsSdkCall(
                physical_resource_id=custom_resources.PhysicalResourceId.of('CustomResourcePublishRecipe'),
                action='publishRecipe',
                service='DataBrew',
                parameters={
                    'Name': brew_recipe.name
                }
            ),
            on_delete=custom_resources.AwsSdkCall(
                action='deleteRecipeVersion',
                service='DataBrew',
                parameters={
                    'Name' : brew_recipe.name,
                    'RecipeVersion': '1.0'
                }
            )

        )


        brew_job = databrew.CfnJob(
            self, 'nycDataSetJob',
            role_arn=brew_role.role_arn,
            dataset_name=f'tlc-trip-record-data-2019-may-{data_sources[0]}',
            name=f'tlc-trip-record-data-2019-may-{data_sources[0]}-job',
            outputs=[
                databrew.CfnJob.OutputProperty(
                    location=databrew.CfnJob.S3LocationProperty(
                        bucket=data_bucket.bucket_name
                    ),
                    format='CSV',
                    compression_format='BZIP2',
                    overwrite=True,
                    format_options=databrew.CfnJob.OutputFormatOptionsProperty(
                        csv=databrew.CfnJob.CsvOutputOptionsProperty(
                            delimiter=','
                        )
                    )
                )
            ],
            recipe=databrew.CfnJob.RecipeProperty(
                name=brew_recipe.ref,
                version='1.0'
            ),
            type='RECIPE'

        )

        start_brew_job = custom_resources.AwsCustomResource(
            self, 'startBrewJob',
            policy=custom_resources.AwsCustomResourcePolicy.from_statements(
              statements=[
                  iam.PolicyStatement(
                      actions=['databrew:*'],
                      effect=iam.Effect.ALLOW,
                      resources=['*']
                  )
              ]
            ),
            log_retention=logs.RetentionDays.ONE_DAY,
            on_update=custom_resources.AwsSdkCall(
                physical_resource_id=custom_resources.PhysicalResourceId.of('CustomResourceStartBrewJob'),
                action='startJobRun',
                service='DataBrew',
                parameters={
                    'Name': brew_job.name
                }
            )

        )

        brew_job.node.add_dependency(publish_recipe)
        start_brew_job.node.add_dependency(brew_job)
        es_cluster.node.add_dependency(start_brew_job)

        producer_task_definition = ecs.TaskDefinition(
            self, 'producerTask',
            compatibility=ecs.Compatibility.FARGATE,
            cpu='4096',
            memory_mib='8192'
        )

        producer_task_definition.add_container(
            'producerContainer',
            image=ecs.ContainerImage.from_asset(
                    directory='../producer/'
            ),
            environment={
                'BUCKET_NAME' : data_bucket.bucket_name,
                'STREAM_NAME': data_stream.stream_name
            },
            cpu=4096,
            memory_limit_mib=8192,
            logging=ecs.LogDriver.aws_logs(
                stream_prefix='producerTask'
            )
        )

        data_stream.grant_write(producer_task_definition.task_role)
        data_bucket.grant_read(producer_task_definition.task_role)


        brew_job_completed_rule = events.Rule(
            self, 'brewJobeCompleted',
            enabled=True,
            event_pattern=events.EventPattern(
                source=['aws.databrew'],
                detail_type=['DataBrew Job State Change'],
                detail={
                    'jobName': [brew_job.name],
                    'state': ['SUCCEEDED']
                }
            ),
            targets=[
                events_target.EcsTask(
                    cluster=es_proxy_service.cluster,
                    task_definition=producer_task_definition,
                    task_count=1
                )
            ]
        )

        kibana_url = cdk.CfnOutput(
            self, 'kibanaUrl',
            value=f"http://{es_proxy_service.load_balancer.load_balancer_dns_name}/_plugin/kibana"
        )

        stream_name = cdk.CfnOutput(
            self, 'streamName',
            value=data_stream.stream_name
        )

        es_vpc_endpoint = cdk.CfnOutput(
            self, 'esVPCEndpoint',
            value=es_cluster.domain_endpoint
        )


