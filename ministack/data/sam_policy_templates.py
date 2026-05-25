"""
AWS SAM policy template definitions.

Source: https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-policy-template-list.html

Each entry maps a template name to a list of IAM policy statement dicts.
Parameter placeholders use the exact {"Ref": "PascalCaseName"} form from the
AWS documentation.  At changeset creation time the caller substitutes those
references with the values supplied in the SAM template.
"""

# fmt: off
SAM_POLICY_TEMPLATES: dict[str, list[dict]] = {

    # ------------------------------------------------------------------
    "AcmGetCertificatePolicy": [
        {
            "Effect": "Allow",
            "Action": ["acm:GetCertificate"],
            "Resource": {
                "Fn::Sub": [
                    "${certificateArn}",
                    {"certificateArn": {"Ref": "CertificateArn"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "AMIDescribePolicy": [
        {
            "Effect": "Allow",
            "Action": ["ec2:DescribeImages"],
            "Resource": "*",
        }
    ],

    # ------------------------------------------------------------------
    "AthenaQueryPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "athena:ListWorkGroups",
                "athena:GetExecutionEngine",
                "athena:GetExecutionEngines",
                "athena:GetNamespace",
                "athena:GetCatalogs",
                "athena:GetNamespaces",
                "athena:GetTables",
                "athena:GetTable",
            ],
            "Resource": "*",
        },
        {
            "Effect": "Allow",
            "Action": [
                "athena:StartQueryExecution",
                "athena:GetQueryResults",
                "athena:DeleteNamedQuery",
                "athena:GetNamedQuery",
                "athena:ListQueryExecutions",
                "athena:StopQueryExecution",
                "athena:GetQueryResultsStream",
                "athena:ListNamedQueries",
                "athena:CreateNamedQuery",
                "athena:GetQueryExecution",
                "athena:BatchGetNamedQuery",
                "athena:BatchGetQueryExecution",
                "athena:GetWorkGroup",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:athena:${AWS::Region}:${AWS::AccountId}:workgroup/${workgroupName}",
                    {"workgroupName": {"Ref": "WorkGroupName"}},
                ]
            },
        },
    ],

    # ------------------------------------------------------------------
    "AWSSecretsManagerGetSecretValuePolicy": [
        {
            "Effect": "Allow",
            "Action": ["secretsmanager:GetSecretValue"],
            "Resource": {
                "Fn::Sub": [
                    "${secretArn}",
                    {"secretArn": {"Ref": "SecretArn"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "AWSSecretsManagerRotationPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "secretsmanager:DescribeSecret",
                "secretsmanager:GetSecretValue",
                "secretsmanager:PutSecretValue",
                "secretsmanager:UpdateSecretVersionStage",
            ],
            "Resource": {
                "Fn::Sub": "arn:${AWS::Partition}:secretsmanager:${AWS::Region}:${AWS::AccountId}:secret:*"
            },
            "Condition": {
                "StringEquals": {
                    "secretsmanager:resource/AllowRotationLambdaArn": {
                        "Fn::Sub": [
                            "arn:${AWS::Partition}:lambda:${AWS::Region}:${AWS::AccountId}:function:${functionName}",
                            {"functionName": {"Ref": "FunctionName"}},
                        ]
                    }
                }
            },
        },
        {
            "Effect": "Allow",
            "Action": ["secretsmanager:GetRandomPassword"],
            "Resource": "*",
        },
    ],

    # ------------------------------------------------------------------
    "CloudFormationDescribeStacksPolicy": [
        {
            "Effect": "Allow",
            "Action": ["cloudformation:DescribeStacks"],
            "Resource": {
                "Fn::Sub": "arn:${AWS::Partition}:cloudformation:${AWS::Region}:${AWS::AccountId}:stack/*"
            },
        }
    ],

    # ------------------------------------------------------------------
    "CloudWatchDashboardPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "cloudwatch:GetDashboard",
                "cloudwatch:ListDashboards",
                "cloudwatch:PutDashboard",
                "cloudwatch:ListMetrics",
            ],
            "Resource": "*",
        }
    ],

    # ------------------------------------------------------------------
    "CloudWatchDescribeAlarmHistoryPolicy": [
        {
            "Effect": "Allow",
            "Action": ["cloudwatch:DescribeAlarmHistory"],
            "Resource": "*",
        }
    ],

    # ------------------------------------------------------------------
    "CloudWatchPutMetricPolicy": [
        {
            "Effect": "Allow",
            "Action": ["cloudwatch:PutMetricData"],
            "Resource": "*",
        }
    ],

    # ------------------------------------------------------------------
    "CodePipelineLambdaExecutionPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "codepipeline:PutJobSuccessResult",
                "codepipeline:PutJobFailureResult",
            ],
            "Resource": "*",
        }
    ],

    # ------------------------------------------------------------------
    "CodePipelineReadOnlyPolicy": [
        {
            "Effect": "Allow",
            "Action": ["codepipeline:ListPipelineExecutions"],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:codepipeline:${AWS::Region}:${AWS::AccountId}:${pipelinename}",
                    {"pipelinename": {"Ref": "PipelineName"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "CodeCommitCrudPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "codecommit:GitPull",
                "codecommit:GitPush",
                "codecommit:CreateBranch",
                "codecommit:DeleteBranch",
                "codecommit:GetBranch",
                "codecommit:ListBranches",
                "codecommit:MergeBranchesByFastForward",
                "codecommit:MergeBranchesBySquash",
                "codecommit:MergeBranchesByThreeWay",
                "codecommit:UpdateDefaultBranch",
                "codecommit:BatchDescribeMergeConflicts",
                "codecommit:CreateUnreferencedMergeCommit",
                "codecommit:DescribeMergeConflicts",
                "codecommit:GetMergeCommit",
                "codecommit:GetMergeOptions",
                "codecommit:BatchGetPullRequests",
                "codecommit:CreatePullRequest",
                "codecommit:DescribePullRequestEvents",
                "codecommit:GetCommentsForPullRequest",
                "codecommit:GetCommitsFromMergeBase",
                "codecommit:GetMergeConflicts",
                "codecommit:GetPullRequest",
                "codecommit:ListPullRequests",
                "codecommit:MergePullRequestByFastForward",
                "codecommit:MergePullRequestBySquash",
                "codecommit:MergePullRequestByThreeWay",
                "codecommit:PostCommentForPullRequest",
                "codecommit:UpdatePullRequestDescription",
                "codecommit:UpdatePullRequestStatus",
                "codecommit:UpdatePullRequestTitle",
                "codecommit:DeleteFile",
                "codecommit:GetBlob",
                "codecommit:GetFile",
                "codecommit:GetFolder",
                "codecommit:PutFile",
                "codecommit:DeleteCommentContent",
                "codecommit:GetComment",
                "codecommit:GetCommentsForComparedCommit",
                "codecommit:PostCommentForComparedCommit",
                "codecommit:PostCommentReply",
                "codecommit:UpdateComment",
                "codecommit:BatchGetCommits",
                "codecommit:CreateCommit",
                "codecommit:GetCommit",
                "codecommit:GetCommitHistory",
                "codecommit:GetDifferences",
                "codecommit:GetObjectIdentifier",
                "codecommit:GetReferences",
                "codecommit:GetTree",
                "codecommit:GetRepository",
                "codecommit:UpdateRepositoryDescription",
                "codecommit:ListTagsForResource",
                "codecommit:TagResource",
                "codecommit:UntagResource",
                "codecommit:GetRepositoryTriggers",
                "codecommit:PutRepositoryTriggers",
                "codecommit:TestRepositoryTriggers",
                "codecommit:UploadArchive",
                "codecommit:GetUploadArchiveStatus",
                "codecommit:CancelUploadArchive",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:codecommit:${AWS::Region}:${AWS::AccountId}:${repositoryName}",
                    {"repositoryName": {"Ref": "RepositoryName"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "CodeCommitReadPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "codecommit:GitPull",
                "codecommit:GetBranch",
                "codecommit:ListBranches",
                "codecommit:BatchDescribeMergeConflicts",
                "codecommit:DescribeMergeConflicts",
                "codecommit:GetMergeCommit",
                "codecommit:GetMergeOptions",
                "codecommit:BatchGetPullRequests",
                "codecommit:DescribePullRequestEvents",
                "codecommit:GetCommentsForPullRequest",
                "codecommit:GetCommitsFromMergeBase",
                "codecommit:GetMergeConflicts",
                "codecommit:GetPullRequest",
                "codecommit:ListPullRequests",
                "codecommit:GetBlob",
                "codecommit:GetFile",
                "codecommit:GetFolder",
                "codecommit:GetComment",
                "codecommit:GetCommentsForComparedCommit",
                "codecommit:BatchGetCommits",
                "codecommit:GetCommit",
                "codecommit:GetCommitHistory",
                "codecommit:GetDifferences",
                "codecommit:GetObjectIdentifier",
                "codecommit:GetReferences",
                "codecommit:GetTree",
                "codecommit:GetRepository",
                "codecommit:ListTagsForResource",
                "codecommit:GetRepositoryTriggers",
                "codecommit:TestRepositoryTriggers",
                "codecommit:GetUploadArchiveStatus",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:codecommit:${AWS::Region}:${AWS::AccountId}:${repositoryName}",
                    {"repositoryName": {"Ref": "RepositoryName"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "ComprehendBasicAccessPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "comprehend:BatchDetectKeyPhrases",
                "comprehend:DetectDominantLanguage",
                "comprehend:DetectEntities",
                "comprehend:BatchDetectEntities",
                "comprehend:DetectKeyPhrases",
                "comprehend:DetectSentiment",
                "comprehend:BatchDetectDominantLanguage",
                "comprehend:BatchDetectSentiment",
            ],
            "Resource": "*",
        }
    ],

    # ------------------------------------------------------------------
    "CostExplorerReadOnlyPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "ce:GetCostAndUsage",
                "ce:GetDimensionValues",
                "ce:GetReservationCoverage",
                "ce:GetReservationPurchaseRecommendation",
                "ce:GetReservationUtilization",
                "ce:GetTags",
            ],
            "Resource": "*",
        }
    ],

    # ------------------------------------------------------------------
    "DynamoDBBackupFullAccessPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:CreateBackup",
                "dynamodb:DescribeContinuousBackups",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:dynamodb:${AWS::Region}:${AWS::AccountId}:table/${tableName}",
                    {"tableName": {"Ref": "TableName"}},
                ]
            },
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:DeleteBackup",
                "dynamodb:DescribeBackup",
                "dynamodb:ListBackups",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:dynamodb:${AWS::Region}:${AWS::AccountId}:table/${tableName}/backup/*",
                    {"tableName": {"Ref": "TableName"}},
                ]
            },
        },
    ],

    # ------------------------------------------------------------------
    "DynamoDBCrudPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:GetItem",
                "dynamodb:DeleteItem",
                "dynamodb:PutItem",
                "dynamodb:Scan",
                "dynamodb:Query",
                "dynamodb:UpdateItem",
                "dynamodb:BatchWriteItem",
                "dynamodb:BatchGetItem",
                "dynamodb:DescribeTable",
                "dynamodb:ConditionCheckItem",
            ],
            "Resource": [
                {
                    "Fn::Sub": [
                        "arn:${AWS::Partition}:dynamodb:${AWS::Region}:${AWS::AccountId}:table/${tableName}",
                        {"tableName": {"Ref": "TableName"}},
                    ]
                },
                {
                    "Fn::Sub": [
                        "arn:${AWS::Partition}:dynamodb:${AWS::Region}:${AWS::AccountId}:table/${tableName}/index/*",
                        {"tableName": {"Ref": "TableName"}},
                    ]
                },
            ],
        }
    ],

    # ------------------------------------------------------------------
    "DynamoDBReadPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:GetItem",
                "dynamodb:Scan",
                "dynamodb:Query",
                "dynamodb:BatchGetItem",
                "dynamodb:DescribeTable",
            ],
            "Resource": [
                {
                    "Fn::Sub": [
                        "arn:${AWS::Partition}:dynamodb:${AWS::Region}:${AWS::AccountId}:table/${tableName}",
                        {"tableName": {"Ref": "TableName"}},
                    ]
                },
                {
                    "Fn::Sub": [
                        "arn:${AWS::Partition}:dynamodb:${AWS::Region}:${AWS::AccountId}:table/${tableName}/index/*",
                        {"tableName": {"Ref": "TableName"}},
                    ]
                },
            ],
        }
    ],

    # ------------------------------------------------------------------
    "DynamoDBReconfigurePolicy": [
        {
            "Effect": "Allow",
            "Action": ["dynamodb:UpdateTable"],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:dynamodb:${AWS::Region}:${AWS::AccountId}:table/${tableName}",
                    {"tableName": {"Ref": "TableName"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "DynamoDBRestoreFromBackupPolicy": [
        {
            "Effect": "Allow",
            "Action": ["dynamodb:RestoreTableFromBackup"],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:dynamodb:${AWS::Region}:${AWS::AccountId}:table/${tableName}/backup/*",
                    {"tableName": {"Ref": "TableName"}},
                ]
            },
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:PutItem",
                "dynamodb:UpdateItem",
                "dynamodb:DeleteItem",
                "dynamodb:GetItem",
                "dynamodb:Query",
                "dynamodb:Scan",
                "dynamodb:BatchWriteItem",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:dynamodb:${AWS::Region}:${AWS::AccountId}:table/${tableName}",
                    {"tableName": {"Ref": "TableName"}},
                ]
            },
        },
    ],

    # ------------------------------------------------------------------
    "DynamoDBStreamReadPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:DescribeStream",
                "dynamodb:GetRecords",
                "dynamodb:GetShardIterator",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:dynamodb:${AWS::Region}:${AWS::AccountId}:table/${tableName}/stream/${streamName}",
                    {
                        "tableName": {"Ref": "TableName"},
                        "streamName": {"Ref": "StreamName"},
                    },
                ]
            },
        },
        {
            "Effect": "Allow",
            "Action": ["dynamodb:ListStreams"],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:dynamodb:${AWS::Region}:${AWS::AccountId}:table/${tableName}/stream/*",
                    {"tableName": {"Ref": "TableName"}},
                ]
            },
        },
    ],

    # ------------------------------------------------------------------
    "DynamoDBWritePolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:PutItem",
                "dynamodb:UpdateItem",
                "dynamodb:BatchWriteItem",
            ],
            "Resource": [
                {
                    "Fn::Sub": [
                        "arn:${AWS::Partition}:dynamodb:${AWS::Region}:${AWS::AccountId}:table/${tableName}",
                        {"tableName": {"Ref": "TableName"}},
                    ]
                },
                {
                    "Fn::Sub": [
                        "arn:${AWS::Partition}:dynamodb:${AWS::Region}:${AWS::AccountId}:table/${tableName}/index/*",
                        {"tableName": {"Ref": "TableName"}},
                    ]
                },
            ],
        }
    ],

    # ------------------------------------------------------------------
    "EC2CopyImagePolicy": [
        {
            "Effect": "Allow",
            "Action": ["ec2:CopyImage"],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:ec2:${AWS::Region}:${AWS::AccountId}:image/${imageId}",
                    {"imageId": {"Ref": "ImageId"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "EC2DescribePolicy": [
        {
            "Effect": "Allow",
            "Action": ["ec2:DescribeRegions", "ec2:DescribeInstances"],
            "Resource": "*",
        }
    ],

    # ------------------------------------------------------------------
    "EcsRunTaskPolicy": [
        {
            "Effect": "Allow",
            "Action": ["ecs:RunTask"],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:ecs:${AWS::Region}:${AWS::AccountId}:task-definition/${taskDefinition}",
                    {"taskDefinition": {"Ref": "TaskDefinition"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "EFSWriteAccessPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "elasticfilesystem:ClientMount",
                "elasticfilesystem:ClientWrite",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:elasticfilesystem:${AWS::Region}:${AWS::AccountId}:file-system/${FileSystem}",
                    {"FileSystem": {"Ref": "FileSystem"}},
                ]
            },
            "Condition": {
                "StringEquals": {
                    "elasticfilesystem:AccessPointArn": {
                        "Fn::Sub": [
                            "arn:${AWS::Partition}:elasticfilesystem:${AWS::Region}:${AWS::AccountId}:access-point/${AccessPoint}",
                            {"AccessPoint": {"Ref": "AccessPoint"}},
                        ]
                    }
                }
            },
        }
    ],

    # ------------------------------------------------------------------
    "EKSDescribePolicy": [
        {
            "Effect": "Allow",
            "Action": ["eks:DescribeCluster", "eks:ListClusters"],
            "Resource": "*",
        }
    ],

    # ------------------------------------------------------------------
    "ElasticMapReduceAddJobFlowStepsPolicy": [
        {
            "Effect": "Allow",
            "Action": "elasticmapreduce:AddJobFlowSteps",
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:elasticmapreduce:${AWS::Region}:${AWS::AccountId}:cluster/${clusterId}",
                    {"clusterId": {"Ref": "ClusterId"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "ElasticMapReduceCancelStepsPolicy": [
        {
            "Effect": "Allow",
            "Action": "elasticmapreduce:CancelSteps",
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:elasticmapreduce:${AWS::Region}:${AWS::AccountId}:cluster/${clusterId}",
                    {"clusterId": {"Ref": "ClusterId"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "ElasticMapReduceModifyInstanceFleetPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "elasticmapreduce:ModifyInstanceFleet",
                "elasticmapreduce:ListInstanceFleets",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:elasticmapreduce:${AWS::Region}:${AWS::AccountId}:cluster/${clusterId}",
                    {"clusterId": {"Ref": "ClusterId"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "ElasticMapReduceModifyInstanceGroupsPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "elasticmapreduce:ModifyInstanceGroups",
                "elasticmapreduce:ListInstanceGroups",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:elasticmapreduce:${AWS::Region}:${AWS::AccountId}:cluster/${clusterId}",
                    {"clusterId": {"Ref": "ClusterId"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "ElasticMapReduceSetTerminationProtectionPolicy": [
        {
            "Effect": "Allow",
            "Action": "elasticmapreduce:SetTerminationProtection",
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:elasticmapreduce:${AWS::Region}:${AWS::AccountId}:cluster/${clusterId}",
                    {"clusterId": {"Ref": "ClusterId"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "ElasticMapReduceTerminateJobFlowsPolicy": [
        {
            "Effect": "Allow",
            "Action": "elasticmapreduce:TerminateJobFlows",
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:elasticmapreduce:${AWS::Region}:${AWS::AccountId}:cluster/${clusterId}",
                    {"clusterId": {"Ref": "ClusterId"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "ElasticsearchHttpPostPolicy": [
        {
            "Effect": "Allow",
            "Action": ["es:ESHttpPost", "es:ESHttpPut"],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:es:${AWS::Region}:${AWS::AccountId}:domain/${domainName}/*",
                    {"domainName": {"Ref": "DomainName"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "EventBridgePutEventsPolicy": [
        {
            "Effect": "Allow",
            "Action": "events:PutEvents",
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:events:${AWS::Region}:${AWS::AccountId}:event-bus/${eventBusName}",
                    {"eventBusName": {"Ref": "EventBusName"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "FilterLogEventsPolicy": [
        {
            "Effect": "Allow",
            "Action": ["logs:FilterLogEvents"],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:logs:${AWS::Region}:${AWS::AccountId}:log-group:${logGroupName}:log-stream:*",
                    {"logGroupName": {"Ref": "LogGroupName"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "FirehoseCrudPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "firehose:CreateDeliveryStream",
                "firehose:DeleteDeliveryStream",
                "firehose:DescribeDeliveryStream",
                "firehose:PutRecord",
                "firehose:PutRecordBatch",
                "firehose:UpdateDestination",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:firehose:${AWS::Region}:${AWS::AccountId}:deliverystream/${deliveryStreamName}",
                    {"deliveryStreamName": {"Ref": "DeliveryStreamName"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "FirehoseWritePolicy": [
        {
            "Effect": "Allow",
            "Action": ["firehose:PutRecord", "firehose:PutRecordBatch"],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:firehose:${AWS::Region}:${AWS::AccountId}:deliverystream/${deliveryStreamName}",
                    {"deliveryStreamName": {"Ref": "DeliveryStreamName"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "KinesisCrudPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "kinesis:AddTagsToStream",
                "kinesis:CreateStream",
                "kinesis:DecreaseStreamRetentionPeriod",
                "kinesis:DeleteStream",
                "kinesis:DescribeStream",
                "kinesis:DescribeStreamSummary",
                "kinesis:GetShardIterator",
                "kinesis:IncreaseStreamRetentionPeriod",
                "kinesis:ListTagsForStream",
                "kinesis:MergeShards",
                "kinesis:PutRecord",
                "kinesis:PutRecords",
                "kinesis:SplitShard",
                "kinesis:RemoveTagsFromStream",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:kinesis:${AWS::Region}:${AWS::AccountId}:stream/${streamName}",
                    {"streamName": {"Ref": "StreamName"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "KinesisStreamReadPolicy": [
        {
            "Effect": "Allow",
            "Action": ["kinesis:ListStreams", "kinesis:DescribeLimits"],
            "Resource": {
                "Fn::Sub": "arn:${AWS::Partition}:kinesis:${AWS::Region}:${AWS::AccountId}:stream/*"
            },
        },
        {
            "Effect": "Allow",
            "Action": [
                "kinesis:DescribeStream",
                "kinesis:DescribeStreamSummary",
                "kinesis:GetRecords",
                "kinesis:GetShardIterator",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:kinesis:${AWS::Region}:${AWS::AccountId}:stream/${streamName}",
                    {"streamName": {"Ref": "StreamName"}},
                ]
            },
        },
    ],

    # ------------------------------------------------------------------
    "KMSDecryptPolicy": [
        {
            "Effect": "Allow",
            "Action": "kms:Decrypt",
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:kms:${AWS::Region}:${AWS::AccountId}:key/${keyId}",
                    {"keyId": {"Ref": "KeyId"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "KMSEncryptPolicy": [
        {
            "Effect": "Allow",
            "Action": "kms:Encrypt",
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:kms:${AWS::Region}:${AWS::AccountId}:key/${keyId}",
                    {"keyId": {"Ref": "KeyId"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "LambdaInvokePolicy": [
        {
            "Effect": "Allow",
            "Action": ["lambda:InvokeFunction"],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:lambda:${AWS::Region}:${AWS::AccountId}:function:${functionName}*",
                    {"functionName": {"Ref": "FunctionName"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "MobileAnalyticsWriteOnlyAccessPolicy": [
        {
            "Effect": "Allow",
            "Action": ["mobileanalytics:PutEvents"],
            "Resource": "*",
        }
    ],

    # ------------------------------------------------------------------
    "OrganizationsListAccountsPolicy": [
        {
            "Effect": "Allow",
            "Action": ["organizations:ListAccounts"],
            "Resource": "*",
        }
    ],

    # ------------------------------------------------------------------
    "PinpointEndpointAccessPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "mobiletargeting:GetEndpoint",
                "mobiletargeting:UpdateEndpoint",
                "mobiletargeting:UpdateEndpointsBatch",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:mobiletargeting:${AWS::Region}:${AWS::AccountId}:apps/${pinpointApplicationId}/endpoints/*",
                    {"pinpointApplicationId": {"Ref": "PinpointApplicationId"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "PollyFullAccessPolicy": [
        {
            "Effect": "Allow",
            "Action": ["polly:GetLexicon", "polly:DeleteLexicon"],
            "Resource": [
                {
                    "Fn::Sub": [
                        "arn:${AWS::Partition}:polly:${AWS::Region}:${AWS::AccountId}:lexicon/${lexiconName}",
                        {"lexiconName": {"Ref": "LexiconName"}},
                    ]
                }
            ],
        },
        {
            "Effect": "Allow",
            "Action": [
                "polly:DescribeVoices",
                "polly:ListLexicons",
                "polly:PutLexicon",
                "polly:SynthesizeSpeech",
            ],
            "Resource": [
                {"Fn::Sub": "arn:${AWS::Partition}:polly:${AWS::Region}:${AWS::AccountId}:lexicon/*"}
            ],
        },
    ],

    # ------------------------------------------------------------------
    "RekognitionDetectOnlyPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "rekognition:DetectFaces",
                "rekognition:DetectLabels",
                "rekognition:DetectModerationLabels",
                "rekognition:DetectText",
            ],
            "Resource": "*",
        }
    ],

    # ------------------------------------------------------------------
    "RekognitionFacesManagementPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "rekognition:IndexFaces",
                "rekognition:DeleteFaces",
                "rekognition:SearchFaces",
                "rekognition:SearchFacesByImage",
                "rekognition:ListFaces",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:rekognition:${AWS::Region}:${AWS::AccountId}:collection/${collectionId}",
                    {"collectionId": {"Ref": "CollectionId"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "RekognitionFacesPolicy": [
        {
            "Effect": "Allow",
            "Action": ["rekognition:CompareFaces", "rekognition:DetectFaces"],
            "Resource": "*",
        }
    ],

    # ------------------------------------------------------------------
    "RekognitionLabelsPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "rekognition:DetectLabels",
                "rekognition:DetectModerationLabels",
            ],
            "Resource": "*",
        }
    ],

    # ------------------------------------------------------------------
    "RekognitionNoDataAccessPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "rekognition:CompareFaces",
                "rekognition:DetectFaces",
                "rekognition:DetectLabels",
                "rekognition:DetectModerationLabels",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:rekognition:${AWS::Region}:${AWS::AccountId}:collection/${collectionId}",
                    {"collectionId": {"Ref": "CollectionId"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "RekognitionReadPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "rekognition:ListCollections",
                "rekognition:ListFaces",
                "rekognition:SearchFaces",
                "rekognition:SearchFacesByImage",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:rekognition:${AWS::Region}:${AWS::AccountId}:collection/${collectionId}",
                    {"collectionId": {"Ref": "CollectionId"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "RekognitionWriteOnlyAccessPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "rekognition:CreateCollection",
                "rekognition:IndexFaces",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:rekognition:${AWS::Region}:${AWS::AccountId}:collection/${collectionId}",
                    {"collectionId": {"Ref": "CollectionId"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "Route53ChangeResourceRecordSetsPolicy": [
        {
            "Effect": "Allow",
            "Action": ["route53:ChangeResourceRecordSets"],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:route53:::hostedzone/${HostedZoneId}",
                    {"HostedZoneId": {"Ref": "HostedZoneId"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "S3CrudPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket",
                "s3:GetBucketLocation",
                "s3:GetObjectVersion",
                "s3:PutObject",
                "s3:PutObjectAcl",
                "s3:GetLifecycleConfiguration",
                "s3:PutLifecycleConfiguration",
                "s3:DeleteObject",
            ],
            "Resource": [
                {
                    "Fn::Sub": [
                        "arn:${AWS::Partition}:s3:::${bucketName}",
                        {"bucketName": {"Ref": "BucketName"}},
                    ]
                },
                {
                    "Fn::Sub": [
                        "arn:${AWS::Partition}:s3:::${bucketName}/*",
                        {"bucketName": {"Ref": "BucketName"}},
                    ]
                },
            ],
        }
    ],

    # ------------------------------------------------------------------
    "S3FullAccessPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectAcl",
                "s3:GetObjectVersion",
                "s3:PutObject",
                "s3:PutObjectAcl",
                "s3:DeleteObject",
                "s3:DeleteObjectTagging",
                "s3:DeleteObjectVersionTagging",
                "s3:GetObjectTagging",
                "s3:GetObjectVersionTagging",
                "s3:PutObjectTagging",
                "s3:PutObjectVersionTagging",
            ],
            "Resource": [
                {
                    "Fn::Sub": [
                        "arn:${AWS::Partition}:s3:::${bucketName}/*",
                        {"bucketName": {"Ref": "BucketName"}},
                    ]
                }
            ],
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation",
                "s3:GetLifecycleConfiguration",
                "s3:PutLifecycleConfiguration",
            ],
            "Resource": [
                {
                    "Fn::Sub": [
                        "arn:${AWS::Partition}:s3:::${bucketName}",
                        {"bucketName": {"Ref": "BucketName"}},
                    ]
                }
            ],
        },
    ],

    # ------------------------------------------------------------------
    "S3ReadPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket",
                "s3:GetBucketLocation",
                "s3:GetObjectVersion",
                "s3:GetLifecycleConfiguration",
            ],
            "Resource": [
                {
                    "Fn::Sub": [
                        "arn:${AWS::Partition}:s3:::${bucketName}",
                        {"bucketName": {"Ref": "BucketName"}},
                    ]
                },
                {
                    "Fn::Sub": [
                        "arn:${AWS::Partition}:s3:::${bucketName}/*",
                        {"bucketName": {"Ref": "BucketName"}},
                    ]
                },
            ],
        }
    ],

    # ------------------------------------------------------------------
    "S3WritePolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:PutObjectAcl",
                "s3:PutLifecycleConfiguration",
            ],
            "Resource": [
                {
                    "Fn::Sub": [
                        "arn:${AWS::Partition}:s3:::${bucketName}",
                        {"bucketName": {"Ref": "BucketName"}},
                    ]
                },
                {
                    "Fn::Sub": [
                        "arn:${AWS::Partition}:s3:::${bucketName}/*",
                        {"bucketName": {"Ref": "BucketName"}},
                    ]
                },
            ],
        }
    ],

    # ------------------------------------------------------------------
    "SageMakerCreateEndpointConfigPolicy": [
        {
            "Effect": "Allow",
            "Action": ["sagemaker:CreateEndpointConfig"],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:sagemaker:${AWS::Region}:${AWS::AccountId}:endpoint-config/${endpointConfigName}",
                    {"endpointConfigName": {"Ref": "EndpointConfigName"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "SageMakerCreateEndpointPolicy": [
        {
            "Effect": "Allow",
            "Action": ["sagemaker:CreateEndpoint"],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:sagemaker:${AWS::Region}:${AWS::AccountId}:endpoint/${endpointName}",
                    {"endpointName": {"Ref": "EndpointName"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "ServerlessRepoReadWriteAccessPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "serverlessrepo:CreateApplication",
                "serverlessrepo:CreateApplicationVersion",
                "serverlessrepo:GetApplication",
                "serverlessrepo:ListApplications",
                "serverlessrepo:ListApplicationVersions",
            ],
            "Resource": [
                {
                    "Fn::Sub": "arn:${AWS::Partition}:serverlessrepo:${AWS::Region}:${AWS::AccountId}:applications/*"
                }
            ],
        }
    ],

    # ------------------------------------------------------------------
    "SESBulkTemplatedCrudPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "ses:GetIdentityVerificationAttributes",
                "ses:SendEmail",
                "ses:SendRawEmail",
                "ses:SendTemplatedEmail",
                "ses:SendBulkTemplatedEmail",
                "ses:VerifyEmailIdentity",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:ses:${AWS::Region}:${AWS::AccountId}:identity/${identityName}",
                    {"identityName": {"Ref": "IdentityName"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "SESBulkTemplatedCrudPolicy_v2": [
        {
            "Effect": "Allow",
            "Action": [
                "ses:SendEmail",
                "ses:SendRawEmail",
                "ses:SendTemplatedEmail",
                "ses:SendBulkTemplatedEmail",
            ],
            "Resource": [
                {
                    "Fn::Sub": [
                        "arn:${AWS::Partition}:ses:${AWS::Region}:${AWS::AccountId}:identity/${identityName}",
                        {"identityName": {"Ref": "IdentityName"}},
                    ]
                },
                {
                    "Fn::Sub": [
                        "arn:${AWS::Partition}:ses:${AWS::Region}:${AWS::AccountId}:template/${templateName}",
                        {"templateName": {"Ref": "TemplateName"}},
                    ]
                },
            ],
        },
        {
            "Effect": "Allow",
            "Action": [
                "ses:GetIdentityVerificationAttributes",
                "ses:VerifyEmailIdentity",
            ],
            "Resource": "*",
        },
    ],

    # ------------------------------------------------------------------
    "SESCrudPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "ses:GetIdentityVerificationAttributes",
                "ses:SendEmail",
                "ses:SendRawEmail",
                "ses:VerifyEmailIdentity",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:ses:${AWS::Region}:${AWS::AccountId}:identity/${identityName}",
                    {"identityName": {"Ref": "IdentityName"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "SESEmailTemplateCrudPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "ses:CreateTemplate",
                "ses:GetTemplate",
                "ses:ListTemplates",
                "ses:UpdateTemplate",
                "ses:DeleteTemplate",
                "ses:TestRenderTemplate",
            ],
            "Resource": "*",
        }
    ],

    # ------------------------------------------------------------------
    "SESSendBouncePolicy": [
        {
            "Effect": "Allow",
            "Action": ["ses:SendBounce"],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:ses:${AWS::Region}:${AWS::AccountId}:identity/${identityName}",
                    {"identityName": {"Ref": "IdentityName"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "SNSCrudPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "sns:ListSubscriptionsByTopic",
                "sns:CreateTopic",
                "sns:SetTopicAttributes",
                "sns:Subscribe",
                "sns:Publish",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:sns:${AWS::Region}:${AWS::AccountId}:${topicName}*",
                    {"topicName": {"Ref": "TopicName"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "SNSPublishMessagePolicy": [
        {
            "Effect": "Allow",
            "Action": ["sns:Publish"],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:sns:${AWS::Region}:${AWS::AccountId}:${topicName}",
                    {"topicName": {"Ref": "TopicName"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "SQSPollerPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "sqs:ChangeMessageVisibility",
                "sqs:ChangeMessageVisibilityBatch",
                "sqs:DeleteMessage",
                "sqs:DeleteMessageBatch",
                "sqs:GetQueueAttributes",
                "sqs:ReceiveMessage",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:sqs:${AWS::Region}:${AWS::AccountId}:${queueName}",
                    {"queueName": {"Ref": "QueueName"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "SQSSendMessagePolicy": [
        {
            "Effect": "Allow",
            "Action": ["sqs:SendMessage*"],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:sqs:${AWS::Region}:${AWS::AccountId}:${queueName}",
                    {"queueName": {"Ref": "QueueName"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "SSMParameterReadPolicy": [
        {
            "Effect": "Allow",
            "Action": ["ssm:DescribeParameters"],
            "Resource": "*",
        },
        {
            "Effect": "Allow",
            "Action": [
                "ssm:GetParameters",
                "ssm:GetParameter",
                "ssm:GetParametersByPath",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:ssm:${AWS::Region}:${AWS::AccountId}:parameter/${parameterName}",
                    {"parameterName": {"Ref": "ParameterName"}},
                ]
            },
        },
    ],

    # ------------------------------------------------------------------
    "SSMParameterWithSlashPrefixReadPolicy": [
        {
            "Effect": "Allow",
            "Action": ["ssm:DescribeParameters"],
            "Resource": "*",
        },
        {
            "Effect": "Allow",
            "Action": [
                "ssm:GetParameters",
                "ssm:GetParameter",
                "ssm:GetParametersByPath",
            ],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:ssm:${AWS::Region}:${AWS::AccountId}:parameter${parameterName}",
                    {"parameterName": {"Ref": "ParameterName"}},
                ]
            },
        },
    ],

    # ------------------------------------------------------------------
    "StepFunctionsExecutionPolicy": [
        {
            "Effect": "Allow",
            "Action": ["states:StartExecution"],
            "Resource": {
                "Fn::Sub": [
                    "arn:${AWS::Partition}:states:${AWS::Region}:${AWS::AccountId}:stateMachine:${stateMachineName}",
                    {"stateMachineName": {"Ref": "StateMachineName"}},
                ]
            },
        }
    ],

    # ------------------------------------------------------------------
    "TextractDetectAnalyzePolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "textract:DetectDocumentText",
                "textract:StartDocumentTextDetection",
                "textract:StartDocumentAnalysis",
                "textract:AnalyzeDocument",
            ],
            "Resource": "*",
        }
    ],

    # ------------------------------------------------------------------
    "TextractGetResultPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "textract:GetDocumentTextDetection",
                "textract:GetDocumentAnalysis",
            ],
            "Resource": "*",
        }
    ],

    # ------------------------------------------------------------------
    "TextractPolicy": [
        {
            "Effect": "Allow",
            "Action": ["textract:*"],
            "Resource": "*",
        }
    ],

    # ------------------------------------------------------------------
    "VPCAccessPolicy": [
        {
            "Effect": "Allow",
            "Action": [
                "ec2:CreateNetworkInterface",
                "ec2:DeleteNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DetachNetworkInterface",
            ],
            "Resource": "*",
        }
    ],
}
# fmt: on
