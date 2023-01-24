resource "aws_iam_role" "mesa-image-objectdetection-role-test" {
  name = "mesa-image-objectdetection-role-test"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "mesa-image-objectdetection-policy-test" {
  name = "mesa-image-objectdetection-policy-test"
  role = aws_iam_role.mesa-image-objectdetection-role-test.id

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [

    {
            "Action": [
                "secretsmanager:*",
                "kms:DescribeKey",
                "kms:ListAliases",
                "kms:ListKeys",
                "lambda:ListFunctions",
                "tag:GetResources"
            ],
            "Effect": "Allow",
            "Resource": "*"
        },
        {
            "Action": [
                "lambda:AddPermission",
                "lambda:CreateFunction",
                "lambda:GetFunction",
                "lambda:InvokeFunction",
                "lambda:UpdateFunctionConfiguration"
            ],
            "Effect": "Allow",
            "Resource": "arn:aws:lambda:*:*:function:SecretsManager*"
        },
        {
            "Action": [
                "serverlessrepo:CreateCloudFormationChangeSet",
                "serverlessrepo:GetApplication"
            ],
            "Effect": "Allow",
            "Resource": "arn:aws:serverlessrepo:*:*:applications/SecretsManager*"
        },
        {
            "Action": [
                "s3:GetObject"
            ],
            "Effect": "Allow",
            "Resource": [
                "arn:aws:s3:::awsserverlessrepo-changesets*",
                "arn:aws:s3:::secrets-manager-rotation-apps-*/*"
            ]
    },
    {
        "Effect": "Allow",
            "Action": [
                "rekognition:*"
            ],
            "Resource": "*"
        },
    {
      "Effect": "Allow",
      "Action": [
        "kinesis:GetRecords",
        "kinesis:GetShardIterator",
        "kinesis:DescribeStream",
        "kinesis:ListStreams"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "sqs:*"
      ],
      "Resource": "${aws_sqs_queue.mesa-image-objectdetection-rekognition-failed-test.arn}"
    }
  ]
}
EOF
}



resource "aws_kinesis_stream" "mesa-image-ingest-test" {
  name           = "mesa.image.ingest.test"
  shard_count    = 10
}

resource "aws_kinesis_stream" "mesa-image-objectdetection-failed-test" {
  name           = "mesa.image.objectdetection.failed.test"
  shard_count    = 10
}

resource "aws_sqs_queue" "mesa-image-objectdetection-rekognition-failed-test" {
  name = "mesa_image_objectdetection_rekognition_failed_test"
}

resource "aws_lambda_function" "mesa_image_objectdetection_rekognition_test" {
  function_name = "mesa_image_objectdetection_rekognition_test"
  #role          = "arn:aws:iam::548136830426 :role/mesa-image-objectdetection-role-test"
  role          = aws_iam_role.mesa-image-objectdetection-role-test.arn
  handler       = "___main_function_in_lambda___"
  runtime       = "python3.9"
  filename      = "lambda_function.zip"
  source_code_hash = filebase64sha256("lambda_function.zip")
  layers        =  ["arn:aws:lambda:us-west-2:548136830426:layer:pandas:1"]

  environment {
    variables = {
      KINESIS_STREAM_NAME = aws_kinesis_stream.mesa-image-ingest-test.name
    }
  }
  
  # ... other function configuration
}

resource "aws_lambda_event_source_mapping" "mesa-image-ingest-source-mapping-test" {
  event_source_arn = aws_kinesis_stream.mesa-image-ingest-test.arn
  function_name    = aws_lambda_function.mesa_image_objectdetection_rekognition_test.function_name
  batch_size       = 100
  starting_position = "TRIM_HORIZON"
  enabled          = true
  maximum_batching_window_in_seconds = 300
  parallelization_factor = 1
  maximum_retry_attempts = 100
  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.mesa-image-objectdetection-rekognition-failed-test.arn
    }
  }
}




 