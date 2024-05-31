### Bootstrapping/configuration
###############################
variable "s3_bucket_arn" {
  type        = string
  default     = "*"
  description = "The ARN for the S3 bucket that the oxbow function will watch"
}

provider "aws" {
  shared_config_files = ["$HOME/.aws/config"]
  profile             = "AWSAdministratorAccess-851725189729"

  default_tags {
    tags = {
      ManagedBy   = "Terraform"
      environment = terraform.workspace
      workspace   = terraform.workspace
    }
  }
}

data "aws_iam_policy_document" "queue" {
  statement {
    effect = "Allow"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions   = ["sqs:SendMessage"]
    resources = ["arn:aws:sqs:*:*:*"]

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      # Allow all S3 buckets send notifications
      values = ["arn:aws:s3:::*"]
    }
  }
}
