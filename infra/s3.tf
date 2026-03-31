resource "aws_s3_bucket" "main" {
  bucket = var.bucket
}

resource "aws_iam_policy" "s3_access" {
  name = "TelecomS3Access"
  description = "Policy to allow access to the S3 bucket for the Telecom pipeline"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid = "AllowS3BucketAccess",
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ],
        Resource = [
          aws_s3_bucket.main.arn,
          "${aws_s3_bucket.main.arn}/*"
        ]
      }
    ]
  })
}