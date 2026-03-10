-- Grant S3/remote source access to the default user
-- Required for s3() table function in ClickHouse 24.8+
GRANT SOURCES ON *.* TO default;
GRANT NAMED COLLECTION ON * TO default;
