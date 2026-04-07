resource "aws_efs_file_system" "clickhouse_data" {
  creation_token   = "telecom-clickhouse-data"
  performance_mode = "generalPurpose"
  throughput_mode  = "bursting"
  encrypted        = true

  tags = {
    Name = "${var.project}-clickhouse-data"
  }
}

resource "aws_efs_backup_policy" "clickhouse_data" {
  file_system_id = aws_efs_file_system.clickhouse_data.id
  backup_policy {
    status = "DISABLED"
  }
}

resource "aws_efs_mount_target" "clickhouse_private_1" {
  file_system_id  = aws_efs_file_system.clickhouse_data.id
  subnet_id       = aws_subnet.private_1.id
  security_groups = [aws_security_group.private.id]
}

resource "aws_efs_mount_target" "clickhouse_private_2" {
  file_system_id  = aws_efs_file_system.clickhouse_data.id
  subnet_id       = aws_subnet.private_2.id
  security_groups = [aws_security_group.private.id]
}