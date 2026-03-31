resource "aws_db_subnet_group" "telecom_db" {
    name = "${var.project}-db-subnet-group"
    subnet_ids = [
        aws_subnet.private_1.id,
        aws_subnet.private_2.id
    ]

    tags = {
        Name = "${var.project}-db-subnet-group"
    }
}

resource "aws_db_instance" "telecom_oltp" {
    identifier = "${var.project}-oltp"
    engine = "postgres"
    engine_version = "17"
    instance_class = "db.t3.micro"
    allocated_storage = 20
    db_name = "${var.project}"
    username = "postgres"
    password = var.db_password
    db_subnet_group_name = aws_db_subnet_group.telecom_db.name
    vpc_security_group_ids = [aws_security_group.rds.id]
    publicly_accessible = false
    skip_final_snapshot = true

    tags = {
        Name = "${var.project}-oltp-db"
    }
}