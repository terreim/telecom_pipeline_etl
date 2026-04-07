# ──────────────────────────────────────
# Public
# ──────────────────────────────────────
resource "aws_security_group" "public" {
  name = "${var.project}-public-sg"
  description = "Airflow webserver security group, inbound only"
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.project}-public-sg"
  }
}

# In case local network allows direct SSH again.
resource "aws_vpc_security_group_ingress_rule" "allow_http_ipv4" {
  security_group_id = aws_security_group.public.id
  ip_protocol       = "tcp"
  from_port         = 80
  to_port           = 80
  cidr_ipv4         = "27.72.57.173/32"
}

resource "aws_vpc_security_group_ingress_rule" "allow_custom_ipv4" {
  security_group_id = aws_security_group.public.id
  ip_protocol       = "tcp"
  from_port         = 8080
  to_port           = 8080
  cidr_ipv4         = "27.72.57.173/32"
}

resource "aws_vpc_security_group_ingress_rule" "allow_ssh_1_ipv4" {
  security_group_id = aws_security_group.public.id
  ip_protocol       = "tcp"
  from_port         = 22
  to_port           = 22
  cidr_ipv4         = "27.72.57.173/32"
}

resource "aws_vpc_security_group_ingress_rule" "allow_public_to_public" {
  security_group_id            = aws_security_group.public.id
  ip_protocol                  = "-1"
  referenced_security_group_id = aws_security_group.public.id
}

# Direct from EC2 Instance Connect.
resource "aws_vpc_security_group_ingress_rule" "allow_ssh_2_ipv4" {
  security_group_id = aws_security_group.public.id
  ip_protocol       = "tcp"
  from_port         = 22
  to_port           = 22
  cidr_ipv4         = "13.239.158.0/29"
}

resource "aws_vpc_security_group_egress_rule" "public_egress" {
  security_group_id = aws_security_group.public.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

# ──────────────────────────────────────
# Private
# ──────────────────────────────────────
resource "aws_security_group" "private" {
  name = "${var.project}-private-sg"
  description = "Internal pipeline services"
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.project}-private-sg"
  }
}

resource "aws_vpc_security_group_ingress_rule" "allow_public_to_private" {
  security_group_id            = aws_security_group.private.id
  ip_protocol                  = "-1"
  referenced_security_group_id = aws_security_group.public.id
}

resource "aws_vpc_security_group_ingress_rule" "allow_private_to_private" {
  security_group_id            = aws_security_group.private.id
  ip_protocol                  = "-1"
  referenced_security_group_id = aws_security_group.private.id
}

resource "aws_vpc_security_group_egress_rule" "private_egress" {
  security_group_id = aws_security_group.private.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

# ──────────────────────────────────────
# RDS
# ──────────────────────────────────────
resource "aws_security_group" "rds" {
  name = "${var.project}-rds-sg"
  description = "Allow access to Telecom pipeline RDS"
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.project}-rds-sg"
  }
}

resource "aws_vpc_security_group_ingress_rule" "allow_public_to_rds" {
  security_group_id            = aws_security_group.rds.id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  referenced_security_group_id = aws_security_group.public.id
}

resource "aws_vpc_security_group_ingress_rule" "allow_private_to_rds" {
  security_group_id            = aws_security_group.rds.id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  referenced_security_group_id = aws_security_group.private.id
}

resource "aws_vpc_security_group_egress_rule" "rds_egress" {
  security_group_id = aws_security_group.rds.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}