provider "kafka" {
  bootstrap_servers = ["localhost:9092"]

  ca_cert_file     = "../secrets/snakeoil-ca-1.crt"
  client_cert_file = "../secrets/kafkacat-ca1-signed.pem"
  client_key_file  = "../secrets/kafkacat-raw-private-key.pem"
  tls_enabled      = true
  skip_tls_verify  = true
}

resource "kafka_topic" "syslog" {
  name               = "syslog"
  replication_factor = 1
  partitions         = 4

  config = {
    "segment.ms"   = "4000"
    "retention.ms" = "86400000"
  }
}

resource "kafka_acl" "test" {
  resource_name                = "syslog-*"
  resource_type                = "Topic"
  resource_pattern_type_filter = "prefixed"
  acl_principal                = "User:Alice"
  acl_host                     = "*"
  acl_operation                = "Write"
  acl_permission_type          = "Allow"
}
