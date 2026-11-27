output "stream_name" { value = aws_kinesis_stream.events.name }
output "stream_arn" { value = aws_kinesis_stream.events.arn }
output "firehose_name" { value = aws_kinesis_firehose_delivery_stream.raw.name }
