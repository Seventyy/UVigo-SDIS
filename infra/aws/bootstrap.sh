#!/usr/bin/env bash
# infra/aws/bootstrap.sh
# Creates (or reuses) every AWS resource of the V2X system. Safe to run many times.
#
#   bash infra/aws/bootstrap.sh
#
# Needs: AWS CLI v2 with a live Learner Lab session (credentials in ~/.aws/credentials).
# Optional env vars: AWS_REGION (us-east-1), INSTANCE_TYPE (t3.small), MQTT_CIDR (0.0.0.0/0)
set -euo pipefail

export AWS_PAGER=""
export MSYS_NO_PATHCONV=1            # Git Bash on Windows: do not rewrite arguments that look like paths
REGION="${AWS_REGION:-us-east-1}"
export AWS_DEFAULT_REGION="$REGION"
INSTANCE_TYPE="${INSTANCE_TYPE:-t3.small}"
MQTT_CIDR="${MQTT_CIDR:-0.0.0.0/0}"  # who may reach the RSU MQTT port (the vehicles' laptops)
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# aws.exe on Windows can end lines with \r, which would corrupt every variable we capture.
aws() { command aws "$@" | tr -d '\r'; }
log() { echo "==> $*"; }

ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
log "account $ACCOUNT, region $REGION"

# ---------------------------------------------------------------- SQS (+ dead-letter queue)
queue_url() { aws sqs get-queue-url --queue-name "$1" --query QueueUrl --output text 2>/dev/null || true; }

DLQ_URL="$(queue_url v2x-events-dlq)"
if [ -z "$DLQ_URL" ]; then
  DLQ_URL="$(aws sqs create-queue --queue-name v2x-events-dlq --query QueueUrl --output text)"
fi
aws sqs set-queue-attributes --queue-url "$DLQ_URL" --attributes '{"MessageRetentionPeriod":"1209600"}'
DLQ_ARN="$(aws sqs get-queue-attributes --queue-url "$DLQ_URL" --attribute-names QueueArn --query Attributes.QueueArn --output text)"

QUEUE_URL="$(queue_url v2x-events)"
if [ -z "$QUEUE_URL" ]; then
  QUEUE_URL="$(aws sqs create-queue --queue-name v2x-events --query QueueUrl --output text)"
fi
REDRIVE="{\"deadLetterTargetArn\":\"$DLQ_ARN\",\"maxReceiveCount\":\"5\"}"
ATTRS="{\"VisibilityTimeout\":\"30\",\"MessageRetentionPeriod\":\"345600\",\"ReceiveMessageWaitTimeSeconds\":\"20\",\"RedrivePolicy\":\"${REDRIVE//\"/\\\"}\"}"
aws sqs set-queue-attributes --queue-url "$QUEUE_URL" --attributes "$ATTRS"
log "SQS v2x-events (+ DLQ, maxReceiveCount 5) ready"

# ---------------------------------------------------------------- SNS
TOPIC_ARN="$(aws sns create-topic --name v2x-alerts --query TopicArn --output text)"
log "SNS v2x-alerts ready: $TOPIC_ARN"

# ---------------------------------------------------------------- DynamoDB (keys and TTL taken from app.py)
ensure_table() {
  local name="$1"; shift
  if aws dynamodb describe-table --table-name "$name" >/dev/null 2>&1; then
    log "table $name already exists"
  else
    aws dynamodb create-table --table-name "$name" --billing-mode PAY_PER_REQUEST "$@" >/dev/null
    aws dynamodb wait table-exists --table-name "$name"
    log "table $name created"
  fi
}
ensure_ttl() {
  local status
  status="$(aws dynamodb describe-time-to-live --table-name "$1" --query TimeToLiveDescription.TimeToLiveStatus --output text)"
  case "$status" in
    ENABLED|ENABLING) ;;
    *) aws dynamodb update-time-to-live --table-name "$1" \
         --time-to-live-specification "Enabled=true,AttributeName=expire_at" >/dev/null ;;
  esac
}

ensure_table v2x-events \
  --attribute-definitions AttributeName=event_id,AttributeType=S \
  --key-schema AttributeName=event_id,KeyType=HASH
ensure_table v2x-windows \
  --attribute-definitions AttributeName=segment_id,AttributeType=S AttributeName=window_start,AttributeType=N \
  --key-schema AttributeName=segment_id,KeyType=HASH AttributeName=window_start,KeyType=RANGE
ensure_table v2x-alerts \
  --attribute-definitions AttributeName=alert_id,AttributeType=S \
  --key-schema AttributeName=alert_id,KeyType=HASH
ensure_ttl v2x-events
ensure_ttl v2x-windows

# ---------------------------------------------------------------- ECR
for repo in v2x-service v2x-rsu; do
  if aws ecr describe-repositories --repository-names "$repo" >/dev/null 2>&1; then
    log "ECR $repo already exists"
  else
    aws ecr create-repository --repository-name "$repo" >/dev/null
    log "ECR $repo created"
  fi
done

# ---------------------------------------------------------------- security groups (default VPC)
VPC_ID="$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)"

ensure_sg() {   # name, description -> prints the group id
  local id
  id="$(aws ec2 describe-security-groups --filters "Name=group-name,Values=$1" "Name=vpc-id,Values=$VPC_ID" \
        --query 'SecurityGroups[0].GroupId' --output text)"
  if [ -z "$id" ] || [ "$id" = "None" ]; then
    id="$(aws ec2 create-security-group --group-name "$1" --description "$2" --vpc-id "$VPC_ID" \
          --query GroupId --output text)"
  fi
  echo "$id"
}
open_port() {   # group-id, port, cidr (an already-existing rule is fine)
  local out
  out="$(aws ec2 authorize-security-group-ingress --group-id "$1" --protocol tcp --port "$2" --cidr "$3" 2>&1)" || {
    echo "$out" | grep -q "InvalidPermission.Duplicate" || { echo "$out" >&2; exit 1; }
  }
}

SG_SERVICE="$(ensure_sg v2x-service-sg 'V2X-SERVICE dashboard and API')"
SG_RSU="$(ensure_sg v2x-rsu-sg 'RSU: MQTT for vehicles, HTTP for SNS')"
open_port "$SG_SERVICE" 8080 0.0.0.0/0
open_port "$SG_RSU" 1883 "$MQTT_CIDR"
open_port "$SG_RSU" 8081 0.0.0.0/0     # SNS must reach the RSU from AWS addresses
log "security groups ready ($SG_SERVICE, $SG_RSU)"

# ---------------------------------------------------------------- EC2 instances
AMI="$(aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
       --query Parameter.Value --output text)"

ensure_instance() {   # name, security-group, user-data text, tags (e.g. "{Key=Role,Value=rsu}")
  local name="$1" sg="$2" userdata="$3" tags="$4" id state
  id="$(aws ec2 describe-instances \
        --filters "Name=tag:Name,Values=$name" Name=instance-state-name,Values=pending,running,stopping,stopped \
        --query 'Reservations[0].Instances[0].InstanceId' --output text)"
  if [ -z "$id" ] || [ "$id" = "None" ]; then
    id="$(aws ec2 run-instances --image-id "$AMI" --instance-type "$INSTANCE_TYPE" \
          --security-group-ids "$sg" --iam-instance-profile Name=LabInstanceProfile \
          --user-data "$userdata" \
          --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$name},$tags]" \
          --query 'Instances[0].InstanceId' --output text)"
    log "instance $name launched ($id)"
  else
    state="$(aws ec2 describe-instances --instance-ids "$id" --query 'Reservations[0].Instances[0].State.Name' --output text)"
    if [ "$state" = "stopped" ]; then
      aws ec2 start-instances --instance-ids "$id" >/dev/null
      log "instance $name was stopped, starting it ($id)"
    else
      log "instance $name already $state ($id)"
    fi
  fi
  aws ec2 wait instance-running --instance-ids "$id"
  echo "$id"
}

SERVICE_ID="$(ensure_instance v2x-service "$SG_SERVICE" "$(cat "$HERE/userdata-service.sh")" '{Key=Role,Value=v2x-service}' | tail -n 1)"
RSU1_UD="$(sed 's/__RSU_ID__/rsu-1/g' "$HERE/userdata-rsu.sh")"
RSU2_UD="$(sed 's/__RSU_ID__/rsu-2/g' "$HERE/userdata-rsu.sh")"
RSU1_ID="$(ensure_instance rsu-1 "$SG_RSU" "$RSU1_UD" '{Key=Role,Value=rsu},{Key=RsuId,Value=rsu-1}' | tail -n 1)"
RSU2_ID="$(ensure_instance rsu-2 "$SG_RSU" "$RSU2_UD" '{Key=Role,Value=rsu},{Key=RsuId,Value=rsu-2}' | tail -n 1)"

ip_of() { aws ec2 describe-instances --instance-ids "$1" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text; }

echo
echo "=================== READY ==================="
echo "SQS queue     : $QUEUE_URL"
echo "SNS topic     : $TOPIC_ARN"
echo "ECR registry  : $ACCOUNT.dkr.ecr.$REGION.amazonaws.com  (v2x-service, v2x-rsu)"
echo "V2X-SERVICE   : http://$(ip_of "$SERVICE_ID"):8080"
echo "RSU 1         : mqtt://$(ip_of "$RSU1_ID"):1883   http://$(ip_of "$RSU1_ID"):8081"
echo "RSU 2         : mqtt://$(ip_of "$RSU2_ID"):1883   http://$(ip_of "$RSU2_ID"):8081"
echo "Instances start empty: containers appear after CI pushes the images and runs deploy.sh."
