#!/bin/bash
# EC2 user data for an RSU host. bootstrap.sh replaces __RSU_ID__ (rsu-1 or rsu-2).
dnf install -y docker
systemctl enable --now docker
mkdir -p /opt/v2x
echo "RSU_ID=__RSU_ID__" > /opt/v2x/rsu.env
cat > /opt/v2x/deploy.sh <<'DEPLOY'
#!/bin/bash
set -euo pipefail
TAG="$1"; REGION=us-east-1
. /opt/v2x/rsu.env
REG=$(aws sts get-caller-identity --query Account --output text).dkr.ecr.$REGION.amazonaws.com
# the public IP changes on every restart; the RSU uses it to subscribe to SNS
TOKEN=$(curl -sf -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 300')
PUBLIC_HOST=$(curl -sf -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/public-ipv4)
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $REG
docker pull $REG/v2x-rsu:$TAG
docker rm -f v2x-rsu 2>/dev/null || true
docker run -d --name v2x-rsu --restart unless-stopped -p 1883:1883 -p 8081:8081 \
  -e AWS_DEFAULT_REGION=$REGION -e RSU_ID=$RSU_ID -e EVENTS_QUEUE=v2x-events -e ALERTS_TOPIC=v2x-alerts \
  -e PUBLIC_HOST=$PUBLIC_HOST -e AGG_WINDOW_S=5 -e BUFFER_MAX=5000 $REG/v2x-rsu:$TAG
for i in $(seq 1 30); do curl -sf localhost:8081/stats && echo && exit 0; sleep 2; done
echo "health check failed"; exit 1
DEPLOY
chmod +x /opt/v2x/deploy.sh
