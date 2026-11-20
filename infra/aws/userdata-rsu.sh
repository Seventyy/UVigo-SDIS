#!/bin/bash
# EC2 user data for an RSU instance. bootstrap.py replaces __RSU_ID__ with rsu-1 or rsu-2.
dnf install -y docker
systemctl enable --now docker
mkdir -p /opt/v2x
cat > /opt/v2x/deploy.sh <<'EOS'
#!/bin/bash
set -euo pipefail
TAG="$1"; REGION=us-east-1; RSU_ID="__RSU_ID__"
REG=$(aws sts get-caller-identity --query Account --output text).dkr.ecr.$REGION.amazonaws.com
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $REG
docker pull $REG/v2x-rsu:$TAG
docker rm -f v2x-rsu 2>/dev/null || true
docker run -d --name v2x-rsu --restart unless-stopped -p 1883:1883 -p 8080:8080 \
  -e RSU_ID=$RSU_ID -e AWS_DEFAULT_REGION=$REGION $REG/v2x-rsu:$TAG
for i in $(seq 1 30); do curl -sf localhost:8080/health && echo && exit 0; sleep 2; done
echo "health check failed"; exit 1
EOS
chmod +x /opt/v2x/deploy.sh
