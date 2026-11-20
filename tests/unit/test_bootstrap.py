"""Test infra/aws/bootstrap.py against a simulated AWS (moto). Skipped when moto is not installed."""
import base64
import json
import sys
from pathlib import Path

import pytest

boto3 = pytest.importorskip("boto3")
moto = pytest.importorskip("moto")

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "infra" / "aws"))
import bootstrap  # noqa: E402

REGION = "us-east-1"


@pytest.fixture
def aws(monkeypatch):
    for k, v in dict(AWS_ACCESS_KEY_ID="x", AWS_SECRET_ACCESS_KEY="x", AWS_SESSION_TOKEN="x",
                     AWS_DEFAULT_REGION=REGION).items():
        monkeypatch.setenv(k, v)
    with moto.mock_aws():
        boto3.client("iam").create_instance_profile(InstanceProfileName="LabInstanceProfile")
        ami = boto3.client("ec2", region_name=REGION).describe_images()["Images"][0]["ImageId"]
        yield ami


def instances_by_role():
    ec2 = boto3.client("ec2", region_name=REGION)
    out = {}
    for r in ec2.describe_instances()["Reservations"]:
        for i in r["Instances"]:
            out.setdefault(next(t["Value"] for t in i["Tags"] if t["Key"] == "Role"), []).append(i)
    return out


def test_creates_everything(aws):
    bootstrap.main(["--ami", aws])
    sqs = boto3.client("sqs", region_name=REGION)
    url = sqs.get_queue_url(QueueName="v2x-events")["QueueUrl"]
    redrive = json.loads(sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["RedrivePolicy"])["Attributes"]["RedrivePolicy"])
    assert redrive["maxReceiveCount"] in (5, "5") and "v2x-events-dlq" in redrive["deadLetterTargetArn"]

    ddb = boto3.client("dynamodb", region_name=REGION)
    assert set(boto3.resource("dynamodb", region_name=REGION).meta.client.list_tables()["TableNames"]) >= {"v2x-events", "v2x-windows", "v2x-alerts"}
    windows = ddb.describe_table(TableName="v2x-windows")["Table"]["KeySchema"]
    assert windows == [{"AttributeName": "segment_id", "KeyType": "HASH"}, {"AttributeName": "window_start", "KeyType": "RANGE"}]
    assert ddb.describe_time_to_live(TableName="v2x-events")["TimeToLiveDescription"]["AttributeName"] == "expire_at"

    assert any(t["TopicArn"].endswith(":v2x-alerts") for t in boto3.client("sns", region_name=REGION).list_topics()["Topics"])
    repos = {r["repositoryName"] for r in boto3.client("ecr", region_name=REGION).describe_repositories()["repositories"]}
    assert repos == {"v2x-service", "v2x-rsu"}

    roles = instances_by_role()
    assert set(roles) == {"v2x-service", "rsu-1", "rsu-2"} and all(len(v) == 1 for v in roles.values())
    ec2 = boto3.client("ec2", region_name=REGION)
    for role in ("rsu-1", "rsu-2"):
        inst = roles[role][0]
        user_data = base64.b64decode(ec2.describe_instance_attribute(InstanceId=inst["InstanceId"], Attribute="userData")["UserData"]["Value"]).decode()
        assert f'RSU_ID="{role}"' in user_data and "__RSU_ID__" not in user_data
        assert inst["MetadataOptions"]["HttpPutResponseHopLimit"] == 2
    rsu_sg = ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": ["v2x-rsu-sg"]}])["SecurityGroups"][0]
    assert {p["FromPort"] for p in rsu_sg["IpPermissions"]} == {1883, 8080}


def test_running_twice_creates_nothing_new(aws):
    bootstrap.main(["--ami", aws])
    bootstrap.main(["--ami", aws])
    assert all(len(v) == 1 for v in instances_by_role().values())
