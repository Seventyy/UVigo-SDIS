#!/usr/bin/env python3
"""Create (or re-check) every AWS resource of the V2X project in an AWS Academy Learner Lab account.

Safe to run again at any time: existing resources are kept, missing ones are created, stopped instances
are started, and a summary with the public IPs is printed at the end (the IPs change after a stop/start).

  python infra/aws/bootstrap.py [--ami ami-xxxx] [--instance-type t3.small]

Needs boto3 and the Learner Lab credentials in ~/.aws/credentials (they change every time you press Start Lab).
"""
import argparse
import json
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

REGION = "us-east-1"
HERE = Path(__file__).resolve().parent
INSTANCE_PROFILE = "LabInstanceProfile"
EVENTS_QUEUE, DLQ, MAX_RECEIVES = "v2x-events", "v2x-events-dlq", 5
ALERTS_TOPIC = "v2x-alerts"
REPOS = ["v2x-service", "v2x-rsu"]
# table -> (key schema, attribute types, TTL attribute); keys are the ones used by services/v2x-service/app.py
TABLES = {
    "v2x-events": ([("event_id", "HASH")], {"event_id": "S"}, "expire_at"),
    "v2x-windows": ([("segment_id", "HASH"), ("window_start", "RANGE")], {"segment_id": "S", "window_start": "N"}, "expire_at"),
    "v2x-alerts": ([("alert_id", "HASH")], {"alert_id": "S"}, None),
}
SECURITY_GROUPS = {"v2x-service-sg": [8080], "v2x-rsu-sg": [1883, 8080]}
# Role tag -> (security group, user-data file, RSU id). CI finds the instances by the Role tag.
INSTANCES = {
    "v2x-service": ("v2x-service-sg", "userdata-service.sh", None),
    "rsu-1": ("v2x-rsu-sg", "userdata-rsu.sh", "rsu-1"),
    "rsu-2": ("v2x-rsu-sg", "userdata-rsu.sh", "rsu-2"),
}


def say(msg):
    print(msg, flush=True)


def ensure_queues(sqs):
    try:
        dlq_url = sqs.get_queue_url(QueueName=DLQ)["QueueUrl"]
    except ClientError:
        dlq_url = sqs.create_queue(QueueName=DLQ)["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    attrs = {"VisibilityTimeout": "30",
             "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": str(MAX_RECEIVES)})}
    try:
        url = sqs.get_queue_url(QueueName=EVENTS_QUEUE)["QueueUrl"]
        sqs.set_queue_attributes(QueueUrl=url, Attributes=attrs)
    except ClientError:
        url = sqs.create_queue(QueueName=EVENTS_QUEUE, Attributes=attrs)["QueueUrl"]
    say(f"  SQS   {EVENTS_QUEUE} (+ dead-letter queue {DLQ}, redrive after {MAX_RECEIVES} receives)")
    return url


def ensure_tables(ddb):
    for name, (keys, types, ttl) in TABLES.items():
        try:
            ddb.describe_table(TableName=name)
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
            ddb.create_table(TableName=name, BillingMode="PAY_PER_REQUEST",
                             KeySchema=[{"AttributeName": k, "KeyType": t} for k, t in keys],
                             AttributeDefinitions=[{"AttributeName": k, "AttributeType": t} for k, t in types.items()])
            ddb.get_waiter("table_exists").wait(TableName=name)
        if ttl:
            try:
                ddb.update_time_to_live(TableName=name, TimeToLiveSpecification={"Enabled": True, "AttributeName": ttl})
            except ClientError as e:
                if "already" not in e.response["Error"]["Message"].lower():
                    raise
        say(f"  DDB   {name}")


def ensure_topic(sns):
    arn = sns.create_topic(Name=ALERTS_TOPIC)["TopicArn"]          # idempotent
    say(f"  SNS   {ALERTS_TOPIC}")
    return arn


def ensure_repos(ecr):
    for name in REPOS:
        try:
            ecr.describe_repositories(repositoryNames=[name])
        except ClientError as e:
            if e.response["Error"]["Code"] != "RepositoryNotFoundException":
                raise
            ecr.create_repository(repositoryName=name)
        say(f"  ECR   {name}")


def ensure_security_groups(ec2):
    vpc = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])["Vpcs"][0]["VpcId"]
    ids = {}
    for name, ports in SECURITY_GROUPS.items():
        found = ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": [name]},
                                                      {"Name": "vpc-id", "Values": [vpc]}])["SecurityGroups"]
        gid = found[0]["GroupId"] if found else ec2.create_security_group(
            GroupName=name, Description=f"V2X project: {name}", VpcId=vpc)["GroupId"]
        for port in ports:
            try:
                ec2.authorize_security_group_ingress(GroupId=gid, IpPermissions=[
                    {"IpProtocol": "tcp", "FromPort": port, "ToPort": port, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}])
            except ClientError as e:
                if e.response["Error"]["Code"] != "InvalidPermission.Duplicate":
                    raise
        ids[name] = gid
        say(f"  SG    {name} (tcp {', '.join(map(str, ports))} from anywhere)")
    return ids


def latest_ami(session_region=REGION):
    ssm = boto3.client("ssm", region_name=session_region)
    return ssm.get_parameter(Name="/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64")["Parameter"]["Value"]


def find_instance(ec2, role):
    resp = ec2.describe_instances(Filters=[{"Name": "tag:Role", "Values": [role]},
                                           {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]}])
    found = [i for r in resp["Reservations"] for i in r["Instances"]]
    return found[0] if found else None


def ensure_instances(ec2, sg_ids, ami, instance_type):
    ids = {}
    for role, (sg_name, userdata_file, rsu_id) in INSTANCES.items():
        inst = find_instance(ec2, role)
        if inst:
            state = inst["State"]["Name"]
            if state == "stopping":
                ec2.get_waiter("instance_stopped").wait(InstanceIds=[inst["InstanceId"]])
                state = "stopped"
            if state == "stopped":
                ec2.start_instances(InstanceIds=[inst["InstanceId"]])
            ids[role] = inst["InstanceId"]
            say(f"  EC2   {role}: already exists ({state})")
            continue
        user_data = (HERE / userdata_file).read_text()
        if rsu_id:
            user_data = user_data.replace("__RSU_ID__", rsu_id)
        resp = ec2.run_instances(
            ImageId=ami, InstanceType=instance_type, MinCount=1, MaxCount=1,
            SecurityGroupIds=[sg_ids[sg_name]], IamInstanceProfile={"Name": INSTANCE_PROFILE}, UserData=user_data,
            # hop limit 2: lets the containers read the instance credentials and public IP from the metadata service
            MetadataOptions={"HttpTokens": "required", "HttpPutResponseHopLimit": 2, "HttpEndpoint": "enabled"},
            TagSpecifications=[{"ResourceType": "instance",
                                "Tags": [{"Key": "Name", "Value": role}, {"Key": "Role", "Value": role}]}])
        ids[role] = resp["Instances"][0]["InstanceId"]
        say(f"  EC2   {role}: created {ids[role]}")
    ec2.get_waiter("instance_running").wait(InstanceIds=list(ids.values()))
    return ids


def summary(ec2, instance_ids, queue_url, topic_arn):
    say("\n=== SUMMARY ===")
    say(f"SQS queue : {queue_url}")
    say(f"SNS topic : {topic_arn}")
    resp = ec2.describe_instances(InstanceIds=list(instance_ids.values()))
    ips = {}
    for r in resp["Reservations"]:
        for i in r["Instances"]:
            role = next(t["Value"] for t in i["Tags"] if t["Key"] == "Role")
            ips[role] = i.get("PublicIpAddress", "-")
    for role in INSTANCES:
        say(f"{role:12s} {instance_ids[role]}  public IP: {ips.get(role)}")
    say("\nService dashboard : http://%s:8080" % ips.get("v2x-service"))
    say("Simulator flags  : --rsu1 %s:1883 --rsu2 %s:1883" % (ips.get("rsu-1"), ips.get("rsu-2")))
    say("(the IPs change after every stop/start: run this script again to see them)")


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--ami", help="AMI id (default: latest Amazon Linux 2023, looked up through SSM)")
    ap.add_argument("--instance-type", default="t3.small")
    args = ap.parse_args(argv)

    try:
        who = boto3.client("sts", region_name=REGION).get_caller_identity()
    except (ClientError, NoCredentialsError) as e:
        sys.exit(f"AWS credentials missing or expired ({e}).\nPress Start Lab, then AWS Details > Show and copy the "
                 "[default] block into ~/.aws/credentials")
    say(f"AWS account {who['Account']}, region {REGION}\n")

    sqs, ddb, sns, ecr, ec2 = (boto3.client(s, region_name=REGION) for s in ("sqs", "dynamodb", "sns", "ecr", "ec2"))
    queue_url = ensure_queues(sqs)
    ensure_tables(ddb)
    topic_arn = ensure_topic(sns)
    ensure_repos(ecr)
    sg_ids = ensure_security_groups(ec2)
    ami = args.ami or latest_ami()
    instance_ids = ensure_instances(ec2, sg_ids, ami, args.instance_type)
    summary(ec2, instance_ids, queue_url, topic_arn)


if __name__ == "__main__":
    main()
