# This is a sample Python script.

import configparser
import json
import time
from enum import Enum

import boto3
import redshift_connector
from redshift_connector.core import Connection, Cursor

from sql_statements import *


# Enum to designate the status of the Redshift cluster
class ClusterStatus(Enum):
    NO_CLUSTER = 1
    UNAVAILABLE = 2
    AVAILABLE = 3
    DELETING = 4
    CREATING = 5


# Don't use access keys - just configure local .aws environment instead
ec2 = boto3.resource('ec2',
                     region_name="us-west-2"
                     )

s3 = boto3.resource('s3',
                    region_name="us-west-2"
                    )

iam = boto3.client('iam',
                   region_name='us-west-2'
                   )

redshift = boto3.client('redshift',
                        region_name="us-west-2"
                        )


# Utility routines


def check_cluster_available(configs: configparser.ConfigParser) -> (ClusterStatus, dict):
    """
    Describe the Redshift cluster and return
    TODO - handle scenarios where the cluster is being created, deleted, or otherwise unavailable

    :param configs: configurations
    :return: The ClusterStatus and the properties returned from redshift.describe_clusters()
    """
    cluster_name = configs.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    try:
        my_cluster_props = redshift.describe_clusters(ClusterIdentifier=cluster_name)['Clusters'][0]

    except Exception as e:
        return ClusterStatus.NO_CLUSTER, None

    # pretty_print_props(my_cluster_props)
    cluster_status = ClusterStatus.UNAVAILABLE
    if 'ClusterStatus' in my_cluster_props:
        cluster_status_str = my_cluster_props['ClusterStatus']

        if cluster_status_str == 'deleting':
            print(f"Cluster {cluster_name} is being deleted ...")
            exit(0)

        if cluster_status_str == 'available':
            print(f"Cluster {cluster_name} is up ...")
            cluster_status = ClusterStatus.AVAILABLE

        elif cluster_status_str == 'creating':
            print(f"Cluster {cluster_name} is being created ...")
            cluster_status = ClusterStatus.CREATING

    if cluster_status == ClusterStatus.AVAILABLE:
        configs.set("DWH", "DWH_ENDPOINT", my_cluster_props['Endpoint']['Address'])
        configs.set("IAM", "ARN", my_cluster_props['IamRoles'][0]['IamRoleArn'])
        return ClusterStatus.AVAILABLE, my_cluster_props

    return ClusterStatus.UNAVAILABLE, my_cluster_props


def wait_cluster_status(configs: configparser.ConfigParser, desired_status: ClusterStatus = ClusterStatus.AVAILABLE):
    """
    Poll for the availability of the Redshift cluster

    :param desired_status: what status are we waiting for?
    :param configs: configurations

    """
    while check_cluster_available(configs)[0] != desired_status:
        print("waiting 10 sec for cluster....")
        time.sleep(10)


def sg_open_port(my_cluster_props: dict, configs: configparser.ConfigParser):
    """
    Open the VPC security group to allow public access to Redshift

    :param my_cluster_props: Redshift cluster properties
    :param configs: configurations for this application
    """
    try:
        vpc = ec2.Vpc(id=my_cluster_props['VpcId'])
        redshift_port = configs.get("DWH", "DWH_PORT")
        default_sg = list(vpc.security_groups.all())[0]
        print(default_sg)
        default_sg.authorize_ingress(
            GroupName=default_sg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(redshift_port),
            ToPort=int(redshift_port)
        )
    except Exception as e:
        pass


def get_configs() -> configparser.ConfigParser:
    """
    Read the dwh.cfg file a load all into a configparser.ConfigParser object
    :return: the configurations object
    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    config.set("DWH", "ROLE_ARN", "")
    return config


# Main routines

def create_role_arn(configs: configparser.ConfigParser) -> str:
    """
    Create a service role to allow Redshift access to S3

    :param configs: configurations

    return: Arn name
    """
    dwh_iam_role_name = configs.get("DWH", "DWH_IAM_ROLE_NAME")
    existing_arn = None
    try:
        existing_arn = iam.get_role(RoleName=dwh_iam_role_name)['Role']['Arn']

    except Exception as ex:
        print("role does not exist")
        existing_arn = None

    if existing_arn is not None:
        print(f"Role {dwh_iam_role_name} exists....")
        configs.set("IAM", "ARN", existing_arn)
        return existing_arn

    try:
        print("1.1 Creating a new IAM Role")
        dwhRole = iam.create_role(
            Path='/',
            RoleName=dwh_iam_role_name,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)

    print("1.2 Attaching Policy")
    iam.attach_role_policy(RoleName=dwh_iam_role_name,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                           )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    role_arn = iam.get_role(RoleName=dwh_iam_role_name)['Role']['Arn']
    configs.set("IAM", "ARN", role_arn)
    return role_arn


def redshift_cluster_up(configs: configparser.ConfigParser) -> (ClusterStatus, dict):
    """
    Create a Redshift cluster based on the parameters in the provided configuration (configs)

    :param configs: configurations
    """
    cluster_status, props = check_cluster_available(configs)
    iam_role = configs.get("IAM", "ARN")
    if cluster_status == ClusterStatus.NO_CLUSTER:
        try:
            # Create Cluster
            response = redshift.create_cluster(
                # HW
                ClusterType=configs.get("DWH", "DWH_CLUSTER_TYPE"),
                NodeType=configs.get("DWH", "DWH_NODE_TYPE"),
                NumberOfNodes=int(configs.get("DWH", "DWH_NUM_NODES")),

                # Identifiers & Credentials
                DBName=configs.get("DWH", "DWH_DB"),
                ClusterIdentifier=configs.get("DWH", "DWH_CLUSTER_IDENTIFIER"),
                MasterUsername=configs.get("DWH", "DWH_DB_USER"),
                MasterUserPassword=configs.get("DWH", "DWH_DB_PASSWORD"),

                # Roles (for s3 access)
                IamRoles=[iam_role]
            )

            # Refresh properties
            cluster_status, props = check_cluster_available(configs)

            # Open port on default security group
            sg_open_port(props, configs)

        except Exception as e:
            print(e)

    if cluster_status != ClusterStatus.AVAILABLE:
        wait_cluster_status(configs)

    if cluster_status == ClusterStatus.AVAILABLE:
        configs.set("DWH", "DWH_ENDPOINT", props['Endpoint']['Address'])
        configs.set("IAM", "ARN", props['IamRoles'][0]['IamRoleArn'])

    return cluster_status, props


def redshift_cluster_down(configs: configparser.ConfigParser):
    """
    Delete the Redshift cluster if it exists and the attached s3 role

    :param configs: configuration data
    """
    available, props = check_cluster_available(configs)
    if available == ClusterStatus.AVAILABLE:
        print("Bring cluster down....")
        cluster_name = configs.get("DWH", "DWH_CLUSTER_IDENTIFIER")
        role_name = configs.get("DWH", "DWH_IAM_ROLE_NAME")
        redshift.delete_cluster(ClusterIdentifier=cluster_name, SkipFinalClusterSnapshot=True)
        iam.detach_role_policy(RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=role_name)


def connect_redshift(configs: configparser.ConfigParser) -> Connection:
    """
    Connect to Redshift

    :param configs: configuration object
    :return: the live Connection
    """
    db_user = configs.get("DWH", "DWH_DB_USER")
    db_password = configs.get("DWH", "DWH_DB_PASSWORD")
    endpoint = configs.get("DWH", "DWH_ENDPOINT")
    db_name = configs.get("DWH", "DWH_DB")

    conn = redshift_connector.connect(
        host=endpoint,
        database=db_name,
        user=db_user,
        password=db_password)

    cursor = conn.cursor()
    return conn


def create_schemas(conn: Connection):
    """
    Create stage and data schemas if they do not exist

    :param conn:
    :return:
    """
    cur = conn.cursor()
    cur.execute(f"create schema if not exists {STAGING_SCHEMA}")
    cur.execute(f"create schema if not exists {DHW_SCHEMA}")
    conn.commit()


def drop_tables(conn: Connection, cur: Cursor):
    """
    Drop all tables using the sql in the global drop_table_queries variable

    :param conn: Redshift connection
    :param cur: Redshift cursor
    """
    print("drop tables ...")
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur: Cursor, conn: Connection):
    """
    Create all tables using the sql in the global create_table_queries variable

    :param conn: Redshift connection
    :param cur: Redshift cursor
    """
    print("create tables ...")
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def init_database() -> (Connection, configparser.ConfigParser):
    """
    Main entry point for creating the Redshift cluster, the database and stag and data schemas

    :return:
        - The redshift_connection.Connection
        - A configuration object of type configparser.ConfigParser
    """
    # get configs
    config = get_configs()

    # create s3 access role
    role_arn = create_role_arn(configs=config)

    # create cluster
    redshift_cluster_up(config)

    # connect to cluster
    conn = connect_redshift(config)

    cursor = conn.cursor()

    # create schemas
    create_schemas(conn=conn)
    drop_tables(cur=cursor, conn=conn)
    create_tables(cur=cursor, conn=conn)

    return conn, config
