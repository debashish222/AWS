import boto3
import pandas as pd

def get_tags(resource_arn, region):
    client = boto3.client('rds', region_name=region)
    response = client.list_tags_for_resource(ResourceName=resource_arn)
    tags = response.get('TagList', [])
    return tags

def get_rds_instances():
    # List of AWS US region codes
    us_regions = [
        'us-east-1', 'us-east-2', 'us-west-1', 'us-west-2'
    ]
    
    rds_instances = {}

    for region in us_regions:
        print(f"Fetching RDS instances in region: {region}")
        client = boto3.client('rds', region_name=region)
        
        instances = []
        next_token = None
        while True:
            response = client.describe_db_instances(Marker=next_token) if next_token else client.describe_db_instances()
            for instance in response['DBInstances']:
                instance_info = {
                    'AccountID': boto3.client('sts').get_caller_identity().get('Account'),
                    'DBIdentifier': instance['DBInstanceIdentifier'],
                    'Status': instance['DBInstanceStatus'],
                    'DBClusterIdentifier': instance.get('DBClusterIdentifier', '-'),
                    'Role': instance.get('DBInstanceRole', '-'),
                    'Engine': instance['Engine'],
                    'EngineVersion': instance['EngineVersion'],
                    'RegionAZ': f"{region}-{instance['AvailabilityZone']}",
                    'AllocatedStorage': instance['AllocatedStorage'],
                    'Actions': ', '.join(instance.get('PendingModifiedValues', {}).keys()),
                    'CPU': instance.get('ProcessorFeatures', ['-'])[0],
                    'CurrentActivity': instance.get('ActivityStreamStatus', '-'),
                    'Maintenance': instance.get('PendingMaintenanceActions', []),
                    'VPC': instance['DBSubnetGroup']['VpcId'],
                    'MultiAZ': instance['MultiAZ'],
                    'StorageType': instance['StorageType'],
                    'Storage': instance['AllocatedStorage'],
                    'ProvisionedIOPS': instance.get('Iops', 0),
                    'StorageThroughput': instance.get('Throughput', 0),
                    'SecurityGroups': ', '.join([sg['VpcSecurityGroupId'] for sg in instance['VpcSecurityGroups']]),
                    'DBSubnetGroupName': instance['DBSubnetGroup']['DBSubnetGroupName'],
                    'DBCertificateExpiry': instance.get('CertificateRotation', {}).get('CertificateExpiry', '-'),
                    'PendingChanges': instance.get('PendingModifiedValues', {}),
                    'CharacterSet': instance.get('CharacterSet', '-'),
                    'OptionGroup': instance['OptionGroupMemberships'][0]['OptionGroupName'],
                    'CreatedTime': instance['InstanceCreateTime'].strftime('%Y-%m-%d %H:%M:%S'),
                    'Encrypted': instance['StorageEncrypted'],
                    'PriorityTier': instance.get('Tier', '-'),
                    'ReplicaLag': ', '.join(instance.get('ReadReplicaDBInstanceIdentifiers', [])),
                }
                
                # Fetch and include tags for each instance
                tags = get_tags(instance['DBInstanceArn'], region)
                for tag in tags:
                    instance_info[f"Tag_{tag['Key']}"] = tag['Value']
                
                instances.append(instance_info)

            next_token = response.get('Marker')
            if not next_token:
                break
        
        rds_instances[region] = instances
    
    return rds_instances

if __name__ == "__main__":
    rds_instances = get_rds_instances()
    
    with pd.ExcelWriter('rds_instances.xlsx') as writer:
        for region, instances in rds_instances.items():
            df = pd.DataFrame(instances)
            df.to_excel(writer, sheet_name=region, index=False)
           
