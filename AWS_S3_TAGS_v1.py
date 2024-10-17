import boto3
import csv
import pandas as pd
from tqdm import tqdm
from botocore.exceptions import ClientError

# Initialize the S3 client
s3_client = boto3.client('s3')

# List all S3 buckets
buckets = s3_client.list_buckets()

# Create an empty DataFrame to store the data
df = pd.DataFrame(columns=["Bucket Name", "Creation Date", "Access"])

# Count the total number of buckets
total_buckets = len(buckets['Buckets'])

# Use tqdm for a progress bar
with tqdm(total=total_buckets, desc="Progress", unit="bucket") as pbar:
    for bucket_info in buckets['Buckets']:
        bucket_name = bucket_info['Name']
        try:
            # Attempt to retrieve bucket creation date
            bucket_creation_date = s3_client.head_bucket(Bucket=bucket_name)['ResponseMetadata']['HTTPHeaders']['date']
            # Retrieve bucket ACL for access information
            bucket_acl = s3_client.get_bucket_acl(Bucket=bucket_name)
            # Extract the display names of grantees for access
            grants = bucket_acl.get('Grants', [])
            access_list = []
            for grant in grants:
                grantee = grant.get('Grantee', {})
                display_name = grantee.get('DisplayName', 'N/A')  # Default to 'N/A' if DisplayName is not present
                access_list.append(display_name)
            access = ", ".join(access_list)

            # Initialize a dictionary for tags
            tags = {}

            # Check if tags exist for the bucket
            try:
                tags_response = s3_client.get_bucket_tagging(Bucket=bucket_name)
                if 'TagSet' in tags_response:
                    for tag in tags_response['TagSet']:
                        tag_key = f"Tag Key: {tag['Key']}"
                        tag_value = f"Tag Value: {tag['Value']}"
                        tags[tag_key] = tag_value
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchTagSet':
                    pass  # No tags exist for the bucket, continue without adding tags
                else:
                    raise e

            # Create a DataFrame for the current bucket's data
            bucket_data = pd.DataFrame([{"Bucket Name": bucket_name, "Creation Date": bucket_creation_date, "Access": access, **tags}])
            # Concatenate the current bucket's data with the main DataFrame
            df = pd.concat([df, bucket_data], ignore_index=True)
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                # Handle the case where the bucket doesn't exist (or we don't have permission to list it)
                pass
            elif e.response['Error']['Code'] == '403':
                print(f"Skipped bucket '{bucket_name}' due to insufficient permissions.")
            elif e.response['Error']['Code'] == '400':
                print(f"Skipped bucket '{bucket_name}' due to a Bad Request.")
            else:
                raise e
        finally:
            pbar.update(1)  # Update the progress bar

# Reorder the columns
column_order = ["Bucket Name", "Creation Date", "Access"] + [col for col in df.columns if col not in ["Bucket Name", "Creation Date", "Access"]]
df = df[column_order]

# Save the DataFrame to a CSV file
csv_file = 's3_bucket_info_with_tags.csv'
df.to_csv(csv_file, index=False)

print(f"Bucket information including creation date, access, and tags saved to {csv_file}")
