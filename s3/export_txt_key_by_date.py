# Export only the KEY of the objects (one time only) that meet the date criteria in the date_threshold variable. Example output: 01H7ATM05JX45DJACBR980N808

import boto3
from datetime import datetime, timedelta, timezone

# Function to extract only the KEY. ex: 01H7ATM05JX45DJACBR980N808
def extract_only_key(key):
    parts = key.split('/')
    if len(parts) >= 2:
        return parts[0]
    return key

# .aws/credentials
session = boto3.Session(profile_name='default')
s3 = session.client('s3')

bucket_name = # bucket_name
continuation_token = None
objects = []

while True:
    # List objects in the bucket with continuation token
    if continuation_token is not None: # cannot be a string
        response = s3.list_objects_v2(Bucket=bucket_name, ContinuationToken=continuation_token)
    else:
        response = s3.list_objects_v2(Bucket=bucket_name)

    # Append objects from the response to the list
    objects.extend(response.get('Contents', []))

    # Check if there are more objects to fetch
    if 'NextContinuationToken' in response:
        continuation_token = response['NextContinuationToken']
    else:
        break

# Calculate the date 90 days ago from today in UTC
# date_threshold = "2023-08-09T00:00:00.000Z"
date_threshold = datetime.now(timezone.utc) - timedelta(days=90)

# Create a set to store the unique parts (unique only)
unique_parts = set()

for obj in objects:
    if obj['LastModified'] < date_threshold:
        key = obj['Key']
        part = extract_only_key(key)
        unique_parts.add(part)

# Create a .txt file
output_file = 'keys.txt'

with open(output_file, 'w') as file:
    for part in unique_parts:
        file.write(f"{part}\n")

print(f"Results saved to {output_file}")
