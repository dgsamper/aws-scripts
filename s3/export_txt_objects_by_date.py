# Export all the objects that meet the date criteria in the date_threshold variable. Example output: 
# Object: 01GSESGT9HBV3K1496KSAQ78NG/chunks/000001, Last Modified: 2023-02-17 09:03:06+00:00
# Object: 01GSESGT9HBV3K1496KSAQ78NG/chunks/000002, Last Modified: 2023-02-17 09:03:08+00:00
# Object: 01GSESGT9HBV3K1496KSAQ78NG/chunks/000003, Last Modified: 2023-02-17 09:03:10+00:00
# Object: 01GSESGT9HBV3K1496KSAQ78NG/chunks/000004, Last Modified: 2023-02-17 09:03:12+00:00
# Object: 01GSESGT9HBV3K1496KSAQ78NG/chunks/000005, Last Modified: 2023-02-17 09:03:14+00:00

import boto3
from datetime import datetime, timedelta, timezone

# .aws/credentials
session = boto3.Session(profile_name='default')
s3 = session.client('s3')

bucket_name = # bucket_name
continuation_token = None
objects = []

while True: 
    # List objects in the bucket with continuation token
    if continuation_token is not None:
        response = s3.list_objects_v2(Bucket=bucket_name, MaxKeys=1000, ContinuationToken=continuation_token)
    else:
        response = s3.list_objects_v2(Bucket=bucket_name, MaxKeys=1000)

    # Append objects from the response to the list
    objects.extend(response.get('Contents', []))

    # Check if there are more objects to fetch
    if 'NextContinuationToken' in response:
        continuation_token = response['NextContinuationToken']
    else:
        break

# date_threshold = "2023-08-09T00:00:00.000Z"
date_threshold = datetime.now(timezone.utc) - timedelta(days=90)

# Create a .txt file for saving the results
output_file = 'objects.txt'

with open(output_file, 'w') as file:
    for obj in objects:
        if obj['LastModified'] < date_threshold:
            result = f"Object: {obj['Key']}, Last Modified: {obj['LastModified']}\n"
            file.write(result)

print(f"Results saved to {output_file}")
