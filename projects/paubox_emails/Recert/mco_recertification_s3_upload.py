import boto3
from botocore.errorfactory import ClientError
import re


tin_resource = boto3.resource('s3').Bucket('acp-data')
ssm_client = boto3.client('ssm')
s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')


for file in tin_resource.objects.filter(Prefix='scratch_vikram/Recertification/Practice_files/'):
    try:
        # S3 objects currently have the file structure scratch_vikram/Recertification/Practice_files/file. We need to grab just the file name from that, so we split around the "/" character
        file_name = re.sub('-{2,}', '', file.key.split('/')[3])
        extra_dashes_file_name = file.key.split('/')[3]
        # File names are structured like {tin}_{date}_{practice_name}.xlsx, we need to pull the tin so we split the string around the "_" character
        tin = file_name.split('_')[0]
        practice_name = file_name.split('_')[2].split('.')[0]
        # Check to see if the folder for the current tin already exists on S3, if it doesn't a 404 error will be returned, and we will jump to the exception
        try:
            s3_client.head_object(
                Bucket='acp-data', Key=f'Recertification/{tin}/')
            s3_folder_exists = True
            print('S3 Bucket exists')
        # If folder doesn't exist on S3, create it
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                s3_client.put_object(
                    Bucket='acp-data', Key=f'Recertification/{tin}/')
                print(f'Successfully created bucket on S3: {tin}')
            else:
                print(f'Error occurred: {e}')
        # Upload file to S3
        s3_resource.Object('acp-data', f'Recertification/{tin}/{file_name}').copy_from(
            CopySource=f'acp-data/scratch_vikram/Recertification/Practice_files/{extra_dashes_file_name}')
        print("File uploaded to S3")
        # Delete old file from scratch_vikram/recertification/practice_files
        s3_resource.Object(
            'acp-data', f'scratch_vikram/Recertification/Practice_files/{file_name}').delete()
    except Exception as e:
        print(f'Error uploading for {tin}: {e}')
