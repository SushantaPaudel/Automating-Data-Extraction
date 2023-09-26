import boto3
import re

def extract_driver_license_details(text):
    # Regular expressions for extracting details from the provided driver's license format
    license_number_match = re.search(r'DRIVER LICENSE DL (\d+) CLASS', text)
    if license_number_match:
        license_number = license_number_match.group(1)
    else:
        license_number = ""

    # Extracting expiration date
    expiration_date_match = re.search(r'EXP (\d{2}/\d{2}/\d{4})', text)
    if expiration_date_match:
        expiration_date = expiration_date_match.group(1)
    else:
        expiration_date = ""

    # Extracting full name
    full_name_match = re.search(r'LN ([A-Z]+) FN ([A-Z\s]+)', text)
    if full_name_match:
        last_name = full_name_match.group(1)
        first_name = full_name_match.group(2)
        full_name = f"{first_name} {last_name}"
    else:
        full_name = ""

    # Extracting address
    address_match = re.search(r'(\d+\s+\w+,\s+\w+,\s+[A-Z]+\d+)', text)
    if address_match:
        address = address_match.group(1)
    else:
        address = ""

    # Extracting date of birth
    dob_match = re.search(r'DOB (\d{2}/\d{2}/\d{4})', text)
    if dob_match:
        date_of_birth = dob_match.group(1)
    else:
        date_of_birth = ""

    # Extracting gender
    gender_match = re.search(r'SEX ([A-Z])', text)
    if gender_match:
        gender = gender_match.group(1)
    else:
        gender = ""

    # Extracting height
    height_match = re.search(r'HGT (\d+\'\d+")', text)
    if height_match:
        height = height_match.group(1)
    else:
        height = ""

    # Extracting weight
    weight_match = re.search(r'WGT (\d+)\s+lb', text)
    if weight_match:
        weight = weight_match.group(1)
    else:
        weight = ""

    return license_number, expiration_date, full_name, address, date_of_birth, gender, height, weight

def lambda_handler(event, context):
    # Initialize AWS clients
    textract_client = boto3.client("textract")
    s3_client = boto3.client('s3')
    dynamodb_client = boto3.client("dynamodb")
    
    # Define the DynamoDB table name
    table_name = "Your DynamoDB table name here"
    
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        key = key.replace("+", " ")
        print(bucket, key)

        # Call Textract to extract text from the PDF
        response = textract_client.start_document_text_detection(
            DocumentLocation={
                'S3Object': {
                    'Bucket': bucket,
                    'Name': key
                }
            }
        )

        # Get the JobId from the Textract response
        job_id = response['JobId']

        # Poll Textract for job completion
        response = textract_client.get_document_text_detection(JobId=job_id)

        while response['JobStatus'] == 'IN_PROGRESS':
            response = textract_client.get_document_text_detection(JobId=job_id)
            
        print('I am response block', response['Blocks'][0])
        
        text = ""
        for item in response["Blocks"]:
            if item["BlockType"] == "LINE":
                text = text + " " + item["Text"]
        
        print(text)
        
        # Extract driver's license details
        extracted_license_number, extracted_expiration_date, extracted_full_name, extracted_address, extracted_dob, extracted_gender, extracted_height, extracted_weight = extract_driver_license_details(text)
        
        print(extracted_license_number)
        print(extracted_expiration_date)
        print(extracted_full_name)
        print(extracted_address)
        print(extracted_dob)
        print(extracted_gender)
        print(extracted_height)
        print(extracted_weight)
        
        # Store the extracted driver's license details in DynamoDB
        response = dynamodb_client.put_item(
            TableName=table_name,
            Item={
                'key': {'S': key},
                'LicenseNumber': {'S': extracted_license_number},
                'ExpirationDate': {'S': extracted_expiration_date},
                'FullName': {'S': extracted_full_name},
                'Address': {'S': extracted_address},
                'DOB': {'S': extracted_dob},
                'Gender': {'S': extracted_gender},
                'Height': {'S': extracted_height},
                'Weight': {'S': extracted_weight},
            },
        )
        
        print(response)
       
    return {
        'statusCode': 200,
        'body': 'Driver\'s license details extraction and storage completed successfully!'
    }
