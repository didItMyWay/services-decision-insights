import boto3
import json
import re

# AWS SQS configuration
sqs = boto3.client('sqs', region_name='us-east-1')  # Replace with your region
queue_url = 'https://sqs.us-east-1.amazonaws.com/YOUR_ACCOUNT_ID/YOUR_QUEUE_NAME'  # Replace with your SQS queue URL

# Validation rules
def validate_book(book):
    errors = []

    # Check for price
    if not book.get('price') or float(book['price']) <= 0:
        errors.append('Invalid price')

    # Check for author name
    if not book.get('author') or len(book['author'].strip()) == 0:
        errors.append('Author name is required')

    # Check for book title
    if not book.get('title') or len(book['title'].strip()) == 0:
        errors.append('Book title is required')

    # Validate URL (if present)
    url_pattern = re.compile(r'^(https?|ftp)://[^\s/$.?#].[^\s]*$')
    if book.get('url') and not url_pattern.match(book['url']):
        errors.append('Invalid URL')

    return errors

# Function to process messages from SQS
def process_messages():
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,  # Adjust batch size
            WaitTimeSeconds=10
        )

        if 'Messages' in response:
            for message in response['Messages']:
                # Parse the message
                body = json.loads(message['Body'])
                print(f"Received book for validation: {body['title']}")

                # Validate the book
                errors = validate_book(body)

                if errors:
                    print(f"Validation failed for book '{body['title']}' - Errors: {errors}")
                    # Optionally, move invalid books to a dead-letter queue or log for further processing
                else:
                    print(f"Book '{body['title']}' is valid!")
                    # Process the valid book (forward to the next service, save, etc.)

                # Delete the message from the queue after processing
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
                print(f"Message deleted: {message['MessageId']}")

        else:
            print("No messages to process")
            
    except Exception as e:
        print(f"Error processing messages: {e}")

# Start processing books from the queue
while True:
    process_messages()
