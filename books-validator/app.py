import boto3
import json
import re

# AWS SQS configuration
sqs = boto3.client('sqs', region_name='us-east-1')  # Replace
validator_queue_url = 'https://sqs.us-east-1.amazonaws.com/YOUR_ACCOUNT_ID/VALIDATOR_QUEUE_NAME'  # Replace with books-validator queue URL
processor_queue_url = 'https://sqs.us-east-1.amazonaws.com/YOUR_ACCOUNT_ID/PROCESSOR_QUEUE_NAME'  # Replace with books-processor queue URL

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
    
    # Check for seller name
    if not book.get('seller_name') or len(book['seller_name'].strip()) == 0:
        errors.append('Seller name is required')

    # Validate URL (if present)
    url_pattern = re.compile(r'^(https?|ftp)://[^\s/$.?#].[^\s]*$')
    if book.get('url') and not url_pattern.match(book['url']):
        errors.append('Invalid URL')

    return errors

# Send valid books to books-processor queue
def send_to_processor_queue(book):
    try:
        response = sqs.send_message(
            QueueUrl=processor_queue_url,
            MessageBody=json.dumps(book)
        )
        print(f"Valid book sent to books-processor queue: {book['title']} (MessageId: {response['MessageId']})")
    except Exception as e:
        print(f"Error sending book to processor queue: {e}")


# Process messages from the validator queue
def process_messages():
    try:
        response = sqs.receive_message(
            QueueUrl=validator_queue_url,
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
                    send_to_processor_queue(body)  # Send valid books to processor queue

                # Delete the message from the queue after processing
                sqs.delete_message(QueueUrl=validator_queue_url, ReceiptHandle=message['ReceiptHandle'])
                print(f"Message deleted: {message['MessageId']}")

        else:
            print("No messages to process")
            
    except Exception as e:
        print(f"Error processing messages: {e}")


# Start processing books
if __name__ == "__main__":
    while True:
        process_messages()
