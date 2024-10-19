import csv
import boto3
import json

# AWS SQS configuration
sqs = boto3.client('sqs', region_name='us-east-1')  # Replace 'us-east-1' with your AWS region
queue_url = 'https://sqs.us-east-1.amazonaws.com/YOUR_ACCOUNT_ID/YOUR_QUEUE_NAME'  # Replace with your queue URL

# Send book to SQS
def send_to_sqs(book):
    try:
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(book)
        )
        print(f"Message sent: {response['MessageId']}")
    except Exception as e:
        print(f"Error sending message: {e}")

# Function to read books from a CSV file and publish to SQS
def process_books(file_path):
    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            book = {
                'merchant_id': row['merchant_id'],
                'title': row['title'],
                'author': row['author'],
                'price': row['price']
            }
            print(f"Processing book: {book['title']} from merchant {book['merchant_id']}")
            send_to_sqs(book)


file_path = './resources/import-data.csv'

# Start processing books
process_books(file_path)
