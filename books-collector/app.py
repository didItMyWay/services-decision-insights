import boto3
import csv
import json
import os

# AWS SQS configuration
sqs = boto3.client('sqs', region_name='us-east-1')  # Replace 
processor_queue_url = 'https://sqs.us-east-1.amazonaws.com/YOUR_ACCOUNT_ID/PROCESSOR_QUEUE_NAME'  # Replace with books-processor queue URL

# CSV file configuration
csv_file_path = 'eligible_books.csv'

# Initialize the CSV file
if not os.path.exists(csv_file_path):
    with open(csv_file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Title', 'Author', 'Price', 'Seller Name', 'URL'])

# Write a single book to the CSV file
def write_book_to_csv(book):
    with open(csv_file_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([
            book.get('title'),
            book.get('author'),
            book.get('price'),
            book.get('seller_name'),
            book.get('url')
        ])
    print(f"Book '{book.get('title')}' written to CSV file.")


# Process messages from the processor queue
def process_messages():
    try:
        response = sqs.receive_message(
            QueueUrl=processor_queue_url,
            MaxNumberOfMessages=10,  # Adjust batch size
            WaitTimeSeconds=10
        )

        if 'Messages' in response:
            for message in response['Messages']:
                # Parse the message
                book = json.loads(message['Body'])
                print(f"Received book for collection: {book['title']}")

                # Write the book to CSV
                write_book_to_csv(book)

                # Delete the message from the queue after processing
                sqs.delete_message(QueueUrl=processor_queue_url, ReceiptHandle=message['ReceiptHandle'])
                print(f"Message deleted: {message['MessageId']}")

        else:
            print("No messages to process")
            
    except Exception as e:
        print(f"Error processing messages: {e}")


# Start reading books from the queue
if __name__ == "__main__":
    while True:
        process_messages()
