import boto3
import json
from decimal import Decimal
from typing import List, Dict

# Initialize SQS client
sqs_client = boto3.client('sqs')
books_validator_queue_url = 'BOOKS_VALIDATOR_QUEUE_URL'
books_collector_queue_url = 'BOOKS_COLLECTOR_QUEUE_URL'

class Book:
    def __init__(self, title: str, price: Decimal, seller_id: str, seller_rating: float):
        self.title = title
        self.price = price
        self.seller_id = seller_id
        self.seller_rating = seller_rating

    @classmethod
    def from_dict(cls, data: Dict):
        return cls(
            title=data['title'],
            price=Decimal(data['price']),
            seller_id=data['seller_id'],
            seller_rating=float(data['seller_rating'])
        )
    
    def to_dict(self) -> Dict:
        return {
            "title": self.title,
            "price": str(self.price),  # Convert Decimal to str for JSON compatibility
            "seller_id": self.seller_id,
            "seller_rating": self.seller_rating
        }

class BooksProcessor:
    def __init__(self, queue_url: str):
        self.queue_url = queue_url

    def poll_books_validator_queue(self) -> List[Book]:
        """Polls the books-validator queue for new messages and parses them into Book objects."""
        response = sqs_client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10
        )
        messages = response.get('Messages', [])
        books = []
        
        for message in messages:
            book_data = json.loads(message['Body'])
            book = Book.from_dict(book_data)
            books.append(book)
            # Delete the message from the queue after processing
            sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
        
        return books

    def filter_by_seller_rating(self, books: List[Book], min_rating: float = 1.0) -> List[Book]:
        """Filter out books from sellers with ratings below the minimum rating."""
        return [book for book in books if book.seller_rating >= min_rating]
    
    def retain_cheapest_per_seller(self, books: List[Book]) -> List[Book]:
        """For each seller, retain only the cheapest book."""
        cheapest_books = {}
        for book in books:
            if (book.seller_id not in cheapest_books or 
                book.price < cheapest_books[book.seller_id].price):
                cheapest_books[book.seller_id] = book
        return list(cheapest_books.values())
    
    def process_books(self, books: List[Book]) -> List[Book]:
        """Apply business rules and prepare the final list of eligible books."""
        filtered_books = self.filter_by_seller_rating(books)
        eligible_books = self.retain_cheapest_per_seller(filtered_books)
        return eligible_books
    
    def send_to_books_collector(self, eligible_books: List[Book]):
        """Send eligible books as a single message to the books-collector SQS."""
        message_body = [book.to_dict() for book in eligible_books]
        sqs_client.send_message(
            QueueUrl=books_collector_queue_url,
            MessageBody=json.dumps(message_body)
        )
        print("Books sent to books-collector:", message_body)


# Main function which continuously poll and process books
if __name__ == "__main__":
    processor = BooksProcessor(books_validator_queue_url)
    
    while True:
        print("Polling for new books...")
        books = processor.poll_books_validator_queue()
        
        if books:
            print(f"Received {len(books)} books for processing.")
            eligible_books = processor.process_books(books)
            processor.send_to_books_collector(eligible_books)
        else:
            print("No new books to process.")
