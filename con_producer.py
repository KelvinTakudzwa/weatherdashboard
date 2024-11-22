from confluent_kafka import Producer
import psycopg2
import json
import time

# Kafka Producer Setup
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(**conf)

# PostgreSQL Connection Setup
def connect_db():
    conn = psycopg2.connect(
        host="localhost",
        port="5433",
        database="Cities",
        user="postgres",
        password="0786682192@Tk"
    )
    return conn

# Function to fetch all cities from PostgreSQL database
def get_all_cities():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("SELECT city_name FROM cities")
    cities = [row[0] for row in cursor.fetchall()]
    conn.close()
    return cities

# Main loop to fetch and send cities to Kafka in batches
def main(batch_size=5):
    while True:
        cities = get_all_cities()
        
        # Send cities in batches
        for i in range(0, len(cities), batch_size):
            batch = cities[i:i + batch_size]
            if batch:
                producer.produce('cities', json.dumps({'cities': batch}).encode('utf-8'))
                print(f'Sent cities: {batch}')
                producer.flush()  # Ensure the message is sent immediately
            else:
                print("No more cities to send.")
                break
            
            time.sleep(10)  # Fetch and send cities every 10 seconds
        
        print("Reached the end of the list, restarting the process...")
        time.sleep(2)  # Optional: pause before restarting

if __name__ == "__main__":
    main()
