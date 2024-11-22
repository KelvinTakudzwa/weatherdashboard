from kafka import KafkaProducer
import psycopg2
import json
import time

# Kafka Producer Setup
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

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

# Function to fetch cities from PostgreSQL database
def get_cities_from_db():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("SELECT city_name FROM cities")  
    cities = [row[0] for row in cursor.fetchall()]
    conn.close()
    return cities

# Continuously fetch cities and send them to Kafka
while True:
    cities = get_cities_from_db()
    if cities:
        producer.send('cities', {'cities': cities})
        print(f'Sent cities: {cities}')
    time.sleep(10)  # Fetch cities every 10 seconds
