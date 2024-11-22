import tkinter as tk
from tkinter import ttk
import requests
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import seaborn as sns
import datetime
from confluent_kafka import Consumer
import json
import threading

# Initialize the main window
root = tk.Tk()
root.title("Dynamic Weather Dashboard")
root.geometry("800x600")
root.configure(bg="#282c34")

# Title label
title_label = ttk.Label(root, text="Dynamic Weather Dashboard", font=("Helvetica", 20, "bold"), background="#282c34", foreground="white")
title_label.pack(pady=10)

# Display temperature information
info_frame = ttk.Frame(root, padding=10)
info_frame.pack(pady=5)

# Weather data history
weather_data_history = []

# Labels to display temperature information
temperature_labels = []

# Function to fetch weather data
def fetch_weather(cities):
    if not cities:
        return

    # Clear previous data
    global weather_data_history
    weather_data_history.clear()  # Clear previous weather data history

    # Clear old temperature labels
    for label in temperature_labels:
        label.destroy()
    temperature_labels.clear()

    # Fetch and update weather data for each city
    temperatures = []
    for city in cities:
        weather_data = get_weather_data(city)
        if weather_data:
            temp = weather_data['main']['temp']
            temperatures.append((city, temp))

            # Add new data to the history list
            weather_data_history.append((datetime.datetime.now().strftime("%H:%M:%S"), city, temp))

            # Create a label for the current city's temperature
            temp_label = ttk.Label(info_frame, text=f"{city}: {temp} °C", background="#282c34", foreground="white")
            temp_label.pack()
            temperature_labels.append(temp_label)

    # Update the chart with the new temperatures
    if temperatures:  # Only update if there is data
        update_charts(temperatures)

# Function to fetch weather data from API
def get_weather_data(city):
    api_key = "fa7f77bdcde59e02f62143aaa826fcc4"
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"

    try:
        response = requests.get(url)
        response.raise_for_status()
        weather_data = response.json()
        return weather_data
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Error occurred: {err}")
    return None

# Real-time city names from Kafka
def kafka_consumer():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'weather-dashboard',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['cities'])
    while True:
        msg = consumer.poll(1.0)  # Timeout of 1 second
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        cities = json.loads(msg.value().decode('utf-8')).get('cities', [])
        if cities:
            print(f"Fetched cities: {cities}")  # Debugging statement
            # Fetch weather data for the new cities
            fetch_weather(cities)

# Function to run Kafka Consumer in a separate thread
def start_kafka_consumer():
    threading.Thread(target=kafka_consumer, daemon=True).start()

# Function to update charts using Seaborn
def update_charts(temperatures):
    plt.clf()  # Clear previous plots

    # Separate city names and temperature values for plotting
    cities = [city for city, temp in temperatures]
    temps = [temp for city, temp in temperatures]

    # Create a Seaborn barplot
    sns.barplot(x=cities, y=temps, palette='viridis')
    plt.xlabel("Cities")
    plt.ylabel("Temperature (°C)")
    plt.title("Weather Data for Cities")
    plt.xticks(rotation=45)

    # Draw the updated chart
    canvas.draw()

# Create a matplotlib figure and canvas
fig = plt.Figure(figsize=(8, 4))
canvas = FigureCanvasTkAgg(fig, master=root)
canvas.get_tk_widget().pack()

# Start Kafka consumer when the app starts
start_kafka_consumer()

# Tkinter event loop
root.mainloop()
