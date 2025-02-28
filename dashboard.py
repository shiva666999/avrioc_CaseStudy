import time
from pymongo import MongoClient
import matplotlib.pyplot as plt

# Initialize MongoDB client
client = MongoClient('mongodb://localhost:27017/')
db = client['user_data']
collection = db['aggregated_data']

# Function to retrieve aggregated data
def get_aggregated_data():
    return list(collection.find())

# Plotting the dashboard metrics
def plot_dashboard(data):
    users = [d['user_id'] for d in data]
    interaction_counts = [d['interaction_count'] for d in data]
    
    plt.bar(users, interaction_counts)
    plt.xlabel('User ID')
    plt.ylabel('Interaction Count')
    plt.title('User Interaction Counts')
    plt.show()

# Dashboard loop to pull data and plot
def update_dashboard():
    while True:
        data = get_aggregated_data()
        plot_dashboard(data)
        time.sleep(5)  # Refresh every 5 seconds

if __name__ == "__main__":
    update_dashboard()
