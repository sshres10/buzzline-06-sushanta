# **🔴⚪ Arsenal vs. Liverpool Live Match Tracker**  

## **Overview**  
A real-time match tracker simulating live football events (goals, shots, possession, and more) using **Kafka** for event streaming and **Matplotlib** for dynamic visualizations.

### **🔹 Features**
✅ **Live match event simulation** (goals, shots, possession updates)  
✅ **Kafka producer-consumer pipeline**  
✅ **Real-time bar chart for match statistics**  
✅ **Line chart tracking event progression over time**  
✅ **Smooth animation for a seamless experience**  

---

## **⚙️ Tech Stack**
- **Programming:** Python  
- **Streaming:** Apache Kafka  
- **Visualization:** Matplotlib  
- **Data Processing:** Pandas  
- **Logging:** Python Logging Module  

## **🛠️ How It Works**
### **1️⃣ Producer (`csv_producer_match.py`)**
- Generates **random match events** such as **goals, shots, possession changes**.
- Sends messages to a Kafka topic in **JSON format**.
- Runs continuously, streaming new match data in real-time.

### **2️⃣ Consumer (`csv_consumer_match.py`)**
- **Consumes Kafka messages** and processes live football match data.
- **Visualizes match statistics** dynamically using Matplotlib.
- **Dual Visualization:**
  - **Bar Chart:** Live statistics (Goals, Shots on Target, Possession).
  - **Line Chart:** Time-series progression of goals & shots.

## 🔴⚪ Come on you Gunners! 🚀





