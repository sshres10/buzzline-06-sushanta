# 🔴⚪ Arsenal vs. Liverpool Live Match Tracker  

## 🏆 Overview  
A real-time **football match tracker** that streams **live match events** (goals, shots, possession, fouls, cards) using **Apache Kafka** for event streaming and **Matplotlib** for dynamic visualizations.  

---

## 🔹 Features  
✅ **Live match event simulation** (goals, shots, possession, fouls, cards, and corner kicks)  
✅ **Optimized Kafka producer-consumer pipeline** with **batch processing** for efficiency  
✅ **Real-time bar chart** displaying live match statistics  
✅ **Line chart tracking match event progression** over time  
✅ **Dynamic Y-axis scaling** for smooth animations  
✅ **Error handling & logging** for Kafka failures and data consistency  

---

## ⚙️ Tech Stack  
- **Programming**: Python  
- **Streaming**: Apache Kafka  
- **Visualization**: Matplotlib  
- **Logging**: Python Logging Module  

---

## 🛠️ How It Works  

### **1️⃣ Producer (`match_event_producer.py`)**  
⚽ **Simulates live match events**, generating random updates for:  
- **Goals, Shots, Possession, Fouls, Yellow & Red Cards, Corner Kicks**  
📤 **Sends messages to a Kafka topic in JSON format**  
♾️ **Runs continuously, streaming new match data in real-time**  
📦 **Implements batch sending for improved efficiency**  

### **2️⃣ Consumer (`match_tracker_consumer.py`)**  
🎙️ **Consumes Kafka messages**, processing live match data in real-time  
📊 **Visualizes match statistics dynamically using Matplotlib**  

🖥️ **Dual Visualization:**  
- **Bar Chart**: Live stats (**Goals, Shots on Target, Possession, Cards, Fouls**)  
- **Line Chart**: **Timeline progression of goals, shots, and match events**  
📈 **Uses batch processing (`poll()`)** for efficient event consumption  
🛠 **Handles Kafka failures gracefully**, ensuring a **smooth streaming experience**  

---

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





