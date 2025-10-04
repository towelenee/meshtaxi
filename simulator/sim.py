import os
import time
import json
import random
import threading
import sys
import paho.mqtt.client as mqtt

# --- Config ---
MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
NUM_TAXIS = int(os.getenv("NUM_TAXIS", "5"))

TOPIC_TAXI_POSITION = "taxi/position"
TOPIC_TAXI_OFFERS = "taxi/offers"
TOPIC_TAXI_RESPONSES = "taxi/responses"

# Always flush output immediately
def log(msg):
    print(msg, flush=True)

# --- MQTT connect with retry ---
def connect_with_retry(client, retries=20, delay=2):
    for attempt in range(1, retries + 1):
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            log(f"✅ Connected to MQTT ({MQTT_BROKER}:{MQTT_PORT})")
            return
        except Exception as e:
            log(f"⚠️ Connection failed ({e}), retry {attempt}/{retries}")
            time.sleep(delay)
    raise RuntimeError("❌ Could not connect to MQTT broker after retries")

# --- Taxi class ---
class Taxi:
    def __init__(self, taxi_id):
        self.id = taxi_id
        self.lat = 43.25 + random.uniform(-0.05, 0.05)
        self.lon = 76.9 + random.uniform(-0.05, 0.05)
        self.available = True
        self.client = mqtt.Client(client_id=f"taxi_{taxi_id}")
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        connect_with_retry(self.client)
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, rc):
        log(f"🚕 Taxi {self.id} connected to MQTT (rc={rc}), subscribing to offers...")
        client.subscribe(f"{TOPIC_TAXI_OFFERS}/{self.id}")
        log(f"✅ Taxi {self.id} subscribed to '{TOPIC_TAXI_OFFERS}'")

    def on_message(self, client, userdata, msg):
        log("{msg}")
        try:
            data = json.loads(msg.payload.decode())
            ride_id = data.get("ride_id")
            chat_id = data.get("chat_id")
            target_taxi = data.get("taxi_id")

            if target_taxi and target_taxi != self.id:
                return

            log(f"📩 Taxi {self.id} got offer for ride {ride_id} (chat_id={chat_id})")

            # Decide to accept
            accept = random.random() < 0.7
            log(f"🚕 Taxi {self.id} {'✅ accepted' if accept else '❌ rejected'} ride {ride_id}")

            # Publish response
            response = {
                "taxi_id": self.id,
                "ride_id": ride_id,
                "accepted": accept,
                "chat_id": chat_id,
            }
            topic = f"{TOPIC_TAXI_RESPONSES}/{self.id}"
            client.publish(topic, json.dumps(response))
            log(f"📤 Taxi {self.id} published response on {topic}")
            if accept:
                self.available = False

        except Exception as e:
            log(f"❌ Taxi {self.id} failed to process offer: {e}")

    def move_randomly(self):
        self.lat += random.uniform(-0.0005, 0.0005)
        self.lon += random.uniform(-0.0005, 0.0005)

    def loop(self):
        while True:
            self.move_randomly()
            msg = {
                "taxi_id": self.id,
                "lat": self.lat,
                "lon": self.lon,
                "available": self.available,
            }
            try:
                self.client.publish(TOPIC_TAXI_POSITION, json.dumps(msg))
                log(f"📡 Taxi {self.id} position: ({self.lat:.5f}, {self.lon:.5f})")
            except Exception as e:
                log(f"❌ Taxi {self.id} publish failed: {e}")
            time.sleep(3)

# --- Thread launcher ---
def start_taxi(taxi_id):
    taxi = Taxi(taxi_id)
    taxi.loop()

if __name__ == "__main__":
    log(f"🚗 Starting {NUM_TAXIS} simulated taxis... (broker={MQTT_BROKER}:{MQTT_PORT})")
    for i in range(NUM_TAXIS):
        threading.Thread(target=start_taxi, args=(i + 1,), daemon=True).start()

    while True:
        time.sleep(3600)
