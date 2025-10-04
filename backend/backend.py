import os
import json
import math
import time
import asyncio
import paho.mqtt.client as mqtt
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, ContextTypes, filters

# ==========================
# CONFIG
# ==========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise EnvironmentError("❌ BOT_TOKEN not set in environment")

MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

TOPIC_TAXI_POSITION = "taxi/position"
TOPIC_TAXI_RESPONSES = "taxi/responses/+"
TOPIC_TAXI_OFFERS = "taxi/offers"
TOPIC_RIDE_REQUESTS = "taxi/requests"

# ==========================
# STATE
# ==========================
taxi_positions = {}        # taxi_id -> (lat, lon, available)
user_requests = {}         # chat_id -> ride data
pending_offers = {}        # taxi_id -> chat_id awaiting response

mqttc = None
app = None
telegram_loop = None

# ==========================
# UTILS
# ==========================


def haversine(lat1, lon1, lat2, lon2):
    """Distance in km"""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))


def connect_mqtt_with_retry(client, broker, port, retries=15, delay=2):
    for attempt in range(1, retries + 1):
        try:
            client.connect(broker, port, 60)
            print(f"✅ Connected to MQTT broker at {broker}:{port}")
            return
        except Exception as e:
            print(f"⚠️ MQTT connection failed ({e}), retry {attempt}/{retries}")
            time.sleep(delay)
    raise RuntimeError("❌ Could not connect to MQTT broker after retries")

# ==========================
# MQTT CALLBACKS
# ==========================


def on_connect(client, userdata, flags, rc):
    print(f"📡 MQTT connected (code {rc})")
    client.subscribe(TOPIC_TAXI_POSITION)
    client.subscribe(TOPIC_TAXI_RESPONSES)


def on_message(client, userdata, msg):
    global taxi_positions, pending_offers, app, telegram_loop

    payload = msg.payload.decode("utf-8")
    topic = msg.topic

    # Taxi sending position
    if topic == TOPIC_TAXI_POSITION:
        try:
            data = json.loads(payload)
            taxi_id = data.get("taxi_id")
            lat = data.get("lat")
            lon = data.get("lon")
            available = data.get("available", True)
            taxi_positions[taxi_id] = (lat, lon, available)
            print(f"🚕 Taxi {taxi_id} @ ({lat:.5f}, {lon:.5f}) {'✅ available' if available else '❌ busy'}")
        except Exception as e:
            print(f"❌ Error parsing taxi position: {e}")

    # Taxi response (accept/decline)
    elif topic.startswith("taxi/responses/"):
        try:
            print(f"📩 Taxi response: {payload}")
            data = json.loads(payload)
            taxi_id = data.get("taxi_id")
            accepted = data.get("accepted")

            if taxi_id not in pending_offers:
                print(f"⚠️ Unexpected response from {taxi_id}")
                return

            chat_id = pending_offers.pop(taxi_id)
            ride = user_requests.get(chat_id)

            if accepted and ride:
                lat, lon, _ = taxi_positions.get(taxi_id, (0, 0, True))
                dist = haversine(lat, lon, ride["from"]["lat"], ride["from"]["lon"])

                asyncio.run_coroutine_threadsafe(
                    app.bot.send_message(
                        chat_id=chat_id,
                        text=f"✅ Taxi {taxi_id} accepted your ride! Distance {dist:.1f} km 🚕"
                    ),
                    telegram_loop
                )

                taxi_positions[taxi_id] = (lat, lon, False)
                print(f"🚖 Taxi {taxi_id} accepted ride for user {chat_id}")
            else:
                asyncio.run_coroutine_threadsafe(
                    app.bot.send_message(
                        chat_id=chat_id,
                        text=f"❌ Taxi {taxi_id} declined the ride."
                    ),
                    telegram_loop
                )
                print(f"❌ Taxi {taxi_id} declined ride")

        except Exception as e:
            print(f"❌ Error parsing taxi response: {e}")

# ==========================
# TELEGRAM HANDLER
# ==========================


async def handle_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    text = update.message.text.strip()
    print(f"📨 Ride request from user {chat_id}: {text}")

    try:
        ride = json.loads(text)
        user_requests[chat_id] = ride
    except Exception:
        await update.message.reply_text("❌ Please send valid JSON ride request.")
        return

    await update.message.reply_text("📍 Searching for nearby taxis...")

    # Find nearest available taxi
    if not taxi_positions:
        await update.message.reply_text("🚫 No taxis available.")
        return

    best_taxi, best_dist = None, float("inf")
    for taxi_id, (lat, lon, available) in taxi_positions.items():
        if not available:
            continue
        dist = haversine(lat, lon, ride["from"]["lat"], ride["from"]["lon"])
        if dist < best_dist:
            best_taxi, best_dist = taxi_id, dist

    if not best_taxi:
        await update.message.reply_text("🚫 No available taxis nearby.")
        return

    await update.message.reply_text(f"📡 Offering ride to taxi {best_taxi} ({best_dist:.1f} km away)...")

    # Offer ride to taxi
    offer = {"chat_id": chat_id, "ride": ride, "taxi_id": best_taxi}
    pending_offers[best_taxi] = chat_id
    mqttc.publish(f"taxi/offers/{best_taxi}", json.dumps(offer))
    print(f"➡️ Published offer to taxi/offers/{best_taxi}: {offer}")

    # Wait for taxi response
    await asyncio.sleep(30)
    if best_taxi in pending_offers:
        pending_offers.pop(best_taxi)
        await update.message.reply_text("⏳ No taxi accepted your ride. Please try again later.")
        print(f"🚫 No taxi accepted ride for user {chat_id}")

# ==========================
# MAIN
# ==========================


def main():
    global mqttc, app, telegram_loop

    mqttc = mqtt.Client()
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    connect_mqtt_with_retry(mqttc, MQTT_BROKER, MQTT_PORT)
    mqttc.loop_start()

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    telegram_loop = asyncio.get_event_loop()

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_request))

    print("🤖 Taxi backend started.")
    app.run_polling()


if __name__ == "__main__":
    main()
