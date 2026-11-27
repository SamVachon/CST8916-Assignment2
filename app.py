# AI assistance: ChatGPT helped integrate device detection and Blob analytics.
"""ShopStream Flask application for CST8916."""

import atexit
import json
import os
import threading
from datetime import datetime, timezone

from azure.core.exceptions import AzureError
from azure.eventhub import EventData, EventHubConsumerClient, EventHubProducerClient
from azure.storage.blob import ContainerClient
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from user_agents import parse

from shop import CATALOG, ShopperAnalytics, normalize_event

app = Flask(__name__)
CORS(app)

CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STR", "").strip()
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME", "clickstream").strip()
CONSUMER_HUB_NAME = os.getenv("EVENT_HUB_CONSUMER_NAME", EVENT_HUB_NAME).strip()
CONSUMER_CONNECTION_STR = os.getenv(
    "EVENT_HUB_CONSUMER_CONNECTION_STR", CONNECTION_STR
).strip()
MAX_BUFFER = 50

if not CONNECTION_STR or not EVENT_HUB_NAME:
    raise RuntimeError(
        "Set EVENT_HUB_CONNECTION_STR and EVENT_HUB_NAME before starting the app."
    )

_analytics = ShopperAnalytics(MAX_BUFFER)
_buffer_lock = threading.Lock()
_consumer_lock = threading.Lock()
_consumer_stopping = threading.Event()
_consumer = None
_consumer_thread = None
_consumer_status = {
    "state": "stopped",
    "message": "Event Hubs consumer has not started.",
    "last_received_at": None,
}


def get_consumer_status():
    with _buffer_lock:
        return dict(_consumer_status)


def _set_consumer_status(state, message):
    with _buffer_lock:
        _consumer_status.update(state=state, message=message)


def send_to_event_hubs(event_dict: dict) -> None:
    with EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        eventhub_name=EVENT_HUB_NAME,
    ) as producer:
        batch = producer.create_batch(partition_key=event_dict.get("user_id") or "anonymous")
        batch.add(EventData(json.dumps(event_dict)))
        producer.send_batch(batch)


def _on_event(partition_context, event):
    if event is None:
        return

    try:
        data = json.loads(event.body_as_str(encoding="UTF-8"))
    except (ValueError, TypeError):
        app.logger.warning("Ignoring an invalid Event Hubs JSON event")
        return
    if not isinstance(data, dict):
        app.logger.warning("Ignoring an Event Hubs event that is not a JSON object")
        return

    with _buffer_lock:
        transport_id = (partition_context.partition_id, event.sequence_number)
        _analytics.consume(data, transport_id=transport_id)
        _consumer_status.update(
            state="receiving",
            message="Events received from Azure Event Hubs.",
            last_received_at=datetime.now(timezone.utc).isoformat(),
        )

    partition_context.update_checkpoint(event)


def _on_consumer_error(partition_context, error):
    app.logger.error("Event Hubs receive failed (%s)", type(error).__name__)
    _set_consumer_status(
        "error",
        "Unable to receive events from Azure Event Hubs. Check the connection "
        "string, hub name, Listen permission, and network access.",
    )


def _receive_events(consumer):
    try:
        with consumer:
            consumer.receive(
                on_event=_on_event,
                on_error=_on_consumer_error,
                starting_position="-1",
            )
    except Exception as error:
        if not _consumer_stopping.is_set():
            _on_consumer_error(None, error)
    finally:
        if _consumer_stopping.is_set():
            _set_consumer_status("stopped", "Event Hubs consumer stopped.")
        else:
            _set_consumer_status(
                "error", "Event Hubs consumer stopped unexpectedly. Check settings and restart the app."
            )


def start_consumer():
    global _consumer, _consumer_thread
    with _consumer_lock:
        if _consumer_thread is not None and _consumer_thread.is_alive():
            return

        try:
            _consumer = EventHubConsumerClient.from_connection_string(
                conn_str=CONSUMER_CONNECTION_STR,
                consumer_group="$Default",
                eventhub_name=CONSUMER_HUB_NAME,
            )
        except ValueError:
            raise RuntimeError("Invalid Event Hubs consumer settings.") from None

        _consumer_stopping.clear()
        _set_consumer_status("waiting", "Waiting to receive events from Azure Event Hubs.")
        _consumer_thread = threading.Thread(
            target=_receive_events,
            args=(_consumer,),
            name="event-hubs-consumer",
            daemon=True,
        )
        _consumer_thread.start()


def stop_consumer():
    with _consumer_lock:
        _consumer_stopping.set()
        if _consumer is not None:
            _consumer.close()
        if _consumer_thread is not None:
            _consumer_thread.join(timeout=5)


def get_device_details(user_agent_header: str) -> dict[str, str]:
    user_agent = parse(user_agent_header)

    if user_agent.is_tablet:
        device_type = "tablet"
    elif user_agent.is_mobile:
        device_type = "mobile"
    elif user_agent.is_pc:
        device_type = "desktop"
    else:
        device_type = "unknown"

    browser = user_agent.browser.family
    os_name = user_agent.os.family
    return {
        "deviceType": device_type,
        "browser": browser if browser and browser != "Other" else "unknown",
        "os": os_name if os_name and os_name != "Other" else "unknown",
    }


def read_recent_analytics(container, prefix, time_field):
    blobs = sorted(
        container.list_blobs(name_starts_with=prefix),
        key=lambda blob: blob.last_modified,
        reverse=True,
    )[:5]

    rows = []
    for blob in blobs:
        contents = container.download_blob(blob.name).readall().decode("utf-8-sig")
        for line in contents.splitlines():
            if line.strip():
                row = json.loads(line)
                if isinstance(row, dict):
                    rows.append(row)

    rows.sort(key=lambda row: row.get(time_field) or "", reverse=True)
    return rows[:20]


@app.route("/")
@app.route("/checkout")
def index():
    return send_from_directory("templates", "client.html")


@app.route("/api/catalog")
def catalog():
    return jsonify({"products": CATALOG, "currency": "CAD"})


@app.route("/dashboard")
def dashboard():
    return send_from_directory("templates", "dashboard.html")


@app.route("/track", methods=["POST"])
def track():
    payload = request.get_json(silent=True)
    try:
        event = normalize_event(payload)
    except ValueError as error:
        return jsonify({"error": str(error)}), 400

    event.update(get_device_details(request.headers.get("User-Agent", "")))
    try:
        send_to_event_hubs(event)
    except (AzureError, ValueError) as error:
        app.logger.error("Event Hubs publish failed (%s)", type(error).__name__)
        return jsonify({
            "error": (
                "Could not confirm publishing to Azure Event Hubs. Check the "
                "connection string, hub name, Send permission, and network access."
            )
        }), 503

    return jsonify({"status": "published", "event": event}), 201


@app.route("/api/events", methods=["GET"])
def get_events():
    try:
        limit = max(1, min(int(request.args.get("limit", 20)), MAX_BUFFER))
    except ValueError:
        limit = 20

    with _buffer_lock:
        result = _analytics.snapshot(limit, request.args.get("user_id"))
        result["consumer"] = dict(_consumer_status)
    return jsonify(result), 200


@app.route("/api/analytics")
def analytics_results():
    connection_string = os.getenv("ANALYTICS_STORAGE_CONNECTION_STRING")
    if not connection_string:
        return jsonify({"error": "Analytics storage is not configured."}), 503

    try:
        with ContainerClient.from_connection_string(
            connection_string,
            container_name=os.getenv("ANALYTICS_CONTAINER", "shopstream-analytics"),
        ) as container:
            traffic = read_recent_analytics(container, "traffic/", "windowEndUtc")
            alerts = read_recent_analytics(container, "alerts/", "alertTimeUtc")
    except (AzureError, ValueError) as error:
        app.logger.warning("Analytics read failed (%s)", type(error).__name__)
        return jsonify({"error": "Analytics results are temporarily unavailable."}), 503

    return jsonify({"traffic": traffic, "alerts": alerts})


@app.route("/health", methods=["GET"])
def health():
    status = get_consumer_status()
    failed = status["state"] in {"error", "stopped"}
    return jsonify({
        "status": "degraded" if failed else "running",
        "consumer": status,
    }), 503 if failed else 200


# Gunicorn imports this module as app:app. Use one worker for the in-memory feed.
start_consumer()
atexit.register(stop_consumer)


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8000)
