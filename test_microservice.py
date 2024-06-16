import json
import requests
import time
from threading import Thread
from http.server import BaseHTTPRequestHandler, HTTPServer
import redis
import pytest
# Set the webhook URL and microservice URL
MICROSERVICE_URL = "http://localhost:6969/api/v1/record"

# Global variable to store the received webhook data
webhook_data = []

# Custom HTTP request handler for the webhook server
class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length).decode('utf-8')
        webhook_data.append(json.loads(post_data))
        self.send_response(200)
        self.end_headers()

# Function to start the webhook server
def start_webhook_server():
    server_address = ('', 7744)
    httpd = HTTPServer(server_address, WebhookHandler)
    httpd.serve_forever()

# Function to send a request to the microservice
def send_request(data):
    response = requests.post(MICROSERVICE_URL, json=data)
    assert response.status_code == 200, f"Request failed with status code {response.status_code}"

# Pytest fixture to start the webhook server
@pytest.fixture(scope="module")
def webhook_server():
    thread = Thread(target=start_webhook_server)
    thread.start()
    yield
    thread.join(1)

# Pytest fixture to clear Redis before running the tests
@pytest.fixture(scope="module", autouse=True)
def clear_redis():
    redis_client = redis.Redis(host='localhost', port=6379)
    redis_client.flushall()

# Test scenario 1: Control group wins
def test_scenario_1(webhook_server):
    webhook_data.clear()
    send_request({"ref_id": "user1", "experiment_id": "exp1", "is_control": True, "event": "start"})
    send_request({"ref_id": "user1", "experiment_id": "exp1", "is_control": True, "event": "goal"})
    send_request({"ref_id": "user2", "experiment_id": "exp1", "is_control": False, "event": "start"})
    send_request({"ref_id": "user1", "experiment_id": "exp1", "is_control": True, "event": "start"})
    send_request({"ref_id": "user1", "experiment_id": "exp1", "is_control": True, "event": "goal"})
    send_request({"ref_id": "user2", "experiment_id": "exp1", "is_control": False, "event": "start"})
    send_request({"ref_id": "user1", "experiment_id": "exp1", "is_control": True, "event": "start"})
    send_request({"ref_id": "user1", "experiment_id": "exp1", "is_control": True, "event": "goal"})
    send_request({"ref_id": "user2", "experiment_id": "exp1", "is_control": False, "event": "start"})
    send_request({"ref_id": "user1", "experiment_id": "exp1", "is_control": True, "event": "start"})
    send_request({"ref_id": "user1", "experiment_id": "exp1", "is_control": True, "event": "goal"})
    send_request({"ref_id": "user2", "experiment_id": "exp1", "is_control": False, "event": "start"})
    send_request({"ref_id": "user1", "experiment_id": "exp1", "is_control": True, "event": "start"})
    send_request({"ref_id": "user1", "experiment_id": "exp1", "is_control": True, "event": "goal"})
    send_request({"ref_id": "user2", "experiment_id": "exp1", "is_control": False, "event": "start"})
    time.sleep(1)
    assert len(webhook_data) >= 1, "Webhook not received"
    assert webhook_data[0]["control_wins"] is True, "Control group did not win"

# Test scenario 2: Variation group wins
def test_scenario_2(webhook_server):
    webhook_data.clear()
    send_request({"ref_id": "user3", "experiment_id": "exp2", "is_control": True, "event": "start"})
    send_request({"ref_id": "user4", "experiment_id": "exp2", "is_control": False, "event": "start"})
    send_request({"ref_id": "user4", "experiment_id": "exp2", "is_control": False, "event": "goal"})
    send_request({"ref_id": "user3", "experiment_id": "exp2", "is_control": True, "event": "start"})
    send_request({"ref_id": "user4", "experiment_id": "exp2", "is_control": False, "event": "start"})
    send_request({"ref_id": "user4", "experiment_id": "exp2", "is_control": False, "event": "goal"})
    send_request({"ref_id": "user3", "experiment_id": "exp2", "is_control": True, "event": "start"})
    send_request({"ref_id": "user4", "experiment_id": "exp2", "is_control": False, "event": "start"})
    send_request({"ref_id": "user4", "experiment_id": "exp2", "is_control": False, "event": "goal"})
    send_request({"ref_id": "user3", "experiment_id": "exp2", "is_control": True, "event": "start"})
    send_request({"ref_id": "user4", "experiment_id": "exp2", "is_control": False, "event": "start"})
    send_request({"ref_id": "user4", "experiment_id": "exp2", "is_control": False, "event": "goal"})
    time.sleep(1)
    assert len(webhook_data) >= 1, "Webhook not received"
    assert webhook_data[0]["control_wins"] is False, "Variation group did not win"
#
# # Test scenario 3: Maximum samples reached, control group wins assuming 100 samples
def test_scenario_3(webhook_server):
    webhook_data.clear()
    for i in range(0, 51):
        send_request({"ref_id": f"user{i}", "experiment_id": "exp3", "is_control": True, "event": "start"})
        if i % 50 == 0:
            send_request({"ref_id": f"user{i}", "experiment_id": "exp3", "is_control": True, "event": "goal"})
    for i in range(0, 51):
        send_request({"ref_id": f"user{i}", "experiment_id": "exp3", "is_control": False, "event": "start"})

    time.sleep(1)
    assert len(webhook_data) >= 1, "Webhook not received"
    assert webhook_data[0]["control_wins"] is True, "Control group did not win"

# Test scenario 4: Maximum samples reached, variation group wins
def test_scenario_4(webhook_server):
    webhook_data.clear()
    for i in range(1, 51):
        send_request({"ref_id": f"user{i}", "experiment_id": "exp4", "is_control": False, "event": "start"})
        if i % 200 == 0:
            send_request({"ref_id": f"user{i}", "experiment_id": "exp4", "is_control": False, "event": "goal"})
    for i in range(0, 51):
        send_request({"ref_id": f"user{i}", "experiment_id": "exp4", "is_control": True, "event": "start"})

    time.sleep(1)
    assert len(webhook_data) >= 1, "Webhook not received"
    assert webhook_data[0]["control_wins"] is False, "Variation group did not win"