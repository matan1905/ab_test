import json
import time
from threading import Thread
import redis
import pytest
import os

# Configuration
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379')
redis_client = redis.Redis.from_url(REDIS_URL)

# Global variable to store the received result data
result_data = []

# Function to handle incoming messages from the 'experiments_results' channel
def result_listener():
    pubsub = redis_client.pubsub()
    pubsub.subscribe('experiments_results')

    for message in pubsub.listen():
        if message['type'] == 'message':
            data = json.loads(message['data'])
            result_data.append(data)

# Function to publish data to the 'experiments_record' channel
def send_request(data):
    redis_client.publish('experiments_record', json.dumps(data))

# Pytest fixture to start the result listener
@pytest.fixture(scope="module", autouse=True)
def start_result_listener():
    thread = Thread(target=result_listener)
    thread.start()
    yield
    thread.join(1)

# Pytest fixture to clear Redis before running the tests
@pytest.fixture(scope="module", autouse=True)
def clear_redis():
    redis_client.flushall()

# Test scenario 1: Control group wins
def test_scenario_1():
    result_data.clear()
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
    assert len(result_data) >= 1, "Result not received"
    assert result_data[0]["control_wins"] is True, "Control group did not win"

# # Test scenario 2: Variation group wins
def test_scenario_2():
    result_data.clear()
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
    assert len(result_data) >= 1, "Result not received"
    assert result_data[0]["control_wins"] is False, "Variation group did not win"

# Test scenario 3: Maximum samples reached, control group wins
def test_scenario_3():
    result_data.clear()
    for i in range(0, 51):
        send_request({"ref_id": f"user{i}", "experiment_id": "exp3", "is_control": True, "event": "start"})
        if i % 42 == 0:
            send_request({"ref_id": f"user{i}", "experiment_id": "exp3", "is_control": True, "event": "goal"})
    for i in range(0, 51):
        send_request({"ref_id": f"user{i}", "experiment_id": "exp3", "is_control": False, "event": "start"})
    time.sleep(4)
    assert len(result_data) >= 1, "Result not received"
    assert result_data[0]["control_wins"] is True, "Control group did not win"

# Test scenario 4: Maximum samples reached, variation group wins
def test_scenario_4():
    result_data.clear()
    for i in range(1, 51):
        send_request({"ref_id": f"user{i}", "experiment_id": "exp4", "is_control": False, "event": "start"})
        if i % 50 == 0:
            send_request({"ref_id": f"user{i}", "experiment_id": "exp4", "is_control": False, "event": "goal"})
    for i in range(0, 51):
        send_request({"ref_id": f"user{i}", "experiment_id": "exp4", "is_control": True, "event": "start"})
    time.sleep(4)
    assert len(result_data) >= 1, "Result not received"
    assert result_data[0]["control_wins"] is False, "Variation group did not win"
