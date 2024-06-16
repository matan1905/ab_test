from flask import Flask, request, jsonify
import redis
from scipy.stats import beta
import os
import logging
from celery import Celery
import requests

app = Flask(__name__)
redis_host = os.environ.get('REDIS_HOST', 'localhost')
redis_client = redis.Redis(host=redis_host, port=6379)
celery = Celery(app.name, broker='redis://' + redis_host + ':6379')

WEBHOOK_URL = os.environ.get('WEBHOOK_URL', 'http://localhost:7744')
SIGNIFICANCE_LEVEL = float(os.environ.get('SIGNIFICANCE_LEVEL', 0.95))
MAX_SAMPLES = int(os.environ.get('MAX_SAMPLES', 500))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@celery.task
def send_webhook(ref_id, experiment_id, winner):
    logger.info(f"Sending webhook for experiment {experiment_id} with ref_id {ref_id} and winner {winner}")
    try:
        response = requests.post(WEBHOOK_URL,
                                 json={'ref_id': ref_id, 'experiment_id': experiment_id, 'control_wins': winner})
        logger.info(f"Webhook response: {response.text}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error sending webhook: {e}")

@app.route('/api/v1/record', methods=['POST'])
def record():
    data = request.get_json()

    if not data or 'ref_id' not in data or 'experiment_id' not in data or 'is_control' not in data or 'event' not in data:
        return jsonify({'error': 'Invalid input data'}), 400

    ref_id = data['ref_id']
    experiment_id = data['experiment_id']
    is_control = data['is_control']
    event = data['event']

    if event not in ['start', 'goal']:
        return jsonify({'error': 'Invalid event type'}), 400

    if event == 'start':
        redis_client.hincrby(f'{experiment_id}:starts', 'control' if is_control else 'variation', 1)
        control_goals, control_starts, variation_goals, variation_starts = get_experiment_stats(experiment_id)
        # Check maximum samples
        if control_starts + variation_starts >= MAX_SAMPLES:
            control = variation_goals / variation_starts < control_goals / control_starts
            send_webhook.delay(ref_id, experiment_id, control)
            logger.info(f"Maximum samples reached for experiment {experiment_id}. Winner: {'control' if control else 'variation'}")
    elif event == 'goal':
        redis_client.hincrby(f'{experiment_id}:goals', 'control' if is_control else 'variation', 1)

        control_goals, control_starts, variation_goals, variation_starts = get_experiment_stats(experiment_id)
        logger.info(f"Experiment {experiment_id} stats: control_goals={control_goals}, control_starts={control_starts}, variation_goals={variation_goals}, variation_starts={variation_starts}")



        if control_starts > 0 and variation_starts > 0:
            control_alpha = control_goals + 1
            control_beta = control_starts - control_goals + 1
            variation_alpha = variation_goals + 1
            variation_beta = variation_starts - variation_goals + 1

            control_samples = beta.rvs(control_alpha, control_beta, size=10000)
            variation_samples = beta.rvs(variation_alpha, variation_beta, size=10000)
            if (control_samples > variation_samples).mean() >= SIGNIFICANCE_LEVEL:
                send_webhook.delay(ref_id, experiment_id, True)
                logger.info(f"Winner found for experiment {experiment_id}: control")
            elif (variation_samples > control_samples).mean() >= SIGNIFICANCE_LEVEL:
                send_webhook.delay(ref_id, experiment_id, False)
                logger.info(f"Winner found for experiment {experiment_id}: variation")
            elif control_starts + variation_starts >= MAX_SAMPLES:
                control = variation_goals / variation_starts < control_goals / control_starts
                send_webhook.delay(ref_id, experiment_id, control)
                logger.info(
                    f"Maximum samples reached for experiment {experiment_id}. Winner: {'control' if control else 'variation'}")

    logger.info(f"Event recorded: {event} for experiment {experiment_id} with ref_id {ref_id} and is_control {is_control}")
    return jsonify({'message': 'Event recorded'})

def get_experiment_stats(experiment_id):
    control_goals = int(redis_client.hget(f'{experiment_id}:goals', 'control') or 0)
    control_starts = int(redis_client.hget(f'{experiment_id}:starts', 'control') or 0)
    variation_goals = int(redis_client.hget(f'{experiment_id}:goals', 'variation') or 0)
    variation_starts = int(redis_client.hget(f'{experiment_id}:starts', 'variation') or 0)
    return control_goals, control_starts, variation_goals, variation_starts

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=6969)