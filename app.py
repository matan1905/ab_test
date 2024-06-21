import json

import redis
from scipy.stats import beta
import os
import logging

# Configuration
redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379')
redis_client = redis.Redis.from_url(redis_url)
SIGNIFICANCE_LEVEL = float(os.environ.get('SIGNIFICANCE_LEVEL', 0.95))
MAX_SAMPLES = int(os.environ.get('MAX_SAMPLES', 100))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def publish_result(ref_id, experiment_id, winner):
    logger.info(f"Publishing result for experiment {experiment_id} with ref_id {ref_id} and winner {winner}")
    result = {
        'ref_id': ref_id,
        'experiment_id': experiment_id,
        'control_wins': winner
    }
    redis_client.publish('experiments_results', json.dumps(result))

def process_record(data):
    if not data or 'ref_id' not in data or 'experiment_id' not in data or 'is_control' not in data or 'event' not in data:
        logger.error('Invalid input data')
        return

    ref_id = data['ref_id']
    experiment_id = data['experiment_id']
    is_control = data['is_control']
    event = data['event']

    if event not in ['start', 'goal']:
        logger.error('Invalid event type')
        return

    control_goals, control_starts, variation_goals, variation_starts = get_experiment_stats(experiment_id)

    if event == 'start':
        redis_client.hincrby(f'{experiment_id}:starts', 'control' if is_control else 'variation', 1)
        logger.info(
            f"Experiment {experiment_id} stats: control_goals={control_goals}, control_starts={control_starts}, variation_goals={variation_goals}, variation_starts={variation_starts}")

        # Check maximum samples
        if control_starts + variation_starts >= MAX_SAMPLES:
            control = variation_goals / variation_starts < control_goals / control_starts
            publish_result(ref_id, experiment_id, control)
            logger.info(f"Maximum samples reached for experiment {experiment_id}. Winner: {'control' if control else 'variation'}")
    elif event == 'goal':
        redis_client.hincrby(f'{experiment_id}:goals', 'control' if is_control else 'variation', 1)
        logger.info(f"Experiment {experiment_id} stats: control_goals={control_goals}, control_starts={control_starts}, variation_goals={variation_goals}, variation_starts={variation_starts}")

        if control_starts > 0 and variation_starts > 0:
            control_alpha = control_goals + 1
            control_beta = control_starts - control_goals + 1
            variation_alpha = variation_goals + 1
            variation_beta = variation_starts - variation_goals + 1

            control_samples = beta.rvs(control_alpha, control_beta, size=10000)
            variation_samples = beta.rvs(variation_alpha, variation_beta, size=10000)
            if (control_samples > variation_samples).mean() >= SIGNIFICANCE_LEVEL:
                publish_result(ref_id, experiment_id, True)
                logger.info(f"Winner found for experiment {experiment_id}: control")
            elif (variation_samples > control_samples).mean() >= SIGNIFICANCE_LEVEL:
                publish_result(ref_id, experiment_id, False)
                logger.info(f"Winner found for experiment {experiment_id}: variation")
            elif control_starts + variation_starts >= MAX_SAMPLES:
                control = variation_goals / variation_starts < control_goals / control_starts
                publish_result(ref_id, experiment_id, control)
                logger.info(f"Maximum samples reached for experiment {experiment_id}. Winner: {'control' if control else 'variation'}")

    logger.info(f"Event recorded: {event} for experiment {experiment_id} with ref_id {ref_id} and is_control {is_control}")

def get_experiment_stats(experiment_id):
    control_goals = int(redis_client.hget(f'{experiment_id}:goals', 'control') or 0)
    control_starts = int(redis_client.hget(f'{experiment_id}:starts', 'control') or 0)
    variation_goals = int(redis_client.hget(f'{experiment_id}:goals', 'variation') or 0)
    variation_starts = int(redis_client.hget(f'{experiment_id}:starts', 'variation') or 0)
    return control_goals, control_starts, variation_goals, variation_starts

def main():
    pubsub = redis_client.pubsub()
    pubsub.subscribe('experiments_record')

    logger.info("Subscribed to 'experiments_record' channel")

    for message in pubsub.listen():
        if message['type'] == 'message':
            data = message['data']
            # if data is a string, convert it to a dictionary
            if isinstance(data, bytes):
                data = json.loads(data)
            process_record(data)

if __name__ == '__main__':
    main()
