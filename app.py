
import json
import time
import threading
import redis
from flask import Flask, request, jsonify
from pymongo import MongoClient
from bson.objectid import ObjectId

# Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
MONGO_URI = 'mongodb://localhost:27017/'

# Initialize clients
app = Flask(__name__)
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
mongo_client = MongoClient(MONGO_URI)
db = mongo_client['app_database']
jobs_collection = db['jobs']

def queue_worker():
    """Background thread to process Redis queue without external worker process."""
    while True:
        try:
            # Blocking pop from Redis queue
            job_data = redis_client.brpop('task_queue', timeout=0)
            if job_data:
                _, payload = job_data
                data = json.loads(payload)
                job_id = data.get('job_id')
                task_payload = data.get('payload')

                # Update MongoDB status
                jobs_collection.update_one(
                    {'_id': ObjectId(job_id)},
                    {'$set': {'status': 'processing'}}
                )

                # Simulate processing logic
                time.sleep(2)
                result_data = f"Processed: {task_payload}"

                # Mark completed
                jobs_collection.update_one(
                    {'_id': ObjectId(job_id)},
                    {'$set': {'status': 'completed', 'result': result_data}}
                )
        except Exception as e:
            print(f"Worker Error: {e}")
            time.sleep(1)

# Start background worker execution alongside Flask
threading.Thread(target=queue_worker, daemon=True).start()

@app.route('/submit', methods=['POST'])
def submit_task():
    data = request.json
    if not data or 'payload' not in data:
        return jsonify({'error': 'Missing payload'}), 400

    payload = data['payload']

    # 1. Store initial state in MongoDB
    insert_result = jobs_collection.insert_one({'payload': payload, 'status': 'queued'})
    job_id = str(insert_result.inserted_id)

    # 2. Push to Redis queue
    redis_client.lpush('task_queue', json.dumps({'job_id': job_id, 'payload': payload}))

    return jsonify({'job_id': job_id, 'status': 'queued'}), 202

@app.route('/status/<job_id>', methods=['GET'])
def check_status(job_id):
    try:
        job = jobs_collection.find_one({'_id': ObjectId(job_id)})
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        
        return jsonify({
            'job_id': str(job['_id']),
            'status': job['status'],
            'result': job.get('result')
        }), 200
    except Exception:
        return jsonify({'error': 'Invalid ID format'}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=443)