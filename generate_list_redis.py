import redis
import random
import string
import time  # Import time for measuring latency
from concurrent.futures import ThreadPoolExecutor  # Import ThreadPoolExecutor for multithreading

# Create a Redis connection pool with a static size of 10
redis_pool = redis.ConnectionPool(host='localhost', port=6379, db=0, max_connections=10)

# Function to get a Redis connection from the pool
def get_redis_connection():
    return redis.Redis(connection_pool=redis_pool)

# Prune Redis memory at the beginning of the program
r = get_redis_connection()
r.flushdb()  # This will delete all keys in the current Redis database

# Function to generate a unique key
def generate_unique_key(existing_keys):
    while True:
        key = ''.join(random.choices(string.ascii_letters + string.digits, k=9))
        if key not in existing_keys:
            existing_keys.add(key)
            return key

# Function to insert a list into Redis
def insert_redis_list(key, words):
    r = get_redis_connection()
    r.lpush(key, *words)
    print(f"Inserted list into Redis with key: {key}")

# Function to fetch items sequentially and measure latency
def fetch_items_sequentially(keys):
    results = []
    latencies = []
    r = get_redis_connection()
    for key in keys:
        start_time = time.time()  # Start timing
        result = r.lrange(key, 0, -1)  # Fetch the list associated with the key
        latency_ms = (time.time() - start_time) * 1000  # Calculate latency in milliseconds
        results.append(result)
        latencies.append(latency_ms)  # Store the latency for this fetch
    average_latency = sum(latencies) / len(latencies) if latencies else 0
    return results, latencies, average_latency

# Function to fetch items using multithreading and measure latency
def fetch_items_multithreaded(keys):
    def fetch_key(key):
        r = get_redis_connection()
        start_time = time.time()  # Start timing
        result = r.lrange(key, 0, -1)  # Fetch the list associated with the key
        latency_ms = (time.time() - start_time) * 1000  # Calculate latency in milliseconds
        return result, latency_ms  # Return both result and latency

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(fetch_key, keys))  # Fetch items in parallel
    items, latencies = zip(*results)
    average_latency = sum(latencies) / len(latencies) if latencies else 0
    return items, average_latency  # Each result contains (fetched_list, latency)

# Generate 747235 unique keys
num_keys = 747235
existing_keys = set()
words = [
    "abcdefghij",  # 10 characters
    "klmnopqrst",  # 10 characters
    "uvwxyzabcd",  # 10 characters
    "efghijklmn",  # 10 characters
    "opqrstuvwx",  # 10 characters
    "yzabcdefgh",   # 10 characters
    "ijklmnopqr",  # 10 characters
    "stuvwxyzab",   # 10 characters
    "cdefghijkl",   # 10 characters
]

for _ in range(num_keys):
    key = generate_unique_key(existing_keys)
    insert_redis_list(key, words)

# Sleep for 2 seconds before fetching the key
time.sleep(2)  # Introduce a 2-second delay

# Example usage of the new functions to fetch items
keys_to_fetch = random.sample(list(existing_keys), 10)  # Randomly select 10 keys
fetched_items_sequential, latencies_sequential, avg_latency_sequential = fetch_items_sequentially(keys_to_fetch)  # Fetch items sequentially
print(f"Fetched items sequentially: {fetched_items_sequential}")
print(f"Latencies for sequential fetch: {latencies_sequential}")
print(f"Average latency for sequential fetch: {avg_latency_sequential:.6f} ms")

fetched_items_multithreaded, avg_latency_multithreaded = fetch_items_multithreaded(keys_to_fetch)  # Fetch items multithreaded
print(f"Fetched items multithreaded: {fetched_items_multithreaded}")
print(f"Average latency for multithreaded fetch: {avg_latency_multithreaded:.6f} ms")

with open('keys.txt', 'w') as f:
    f.write(','.join(keys_to_fetch))  # For Redis

print(f"Wrote {len(keys_to_fetch)} keys to keys.txt")  # For Redis

# Close the connection pool
redis_pool.disconnect()
