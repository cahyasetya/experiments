import psycopg2
from psycopg2 import pool
import random
import time
from concurrent.futures import ThreadPoolExecutor

# Create a connection pool
connection_pool = None

def create_connection_pool(conn_params, pool_size=10):
    global connection_pool
    connection_pool = pool.ThreadedConnectionPool(pool_size, pool_size, **conn_params)

# Function to fetch a specific item and measure latency
def fetch_item(customer_id):
    conn = connection_pool.getconn()
    try:
        cur = conn.cursor()

        # Measure latency for fetching one item in milliseconds
        start_time = time.time()

        # Fetch the item with the given customer_id
        cur.execute("SELECT * FROM past_purchased_items WHERE customer_id = %s;", (customer_id,))
        item = cur.fetchone()

        end_time = time.time()
        latency_ms = (end_time - start_time) * 1000  # Convert to milliseconds

        cur.close()
        return item, latency_ms
    finally:
        connection_pool.putconn(conn)

# Function to fetch items sequentially
def fetch_items_sequentially(sample_customer_ids):
    items = []
    latencies = []
    for customer_id in sample_customer_ids:
        item, latency_ms = fetch_item(customer_id)
        items.append(item)
        latencies.append(latency_ms)
        print(f"Fetched item: {item}")
        print(f"Latency for fetching one item: {latency_ms:.6f} ms")

    # Calculate average latency
    average_latency = sum(latencies) / len(sample_customer_ids)
    print(f"Average latency for fetching {len(sample_customer_ids)} items sequentially: {average_latency:.6f} ms")
    return items, latencies, average_latency

# Function to fetch items using multithreading
def fetch_items_multithreaded(sample_customer_ids):
    with ThreadPoolExecutor(max_workers=len(sample_customer_ids)) as executor:
        results = list(executor.map(fetch_item, sample_customer_ids))

    items, latencies = zip(*results)

    for item, latency_ms in results:
        print(f"Fetched item: {item}")
        print(f"Latency for fetching one item: {latency_ms:.6f} ms")

    # Calculate average latency
    average_latency = sum(latencies) / len(sample_customer_ids)
    print(f"Average latency for fetching {len(sample_customer_ids)} items using multithreading: {average_latency:.6f} ms")

    return items, average_latency

# Database connection parameters
conn_params = {
    'dbname': 'experiments',
    'user': 'owl',
    'password': '',  # Empty password
    'host': 'localhost',
    'port': '5432'
}

# Create the connection pool
create_connection_pool(conn_params)

# Use a connection from the pool to set up the table
conn = connection_pool.getconn()
cur = conn.cursor()

# Drop the past_purchased_items table if it exists
cur.execute("DROP TABLE IF EXISTS past_purchased_items;")

# Create the past_purchased_items table with items as TEXT
cur.execute("""
CREATE TABLE past_purchased_items (
    customer_id VARCHAR(9) PRIMARY KEY,
    items TEXT
);
""")

# Set to keep track of used customer IDs
used_ids = set()
sample_customer_ids = []
sample_size = 100  # Number of customer IDs to sample

# Insert 747235 rows
for i in range(747235):
    while True:
        # Generate a random unique customer ID with 9 digits
        customer_id = f'{random.randint(0, 999999999):09d}'
        if customer_id not in used_ids:
            used_ids.add(customer_id)
            if i < sample_size:
                sample_customer_ids.append(customer_id)
            break

    # Generate a list of items and convert it to a comma-separated string
    items = ','.join([f'item_{j:02d}_xyz' for j in range(1, 10)])  # Generates 'item_01_xyz,item_02_xyz,...,item_09_xyz'

    # Insert the customer ID and items into the table
    cur.execute(
        "INSERT INTO past_purchased_items (customer_id, items) VALUES (%s, %s)",
        (customer_id, items)
    )
    if (i + 1) % 10000 == 0:  # Commit every 10,000 rows to avoid memory issues
        conn.commit()
        print(f"Inserted {i + 1} rows...")

# Final commit for any remaining rows
conn.commit()
print("Inserted all rows.")

# Create an index on customer_id
cur.execute("CREATE INDEX idx_customer_id ON past_purchased_items (customer_id);")
conn.commit()
print("Created index on customer_id.")

# Close the cursor and return the connection to the pool
cur.close()
connection_pool.putconn(conn)

time.sleep(2)

# Call the functions
num_fetches = 10  # Number of items to fetch
fetch_sample = random.sample(sample_customer_ids, num_fetches)

print("\nFetching items sequentially:")
fetched_items_sequential, latencies_sequential, avg_latency_sequential = fetch_items_sequentially(fetch_sample)
print(f"Fetched items sequentially: {fetched_items_sequential}")
print(f"Latencies for sequential fetch: {latencies_sequential}")
print(f"Average latency for sequential fetch: {avg_latency_sequential:.6f} ms")

print("\nFetching items using multithreading:")
fetched_items_multithreaded, avg_latency_multithreaded = fetch_items_multithreaded(fetch_sample)
print(f"Fetched items multithreaded: {fetched_items_multithreaded}")
print(f"Average latency for multithreaded fetch: {avg_latency_multithreaded:.6f} ms")

# Close the connection pool
connection_pool.closeall()
