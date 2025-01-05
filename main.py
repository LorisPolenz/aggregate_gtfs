import os
import duckdb
from io import BytesIO
from minio import Minio
from datetime import datetime

# Connect to an in-memory DuckDB instance
conn = duckdb.connect(database=':memory:')

# Connect to the MinIO server
s3_client = Minio(os.getenv('S3_ENDPOINT'),
                  os.getenv('S3_ACCESS_KEY'),
                  os.getenv('S3_SECRET_KEY'),
                  secure=True)

s3_remote = Minio(os.getenv('REMOTE_S3_ENDPOINT'),
                  os.getenv('REMOTE_S3_ACCESS_KEY'),
                  os.getenv('REMOTE_S3_SECRET_KEY'),
                  secure=True)


def get_newest_directory(bucket_name):
    objects = s3_client.list_objects(bucket_name)
    directories = {}

    for obj in objects:
        # Check if the object is a directory (ends with '/')

        if obj.is_dir and obj.object_name.startswith('gtfs_fp'):
            object_date = obj.object_name.split('_')[-1].replace('/', '')
            obj_ts = datetime.strptime(object_date, '%Y-%m-%d').timestamp()

            directories[obj.object_name] = obj_ts

    if not directories:
        raise Exception("No directories found in the bucket.")

    # Find the newest directory by the most recent timestamp
    newest_directory = max(directories, key=directories.get)
    return newest_directory


newest_fp_dir = get_newest_directory(os.getenv('S3_BUCKET_TIMETABLE'))

gtfs_fp = ['stops.parquet', 'routes.parquet', 'trips.parquet']

for file in gtfs_fp:
    # Download the Parquet file from the MinIO server
    s3_client.fget_object(
        bucket_name=os.getenv('S3_BUCKET_TIMETABLE'),
        object_name=newest_fp_dir + file,
        file_path="/tmp/" + file
    )

s3_client.fget_object(
    bucket_name=os.getenv('S3_BUCKET_STOPTIME'),
    object_name='stoptime_updates.parquet',
    file_path="/tmp/stoptime_updates.parquet"
)

# Define the paths to your Parquet files
stops = '/tmp/stops.parquet'
routes = '/tmp/routes.parquet'
trips = '/tmp/trips.parquet'
updates = '/tmp/stoptime_updates.parquet'


# Run a SELECT query joining the Parquet files
result = conn.execute(f"""
    SELECT
        stops.stop_name,
        updates.StopSequence,
        updates.start_datetime,
        updates.platform,
        updates."Arrival.Delay",
        updates."Departure.Delay",
        routes.route_short_name,
        trips.trip_headsign
    FROM read_parquet('{stops}') AS stops
    JOIN read_parquet('{updates}') AS updates
        ON stops.stop_id = updates.StopId
    JOIN read_parquet('{routes}') AS routes
        ON updates.route_id = routes.route_id
    JOIN read_parquet('{trips}') AS trips
        ON updates.trip_id = trips.trip_id;
""").fetchdf()

parquet_buffer = BytesIO()
result.to_parquet(parquet_buffer)
parquet_buffer.seek(0)

s3_remote.put_object(
    bucket_name=os.getenv('REMOTE_S3_BUCKET_PUBLIC'),
    object_name="current_feed.parquet",
    data=parquet_buffer,
    length=parquet_buffer.getbuffer().nbytes,
    content_type='application/octet-stream'
)

s3_remote.put_object(
    bucket_name=os.getenv('REMOTE_S3_BUCKET_PUBLIC'),
    object_name=f"feed_{int(datetime.now().timestamp())}.parquet",
    data=parquet_buffer,
    length=parquet_buffer.getbuffer().nbytes,
    content_type='application/octet-stream'
)
