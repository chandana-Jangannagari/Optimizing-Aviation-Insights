import pandas as pd

input_file = "Data/Bronze/flights.csv"

batch_output = "Data/Processed/flights_batch.csv"
stream_output = "Data/Processed/flights_stream.csv"

chunk_size = 100000

batch_first = True
stream_first = True

for chunk in pd.read_csv(input_file, chunksize=chunk_size):

    batch_chunk = chunk[chunk["MONTH"] <= 10]
    stream_chunk = chunk[chunk["MONTH"] >= 11]

    batch_chunk.to_csv(
        batch_output,
        mode="w" if batch_first else "a",
        header=batch_first,
        index=False
    )

    stream_chunk.to_csv(
        stream_output,
        mode="w" if stream_first else "a",
        header=stream_first,
        index=False
    )

    batch_first = False
    stream_first = False

print("Dataset split completed successfully")