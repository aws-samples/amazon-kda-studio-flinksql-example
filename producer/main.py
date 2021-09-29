import os
import pandas as pd
import boto3
import random
import logging
import time

kinesis = boto3.client('kinesis')
s3 = boto3.resource('s3')

logging.basicConfig(level=logging.INFO)

STREAM_NAME = os.getenv('STREAM_NAME')
BUCKET_NAME = os.getenv('BUCKET_NAME')

def main():
    bucket = s3.Bucket(BUCKET_NAME)
    directory = './data'
    if not os.path.exists(directory):
        os.mkdir(directory)
    for obj in bucket.objects.filter():
        bucket.download_file(obj.key, os.path.join(directory, obj.key))
        logging.info(f'{obj.key} downloaded')

    logging.info('Creating dataframe')

    files = []
    for filename in os.listdir(directory):
        if filename.endswith(".bz2"):
            files.append(os.path.join(directory, filename))
        else:
            pass
    li = []
    for file in files:
        dfx = pd.read_csv(file)
        dfx = dfx.loc[dfx['DOLocationID'] == 1]
        # File no3. needs some cleaning as it contains records out of range
        if file.endswith('3.csv.bz2'):
            dfx = dfx.sort_values(by=['tpep_pickup_datetime'])
            dfx = dfx.iloc[275:len(dfx) - 190]
        li.append(dfx)

    df = pd.concat(li, axis=0, ignore_index=True)
    df = df.sort_values(by=['tpep_pickup_datetime'])
    df.reset_index(drop=True, inplace=True)
    logging.info('Dataframe created, starting streaming data')

    start_time = time.time()
    for index, row in df.iterrows():
        kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=df.iloc[index].to_json(),
            PartitionKey=str(random.randint(1, 19))
        )
        if not index % 1000:
            time_diff = time.time() - start_time
            logging.info(f'Sending with {round(index / time_diff)} records per second')



    logging.info('Reached end of data')



if __name__ == '__main__':
    main()