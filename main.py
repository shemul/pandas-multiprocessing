import argparse
import multiprocessing as mp
import os
import time
from datetime import datetime

import pandas as pd


def calculate_age(dob):
    dob = datetime.strptime(dob, '%Y-%m-%d')
    age = (datetime.today() - dob).days
    return int(age / 365)

def process_df(chunk):
    age_list = []
    for i ,j in enumerate(chunk.index):
        dob = chunk.at[j,"dob"]
        age_list.append((calculate_age(dob)))
    chunk['age'] = age_list
    return chunk

def run(param):

    try:
        reader = pd.read_csv(param.input_csv, chunksize=param.chunk_size)
        pool = mp.Pool(param.pool)
        funclist = []
        for df in reader:
            # process each data frame
            f = pool.apply_async(process_df,[df])
            funclist.append(f)

        result = []
        for f in funclist:
            result.append(f.get())

        training = pd.concat(result)

        try:
            os.remove(param.output_csv)
        except Exception as ex:
            pass
        training.to_csv(param.output_csv, mode='a', header=True, index=False)

    except Exception as e:
        import traceback
        print("something wrong", e)
        traceback.print_exc()


class RunConfig(object):
    def __init__(self, args):
        self.input_csv = args.input_csv
        self.output_csv = args.output_csv
        self.chunk_size = args.chunk_size
        self.pool = args.pool

if __name__ == '__main__':
    chunksize = os.getenv("chunksize")
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_csv", type=str, default=os.getenv("input_csv"))
    parser.add_argument("--output_csv", type=str, default=os.getenv("output_csv"))
    parser.add_argument("--chunk_size", type=int, default=os.getenv("chunk_size"))
    parser.add_argument("--pool", type=int, default=os.getenv("pool"))
    args = parser.parse_args()
    print(args)
    start_timestamp = time.time()
    run(RunConfig(args))
    print(str(time.time() - start_timestamp))