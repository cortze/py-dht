import argparse
import time
import os
import random
from dht.hashes import Hash, BitArray
from benchmark import Benchmark, NonSucceedBenchmark, display_benchmark_metrics

def main(args):
    # check the args
    tag_base = args.t + "_hashes"
    out_folder = args.o
    iterations = int(args.i)

    # check if the output folder exists
    try:
        os.mkdir(out_folder)
    except FileExistsError:
        pass
    except Exception as e:
        print(f"benchmark interrupted: {e}")
        exit(1)

    # -- list of benchmarks related to dht.hashes --
    # 1- hash creation task
    name, result_df = hash_creation_benchmark(tag_base, iterations)
    display_benchmark_metrics(name, result_df)
    result_df.to_csv(out_folder+'/'+name)

    # 2- calculate distances between hashes
    name, result_df = hash_distance_benchmark(tag_base, iterations)
    display_benchmark_metrics(name, result_df)
    result_df.to_csv(out_folder + '/' + name)

    # 3- calculate time of comparing shared bits
    name, result_df = hash_shared_bits(tag_base, iterations)
    display_benchmark_metrics(name, result_df)
    result_df.to_csv(out_folder + '/' + name)


def hash_creation_benchmark(tag_base: str, i: int):
    """ benchmark the time to compose a Hash value with the given implementation """
    b_name = tag_base + f'_hash_creation'

    def task() -> float:
        # initialization
        random_id = random.sample(range(20000), 1)        
        # measurement
        start = time.time()
        _ = Hash(random_id[0])
        return time.time() - start
    b = Benchmark(
        name='hash_creation',
        tag=tag_base,
        task_to_measure=task,
        number_of_times=i)
    df = b.run(timeout=0)
    return b_name, df


def hash_distance_benchmark(tag_base: str, i: int):
    """ benchmarks the time it takes to get the distance between 2 Hashes """
    b_name = tag_base + f'_distance_between_hashes'

    def task() -> float:
        # initialization
        random_id_1 = random.sample(range(20000), 1)
        random_id_2 = random.sample(range(20000), 1)
        h1 = Hash(random_id_1[0])
        h2 = Hash(random_id_2[0])
        # measurement
        start = time.time()
        _ = h1.xor_to_hash(h2)
        return time.time() - start

    b = Benchmark(
        name='hash_creation',
        tag=tag_base,
        task_to_measure=task,
        number_of_times=i)
    df = b.run(timeout=0)
    return b_name, df


def hash_shared_bits(tag_base: str, i: int):
    """ benchmarks the time it takes to compare the shared upper bits between 2 Hashes """
    b_name = tag_base + f'_shared_bits_between_hashes'
    def task() -> float:
        # initialization
        random_id_1 = random.sample(range(20000), 1)
        random_id_2 = random.sample(range(20000), 1)
        h1 = Hash(random_id_1[0])
        h2 = Hash(random_id_2[0])

        # measurement
        start = time.time()
        _ = h1.xor_to_hash(h2)
        return time.time() - start
    b = Benchmark(
        name='hash_creation',
        tag=tag_base,
        task_to_measure=task,
        number_of_times=i)
    df = b.run(timeout=0)
    return b_name, df

def hash_shared_bits(tag_base: str, i: int):
    """ benchmarks the time it takes to compare the shared upper bits between 2 Hashes """
    b_name = tag_base + f'_shared_bits_between_hashes'
    def task() -> float:
        # initialization
        random_id_1 = random.sample(range(20000), 1)
        random_id_2 = random.sample(range(20000), 1)
        h1 = Hash(random_id_1[0])
        h2 = Hash(random_id_2[0])

        # measurement
        start = time.time()
        _ = h1.xor_to_hash(h2)
        return time.time() - start
    b = Benchmark(
        name='hash_creation',
        tag=tag_base,
        task_to_measure=task,
        number_of_times=i)
    df = b.run(timeout=0)
    return b_name, df

if __name__ == "__main__":
    """ run the benchmarks under the given tag and parameters """
    args = argparse.ArgumentParser()
    args.add_argument('-t')
    args.add_argument('-o')
    args.add_argument('-i')
    a = args.parse_args()
    main(a)


