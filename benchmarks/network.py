import argparse
import time
import os
import random
from dht import DHTNetwork, DHTClient
from benchmark import Benchmark, NonSucceedBenchmark, display_benchmark_metrics

def main(args):
    # check the args
    tag_base = args.t + "_network"
    out_folder = args.o
    iterations = int(args.i)
    k = int(args.k)
    network_size = int(args.n)

    # check if the output folder exists
    try:
        os.mkdir(out_folder)
    except FileExistsError:
        pass
    except Exception as e:
        print(f"benchmark interrupted: {e}")
        exit(1)

    # -- list of benchmarks related to dht.dht --
    # 1- network initialization
    name, result_df = dht_network_initialization(tag_base, iterations, k, network_size)
    display_benchmark_metrics(name, result_df)
    result_df.to_csv(out_folder+'/'+name+'.csv')

    # 2-
    name, result_df = dht_network_bootstrap_node(tag_base, iterations, k, network_size)
    display_benchmark_metrics(name, result_df)
    result_df.to_csv(out_folder+'/'+name+'.csv')

    # 3-
    name, result_df = dht_network_bootstrap(tag_base, iterations, k, network_size)
    display_benchmark_metrics(name, result_df)
    result_df.to_csv(out_folder+'/'+name+'.csv')

    exit(0)


def gen_network(k: int, network_size: int):
    node_ids = random.sample(range(network_size), network_size)
    network = DHTNetwork(networkID=0, errorRate=0)
    for node in node_ids:
        node = DHTClient(node, network, k, a=3, b=k, stuckMaxCnt=5)
        network.add_new_node(node)
    return network, node_ids


def get_dht_cli(network):
    random_id = random.sample(range(network.len()), 1)[0]
    dht_cli = network.nodeStore.get_node(random_id)
    return dht_cli


def dht_network_initialization(tag_base: str, i: int, k: int, network_size: int):
    """ benchmarks the time it takes to find the closest nodes to a key in the rt """
    b_name = tag_base + f'_n_initialization'

    def task() -> float:
        # measurement
        start = time.time()
        _, _ = gen_network(k, network_size)
        return time.time() - start

    b = Benchmark(
        name='n_initialization',
        tag=tag_base,
        task_to_measure=task,
        number_of_times=i)
    df = b.run(timeout=0)
    return b_name, df


def dht_network_bootstrap_node(tag_base: str, i: int, k: int, network_size: int):
    """ benchmarks the time it takes to find the closest nodes to a key in the rt """
    b_name = tag_base + f'_bootstrap_node'

    def task() -> float:
        # init
        n, _ = gen_network(k, network_size)
        p = get_dht_cli(n)

        # measurement
        start = time.time()
        _ = n.bootstrap_node(p.ID, k, 100)
        return time.time() - start

    b = Benchmark(
        name='bootstrap_node',
        tag=tag_base,
        task_to_measure=task,
        number_of_times=i)
    df = b.run(timeout=0)
    return b_name, df

def dht_network_bootstrap(tag_base: str, i: int, k: int, network_size: int):
    """ benchmarks the time it takes to find the closest nodes to a key in the rt """
    b_name = tag_base + f'_bootstrap_network'

    def task() -> float:
        # init
        n, ids = gen_network(k, network_size)

        # measurement
        start = time.time()
        for id in ids:
            _ = n.bootstrap_node(id, k, 100)
        return time.time() - start

    b = Benchmark(
        name='bootstrap_network',
        tag=tag_base,
        task_to_measure=task,
        number_of_times=i)
    df = b.run(timeout=0)
    return b_name, df



if __name__ == "__main__":
    """ run the benchmarks under the given tag and parameters """
    args = argparse.ArgumentParser()
    args.add_argument('-t')  # test tag
    args.add_argument('-o')  # output folder
    args.add_argument('-i')  # number of iterations (for statistical robustness)
    args.add_argument('-k')  # bucket size
    args.add_argument('-n')  # network size (to compose the rt)
    a = args.parse_args()
    main(a)


