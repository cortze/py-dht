import argparse
import time
import os
import random
from dht.hashes import Hash
from dht.routing_table import RoutingTable
from benchmark import Benchmark, NonSucceedBenchmark, display_benchmark_metrics


def main(args):
    # check the args
    tag_base = args.t + "_routing"
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

    # -- list of benchmarks related to dht.hashes --
    # 1- hash creation task
    name, result_df = routing_new_discover_node(tag_base, iterations, k, network_size)
    display_benchmark_metrics(name, result_df)
    result_df.to_csv(out_folder+'/'+name+'.csv')

    # 2- get closest nodes to hash
    name, result_df = routing_closest_to_hash(tag_base, iterations, k, network_size)
    display_benchmark_metrics(name, result_df)
    result_df.to_csv(out_folder+'/'+name+'.csv')

    # 3- get the routing table
    name, result_df = routing_get_rt(tag_base, iterations, k, network_size)
    display_benchmark_metrics(name, result_df)
    result_df.to_csv(out_folder+'/'+name+'.csv')

    exit(0)


def gen_rt(k: int, network_size: int):
    control = random.sample(range(network_size), 1)[0]
    node_ids = range(network_size)
    rt = RoutingTable(control, k)
    for node in node_ids:
        rt.new_discovered_peer(node)
    return rt


def routing_new_discover_node(tag_base: str, i: int, k: int, network_size: int):
    """ benchmarks the time it takes to add a new peer to a filled rt """
    b_name = tag_base + f'_new_discv_node'

    def task() -> float:
        # initialization
        rt = gen_rt(k, network_size)
        new_node = random.sample(range(4 * network_size), 1)[0]

        # measurement
        start = time.time()
        rt.new_discovered_peer(new_node)
        return time.time() - start
    b = Benchmark(
        name='new_discv_node',
        tag=tag_base,
        task_to_measure=task,
        number_of_times=i)
    df = b.run(timeout=0)
    return b_name, df


def routing_closest_to_hash(tag_base: str, i: int, k: int, network_size: int):
    """ benchmarks the time it takes to return the rt """
    b_name = tag_base + f'_closest_to_hash'

    def task() -> float:
        # initialization
        rt = gen_rt(k, network_size)
        looking_for = random.sample(range(4 * network_size), 1)[0]
        lookingH = Hash(looking_for)

        # measurement
        start = time.time()
        rt.get_closest_nodes_to(lookingH)
        return time.time() - start

    b = Benchmark(
        name='closest_to_hash',
        tag=tag_base,
        task_to_measure=task,
        number_of_times=i)
    df = b.run(timeout=0)
    return b_name, df


def routing_get_rt(tag_base: str, i: int, k: int, network_size: int):
    """ benchmarks the time it takes to find the closest nodes to a key in the rt """
    b_name = tag_base + f'_closest_to_hash'

    def task() -> float:
        # initialization
        rt = gen_rt(k, network_size)

        # measurement
        start = time.time()
        _ = rt.get_routing_nodes()
        return time.time() - start

    b = Benchmark(
        name='closest_to_hash',
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


