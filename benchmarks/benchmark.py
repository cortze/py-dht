import time
import pandas as pd


class NonSucceedBenchmark(Exception):
    """ Benchmark errors """
    def __init__(self, e):
        self.exception = e

    def info(self):
        return self.exception



class Benchmark:
    """ Benchmark itself :) """
    def __init__(self, name: str, tag: str, task_to_measure, number_of_times: int):
        """ receives any kind of processed that needs to be measured, task must return the duration of the process """
        self.tag = tag
        self.name = name
        self.number_of_times = number_of_times
        self.task_to_measure = task_to_measure
        self.results = []

    def run(self, timeout: int):
        # print(f"running benchmark {self.name} ({self.tag})")
        b_start_time = time.time()
        for i in range(self.number_of_times):
            round_result = Result(i)
            duration = time.time()
            try: 
                duration = self.task_to_measure()

            except Exception as e:
                duration = time.time() 
                round_result.failed()
                print(f"error running benchmark {self.name} ({self.tag}) - {e}")

            round_result.finished(duration)
            # print(f"round {round_result.round} finished in {round_result.duration_s}s")
            self.results.append(round_result)

        # print(f"finished benchmark {self.name} ({self.tag}) in {(time.time() - b_start_time)} secs")
        return self.return_df()

    def return_df(self) -> pd.DataFrame:
        pd_rows = {
            'round': [],
            'start_time': [],
            'finish_time': [],
            'task_prep_time': [],
            'duration_s': [],
            'failed': []}
        for r in self.results:
            pd_rows['round'].append(r.round)
            pd_rows['start_time'].append(r.start_time)
            pd_rows['finish_time'].append(r.finish_time)
            pd_rows['task_prep_time'].append(r.prep_time_s)
            pd_rows['duration_s'].append(r.duration_s)
            pd_rows['failed'].append(r.has_failed)
        try:
            df = pd.DataFrame(pd_rows)
        except TypeError as e:
            print(f"error with benchmark {self.name} ({self.tag})")
            raise NonSucceedBenchmark()
        return df


class Result:
    """ Benchmark Results :) """
    def __init__(self, r: int):
        self.round = r
        self.start_time = time.time() 
        self.finish_time = 0.0 
        self.duration_s = 0.0 
        self.prep_time_s = 0.0 
        self.has_failed = False

    def finished(self, duration: float):
        self.finish_time = time.time()
        self.duration_s = duration 
        self.prep_time_s = (self.finish_time - self.start_time) - self.duration_s

    def failed(self):
        self.has_failed = True


def display_benchmark_metrics(name, df):
    """ display the summary of a given benchmark-results """
    print(f'-- benchmark: {name} --')
    print(f"rounds          : {len(df)}")
    print(f'failed rounds   : {df.failed.sum()}')
    print(f'prep time (s)   : {df.task_prep_time.mean()}')
    print(f"avg (s)         : {df.duration_s.mean()}")
    print(f"median (s)      : {df.duration_s.median()}")
    print(f"p90_duration (s): {df.duration_s.quantile(0.9)}")

