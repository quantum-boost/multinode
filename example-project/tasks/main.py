from dataclasses import dataclass
from typing import List

import numpy as np
from multinode import Multinode


mn = Multinode()


@dataclass
class AnnealingResult:
    route: np.ndarray
    distance: float


# This is a master function that just schedules the work to be done by
# other workers, so it doesn't need to use significant resources.
@mn.function(cpu=0.1, memory="1 GiB")
def solve_tsp(
    n_cities: int,
    distances: np.ndarray,
    top_level_iterations: int = 5,
    worker_iterations: int = 500_000,
    n_workers: int = 4,
    initial_temperature: float = 20.0,
    cooling_rate: float = 0.999997,
):
    """
    Tries to find the traveling salesman problem using distributed simulated annealing.

    :param n_cities: number of cities to visit. They will be named `0` to `n_cities - 1`
    :param distances: numpy array of distances between the cities. `distances[i, j]`
        is the distance between cities `i` and `j`
    :param top_level_iterations: number of top-level iterations to run. Each top level
        iteration includes dispatching `n_workers` for `worker_iterations` and yielding
        the best result so far.
    :param worker_iterations: number of iterations each worker runs
        independently for each of the `top_level_iterations`
    :param n_workers: how many workers to run in parallel
    :param initial_temperature: initial temperature for simulated annealing
    :param cooling_rate: cooling rate for simulated annealing

    :return: list of city names in the order they should be visited
    """
    cities = np.arange(n_cities)

    # Start with a random route
    curr_route = np.random.permutation(cities)
    curr_distance = compute_distance(curr_route, distances)
    curr_temp = initial_temperature
    for _ in range(top_level_iterations):
        # We're going to run `n_workers` workers in parallel.
        # We need to prepare arguments for each of them.
        arguments = [
            (
                curr_route,
                distances,
                worker_iterations,
                curr_temp,
                cooling_rate,
                # Run each worker with different seed to ensure diverse results
                np.random.randint(100_000),
            )
            for _ in range(n_workers)
        ]
        # Run all workers in parallel and obtain their results using `starmap` function
        worker_results = simulated_annealing.starmap(arguments)
        best_worker_result = min(worker_results, key=lambda x: x.distance)

        if best_worker_result.distance < curr_distance:
            curr_route = best_worker_result.route
            curr_distance = best_worker_result.distance

        # Since we performed `worker_iterations` steps, we need to
        # reduce the temperature by `cooling_rate` to the power of `worker_iterations`
        curr_temp *= cooling_rate ** worker_iterations

        # Report intermediate results using yield
        yield curr_route, curr_distance


# Workers perform expensive computation and so may need more CPU resources
@mn.function(cpu=4, memory="1 GiB")
def simulated_annealing(
    curr_route: np.ndarray,
    distances: np.ndarray,
    n_iterations: int = 1000,
    temp: float = 1000,
    cooling_rate: float = 0.999,
    seed: int = 0,
):
    np.random.seed(seed)
    curr_distance = compute_distance(curr_route, distances)
    for _ in range(n_iterations):
        new_route = generate_neighbor(curr_route)
        new_distance = compute_distance(new_route, distances)
        # Decide whether to accept the new route.
        # If you analyze the equation below you will find that:
        #   1. If `new_distance < curr_distance` then `prob > 1`, so we always accept it
        #   2. If `new_distance > curr_distance` then `prob < 1`, so we accept it with
        #      probability `prob`. The bigger the difference between distances,
        #      (i.e., the worse the new solution is) the smaller the probability.
        #      Also, the bigger the temperature the bigger the probability.
        prob = np.exp((curr_distance - new_distance) / temp)
        if np.random.rand() < prob:
            curr_route, curr_distance = new_route, new_distance

        # Reduce the temperature
        temp *= cooling_rate

    # We can simply return final result if we don't want to yield any intermediate ones
    return AnnealingResult(route=curr_route, distance=curr_distance)


def compute_distance(route: np.ndarray, distances: np.ndarray) -> float:
    """Compute total distance of the route"""
    return np.sum(distances[route[:-1], route[1:]]) + distances[route[-1], route[0]]


def generate_neighbor(route: np.ndarray) -> List[str]:
    """Generate neighboring solution by swapping two random cities"""
    new_route = route.copy()
    a, b = np.random.choice(route.size, 2, replace=False)
    new_route[a], new_route[b] = new_route[b], new_route[a]
    return new_route
