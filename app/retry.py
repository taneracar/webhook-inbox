import random


def backoff_s(attempt: int, base_s: float, cap_s: float, rng: random.Random) -> float:
    """Exponential backoff with full jitter.

    attempt is 1 for the first retry. The ceiling doubles each time up to cap;
    the actual delay is uniform in [0, ceiling], which spreads retries out so a
    recovering downstream is not hit by every failed event at the same moment.
    """
    ceiling = min(cap_s, base_s * (2 ** (attempt - 1)))
    return rng.uniform(0, ceiling)
