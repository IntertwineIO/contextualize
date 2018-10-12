#!/usr/bin/env python
# -*- coding: utf-8 -*-
import math
import pytest

from utils.statistics import HUMAN_SELECTION_LAMBDA, human_dwell_time, human_selection_shuffle


@pytest.mark.unit
@pytest.mark.parametrize(
    ('mu', 'sigma', 'base', 'multiplier', 'minimum', 'maximum'),
    [(0,    0.5,     1,      1,            1,         4),
     (0,    0.25,    1,      2,            1,         4),
     (0,    0.5,     1,      2,            1,         None),
     (0,    0.25,    1,      4,            1,         None),
     ])
def test_human_dwell_time(mu, sigma, base, multiplier, minimum, maximum):
    """Test that human dwell times are unique and within expected ranges"""
    lower = minimum
    upper = float('Inf') if maximum is None else maximum
    iterations = 10

    unique_dwell_times = set()
    unique_threshold = 0.9

    cumulative_dwell_time = 0
    lognormal_mean = math.exp(mu + (sigma ** 2) / 2)
    dwell_threshold = base + lognormal_mean * multiplier * (2 / sigma)

    for _ in range(iterations):
        dwell_time = human_dwell_time(mu, sigma, base, multiplier, minimum, maximum)
        assert dwell_time >= lower
        assert dwell_time <= upper
        unique_dwell_times.add(dwell_time)
        cumulative_dwell_time += dwell_time

    assert len(unique_dwell_times) / iterations >= unique_threshold
    assert cumulative_dwell_time / iterations <= dwell_threshold


def average_by_group(array, group_size):
    """Calculate array averages per group, given a group size"""
    length = len(array)
    num_groups = math.ceil(length / group_size)
    averages = []
    remainder = array
    for _ in range(num_groups):
        group = remainder[:group_size]
        averages.append(sum(group) / len(group))
        remainder = remainder[group_size:]
    return averages


@pytest.mark.unit
@pytest.mark.parametrize('lambd', (HUMAN_SELECTION_LAMBDA, 2 * HUMAN_SELECTION_LAMBDA))
def test_human_selection_shuffle(seed, lambd):
    """Test human selection shuffle via unsteadily increasing values"""
    iterations = 10
    array_size = 20
    group_size = 5

    total_deltas = iterations * array_size
    delta_threshold = 0.5
    cumulative_delta = 0

    total_progressions = iterations * (math.ceil(array_size / group_size) - 1)
    progression_threshold = 0.5
    cumulative_progression = 0

    unshuffled = range(array_size)

    for _ in range(iterations):
        array = list(range(array_size))
        human_selection_shuffle(array, lambd=lambd)
        shuffled = array
        assert len(shuffled) == array_size
        cumulative_delta += sum(abs(s - u) for s, u in zip(shuffled, unshuffled))

        shuffled_averages = average_by_group(shuffled, group_size)
        prior = None
        for value in shuffled_averages:
            if prior:
                cumulative_progression += (1 if value >= prior else -1)
            prior = value

    # These can randomly fail, but they should be extraordinarily rare
    assert cumulative_delta / total_deltas >= delta_threshold
    assert cumulative_progression / total_progressions >= progression_threshold
