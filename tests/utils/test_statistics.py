#!/usr/bin/env python
# -*- coding: utf-8 -*-
import math
import pytest

from utils.statistics import HUMAN_SELECTION_LAMBDA, human_selection_shuffle


def average_by_group(array, group_size):
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
