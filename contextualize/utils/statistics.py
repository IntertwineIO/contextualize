#!/usr/bin/env python
# -*- coding: utf-8 -*-
import math
import random
from collections import namedtuple

from contextualize.utils.structures import DotNotatableOrderedDict
from contextualize.utils.tools import represent


# https://searchenginewatch.com/sew/study/2276184/no-1-position-in-google-gets-33-of-search-traffic-study
FIRST_SELECTION_PROBABILITY = 0.325
# Cumulative exponential distribution:
#      F = 1 - exp(- lambda * x)
# lambda = - log(1 - F) / x
#        = - log(1 - 0.325) / 1 = 0.393
HUMAN_SELECTION_LAMBDA = 0.393


def random_exponential_index(lambd=1, length=None, attempts=10):
    """
    Random Exponential Index

    Use exponential probability distribution to generate a random index.

    I/O:
    lambd=1:        Exponential distribution lambda aka rate parameter
    length=None:    Length of implied list for which index is generated
    attempts=10:    Max attempts to generate index less than length
    return:         Random integer less than length using exponential
                    probability distribution with given lambda. If all
                    attempts fail, return 0, the mode.
    raise:          ValueError if length less than 1
    """
    length = float('Inf') if length is None else length
    if length < 1:
        raise ValueError('Length must be at least 1 to generate index')
    if length > 1:
        for _ in range(attempts):
            value = random.expovariate(lambd)
            if value < length:
                return math.floor(value)
    return 0


def human_selection_shuffle(values, lambd=HUMAN_SELECTION_LAMBDA):
    """
    Human Selection Shuffle

    Shuffle list of values in place via exponential distribution that
    approximates likelihood of user clicking the nth element. The
    distribution is used repeatedly to select multiple values. Once a
    value has been selected, it is no longer eligible for selection and
    any following elements have their indices lowered by 1.

    I/O:
    values:         List of values to be shuffled in place
    lambd=0.393:    Exponential distribution lambda aka rate parameter.
                    Default is HUMAN_SELECTION_LAMBDA with value 0.393,
                    derived from first selection probability of 32.5%
    return:         None
    """
    num_values = len(values)
    if num_values < 2:
        return
    num_remaining = num_values
    while num_remaining:
        selected_index = random_exponential_index(lambd, length=num_remaining)
        selected = values.pop(selected_index)
        values.append(selected)
        num_remaining -= 1


def random_lognormal(mu=0, sigma=1, minimum=0, maximum=None, attempts=10):
    """
    Random Lognormal

    Use lognormal probability distribution to generate a random value.

    I/O:
    mu=0:           Mean of ln(X), given lognormal random variable X
    sigma=1:        Standard deviation of ln(X), given X per above
    minimum=0:      Minimum allowable value
    maximum=None:   Maximum allowable value; default to Infinity if None
    attempts=10:    Number of attempts to generate value between
                    minimum and maximum
    return:         Random value sampled from lognormal probability
                    distribution with given mu and sigma between minimum
                    and maximum. If all attempts fail, return mode:
                    exp(mu - (sigma ** 2))
    """
    maximum = float('Inf') if maximum is None else maximum
    for _ in range(attempts):
        value = random.lognormvariate(mu, sigma)
        if minimum <= value <= maximum:
            return value
    return math.exp(mu - (sigma ** 2))  # mode


def human_dwell_time(mu=0, sigma=1, base=0, multiplier=1,
                     minimum=0, maximum=None, attempts=10):
    """
    Human Dwell Time

    "Users' dwell time on online articles (jokes, news etc.) follows a
    log-normal distribution"
    - https://en.wikipedia.org/wiki/Log-normal_distribution
    """
    lognormal_min = (minimum - base) / multiplier
    lognormal_max = (maximum - base) / multiplier if maximum is not None else None
    lognormal_value = random_lognormal(
        mu=mu, sigma=sigma, minimum=lognormal_min, maximum=lognormal_max, attempts=attempts)
    return base + lognormal_value * multiplier


class HumanDwellTime:

    Arguments = namedtuple('HumanDwellTimeArguments', 'mu sigma base multiplier minimum maximum')

    ARGUMENT_DEFAULTS = Arguments(mu=0, sigma=0.5, base=1, multiplier=1, minimum=1, maximum=3)

    def __init__(self,
                 mu=ARGUMENT_DEFAULTS.mu,
                 sigma=ARGUMENT_DEFAULTS.sigma,
                 base=ARGUMENT_DEFAULTS.base,
                 multiplier=ARGUMENT_DEFAULTS.multiplier,
                 minimum=ARGUMENT_DEFAULTS.minimum,
                 maximum=ARGUMENT_DEFAULTS.maximum):
        self.mu = mu
        self.sigma = sigma
        self.base = base
        self.multiplier = multiplier
        self.minimum = minimum
        self.maximum = maximum

    def as_arguments(self):
        return self.Arguments(**self.__dict__)

    def random_delay(self):
        return human_dwell_time(**self.__dict__)

    def __repr__(self):
        return represent(self, **self.__dict__)
