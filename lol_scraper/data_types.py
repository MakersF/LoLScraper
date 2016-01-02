from collections import defaultdict, namedtuple
from enum import Enum, unique
import datetime
import math
import itertools
import time as _time

@unique
class Queue(Enum):
    RANKED_SOLO_5x5 = 0
    RANKED_TEAM_3x3 = 1
    RANKED_TEAM_5x5 = 2

@unique
class Tier(Enum):
    challenger = 0
    master = 1
    diamond = 2
    platinum = 3
    gold = 4
    silver = 5
    bronze = 6

    @classmethod
    def equals_and_above(cls, tier):
        for t in cls:
            if t.is_better_or_equal(tier):
                yield t

    @classmethod
    def all_tiers_below(cls, tier):
        for t in cls:
            if not t.is_better_or_equal(tier):
                yield  t

    def __hash__(self):
        return self.value

    def __eq__(self, other):
        return hasattr(other, "value") and self.value == other.value

    def best(self, other):
        if self.value <= other.value:
            return self
        return other

    def worst(self, other):
        if self.value >= other.value:
            return self
        return other

    def is_better_or_equal(self, other):
        return self.value <= other.value

    @classmethod
    def parse(cls, tier):
        initial = tier[0].lower()
        if initial == 'b':
            return cls.bronze
        elif initial == 's':
            return cls.silver
        elif initial == 'g':
            return cls.gold
        elif initial == 'p':
            return cls.platinum
        elif initial == 'd':
            return cls.diamond
        elif initial == 'm':
            return cls.master
        elif initial == 'c':
            return cls.challenger
        else:
            raise ValueError("No Tier with name {}".format(tier))

@unique
class Maps(Enum):
    SUMMONERS_RIFT = 11

class TierSet():
    """
    Class to keep players ids separated by tiers.
    """

    def __init__(self, tiers=None, max_items_per_set = 0):
        self._max_items_per_set = max_items_per_set
        self._tiers = defaultdict(set)
        if tiers:
            for tier in Tier:
                try:
                    to_add = tiers[tier]
                except KeyError:
                    to_add = None
                if to_add:
                    self._tiers[tier] = set(to_add)

    def __bool__(self):
        for set in self._tiers.values():
            if set:
                return True
        return False

    def __len__(self):
        length = 0
        for set in self._tiers.values():
            length += len(set)
        return length

    def __getitem__(self, item):
        return self._tiers[item]

    def __str__(self):
        return str(self._tiers)

    def __iadd__(self, other):
        self.update(other)
        return self

    def __isub__(self, other):
        self.difference_update(other)
        return self

    def __contains__(self, item):
        for set in self._tiers.values():
            if item in set:
                return True
        return False

    def update_tier(self, values, tier):
        if values:
            tier_set = self._tiers[tier]
            if self._max_items_per_set:
                can_add = max(0, self._max_items_per_set - len(tier_set))
                if can_add >= len(values):
                    tier_set.update(values)
                else:
                    tier_set.update(itertools.islice(values, can_add))
            else:
                tier_set.update(values)

    def update(self, other):
        for tier, addition in other._tiers.items():
            if addition:
                self.update_tier(addition, tier)

    def difference_update(self, other):
        for tier, values in self._tiers.items():
            if values:
                difference = other._tiers.get(tier, None)
                if difference:
                    self._tiers[tier].difference_update(difference)

    def clear(self):
        for tier in Tier:
            current = self._tiers[tier]
            if current:
                current.clear()

    def consume(self, tier, minimum_number=1, percentage=0):
        try:
            set = self._tiers[tier]
            length = len(set)
            elements_to_consume = min(length, max(minimum_number, int(math.floor(percentage * length))))
            for _ in range(elements_to_consume):
                yield set.pop()
        except KeyError:
            raise StopIteration

    def to_json(self):
        dct = {}
        for tier, values in self._tiers.items():
            if values:
                dct[tier.name] = list(values)
        return dct

    def from_json(self, json_dump):
        for tier_name, values in json_dump.items():
            if values:
                self._tiers[Tier.parse(tier_name)] = set(values)
        return self

    def __iter__(self):
        for set in self._tiers.values():
            if set:
                for id in set:
                    yield id

class TierSeed(TierSet):

    def __init__(self, tiers=None, max_items_per_set=1000):
        super().__init__(tiers=tiers, max_items_per_set=max_items_per_set)

    def get_player_tier(self, player_id):
        for tier in Tier:
            tier_set = self._tiers[tier]
            if player_id in tier_set:
                return tier
        raise ValueError("{0} is not registered in the TierSeed".format(player_id))

    def remove_players_below_tier(self, tier):
        for t in Tier.all_tiers_below(tier):
            self._tiers.pop(t, None)

class TimeSlice(namedtuple('TimeSliceBase', ['begin', 'end'])):

    def __str__(self):
        return "({},{})".format(datetime.datetime.utcfromtimestamp(self.begin/1000),
                                datetime.datetime.utcfromtimestamp(self.end/1000))

class SimpleCache():

    def __init__(self):
        self.store = {}

    def set(self, key, value, time=0):
        self.store[key] = (value, time, _time.time())

    def get(self, key, default=None):
        item = self.store.get(key, None)
        if item is None:
            return default
        else:
            value, time, put_time = item
            current_time = _time.time()
            # time = 0 means it never expires
            if put_time + time > current_time or time == 0:
                # it is still valid
                return value
            else:
                # expired value
                del self.store[key]
                return default

def cache_autostore(key, duration, cache, args_to_str=None, on_change=None):
    def make_key(*args, **kwargs):
        if args_to_str:
            return str(key) + args_to_str(*args, **kwargs)
        else:
            return key

    def function_decorator(wrapped):
        def wrapper(*args, **kwargs):
            sentinel = object()
            composed_key = make_key(*args, **kwargs)
            # get the value
            cached_value = cache.get(composed_key, sentinel)

            # if the value is saved or expired
            if cached_value is sentinel:
                # call the function which gives the real value
                new_value = wrapped(*args, **kwargs)
                # store it
                cache.set(composed_key, new_value, duration)
                # if we want to be notified of the change
                if on_change:
                    # get the old value
                    old = cache.get(composed_key + "_old")
                    # set the new value
                    cache.set(composed_key + "_old", new_value, 0)
                    if old != new_value:
                        on_change(old, new_value)
                # Then return the new value
                return new_value
            else:
                # The value stored was still fresh, return it
                return cached_value
        return wrapper
    return function_decorator

epoch = datetime.datetime.utcfromtimestamp(0)

def unix_time(dt):
    delta = dt - epoch
    return delta.total_seconds()

def slice_time(begin, end=None, duration=datetime.timedelta(days=2)):
    """
    :param begin: datetime
    :param end: datetime
    :param duration: timedelta
    :return: a generator for a set of timeslices of the given duration
    """
    duration_ms = int(duration.total_seconds() * 1000)
    previous = int(unix_time(begin) * 1000)
    next = previous + duration_ms
    now_ms = unix_time(datetime.datetime.now())*1000
    end_slice = now_ms if not end else min(now_ms, int(unix_time(end) * 1000))

    while next < end_slice:
        yield TimeSlice(previous, next)
        previous = next
        next += duration_ms
        now_ms = unix_time(datetime.datetime.now())*1000
        end_slice = now_ms if not end else min(now_ms, int(unix_time(end) * 1000))
    yield TimeSlice(previous, end_slice)
