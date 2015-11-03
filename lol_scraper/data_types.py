from collections import defaultdict, namedtuple
from enum import Enum, unique
import datetime
import math

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
                to_add = tiers.get(tier, None)
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

    def update_tier(self, values, tier):
        if values:
            tier_set = self._tiers[tier]
            if self._max_items_per_set and len(tier_set) + len(values) > self._max_items_per_set:
                return
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
            elements_to_consume = max(minimum_number, int(math.floor(percentage * len(set))))
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
