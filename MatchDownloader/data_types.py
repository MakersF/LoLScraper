from collections import defaultdict
from enum import Enum, unique

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
        self._max_items_per_set = 0
        self._tiers = defaultdict(set)
        if tiers:
            for tier in Tier:
                to_add = tiers.get(tier, None)
                if to_add:
                    self._tiers[tier] = set(to_add)

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

    def update(self, other):
        for tier, addition in other._tiers.items():
            if addition:
                tier_set = self._tiers[tier]
                if self._max_items_per_set and len(tier_set) + len(addition) > self._max_items_per_set:
                    continue
                else:
                    tier_set.update(addition)

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

    def consume(self, tier, number=None):
        from itertools import count
        try:
            set = self._tiers[tier]
            generator = count() if not number else range(number)
            for _ in generator:
                yield set.pop()
        except:
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