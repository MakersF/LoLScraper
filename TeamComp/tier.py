from collections import defaultdict
from enum import Enum, unique
from cassiopeia.dto.summonerapi import get_summoners_by_name
from cassiopeia.dto.leagueapi import get_league_entries_by_summoner

def slice(start, stop, step):
    """
    Generate pairs so that you can slice from start to stop, step elements at a time
    :param start: The start of the generated series
    :param stop: The last of the generated series
    :param step: The difference between the first element of the returned pair and the second
    :return: A pair that you can use to slice
    """
    if step == 0:
        raise ValueError("slice() arg 3 must not be zero")
    if start==stop:
        raise StopIteration

    previous = start
    next = start + step
    while next < stop:
        yield previous, next
        previous += step
        next += step
    yield previous, stop


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

def leagues_by_summoner_ids(summoner_ids, queue=Queue.RANKED_SOLO_5x5):
    """
    Takes in a list of players ids and divide them by league tiers.
    :param summoner_ids: a list containing the ids of players
    :param queue: the queue to consider
    :return: a dictionary tier -> set of ids
    """
    summoners_league = defaultdict(set)
    for start, end in slice(0, len(summoner_ids), 10):
        for id, leagues in get_league_entries_by_summoner(summoner_ids[start:end]).items():
            for league in leagues:
                if Queue[league.queue]==queue:
                    summoners_league[Tier.parse(league.tier)].add(int(id))
    return summoners_league

def update_participants(tier_seed, participantsIdentities, minimum_tier=Tier.bronze, queue=Queue.RANKED_SOLO_5x5):
    """
    Add the participants of a match to a TierSeed if they are at least minimum_tier. Return the tier of the lowest tier
    player in the match
    :param tier_seed: the TierSeed to update
    :param participantsIdentities: the match participants
    :param minimum_tier: the minimum tier that a participant must be in order to be added
    :param queue: the queue over which the tier of the player is considered
    :return: the tier of the lowest tier player in the match
    """
    match_tier = Tier.challenger
    leagues = leagues_by_summoner_ids([p.player.summonerId for p in participantsIdentities], queue)
    for league, ids in leagues.items():
        # challenger is 0, bronze is 6
        if league.is_better_or_equal(minimum_tier):
            tier_seed[league].update(ids)
        match_tier = match_tier.worst(league)
    return match_tier

def summoner_names_to_id(summoners):
    """
    Gets a list of summoners names and return a dictionary mapping the player name to his/her summoner id
    :param summoners: a list of player names
    :return: a dictionary name -> id
    """
    ids = {}
    for start, end in slice(0, len(summoners), 40):
        result = get_summoners_by_name(summoners[start:end])
        for name, summoner in result.items():
            ids[name] = summoner.id
    return ids


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
        for tier, value in self._tiers.items():
            if dct[tier.name]:
                dct[tier.name] = list(value)
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