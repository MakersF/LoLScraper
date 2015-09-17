from collections import defaultdict
from enum import Enum, unique
from cassiopeia.dto.summonerapi import get_summoners_by_name
from cassiopeia.dto.leagueapi import get_league_entries_by_summoner

def slice(start, stop, step):
    rg = range(start, stop, step)
    for begin, end in zip(rg[:-1], rg[1:]):
        yield begin, end
    yield rg[-1], stop

@unique
class Queue(Enum):
    RANKED_SOLO_5x5 = 0
    RANKED_TEAM_3x3 = 1
    RANKED_TEAM_5x5 = 2

class Tier:
    challenger = 0
    master = 1
    diamond = 2
    platinum = 3
    gold = 4
    silver = 5
    bronze = 6

    num_tiers = 7

    def is_better(first, second):
        return min(first, second)

    @staticmethod
    def worse(first, second):
        return max(first, second)

    @staticmethod
    def is_better_or_equal(better, worse):
        return better <= worse

    @classmethod
    def to_string(cls, tier):
        return Tier.__strings[tier]

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
    summoners_league = defaultdict(set)
    for start, end in slice(0, len(summoner_ids), 10):
        for id, leagues in get_league_entries_by_summoner(summoner_ids[start:end]).items():
            for league in leagues:
                if Queue[league.queue]==queue:
                    summoners_league[Tier.parse(league.tier)].add(int(id))
    return summoners_league

def update_participants(tier_seed, participantsIdentities, minimum_tier=Tier.bronze, queue=Queue.RANKED_SOLO_5x5):
    match_tier = Tier.challenger
    leagues = leagues_by_summoner_ids([p.player.summonerId for p in participantsIdentities], queue)
    for league, ids in leagues.items():
        # challenger is 0, bronze is 6
        if Tier.is_better_or_equal(league, minimum_tier):
            tier_seed[league].update(ids)
        match_tier = Tier.worse(match_tier, league)
    return match_tier

def summoner_names_to_id(summoners):
    ids = {}
    for start, end in slice(0, len(summoners), 40):
        result = get_summoners_by_name(summoners[start:end])
        for name, summoner in result.items():
            ids[name] = summoner.id
    return ids


class TierSeed():

    def __init__(self, tiers=None):
        self._tiers = [None for x in range(Tier.num_tiers)]
        if tiers:
            for tier in range(Tier.num_tiers):
                to_add = tiers[tier]
                if to_add:
                    self._tiers[tier] = set(to_add)

    def __getitem__(self, item):
        itm = self._tiers[item]
        if not itm:
            itm = set()
            self._tiers[item] = itm
        return itm

    def __str__(self):
        return str(self._tiers)

    def __iadd__(self, other):
        self.update(other)
        return self

    def __isub__(self, other):
        self.difference_update(other)
        return self

    def update(self, other):
        for tier in range(Tier.num_tiers):
            addition = other._tiers[tier]
            if addition:
                current = self._tiers[tier]
                if not current:
                    current = set(addition)
                    self._tiers[tier] = current
                    continue
                current.update(addition)

    def difference_update(self, other):
        for tier in range(Tier.num_tiers):
            difference = other._tiers[tier]
            if difference:
                current = self._tiers[tier]
                if not current:
                    continue
                current.difference_update(difference)

    def clear(self):
        for tier in range(Tier.num_tiers):
            current = self._tiers[tier]
            if current:
                current.clear()

    def __iter__(self):
        for ids in self._tiers:
            if ids:
                for id in ids:
                    yield id

    def get_player_tier(self, player_id):
        for tier in range(Tier.num_tiers):
            tier_set = self[tier]
            if tier_set and player_id in tier_set:
                return tier
        raise ValueError("{0} is not registered in the TierSeed".format(player_id))
