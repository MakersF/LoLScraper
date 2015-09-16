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

    def __lt__(self,other):
        return self.value > other.value

    def __le__(self,other):
        return self.value >= self.value

    def __gt__(self,other):
        return self.value <= other.value

    def __ge__(self,other):
        return self.value <= other.value

    def __eq__(self,other):
        return self.value == other.value

    def __ne__(self,other):
        return self.value != other.value

@unique
class Maps(Enum):
    SUMMONERS_RIFT = 11

def leagues_by_summoner_ids(summoner_ids, queue=Queue.RANKED_SOLO_5x5):
    summoners_league = defaultdict(set)
    for start, end in slice(0, len(summoner_ids), 10):
        for id, leagues in get_league_entries_by_summoner(summoner_ids[start:end]).items():
            for league in leagues:
                if Queue[league.queue]==queue:
                    summoners_league[Tier[league.tier.lower()]].add(int(id))
    return summoners_league

def update_participants(tier_seed, participantsIdentities, queue=Queue.RANKED_SOLO_5x5, minimum_tier=Tier.bronze):
    match_tier = Tier.challenger
    leagues = leagues_by_summoner_ids([p.player.summonerId for p in participantsIdentities], queue)
    for league, ids in leagues.items():
        if league >= minimum_tier:
            tier_seed[league].update(ids)
            match_tier = min(match_tier, league)
    return match_tier

def summoner_names_to_id(summoners):
    ids = {}
    for start, end in slice(0, len(summoners), 40):
        result = get_summoners_by_name(summoners[start:end])
        for name, summoner in result.items():
            ids[name] = summoner.id
    return ids


class TierSeed():

    _tiers = dict()

    def __init__(self, tiers={}):
        for tier in Tier:
            to_add = tiers.get(tier, set())
            if isinstance(to_add, set):
                self._tiers[tier] = to_add
            else:
                self._tiers[tier] = set(to_add)


    def __getitem__(self, item):
        if isinstance(item, Tier):
            return self._tiers[item]
        else:
            raise ValueError("TierSeed has no tier '{}'".format(item))

    def __str__(self):
        return str(self._tiers)

    def __iadd__(self, other):
        self.update(other)
        return self

    def __isub__(self, other):
        self.difference_update(other)
        return self

    def get(self, *args, **kwargs):
        return self._tiers.get(*args, **kwargs)

    def update(self, other):
        for tier in Tier:
            current = self[tier]
            addition = other[tier]
            current.update(addition)

    def difference_update(self, other):
        for tier in Tier:
            current = self[tier]
            difference = other[tier]
            current.difference_update(difference)

    def __iter__(self):
        for tier, ids in self._tiers.items():
            for id in ids:
                yield id

    def to_dict(self, **kwargs):
        return self._tiers

    def get_player_tier(self, player_id):
        for tier in Tier:
            tier_set = self[tier]
            if player_id in tier_set:
                return tier
        raise ValueError("{0} is not registered in the TierSeed".format(player_id))
