from collections import defaultdict

from data_types import Tier, Queue
from cassiopeia.dto.summonerapi import get_summoners_by_name
from cassiopeia.dto.leagueapi import get_league_entries_by_summoner

def _slice(start, stop, step):
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

def leagues_by_summoner_ids(summoner_ids, queue=Queue.RANKED_SOLO_5x5):
    """
    Takes in a list of players ids and divide them by league tiers.
    :param summoner_ids: a list containing the ids of players
    :param queue: the queue to consider
    :return: a dictionary tier -> set of ids
    """
    summoners_league = defaultdict(set)
    for start, end in _slice(0, len(summoner_ids), 10):
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
    for start, end in _slice(0, len(summoners), 40):
        result = get_summoners_by_name(summoners[start:end])
        for name, summoner in result.items():
            ids[name] = summoner.id
    return ids