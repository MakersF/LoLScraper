import logging
import datetime
import random
import threading
import os

from urllib.error import URLError
from concurrent.futures import ThreadPoolExecutor, as_completed

from cassiopeia import baseriotapi
from cassiopeia.dto.leagueapi import get_challenger, get_master
from cassiopeia.dto.matchlistapi import get_match_list
from cassiopeia.dto.matchapi import get_match
from cassiopeia.type.api.exception import APIError

from lol_scraper.data_types import TierSet, TierSeed, Tier, Queue, Maps, unix_time, SimpleCache, cache_autostore
from lol_scraper.summoners_api import get_tier_from_participants, summoner_names_to_id, leagues_by_summoner_ids

version_key = 'current_version'
delta_30_days = datetime.timedelta(days=30)
cache = SimpleCache()
LATEST = "latest"
max_analyzed_players_size = int(os.environ.get('MAX_ANALYZED_PLAYERS_SIZE', 50000))
EVICTION_RATE = float(os.environ.get('EVICTION_RATE', 0.5))  # Half of the analyzed players
concurrent_threads = int(os.environ.get('CONCURRENT_THREAD', 4))

patch_changed_lock = threading.Lock()
patch_changed = False


class FetchingException(Exception):

    def __init__(self, match):
        self.match = match

    def __repr__(self):
        return "Exception while fetching match {}".format(self.match)

    def __str__(self):
        return self.__repr__()


def riot_time(dt):
    if dt is None:
        dt = datetime.datetime.now()
    return int(unix_time(dt) * 1000)


def set_patch_changed(*args, **kwargs):
    with patch_changed_lock:
        global patch_changed
        patch_changed = True


def get_patch_changed():
    with patch_changed_lock:
        global patch_changed
        return bool(patch_changed)


@cache_autostore(version_key, 60 * 60, cache, on_change=set_patch_changed)
def get_last_patch_version():
    version_extended = baseriotapi.get_versions()[0]
    version = ".".join(version_extended.split(".")[:2])
    logging.getLogger(__name__).info("Fetching version {}".format(version))
    return version


def check_minimum_patch(patch, minimum):
    if not minimum:
        return True
    if minimum.lower() != LATEST:
        return patch >= minimum
    else:
        try:
            version = get_last_patch_version()
            return patch >= version
        except:
            # in case the connection failed, do not store it, and try next round
            # Reject every version as we are not sure which is the latest version
            # and we don't want to pollute the data with patches with the wrong version
            return False


def fetch_player(player_id, conf):
    match_list = get_match_list(player_id, begin_time=riot_time(conf['start']), end_time=riot_time(conf['end']),
                                ranked_queues=conf['queue'])
    matches = [match.matchId for match in match_list.matches]
    return matches, player_id


def fetch_match(match_id, conf):
    try:
        match = get_match(match_id, conf['include_timeline'])
        if match.mapId == Maps[conf['map_type']].value:
            match_min_tier, participant_tiers = get_tier_from_participants(match.participantIdentities,
                                                                           Tier.parse(conf['minimum_tier']),
                                                                           Queue[conf['queue']])

            valid = match_min_tier.is_better_or_equal(Tier.parse(conf['minimum_tier']))\
                    and check_minimum_patch(match.matchVersion, conf['minimum_patch'])
            return match, match_min_tier if valid else None, participant_tiers
    except Exception as e:
        raise FetchingException(match_id) from e


def handle_exception(e, logger):
    if isinstance(e, APIError):
        if 400 <= e.error_code < 500:
            # Might be a connection problem
            logger.warning("Encountered error {}".format(e))
        elif 500 <= e.error_code < 600:
            # Server problem. Let's give it some time
            logger.warning("Encountered error {}".format(e))
        else:
            logger.error("Encountered error {}".format(e))
    elif isinstance(e, URLError):
        logger.error("Encountered error {}. You are having connection issues".format(e))
    else:
        logger.exception("Encountered unexpected exception {}".format(e))


def download_matches(match_downloaded_callback, end_of_time_slice_callback, conf):
    logger = logging.getLogger(__name__)
    if conf['logging_level'] != logging.NOTSET:
        logger.setLevel(conf['logging_level'])
    else:
        # possibly set the level to warning
        pass

    def checkpoint(players_to_analyze, analyzed_players, matches_to_download_by_tier, downloaded_matches, total_matches):
        logger.info("Reached the checkpoint.".format(datetime.datetime.now().strftime("%m-%d %H:%M:%S"), total_matches))
        if end_of_time_slice_callback:
            end_of_time_slice_callback(players_to_analyze, analyzed_players, matches_to_download_by_tier,
                                       downloaded_matches, total_matches)

    players_to_analyze = TierSeed(tiers=conf['seed_players_by_tier'])

    total_matches = 0
    downloaded_matches = set(conf['downloaded_matches'])
    logger.info("{} previously downloaded matches".format(len(downloaded_matches)))

    matches_to_download_by_tier = conf['matches_to_download_by_tier']
    logger.info("{} matches to download".format(len(matches_to_download_by_tier)))
    analyzed_players = set()
    pool = ThreadPoolExecutor(max_workers=concurrent_threads)
    try:
        logger.info("Starting fetching..")

        while (players_to_analyze or matches_to_download_by_tier) and \
                not conf.get('exit', False):

            for tier in Tier.equals_and_above(Tier.parse(conf['minimum_tier'])):
                try:

                    if conf.get('exit', False):
                        logger.info("Got exit request")
                        break

                    logger.info("Starting player matchlist download for tier {}. Players in queue: {}. Downloads in queue: {}. Downloaded: {}"
                                .format(tier.name, len(players_to_analyze), len(matches_to_download_by_tier), total_matches))

                    futures = [
                        pool.submit(fetch_player, player_id, conf)
                        for player_id in players_to_analyze.consume(tier, 10) if player_id not in analyzed_players
                    ]
                    for future in as_completed(futures):
                        try:
                            matches, player_id = future.result()
                            matches_to_download_by_tier[tier].update(matches)
                            analyzed_players.add(player_id)
                        except Exception as e:
                            handle_exception(e, logger)

                    if conf.get('exit', False):
                        logger.info("Got exit request")
                        break

                    logger.info("Starting matches download for tier {}. Players in queue: {}. Downloads in queue: {}. Downloaded: {}"
                                .format(tier.name, len(players_to_analyze), len(matches_to_download_by_tier), total_matches))

                    futures = [
                        pool.submit(fetch_match, match_id, conf)
                        for match_id in matches_to_download_by_tier.consume(tier, 10, 0.2) if match_id not in downloaded_matches
                    ]
                    for future in as_completed(futures):
                        try:
                            match, match_min_tier, participant_tiers = future.result()
                            match_id = match.matchId
                            for tier, ids in participant_tiers.items():
                                players_to_analyze.update_tier(ids, tier)

                            downloaded_matches.add(match_id)
                            if match_min_tier:
                                match_downloaded_callback(match, match_min_tier.name)
                                total_matches += 1
                        except Exception as e:
                            handle_exception(e, logger)


                    # analyzed_players grows indefinitely. This doesn't make sense, as after a while a player have new matches
                    # So when the list grows too big we remove a part of the players, so they can be analyzed again.
                    if len(analyzed_players) > max_analyzed_players_size:
                        analyzed_players = {player_id for player_id in analyzed_players if
                                            random.random() < EVICTION_RATE}

                    # When a new patch is released, we can clear all the analyzed players and downloaded_matches if minimum_patch == 'latest'
                    if conf['minimum_patch'].lower() == LATEST and get_patch_changed():
                        analyzed_players = set()
                        downloaded_matches = set()

                except Exception as e:
                    handle_exception(e, logger)

    finally:
        # Always call the checkpoint, so that we can resume the download in case of exceptions.
        logger.info("Calling checkpoint callback")
        checkpoint(players_to_analyze, analyzed_players, matches_to_download_by_tier, downloaded_matches, total_matches)


def prepare_config(config):
    runtime_config = {}

    runtime_config['logging_level'] = logging._nameToLevel[config.get('logging_level', 'NOTSET')]

    # Parse the time boundaries
    runtime_config['end'] = None if not 'end_time' in config else datetime.datetime(**config['end_time'])
    runtime_config['start'] = None if not 'start_time' in config else datetime.datetime(**config['start_time'])

    if runtime_config['start'] is None:
        runtime_config['start'] = (runtime_config['end'] if runtime_config[
            'end'] else datetime.datetime.now()) - delta_30_days

    runtime_config['minimum_patch'] = config.get('minimum_patch', "")
    runtime_config['queue'] = config.get('queue', Queue.RANKED_SOLO_5x5.name)
    runtime_config['map_type'] = config.get('map', Maps.SUMMONERS_RIFT.name)
    runtime_config['minimum_tier'] = config.get('minimum_tier', Tier.bronze.name).lower()

    runtime_config['include_timeline'] = config.get('include_timeline', True)

    runtime_config['downloaded_matches'] = config.get('downloaded_matches', [])

    runtime_config['matches_to_download_by_tier'] = config.get('matches_to_download_by_tier', TierSet())

    runtime_config['seed_players_by_tier'] = config.get('seed_players_by_tier', None)

    if not runtime_config['seed_players_by_tier']:
        seed_players_by_tier = None
        while True:
            try:
                config_seed_players = config.get('seed_players', None)
                if config_seed_players is None:
                    # Let's use challenger and master tier players as seed
                    tiers = {
                        Tier.challenger: map(lambda league_entry_dto: league_entry_dto.playerOrTeamId,
                                             get_challenger(runtime_config['queue']).entries),
                        Tier.master: map(lambda league_entry_dto: league_entry_dto.playerOrTeamId,
                                         get_master(runtime_config['queue']).entries)
                    }
                    seed_players_by_tier = TierSeed(tiers=tiers)
                else:
                    # We have a list of seed players. Let's use it
                    seed_players = list(summoner_names_to_id(config_seed_players).values())
                    seed_players_by_tier = TierSeed(
                            tiers=leagues_by_summoner_ids(seed_players, Queue[runtime_config['queue']]))

                break
            except APIError:
                logger = logging.getLogger(__name__)
                logger.exception("APIError while initializing the script")
                # sometimes the network might have problems during the start. We don't want to crash just
                # because of that. Keep trying!
        seed_players_by_tier.remove_players_below_tier(Tier.parse(runtime_config['minimum_tier']))
        runtime_config['seed_players_by_tier'] = seed_players_by_tier._tiers

    return runtime_config


def setup_riot_api(conf):
    cassioepia = conf['cassiopeia']
    baseriotapi.set_api_key(cassioepia['api_key'])
    baseriotapi.set_region(cassioepia['region'])

    limits = cassioepia.get('rate_limits', None)
    if limits is not None:
        if isinstance(limits[0], (list, tuple)):
            baseriotapi.set_rate_limits(*limits)
        else:
            baseriotapi.set_rate_limit(limits[0], limits[1])

    baseriotapi.print_calls(cassioepia.get('print_calls', False))
