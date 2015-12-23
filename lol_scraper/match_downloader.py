from contextlib import closing, suppress
import logging
import datetime
import os
import argparse
import random
import threading

from json import loads, dumps
from time import sleep
from itertools import takewhile
from urllib.error import URLError

from cassiopeia import baseriotapi
from cassiopeia.dto.matchlistapi import get_match_list
from cassiopeia.dto.matchapi import get_match
from cassiopeia.type.api.exception import APIError

from persist import TierStore, JSONConfigEncoder
from data_types import TierSet, TierSeed, Tier, Queue, Maps, unix_time, SimpleCache, cache_autostore
from summoners_api import update_participants, summoner_names_to_id, leagues_by_summoner_ids

version_key = 'current_version'
current_state_extension = '.checkpoint'
delta_30_days = datetime.timedelta(days=30)
cache = SimpleCache()
LATEST = "latest"
MAX_ANALYZED_PLAYERS_SIZE = 50000
EVICTION_RATE = 0.5 # Half of the analyzed players

patch_changed_lock = threading.Lock()
patch_changed = False

def make_store_callback(store):
    def store_callback(match, tier):
        store.store(match.to_json(sort_keys=False,indent=None), tier)
    return store_callback

def riot_time(dt):
    if dt is None:
        dt = datetime.datetime.now()
    return int(unix_time(dt) * 1000)

def set_patch_changed(*args, **kwargs):
    patch_changed_lock.aquire()
    try:
        global  patch_changed
        patch_changed = True
    finally:
        patch_changed_lock.release()

def get_patch_changed():
    patch_changed_lock.aquire()
    try:
        global  patch_changed
        return bool(patch_changed)
    finally:
        patch_changed_lock.release()

cache_autostore(version_key, 60*60, cache, on_change=set_patch_changed)
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



def download_matches(match_downloaded_callback, end_of_time_slice_callback, conf):
    logger = logging.getLogger(__name__)
    if conf['logging_level'] != logging.NOTSET:
        logger.setLevel(conf['logging_level'])
    else:
        # possibly set the level to warning
        pass

    def checkpoint(players_to_analyze, analyzed_players, matches_to_download_by_tier, downloaded_matches, total_matches, max_match_id):
        logger.info("Reached the checkpoint.".format(datetime.datetime.now().strftime("%m-%d %H:%M:%S"), total_matches))
        if end_of_time_slice_callback:
            end_of_time_slice_callback(players_to_analyze, analyzed_players, matches_to_download_by_tier, downloaded_matches, total_matches, max_match_id)

    players_to_analyze = TierSeed(tiers=conf['seed_players_by_tier'])

    total_matches = 0
    conf['maximum_downloaded_match_id'] = 0
    downloaded_matches = set(conf['downloaded_matches'])
    logger.info("{} previously downloaded matches".format(len(downloaded_matches)))

    matches_to_download_by_tier = conf['matches_to_download_by_tier']
    logger.info("{} matches to download".format( len(matches_to_download_by_tier)))
    analyzed_players = set()
    try:
        logger.info("Starting fetching..")

        while (players_to_analyze or matches_to_download_by_tier) and\
                not conf.get('exit', False):

            # When an exception is raised, the loop starts again. If the player download part raises exceptions often, it will be skipped
            # many times while we always download players. This tries to fix it
            working_on_matches = False

            for tier in Tier.equals_and_above(Tier.parse(conf['minimum_tier'])):
                try:
                    if conf.get('exit', False):
                        logger.info("Got exit request")
                        break

                    if not working_on_matches:
                        logger.info("Starting player matchlist download for tier {}. Players in queue: {}. Downloads in queue: {}. Downloaded: {}"
                                  .format(tier.name, len(players_to_analyze), len(matches_to_download_by_tier), total_matches))

                        for player_id in players_to_analyze.consume(tier, 10):
                            if player_id not in analyzed_players:
                                match_list = get_match_list(player_id, begin_time=riot_time(conf['start']), end_time=riot_time(conf['end']), ranked_queues=conf['queue'])
                                for match in match_list.matches:
                                    match_id = match.matchId
                                    if match_id > conf['minimum_match_id']:
                                        matches_to_download_by_tier[tier].add(match_id)
                                analyzed_players.add(player_id)

                    if conf.get('exit', False):
                        logger.info("Got exit request")
                        break

                    logger.info("Starting matches download for tier {}. Players in queue: {}. Downloads in queue: {}. Downloaded: {}"
                              .format(tier.name, len(players_to_analyze), len(matches_to_download_by_tier), total_matches))

                    working_on_matches = True
                    for match_id in takewhile(lambda _: not conf.get('exit', False),
                                              matches_to_download_by_tier.consume(tier, 10, 0.2)):
                        match = get_match(match_id, conf['include_timeline'])
                        if match.mapId == Maps[conf['map_type']].value:
                            match_min_tier = update_participants(players_to_analyze, match.participantIdentities, Tier.parse(conf['minimum_tier']), Queue[conf['queue']])

                            if match_id not in downloaded_matches and match_min_tier.is_better_or_equal(Tier.parse(conf['minimum_tier']))\
                                    and check_minimum_patch(match.matchVersion,conf['minimum_patch']):

                                conf['maximum_downloaded_match_id'] = max(match_id, conf['maximum_downloaded_match_id'])
                                match_downloaded_callback(match, match_min_tier.name)
                                total_matches += 1

                            downloaded_matches.add(match_id)
                    working_on_matches = False

                    # analyzed_players grows indefinitely. This doesn't make sense, as after a while a player have new matches
                    # So when the list grows too big we remove a part of the players, so they can be analyzed again.
                    if len(analyzed_players) > MAX_ANALYZED_PLAYERS_SIZE:
                        analyzed_players = { player_id for player_id in analyzed_players if random.random() < EVICTION_RATE }

                    # When a new patch is released, we can clear all the analyzed players and downloaded_matches if minimum_patch == 'latest'
                    if conf['minimum_patch'].lower() != LATEST and get_patch_changed():
                        analyzed_players = set()
                        downloaded_matches = set()

                except APIError as e:
                    if 400 <= e.error_code < 500:
                        # Might be a connection problem
                        logger.warning("Encountered error {}".format(e))
                        sleep(2)
                        continue
                    elif 500 <= e.error_code < 600:
                        # Server problem. Let's give it some time
                        logger.warning("Encountered error {}".format(e))
                        sleep(2)
                        continue
                    else:
                        logger.error("Encountered error {}".format(e))
                        continue
                except URLError as e:
                    logger.error("Encountered error {}. You are having connection issues".format(e))
                    # Connection error. You are unable to reach out to the network. Sleep!
                    sleep(10)
                    continue
                except Exception as e:
                    logger.exception("Encountered unexpected exception {}".format(e))
                    continue

    finally:
        #Always call the checkpoint, so that we can resume the download in case of exceptions.
        checkpoint(players_to_analyze, analyzed_players, matches_to_download_by_tier, downloaded_matches, total_matches, conf['maximum_downloaded_match_id'])

def prepare_config(config):

    runtime_config = {}

    runtime_config['logging_level'] = logging._nameToLevel[config.get('logging_level', 'NOTSET')]


    # Parse the time boundaries
    runtime_config['end'] = None if not 'end_time' in config else datetime.datetime(**config['end_time'])
    runtime_config['start'] = None if not 'start_time' in config else datetime.datetime(**config['start_time'])

    if runtime_config['start'] is None:
        runtime_config['start'] = (runtime_config['end'] if runtime_config['end'] else datetime.datetime.now()) - delta_30_days


    runtime_config['minimum_patch'] = config.get('minimum_patch', "")
    runtime_config['queue'] = config.get('queue', Queue.RANKED_SOLO_5x5.name)
    runtime_config['map_type'] = config.get('map', Maps.SUMMONERS_RIFT.name)
    runtime_config['minimum_tier'] = config.get('minimum_tier', Tier.bronze.name).lower()

    runtime_config['include_timeline'] = config.get('include_timeline', True)

    runtime_config['minimum_match_id'] = config.get('minimum_match_id', 0)

    runtime_config['downloaded_matches'] = config.get('downloaded_matches', [])

    runtime_config['matches_to_download_by_tier'] = config.get('matches_to_download_by_tier', TierSet())

    runtime_config['seed_players_by_tier'] = config.get('seed_players_by_tier', None)

    if not runtime_config['seed_players_by_tier']:
        seed_players_by_tier = None
        while True:
            try:
                seed_players = list(summoner_names_to_id(config['seed_players']).values())
                seed_players_by_tier = TierSeed(tiers=leagues_by_summoner_ids(seed_players, Queue[runtime_config['queue']]))
                break
            except APIError:
                logger = logging.getLogger(__name__)
                logger.exception("APIError while initializing the script")
                # sometimes the network might have problems during the start. We don't want to crash just
                # because of that. Keep trying!
                sleep(5)
        seed_players_by_tier.remove_players_below_tier(Tier.parse(runtime_config['minimum_tier']))
        runtime_config['seed_players_by_tier'] = seed_players_by_tier._tiers

    return runtime_config

def setup_riot_api(conf):
    cassioepia = conf['cassiopeia']
    baseriotapi.set_api_key(cassioepia['api_key'])
    baseriotapi.set_region(cassioepia['region'])

    limits = cassioepia.get('rate_limits', None)
    if limits is not None:
        if isinstance(limits[0], list):
            baseriotapi.set_rate_limits(*limits)
        else:
            baseriotapi.set_rate_limit(limits[0], limits[1])

    baseriotapi.print_calls(cassioepia.get('print_calls', False))

def download_from_config(conf, store_callback, checkpoint_callback):
    setup_riot_api(conf)
    runtime_config = prepare_config(conf)

    download_matches(store_callback, checkpoint_callback, runtime_config)

def time_slice_end_callback(config_file, players_to_analyze, analyzed_players, matches_to_download_by_tier, downloaded_matches, total_matches, max_match_id):
        current_state={}
        current_state['minimum_match_id'] = max_match_id
        current_state['seed_players_by_tier'] = players_to_analyze.to_json()
        with open(config_file+current_state_extension, 'wt') as state:
            state.write(dumps(current_state, cls=JSONConfigEncoder, indent=4))

def main(configuration_file, no_state=False):
    with open(configuration_file, 'rt') as config_file:
        json_conf = loads(config_file.read())

    with suppress(FileNotFoundError), open(configuration_file+current_state_extension, 'rt') as state:
        current_state = loads(state.read())
        json_conf.update(current_state)
        if "seed_players_by_tier" in json_conf:
            # Parse the tier name to the an instance of the Tier class
            json_conf['seed_players_by_tier'] = {Tier.parse(tier_name):players for tier_name, players in json_conf['seed_players_by_tier'].items()}


    base_file_name = json_conf.get('base_file_name', '')
    matches_per_file = json_conf.get('matches_per_file', 0)
    destination_directory = json_conf['destination_directory']
    # Allow the directory to be relative to the config file.
    if destination_directory.startswith('__file__'):
        configuration_file_dir = os.path.dirname(os.path.realpath(configuration_file))
        destination_directory=destination_directory.replace('__file__', configuration_file_dir)

    checkpoint_callback = lambda *args, **kwargs: time_slice_end_callback(configuration_file, *args, **kwargs) if not no_state else None

    with closing(TierStore(destination_directory, matches_per_file, base_file_name)) as store:
        download_from_config(json_conf, make_store_callback(store), checkpoint_callback)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('configuration_file',help='The json file to hold the configuration of the download session '
                                                  'you want to start by running this script. Might be a file saved '
                                                  'from a previous session',action='store')
    parser.add_argument('--no-state', action='store_true', help='Do not store in a .state file the current state of '
                                                           'execution, so that if the process is stopped it can be '
                                                           'resumed from the last state saved',
                        default=False)
    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s, %(levelname)s, %(name)s, %(message)s',
                        datefmt="%m-%d %H:%M:%S",
                        level=logging.INFO)

    main(args.configuration_file, args.no_state)