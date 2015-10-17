from contextlib import closing, suppress
from itertools import takewhile
import datetime
import os
import argparse
from json import loads, dumps

from cassiopeia import baseriotapi
from cassiopeia.dto.matchlistapi import get_match_list
from cassiopeia.dto.matchapi import get_match
from cassiopeia.type.api.exception import APIError
from persist import TierStore, JSONConfigEncoder, datetime_to_dict
from data_types import TierSet, TierSeed, Tier, Queue, Maps, slice_time
from summoners_api import update_participants, summoner_names_to_id, leagues_by_summoner_ids

current_state_extension = '.checkpoint'
delta_3_hours = datetime.timedelta(hours=3)
delta_30_days = datetime.timedelta(days=30)

def make_store_callback(store):
    def store_callback(match, tier):
        store.store(match.to_json(sort_keys=False,indent=None), tier)
    return store_callback

def download_matches(store_callback, seed_players_by_tier, minimum_tier = Tier.bronze,
                     start=None,end=None, duration=delta_3_hours,
                     include_timeline=True, matches_per_time_slice=2000,
                     map_type = Maps.SUMMONERS_RIFT, queue=Queue.RANKED_SOLO_5x5, end_of_time_slice_callback=None,
                     prints_on=False, minimum_match_id=0, starting_matches_in_first_time_slice=0):

    if not start and not end:
        raise ValueError("You must specify at least one of parameters start and end")

    if start is None:
        start = end - delta_30_days
    else:
        start = start if not end else min(start, end)

    def checkpoint(time_slice, players_to_analyze, total_matches, time_slice_downloaded_matches, max_match_id):
        if prints_on:
                print("{} - Reached the checkpoint."
                      .format(datetime.datetime.now().strftime("%m-%d %H:%M:%S"), total_matches))
        if end_of_time_slice_callback:
            end_of_time_slice_callback(datetime.datetime.utcfromtimestamp(time_slice.end/1000), players_to_analyze,
                                       total_matches, time_slice_downloaded_matches, max_match_id)

    players_to_analyze = TierSeed(tiers=seed_players_by_tier)

    total_matches = 0
    # We store the maximum match id ever downloaded. Since the set pop is in hash order, and hash(int)=int, we are
    # guaranteed to not have popped any match id greater of the maximum_downloaded_id. When restarting by loading a state
    # we can prevent to store matches with a smaller id in order to avoid match duplicates. This leaves out some
    # matches which could be valid, but since usually it's not important which specific match we download, it's a safe
    # constraint to put in order to allow correct resumes.
    maximum_downloaded_id = 0
    for time_slice in slice_time(start, end, duration):
        if prints_on:
            print("{} - Starting time slice {}. Downloaded {} matches so far.".format(datetime.datetime.now().strftime("%m-%d %H:%M:%S") ,time_slice, total_matches))
        #It's impossible that matches overlap between time slices. Reset the history of downloaded matches
        downloaded_matches_by_tier = TierSet()
        matches_to_download_by_tier = TierSet()
        analyzed_players = TierSeed()

        matches_in_time_slice = starting_matches_in_first_time_slice
        starting_matches_in_first_time_slice = 0
        try:
            # Iterate until matches_in_time_slice is big enough, and stop anyways after matches_per_time_slice iterations
            # this ensures the script will always terminate even in strange situations
            # (like when all our seeds have no matches in the time slice)
            for _ in takewhile(lambda x: matches_in_time_slice <= matches_per_time_slice, range(matches_per_time_slice)):
                for tier in Tier.equals_and_above(minimum_tier):
                    if prints_on:
                        print("{} - Starting player download for tier {}. Players in queue: {}"
                              .format(datetime.datetime.now().strftime("%m-%d %H:%M:%S"), tier.name, len(players_to_analyze)))

                    for player_id in players_to_analyze.consume(tier, 10):
                        try:
                            match_list = get_match_list(player_id, begin_time=time_slice.begin, end_time=time_slice.end, ranked_queues=queue.name)
                            for match in match_list.matches:
                                match_id = match.matchId
                                if not match_id in downloaded_matches_by_tier[tier] and match_id > minimum_match_id:
                                    matches_to_download_by_tier[tier].add(match_id)

                            analyzed_players[tier].add(player_id)
                        except APIError as e:
                            if 400 <= e.error_code < 600:
                                # Might be a connection problem
                                if prints_on:
                                    print("{} - Encountered error {}"
                                            .format(datetime.datetime.now().strftime("%m-%d %H:%M:%S"), e))
                                from time import sleep
                                sleep(5)
                                continue
                            else:
                                raise e

                    if prints_on:
                        print("{} - Starting matches download for tier {}. Downloads in queue: {}. Downloaded: {}"
                              .format(datetime.datetime.now().strftime("%m-%d %H:%M:%S"), tier.name,
                                      len(matches_to_download_by_tier), matches_in_time_slice))

                    for match_id in matches_to_download_by_tier.consume(tier, 10, 0.2):
                        try:
                            match = get_match(match_id, include_timeline)
                            if match.mapId == map_type.value:
                                match_min_tier = update_participants(players_to_analyze, match.participantIdentities, minimum_tier, queue)
                                if match_min_tier.is_better_or_equal(minimum_tier):
                                    maximum_downloaded_id = max(maximum_downloaded_id, match_id)
                                    store_callback(match, match_min_tier.name)
                                    matches_in_time_slice += 1
                                downloaded_matches_by_tier[tier].add(match_id)
                        except APIError as e:
                            if 400 <= e.error_code < 600:
                                # Might be a connection problem
                                if prints_on:
                                    print("{} - Encountered error {}"
                                            .format(datetime.datetime.now().strftime("%m-%d %H:%M:%S"), e))
                                from time import sleep
                                sleep(5)
                                continue
                            else:
                                raise e
                    players_to_analyze -= analyzed_players
            total_matches += matches_in_time_slice
        finally:
            #Always call the checkpoint, so that we can resume the download in case of exceptions.
            checkpoint(time_slice, players_to_analyze, total_matches, matches_in_time_slice, maximum_downloaded_id)

def download_from_config(config, config_file, save_state=True, store_callback = None):

    prints_on = config.get('prints_on', False)

    #Set up the api
    cassioepia = config['cassiopeia']
    baseriotapi.set_api_key(cassioepia['api_key'])
    baseriotapi.set_region(cassioepia['region'])

    limits = cassioepia.get('rate_limits', None)
    if limits:
        if isinstance(limits[0], list):
            baseriotapi.set_rate_limits(*limits)
        else:
            baseriotapi.set_rate_limit(limits[0], limits[1])

    baseriotapi.print_calls(cassioepia.get('print_calls', False))

    destination_directory = config['destination_directory']

    # Allow the directory to be relative to the config file.
    if destination_directory.startswith('__file__'):
        configuration_file_dir = os.path.dirname(os.path.realpath(config_file))
        destination_directory=destination_directory.replace('__file__', configuration_file_dir)

    # Parse the time boundaries
    now = datetime.datetime.now()
    end = None if not 'end_time' in config else min(now, datetime.datetime(**config['end_time']))
    start = None if not 'start_time' in config else datetime.datetime(**config['start_time'])
    duration = max(delta_3_hours, datetime.timedelta(**config.get('time_slice_duration', {'days':2} )))

    matches_per_time_slice = config.get('matches_per_time_slice', 2000)
    matches_per_file = config.get('matches_per_file', 0)

    queue = Queue[config.get('queue', Queue.RANKED_SOLO_5x5.name)]
    map_type = Maps[config.get('map', Maps.SUMMONERS_RIFT.name)]
    minimum_tier = Tier.parse(config.get('minimum_tier', Tier.bronze.name).lower())

    include_timeline = config.get('include_timeline', True)

    seed_players = list(summoner_names_to_id(config['seed_players']).values())
    seed_players_by_tier = TierSeed(tiers=leagues_by_summoner_ids(seed_players, queue))

    checkpoint_players = config.get('checkpoint_players', None)
    if checkpoint_players:
        checkpoint_players_by_tier = TierSeed().from_json(checkpoint_players)
        seed_players_by_tier.update(checkpoint_players_by_tier)
        if prints_on:
            print("Loaded {} players from the checkpoint".format(len(checkpoint_players_by_tier)))

    seed_players_by_tier.remove_players_below_tier(minimum_tier)

    base_file_name = config.get('base_file_name', '')

    def time_slice_end_callback(time_slice_end, players_to_analyze, total_matches, matches_in_time_slice, maximum_downloaded_id):
        current_state={}
        current_state['start_time'] = datetime_to_dict(time_slice_end)
        current_state['checkpoint_players'] = players_to_analyze.to_json()
        current_state['minimum_match_id'] = maximum_downloaded_id
        current_state['matches_in_time_slice'] = matches_in_time_slice
        with open(config_file+current_state_extension, 'wt') as state:
            state.write(dumps(current_state, cls=JSONConfigEncoder, indent=4, sort_keys=True))

    ts_end_callback = time_slice_end_callback if save_state else None
    minimum_match_id = config.get('minimum_match_id', 0)
    starting_matches_in_first_time_slice = config.get('matches_in_time_slice', 0)

    if not store_callback:
        with closing(TierStore(destination_directory, matches_per_file, base_file_name)) as store:
            download_matches(make_store_callback(store), seed_players_by_tier._tiers, minimum_tier, start, end, duration,
                             include_timeline, matches_per_time_slice, map_type, queue, ts_end_callback,
                             prints_on, minimum_match_id, starting_matches_in_first_time_slice)
    else:
        download_matches(store_callback, seed_players_by_tier._tiers, minimum_tier, start, end, duration,
                             include_timeline, matches_per_time_slice, map_type, queue, ts_end_callback,
                             prints_on, minimum_match_id, starting_matches_in_first_time_slice)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('configuration_file',help='The json file to hold the configuration of the download session '
                                                  'you want to start by running this script. Might be a file saved '
                                                  'from a previous session',action='store')
    parser.add_argument('--no-state', action='store_true', help='Do not store in a .state file the current state of '
                                                           'execution, so that if the process is stopped it can be '
                                                           'resumed from the last state saved',
                        default=False)
    args = parser.parse_args()

    with open(args.configuration_file, 'rt') as config_file:
        json_conf = loads(config_file.read())

    with suppress(FileNotFoundError), open(args.configuration_file+current_state_extension, 'rt') as state:
        current_state = loads(state.read())
        json_conf.update(current_state)

    download_from_config(json_conf, args.configuration_file, not args.no_state)

if __name__ == '__main__':
    main()