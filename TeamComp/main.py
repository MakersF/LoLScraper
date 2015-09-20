from collections import namedtuple
from contextlib import closing, suppress
import datetime
import os
import argparse
from json import loads, dumps
from cassiopeia import baseriotapi
from cassiopeia.dto.matchlistapi import get_match_list
from cassiopeia.dto.matchapi import get_match
from persist.store import TierStore
from persist.config import JSONConfigEncoder
from tier import TierSeed, update_participants, Tier, Queue, Maps, summoner_names_to_id, leagues_by_summoner_ids

current_state_extension = '.state'
epoch = datetime.datetime.utcfromtimestamp(0)
delta_3_hours = datetime.timedelta(hours=3)

class TimeSlice(namedtuple('TimeSliceBase', ['begin', 'end'])):

    def __str__(self):
        return "({},{})".format(datetime.datetime.utcfromtimestamp(self.begin/1000),
                                datetime.datetime.utcfromtimestamp(self.end/1000))

def unix_time(dt):
    delta = dt - epoch
    return delta.total_seconds()

def slice_time(begin=datetime.datetime.utcfromtimestamp(0), end=datetime.datetime.now(), duration=datetime.timedelta(days=2)):
    """
    :param begin: datetime
    :param end: datetime
    :param duration: timedelta
    :return: a generator for a set of timeslices of the given duration
    """

    begin_ms = int(unix_time(begin) * 1000)
    end_ms = int(unix_time(end) * 1000)
    duration_ms = int(duration.total_seconds() * 1000)
    sliced = range(begin_ms, end_ms, duration_ms)
    for start, stop in zip(sliced[:-1], sliced[1:]):
        yield TimeSlice(start, stop)
    yield TimeSlice(sliced[-1], end_ms)

def make_store_callback(store):
    def store_callback(match_json, tier):
        store.store(match_json, tier)
    return store_callback

def download_matches(store_callback, seed_players, minimum_tier = Tier.bronze,
                     start=epoch,end=datetime.datetime.now(), duration=delta_3_hours,
                     include_timeline=True, matches_per_time_slice=2000,
                     map_type = Maps.SUMMONERS_RIFT, queue=Queue.RANKED_SOLO_5x5, end_of_time_slice_callback=None,
                     prints_on=False):

    players_to_analyze = TierSeed(tiers=leagues_by_summoner_ids(seed_players, queue))
    analyzed_players = TierSeed()

    total_matches = 0
    for time_slice in slice_time(start, end, duration):
        if prints_on:
            print("{} - Starting time slice {}. Downloaded {} matches so far.".format(datetime.datetime.now().strftime("%m-%d %H:%M") ,time_slice, total_matches))
        #It's impossible that matches overlap between time slices. Reset the history of downloaded matches
        downloaded_matches = set()
        matches_to_download = set()
        analyzed_players.clear()

        # we add a small number every loop. This ensures eventually we will come out of the loop. This might happen
        # if, for example, out seed players haven't played in the time we are interested.
        ensure_to_end_the_loop_eventually = 0
        epsilon = 0.0001

        # TODO if len(players_to_analyze) == 0 sample some analyzed players
        while len(downloaded_matches) + ensure_to_end_the_loop_eventually < matches_per_time_slice:
            ensure_to_end_the_loop_eventually += epsilon
            # Fetch all the matches of the players to analyze
            for player_id in players_to_analyze:
                match_list = get_match_list(player_id, begin_time=time_slice.begin, end_time=time_slice.end, ranked_queues=queue)
                for match in match_list.matches:
                    matches_to_download.add(match.matchId)

            #Remove the matches already downloaded
            matches_to_download -= downloaded_matches
            analyzed_players += players_to_analyze
            players_to_analyze.clear()

            for match_id in matches_to_download:
                match = get_match(match_id, include_timeline)
                if match.mapId == map_type.value:
                    match_min_tier = update_participants(players_to_analyze, match.participantIdentities, minimum_tier, queue)
                    if match_min_tier.is_better_or_equal(minimum_tier):
                        store_callback(match.to_json(sort_keys=False,indent=None), match_min_tier.name)

            players_to_analyze -= analyzed_players
            downloaded_matches.update(matches_to_download)
            matches_to_download.clear()

        total_matches += len(downloaded_matches)

        # TODO make it so that if you stop the process in the middle of calculations, restarting it with the state
        # doesn't produce duplicated matches. Basically, keep in memory all the matches between states checkpoints
        # and flush them on time slice end. Would that be useful?
        if end_of_time_slice_callback:
            if prints_on:
                print("{} - Calling end_of_time callback".format(datetime.datetime.now().strftime("%m-%d %H:%M")))
            end_of_time_slice_callback(time_slice.end, players_to_analyze, downloaded_matches, total_matches)

def download_from_config(config, config_file, save_state=True):

    prints_on = config.get('prints_on', False)

    #Set up the api
    cassioepia = config['cassiopeia']
    baseriotapi.set_api_key(cassioepia['api_key'])
    baseriotapi.set_region(cassioepia['region'])
    baseriotapi.print_calls(cassioepia.get('print_calls', False))

    destination_directory = config['destination_directory']

    # Allow the directory to be relative to the config file.
    if destination_directory.startswith('__file__'):
        configuration_file_dir = os.path.dirname(os.path.realpath(config_file))
        destination_directory=destination_directory.replace('__file__', configuration_file_dir)

    # Parse the time boundaries
    now = datetime.datetime.now()
    end = now if not 'end_time' in config else min(now, datetime.datetime(**config['end_time']))
    start = epoch if not 'start_time' in config else datetime.datetime(**config['start_time'])
    start = min(start, end)
    duration = max(delta_3_hours, datetime.timedelta(**config.get('time_slice_duration', {'days':2} )))

    matches_per_time_slice = config.get('matches_per_time_slice', 2000)
    matches_per_file = config.get('matches_per_file', 100)

    queue = Queue[config.get('queue', Queue.RANKED_SOLO_5x5.name)]
    map_type = Maps[config.get('map', Maps.SUMMONERS_RIFT.name)]
    minimum_tier = Tier.parse(config.get('minimum_tier', Tier.bronze.name).lower())

    include_timeline = config.get('include_timeline', True)

    seed_players = list(summoner_names_to_id(config['seed_players']).values())

    base_file_name = config.get('base_file_name', '')

    def time_slice_end_callback(time_slice_end, players_to_analyze, downloaded_matches, total_matches):
        current_state={}
        current_state['start_time'] = time_slice_end
        current_state['seed_players'] = players_to_analyze
        with open(config_file+current_state_extension, 'wt') as state:
            state.write(dumps(current_state, cls=JSONConfigEncoder))

    ts_end_callback = time_slice_end_callback if save_state else None

    with closing(TierStore(destination_directory, matches_per_file, base_file_name)) as store:
        download_matches(make_store_callback(store), seed_players, minimum_tier, start, end, duration,
                         include_timeline, matches_per_time_slice, map_type, queue, ts_end_callback, prints_on)

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

    with open(args.configuration_file, 'rt') as config_file:
        json_conf = loads(config_file.read())

    with suppress(FileNotFoundError), open(args.configuration_file+current_state_extension, 'rt') as state:
        current_state = loads(state.read())
        json_conf.update(current_state)

    download_from_config(json_conf, args.configuration_file, not args.no_state)
