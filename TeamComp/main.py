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
class TimeSlice(namedtuple('TimeSliceBase', ['begin', 'end'])):

    def __str__(self):
        return "({},{})".format(datetime.datetime.utcfromtimestamp(self.begin/1000),
                                datetime.datetime.utcfromtimestamp(self.end/1000))

def unix_time(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
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

def download_matches(json_conf, configuration_file_path, do_store_state=True):

    cassioepia = json_conf['cassiopeia']
    baseriotapi.set_api_key(cassioepia['api_key'])
    baseriotapi.set_region(cassioepia['region'])
    baseriotapi.print_calls(cassioepia.get('print_calls', False))

    destination_directory = json_conf['destination_directory']

    #allow the directory to be relative to the config file.
    if destination_directory.startswith('__file__'):
        configuration_file_dir = os.path.dirname(os.path.realpath(configuration_file_path))
        destination_directory=destination_directory.replace('__file__', configuration_file_dir)

    epoch = datetime.datetime.utcfromtimestamp(0)
    now = datetime.datetime.now()
    start = epoch if not 'start_time' in json_conf else datetime.datetime(**json_conf['start_time'])
    end = now if not 'end_time' in json_conf else min(now, datetime.datetime(**json_conf['end_time']))
    delta_3_hours = datetime.timedelta(hours=3)
    duration = max(delta_3_hours, datetime.timedelta(**json_conf.get('time_slice_duration', {'days':2} )))

    matches_per_time_slice = json_conf.get('matches_per_time_slice', 2000)
    matches_per_file = json_conf.get('matches_per_file', 1000)

    queue = Queue[json_conf.get('queue', Queue.RANKED_SOLO_5x5.name)]
    map = Maps[json_conf.get('map', Maps.SUMMONERS_RIFT.name)]
    minimum_tier = Tier[json_conf.get('minimum_tier', Tier.bronze.name).lower()]

    include_timeline = json_conf.get('include_timeline', True)

    analyzed_players = TierSeed()

    seed_players = list(summoner_names_to_id(json_conf['seed_players']).values())
    players_to_analyze = TierSeed(tiers=leagues_by_summoner_ids(seed_players, queue))

    base_file_name = json_conf.get('base_file_name', '')
    with closing(TierStore(destination_directory, matches_per_file, base_file_name)) as store:
        for time_slice in slice_time(start, end, duration):
            #It's impossible that matches overlap between time slices. Reset the history of downloaded matches
            downloaded_matches = set()
            matches_to_download = set()
            while len(downloaded_matches) < matches_per_time_slice:
                # Fetch all the matches of the players to analyze
                for player_id in players_to_analyze:
                    match_list = get_match_list(player_id, begin_time=time_slice.begin, end_time=time_slice.end)
                    for match in match_list.matches:
                        matches_to_download.add(match.matchId)

                #Remove the matches already downloaded
                matches_to_download -= downloaded_matches
                analyzed_players += players_to_analyze
                players_to_analyze = TierSeed()

                for match_id in matches_to_download:
                    match = get_match(match_id, include_timeline)
                    if match.mapId == map.value:
                        match_min_tier = update_participants(players_to_analyze, match.participantIdentities, queue, minimum_tier)
                        store.store(match.to_json(sort_keys=False,indent=None), match_min_tier)

                players_to_analyze -= analyzed_players
                downloaded_matches += matches_to_download
                matches_to_download.clear()

            if do_store_state:
                current_state={}
                current_state['start_time'] = time_slice.end
                current_state['seed_players'] = players_to_analyze
                with open(configuration_file_path+current_state_extension, 'wt') as state:
                    state.write(dumps(current_state, cls=JSONConfigEncoder))


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
        json_conf = loads(config_file)

    with suppress(FileNotFoundError), open(args.configuration_file+current_state_extension, 'rt') as state:
        current_state = loads(state.read())
        json_conf.update(current_state)

    download_matches(json_conf, args.configuration_file, not args.no_state)
