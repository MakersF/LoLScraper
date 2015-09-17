import unittest
import os
from collections import defaultdict
import json
from tier import summoner_names_to_id, slice, leagues_by_summoner_ids, Tier, update_participants, TierSeed
from cassiopeia import baseriotapi
from cassiopeia.type.dto.match import MatchDetail



class TierTest(unittest.TestCase):

    api_key = None
    def setUp(self):
        if not self.api_key:
            with open(os.path.join(os.path.dirname(__file__), '../../api-key'),'rt') as f:
                self.api_key = f.read()[:36]
        baseriotapi.print_calls(False)
        baseriotapi.set_region('euw')
        baseriotapi.set_api_key(self.api_key)

    def test_slice_middle_values(self):
        source = [x for x in range(50)]
        lst = [(x, x+1) for x in range(50)]
        for (b1,e1), (b2,e2) in zip(lst, slice(0,50,1)):
            self.assertEqual(source[b1:e1], source[b2:e2])

    def test_slice_end_value(self):
        step = 3
        lst = [ x for x in range(20)]
        begin = [0, 3, 6, 9,12,15,18]
        end =   [3, 6, 9,12,15,18,20]
        generator = slice(0,20,step)
        for (b1,e1), (b2,e2) in zip(zip(begin, end), generator):
            self.assertEqual(lst[b1:e1], lst[b2:e2])
        try:
            next(generator)
            self.fail()
        except StopIteration:
            self.assertTrue(True)

    def test_leagues_by_summoner(self):
        summoners = [44256841, 21653685, 22447540, 22281234, 29378330, 23836705, 23746128, 23742827, 35473944,
                     56067323, 29600081, 26191711]
        leagues = {
                    Tier.challenger : {44256841, 21653685, 22447540},
                    Tier.master : {22281234},
                    Tier.diamond : {29378330, 23746128, 23742827},
                    Tier.platinum : {23836705, 29600081},
                    Tier.gold : {35473944},
                    Tier.silver :{56067323},
                    Tier.bronze : {26191711}
                }
        result = leagues_by_summoner_ids(summoners)
        for tier in Tier:
            self.assertEqual(leagues[tier], result.get(tier, set()), tier.name)

    def test_names_to_id(self):
        names = ['cwfreeze', 'makersf', 'zoffo', 'w4sh', "sirnukesalot", "exngodzukee",
                 "lamept", "hiddenlaw", "dijio", "zockchock", "sirkillerlord"]

        ids = {'cwfreeze':44256841, 'makersf':45248415, 'zoffo':46677170,
               'w4sh':23367874, 'exngodzukee': 22447540, 'sirnukesalot': 21653685, 'dijio': 23742827,
               'hiddenlaw': 23746128,'lamept': 23836705,'sirkillerlord': 26191711,'zockchock': 56067323}
        result = summoner_names_to_id(names)
        self.assertEqual(ids, result)

    def test_empty_tier(self):
        tier = TierSeed()
        self.assertEqual(7, len(tier._tiers))
        for values in tier._tiers.values():
            self.assertEqual(set(), values)

    def test_update_participants(self):
        match_file = os.path.join(os.path.dirname(__file__), 'match_string.json')
        with open(match_file, 'rt') as f:
            match_string = f.read()
        match = MatchDetail(json.loads(match_string))
        baseriotapi.set_region('na')
        result = TierSeed()
        update_participants(result, match.participantIdentities)
        leagues = defaultdict(set)
        leagues[Tier.diamond] = {21589368}
        leagues[Tier.platinum] = {22668834, 41304754, 25957444, 30555923, 42652920, 507594, 22622940}
        leagues[Tier.gold] = {47531989, 21846758}
        for tier in Tier:
            self.assertEqual(leagues[tier], result.get(tier, set()), tier.name)

if __name__ == '__main__':
    unittest.main()