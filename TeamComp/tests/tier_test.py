import unittest
import os
from collections import defaultdict
import json
from tier import summoner_names_to_id, slice, leagues_by_summoner_ids, Tier, update_participants, TierSet, TierSeed
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

    def list_to_tier_initializer(self, list):
        initializer = {}
        for tier in Tier:
            if list[tier.value]:
                initializer[tier] = set(list[tier.value])
        return initializer

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
                    Tier.diamond : {29378330, 23746128, 23742827, 23836705, 23836705},
                    Tier.platinum : {29600081},
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
        self.assertEqual(0, len(tier._tiers))
        for t in Tier:
            self.assertEqual(set(), tier._tiers[t])

    def test_initialized_tier(self):
        l = self.list_to_tier_initializer([[1,2,3,4,5,6], None, None, [2,3,4], None, None, [1,]])
        t = TierSet(l)
        for tier in Tier:
            if l.get(tier, None):
                self.assertEqual(set(l[tier]), t[tier])
            else:
                self.assertEqual(set(), t._tiers[tier])

    def test_autoinit_on_get(self):
        tier = TierSet()
        self.assertEqual(0, len(tier._tiers))
        for t in Tier:
            self.assertEqual(set(), tier[t])
        self.assertEqual(7, len(tier._tiers))

    def test_tier_addition_not_overlapped(self):
        l1 = self.list_to_tier_initializer([[1,2,3,4], None, None, None, None, None, None])
        t1 = TierSet(l1)
        l2 = self.list_to_tier_initializer([None, [1,2,3,4], None, None, None, None, None])
        t2 = TierSet(l2)
        target = self.list_to_tier_initializer([[1,2,3,4], [1,2,3,4], None, None, None, None, None])
        t1 += t2
        for t in Tier:
            if target.get(t, None):
                self.assertEqual(target[t], t1[t])
            else:
                self.assertEqual(set(), t1._tiers[t])

    def test_tier_addition_overlapped(self):
        l1 = self.list_to_tier_initializer([[1,2,3,4], None, None, None, None, None, None])
        t1 = TierSet(l1)
        t2 = TierSet(l1)
        t1 += t2
        for t in Tier:
            if l1.get(t, None):
                self.assertEqual(set(l1[t]), t1[t])
            else:
                self.assertEqual(set(), t1._tiers[t])

    def test_tier_addition_partially_overlapped(self):
        l1 = self.list_to_tier_initializer([[1,2,3,4], None, None, None, None, None, None])
        t1 = TierSet(l1)
        l2 = self.list_to_tier_initializer([[3,4,5,6,], None, None, None, None, None, None])
        t2 = TierSet(l2)
        target = self.list_to_tier_initializer([[1,2,3,4,5,6], None, None, None, None, None, None])
        t1 += t2
        for t in Tier:
            if target.get(t, None):
                self.assertEqual(set(target[t]), t1[t])
            else:
                self.assertEqual(set(), t1._tiers[t])

    def test_tier_addition_disjoint(self):
        l1 = self.list_to_tier_initializer([[1,2,3,4], None, None, None, None, None, None])
        t1 = TierSet(l1)
        l2 = self.list_to_tier_initializer([[5,6,7,8], None, None, None, None, None, None])
        t2 = TierSet(l2)
        target = self.list_to_tier_initializer([[1,2,3,4,5,6,7,8], None, None, None, None, None, None])
        t1 += t2
        for t in Tier:
            if target.get(t, None):
                self.assertEqual(set(target[t]), t1[t])
            else:
                self.assertEqual(set(), t1._tiers[t])

    def test_tier_subtraction_empty_second(self):
        l1 = self.list_to_tier_initializer([[1,2,3,4], [5,8,1], None, None, None, None, None])
        t1 = TierSet(l1)
        t2 = TierSet()
        t1 -= t2
        for t in Tier:
            if l1.get(t, None):
                self.assertEqual(set(l1[t]), t1[t])
            else:
                self.assertEqual(set(), t1._tiers[t])

    def test_tier_subtraction_empty_first(self):
        t1 = TierSet()
        l2 = self.list_to_tier_initializer([[1,2,3,4], None, None, None, None, [7], None])
        t2 = TierSet(l2)
        t1 -= t2
        for t in Tier:
            self.assertEqual(set(), t1._tiers[t])

    def test_tier_subtraction_overlapped(self):
        l = self.list_to_tier_initializer([[1,2,3,4], None, [3,4,5,6,7], None, None, None, None])
        t1 = TierSet(l)
        t2 = TierSet(l)
        t1 -= t2
        for t in Tier:
            if l.get(t, None):
                self.assertEqual(set(), t1[t])

    def test_update_participants(self):
        match_file = os.path.join(os.path.dirname(__file__), 'match_string.json')
        with open(match_file, 'rt') as f:
            match_string = f.read()
        match = MatchDetail(json.loads(match_string))
        baseriotapi.set_region('na')
        result = TierSeed()
        min_tier = update_participants(result, match.participantIdentities)
        leagues = defaultdict(set)
        leagues[Tier.diamond] = {21589368}
        leagues[Tier.platinum] = {22668834, 41304754, 25957444, 30555923, 42652920, 507594, 22622940}
        leagues[Tier.gold] = {47531989, 21846758}
        for tier in Tier:
            self.assertEqual(leagues[tier], result[tier])
        self.assertEqual(min_tier, Tier.gold)

    def test_update_participants_min_tier(self):
        match_file = os.path.join(os.path.dirname(__file__), 'match_string.json')
        with open(match_file, 'rt') as f:
            match_string = f.read()
        match = MatchDetail(json.loads(match_string))
        baseriotapi.set_region('na')
        result = TierSeed()
        min_tier = update_participants(result, match.participantIdentities, Tier.platinum)
        leagues = defaultdict(set)
        leagues[Tier.diamond] = {21589368}
        leagues[Tier.platinum] = {22668834, 41304754, 25957444, 30555923, 42652920, 507594, 22622940}
        for tier in Tier:
            self.assertEqual(leagues[tier], result[tier])
        self.assertEqual(min_tier, Tier.gold)

if __name__ == '__main__':
    unittest.main()
