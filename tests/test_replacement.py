
import unittest

import schemas.testdb
from schemas.testdb.instance_types import *

class TestReplacement(unittest.TestCase):
    
    def setUp(self):
        self.db = schemas.testdb.testdb()
        self.tab_G = self.db.aquire_table_G()
        self.tab_D = self.db.aquire_table_D()
    
    def throws(self, func, *args):
        try:
            func(*args)
        except:
            return
        assert False
    
    def test_replacement(self):
        
        # Fill tables
        self.tab_G.insert( G(id = 1))
        self.tab_G.insert( G(id = 2))
        self.tab_D.insert( D(id = 2, G_id = 1, pk1 = 1, pk2 = 'foo', lk = 'bar'))
        
        # DNE; treats as insert
        self.tab_D.replace_id(3, D(id = 3, G_id = 1, pk1 = 2, pk2 = 'moo', lk = 'bing'))
        assert self.tab_D.get_id(2) and self.tab_D.get_id(3)
        
        # Replace w/ identity
        self.tab_D.replace_id( 2, D(pk2 = 'foo', pk1 = 1, G_id = 1, lk = 'bar', id = 2))
        # Changed referenced G
        self.tab_D.replace_id( 2, D(pk2 = 'foo', pk1 = 1, G_id = 2, lk = 'bar', id = 2))
        # Change lk by id (unique, but not foreign held)
        self.tab_D.replace_id( 2, D(pk2 = 'foo', pk1 = 1, G_id = 1, lk = 'baz', id = 2))
        # Change lk by lk (unique, but not foreign held)
        self.tab_D.replace_lk( 'baz', D(pk2 = 'foo', pk1 = 1, G_id = 1, lk = 'zing', id = 2))
        
        # Violates G.id FK-constraint
        self.throws( self.tab_D.replace_id, 2, D(pk2 = 'foo', pk1 = 1, G_id = 3, lk = 'bar', id = 2))
        # Violates id FK-held equality
        self.throws( self.tab_D.replace_lk, 'zing', D(pk2 = 'foo', pk1 = 1, G_id = 1, lk = 'zing', id = 3))
        # Violates pk1_pk2 FK-held equality
        self.throws( self.tab_D.replace_lk, 'zing', D(pk2 = 'moo', pk1 = 1, G_id = 1, lk = 'zing', id = 2)) 
        # Violates lk uniqueness
        self.throws( self.tab_D.replace_lk, 'zing', D(pk2 = 'foo', pk1 = 1, G_id = 1, lk = 'bing', id = 2))
        
        # None of these should have altered the table
        assert self.tab_D.get_lk('zing') and self.tab_D.get_lk('bing')
        return
    
