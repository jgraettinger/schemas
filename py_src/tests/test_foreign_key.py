
import unittest

import schemas.testdb
from schemas.testdb import *

class TestForeignKey(unittest.TestCase):
    
    def setUp(self):
        self.db = testdb()
        self.tab_A = self.db.aquire_table_A()
        self.tab_B = self.db.aquire_table_B()
        self.tab_C = self.db.aquire_table_C()
    
    def throws(self, func, *args):
        try:
            func(*args)
        except:
            return
        assert False
    
    def test_foreign_key(self):
        
        # Fill tables
        self.tab_A.insert(A(id = 0, desc = 'foo'))
        self.tab_A.insert(A(id = 1, desc = 'bar'))
        
        self.tab_B.insert(B(id = 0, A_id = 0))
        self.tab_B.insert(B(id = 1, A_id = 1))
        
        # FK violation (no A)
        self.throws( self.tab_B.insert, B(id = 2, A_id = 2))
        
        self.tab_C.insert(C(id = '0', par_id = None, B_id = 0))
        self.tab_C.insert(C(id = '1', par_id = '0', B_id = 1))
        self.tab_C.insert(C(id = '3', par_id = None, B_id = 1))
        
        # FK violation (no B)
        self.throws( self.tab_C.insert, C(id = '4', par_id = None, B_id = 2))
        
        # Check cascaded deletions
        # Will delete 1 A, 1 B, and 2 C's
        assert self.tab_A.delete_desc('foo') == 4
        # Will delete 1 A, 1 B, and 1 C
        assert self.tab_A.delete_id(1) == 3
        
        # All tables now empty
        assert all((
            not [i for i in self.tab_A],
            not [i for i in self.tab_B],
            not [i for i in self.tab_C],
        ))
        return
    
