
import schemas.testdb
from schemas.testdb import *

def throws(func, *args):
    try:
        func(*args)
    except:
        return
    
    assert False

db = testdb()

tab_G = db.aquire_table_G()
tab_D = db.aquire_table_D()

tab_G.insert( G(id = 1))
tab_G.insert( G(id = 2))
tab_D.insert( D(id = 2, G_id = 1, pk1 = 1, pk2 = 'foo', lk = 'bar'))

# DNE; treats as insert
tab_D.replace_id(3, D(id = 3, G_id = 1, pk1 = 2, pk2 = 'moo', lk = 'bing'))
assert tab_D.get_id(2) and tab_D.get_id(3)

# Replace w/ identity
tab_D.replace_id( 2, D(pk2 = 'foo', pk1 = 1, G_id = 1, lk = 'bar', id = 2))
# Changed referenced G
tab_D.replace_id( 2, D(pk2 = 'foo', pk1 = 1, G_id = 2, lk = 'bar', id = 2))
# Change lk by id (unique, but not foreign held)
tab_D.replace_id( 2, D(pk2 = 'foo', pk1 = 1, G_id = 1, lk = 'baz', id = 2))
# Change lk by lk (unique, but not foreign held)
tab_D.replace_lk( 'baz', D(pk2 = 'foo', pk1 = 1, G_id = 1, lk = 'zing', id = 2))

# Violates G.id FK-constraint
throws( tab_D.replace_id, 2, D(pk2 = 'foo', pk1 = 1, G_id = 3, lk = 'bar', id = 2))
# Violates id FK-held equality
throws( tab_D.replace_lk, 'zing', D(pk2 = 'foo', pk1 = 1, G_id = 1, lk = 'zing', id = 3))
# Violates pk1_pk2 FK-held equality
throws( tab_D.replace_lk, 'zing', D(pk2 = 'moo', pk1 = 1, G_id = 1, lk = 'zing', id = 2)) 
# Violates lk uniqueness
throws( tab_D.replace_lk, 'zing', D(pk2 = 'foo', pk1 = 1, G_id = 1, lk = 'bing', id = 2))

# None of these should have altered the table
assert tab_D.get_lk('zing') and tab_D.get_lk('bing')

