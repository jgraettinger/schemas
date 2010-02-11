
from schemas.testdb import *

db = testdb('test_foreign_key', 1 << 20)

tab_a = db.aquire_table_A()
tab_b = db.aquire_table_B()
tab_c = db.aquire_table_C()

# Fill tables
tab_a.insert(A(id = 0, desc = 'foo'))
tab_a.insert(A(id = 1, desc = 'bar'))

tab_b.insert(B(id = 0, A_id = 0))
tab_b.insert(B(id = 1, A_id = 1))
try:
    # FK violation
    tab_b.insert(B(id = 2, A_id = 2))
    assert False
except: pass

tab_c.insert(C(id = '0', par_id = None, B_id = 0))
tab_c.insert(C(id = '1', par_id = '0', B_id = 1))
tab_c.insert(C(id = '3', par_id = None, B_id = 1))
try:
    # FK violation
    tab_c.insert(C(id = '4', par_id = None, B_id = 2))
    assert False
except: pass

# Check cascaded deletions
# Will delete 1 A, 1 B, and 2 C's
assert tab_a.delete_desc('foo') == 4
# Will delete 1 A, 1 B, and 1 C
assert tab_a.delete_id(1) == 3

# All tables now empty
assert all((
    not [i for i in tab_a],
    not [i for i in tab_b],
    not [i for i in tab_c],
))

