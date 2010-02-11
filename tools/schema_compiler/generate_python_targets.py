
from database import Database
from renderer import CodeRenderer
from hpp_head import hpp_head
import sys, os

dry_run = '--dry-run' in sys.argv
if dry_run:
    sys.argv.remove('--dry-run')

if len(sys.argv) != 3:
    print "Usage: %s --dry-run <schema in> <dir out>" % sys.argv[0]
    sys.exit(-1)

def write_py_init(db, py_base):
    """
    Database module import.
    """
    r = CodeRenderer()
    r.lines("""
    from _%(db_name)s import %(db_name)s
    
    import %(db_name)s_types
    from %(db_name)s_types import *
    
    # Encapsulate presenting constructor with python entity types
    %(db_name)s.__init__ = lambda self, name, size: \
        %(db_name)s.__init__(self, name, size, %(db_name)s_types.__dict__)
    """, 0,
    db_name = db._name,
    )
    
    path = os.sep.join((py_base, '__init__.py'))
    if not dry_run: print >> open(path, 'w'), r.render()
    return path

def write_py_types(db, py_base):
    """
    Writes python types for database entities
    """
    r = CodeRenderer()
    for ent in database._entities.values():
        ent.render_python_class(r)
    
    path = os.sep.join((py_base, '%s_types.py' % db._name))
    if not dry_run: print >> open(path, 'w'), r.render()
    return path


database = Database.parse_schema(sys.argv[1])

out_base  = sys.argv[2]

# Create if DNE
try:
    if not dry_run: os.makedirs(out_base)
except:
    pass

print "Wrote:"
print write_py_init(database, out_base)
print write_py_types(database, out_base)

