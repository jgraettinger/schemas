
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

def write_database_hpp(db, cpp_base):
    """
    Entity storage, containers, common types & database declaration
    """
    r = CodeRenderer()
    for l in hpp_head.split('\n'):
        r.line(l)
    
    r.line('// forward declarations')
    for ent in database._entities.values():
        ent.render_cpp_fwd_declaration(r)
    for ent in database._entities.values():
        ent.render_cpp_row_storage(r)
    for ent in database._entities.values():
        ent.render_cpp_container(r)
    
    r.line()
    database.render_cpp_declaration(r)
    
    path = cpp_base + '%s' % db._name
    if not dry_run: print >> open(path + '.cpp', 'w'), r.render()
    return path

def write_database_cpp(db, cpp_base):
    """
    Database implementation, except for aquire_table_* methods
    """
    r = CodeRenderer()
    r.line('#include "%s.hpp"' % db._name).line()
    
    r.line('// bindings require table classes to be complete types')
    for ent in db._entities.values():
        ent.render_cpp_table_declaration(r)
    
    db.render_cpp_definition(r)
    db.render_bindings_declaration(r)
    db.render_bindings_definition(r)
    
    path = cpp_base + '%s' % db._name
    if not dry_run: print >> open(path + '.cpp', 'w'), r.render()
    return path

def write_table_cpp(db, ent, cpp_base):
    """
    Table implementation, including aquire_table* methods
    """
    r = CodeRenderer()
    r.line('#include "%s.hpp"' % db._name).line()
    
    ent.render_cpp_table_declaration(r)
    ent.render_cpp_table_definition(r)
    ent.render_cpp_database_members(r)
    ent.render_bindings(r)
    
    path = cpp_base + 'table_%s' % ent._name
    if not dry_run: print >> open(path + '.cpp', 'w'), r.render()
    return path


database = Database.parse_schema(sys.argv[1])

if dry_run:
    out_base = ""
else:
    out_base = sys.argv[2]
    if out_base.endswith('.cpp'):
        out_base = out_base[:out_base.rindex(os.sep)]
    out_base += os.sep

# Create if DNE
try:
    if not dry_run: os.makedirs(out_base)
except:
    pass

write_database_hpp(database, out_base)
print write_database_cpp(database, out_base)

for ent in database._entities.values():
    print write_table_cpp(database, ent, out_base)

