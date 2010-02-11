
from database import Database
from renderer import CodeRenderer
from hpp_head import hpp_head
import sys, os

if len(sys.argv) != 3:
    print "Usage: %s <schema in> <dir out>" % sys.argv[0]
    sys.exit(-1)

def write_if_changed(path, contents):
    cur_contents = None
    try:
        cur_contents = open(path).read()
    except:
        pass
    
    if contents != cur_contents:
        open(path, 'w').write( contents)
        print >> sys.stderr, "%s: contents changed" % path
#    else:
        #print >> sys.stderr, "%s: contents unchanged" % path
    return

def write_py_init(db, path_out):
    """
    Database module import.
    """
    r = CodeRenderer()
    r.lines("""
    from _%(db_name)s import %(db_name)s as _%(db_name)s
    
    import %(db_name)s_types as __pytypes
    from %(db_name)s_types import *
    del %(db_name)s_types
    
    # Encapsulate presenting constructor with python entity types
    def %(db_name)s():
        return _%(db_name)s(__pytypes.__dict__)
    """, 4,
    db_name = db._name,
    )
    
    path = os.sep.join((path_out, '__init__.py'))
    write_if_changed(path, r.render())
    return path

def write_py_types(db, path_out):
    """
    Writes python types for database entities
    """
    r = CodeRenderer()
    for ent in database._entities.values():
        ent.render_python_class(r)
    r.line()
    
    path = os.sep.join((path_out, '%s_types.py' % db._name))
    write_if_changed(path, r.render())
    return path

def write_database_cpp(database, path_out):
    """
    Database implementation, except for aquire_table_* methods
    """
    r = CodeRenderer()
    
    for l in hpp_head.split('\n'):
        r.line(l)
    
    r.line('// forward declarations')
    for ent in database._entities.values():
        ent.render_cpp_fwd_declaration(r)
    
    r.line().line('// row storage')
    for ent in database._entities.values():
        ent.render_cpp_row_storage(r)
    
    r.line().line('// row containers')
    for ent in database._entities.values():
        ent.render_cpp_container(r)
    
    r.line()
    database.render_cpp_declaration(r)
    
    r.line('// bindings require table classes to be complete types')
    for ent in database._entities.values():
        ent.render_cpp_table_declaration(r)
    
    database.render_cpp_definition(r)
    database.render_bindings_declaration(r)
    database.render_bindings_definition(r)
    r.line()

    path = os.sep.join((path_out, '%s.cpp' % database._name))
    write_if_changed(path, r.render())
    return path

def write_table_cpp(database, ent, path_out):
    """
    Table implementation, including aquire_table* methods
    """
    r = CodeRenderer()
    for l in hpp_head.split('\n'):
        r.line(l)
    
    related_ents = set([ent])
    [ related_ents.add(e) for e in ent.get_referenced()  ]
    [ related_ents.add(e) for e in ent.get_referencing() ]
    related_ents = sorted(related_ents, key = lambda e: e._name)
    
    r.line('// forward declarations')
    for r_ent in database._entities.values():
        r_ent.render_cpp_fwd_declaration(r)
    
    r.line().line('// row storage of related tables')
    for r_ent in related_ents:
        r_ent.render_cpp_row_storage(r)
    
    r.line().line('// row containers of related tables')
    for r_ent in related_ents:
        r_ent.render_cpp_container(r)
    
    r.line()
    database.render_cpp_declaration(r)
    
    ent.render_cpp_table_declaration(r)
    ent.render_cpp_table_definition(r)
    ent.render_cpp_database_members(r)
    ent.render_bindings(r)
    r.line()
    
    path = os.sep.join((path_out, 'table_%s.cpp' % ent._name))
    write_if_changed(path, r.render())
    return path


database = Database.parse_schema(sys.argv[1])
path_out = sys.argv[2]

# Create if DNE
try:
    os.makedirs(path_out)
except:
    pass

print write_py_init(database, path_out)
print write_py_types(database, path_out)
print write_database_cpp(database, path_out)

for ent in database._entities.values():
    print write_table_cpp(database, ent, path_out)

