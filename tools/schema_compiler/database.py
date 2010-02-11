
from entity import Entity
from ddl_core import *


class Database(object):
    __slots__ = ['_name', '_entities']
    
    def __init__(self, _database_name, **kwargs):
        self._name = _database_name
        self._entities = {}
        
        for ent_name, ent in kwargs.iteritems():
            assert isinstance(ent, Entity)
            ent._name = ent_name
            ent._database = self
            
            self._entities[ent_name] = ent
        
        return
    
    def render_bindings_declaration(self, r):
        
        r.line('void make_database_%s_bindings();' % self._name)
        for ent in self._entities.keys():
            r.line('void make_table_%s_bindings();' % ent)
        
        r.line().line('BOOST_PYTHON_MODULE(_%s)' % self._name)
        r.line('{').indent()
        
        r.line('make_database_%s_bindings();' % self._name)
        for ent in self._entities.keys():
            r.line('make_table_%s_bindings();' % ent)
        r.deindent().line('}')
        return 
    
    def render_bindings_definition(self, r):
        
        r.lines("""
        void make_database_%(dbname)s_bindings()
        {
            boost::python::class_<
                %(dbname)s,
                %(dbname)s::ptr_t,
                boost::noncopyable
            >("%(dbname)s",
                boost::python::init<const std::string &, size_t,
                    const boost::python::object &>())
            .def("destroy", &%(dbname)s::destroy)
            .staticmethod("destroy");""",
        8, dbname = self._name).indent()
        
        for ent in self._entities.keys():
            r.unputc()
            r.line('.def("aquire_table_%s", &%s::aquire_table_%s);' % (
                ent, self._name, ent))
        
        r.deindent().line('}')
        return
     
    def render_cpp_declaration(self, r):
        
        aq_tables = sorted(
            'table_%s_ptr_t aquire_table_%s();' % (i,i) \
                for i in self._entities.keys()
        )
        aq_tables = '\n'.join(' ' * 16 + i for i in aq_tables)
        
        r.lines("""
            class %(dbname)s :
                public boost::enable_shared_from_this<%(dbname)s>
            {
            public:
                
                typedef boost::shared_ptr<%(dbname)s> ptr_t;
                
                %(dbname)s(const std::string & name, size_t size,
                    const boost::python::object & klass_dict);
                
                static void destroy(const std::string & name);
                
                %(aq_tables)s
                
            private:
                std::string           _name;
                boost::python::object _klass_dict;
                managed_memory_t      _shmemory;
                void_allocator_t      _alloc;
            };
        """, 12,
            dbname = self._name,
            aq_tables = aq_tables.lstrip(),
        )
        return
    
    def render_cpp_definition(self, r):
        
        r.lines("""
            %(dbname)s::%(dbname)s(
                const std::string & name, size_t size,
                const boost::python::object & klass_dict
            ) :
                _name(name),
                _klass_dict(klass_dict),
                _shmemory(boost::interprocess::open_or_create, name.c_str(), size),
                _alloc(_shmemory.get_segment_manager())
            { }
            
            void %(dbname)s::destroy(const std::string & name)
            { boost::interprocess::shared_memory_object::remove(name.c_str()); }
        """, 12,
            dbname = self._name
        )
        return
    
    @staticmethod
    def parse_schema(schema_path):
        
        # Map schema DDL keywords to
        #  implementing python types
        schema_keywords = {
            'int':            Integer(),
            'long':           Long(),
            'float':          Float(),
            'double':         Double(),
            'bool':           Boolean(),
            'str':            String(),
            'interned':       String(),
            'hashed':         Hashed,
            'hashed_unique':  HashedUnique,
            'ordered':        Ordered,
            'ordered_unique': OrderedUnique,
            'optional':       Optional,
            'cascade':        Cascaded,
            'restrict':       Restricted,
            'entity':         Entity,
            'relation':       Entity,
            'database':       Database,
        }
        
        schema_globals = {}
        execfile(schema_path, schema_keywords, schema_globals)
        return Database(**schema_globals)

