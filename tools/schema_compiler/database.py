
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
                boost::python::init<
                    const boost::python::object &>());""",
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
        aq_tables = '\n'.join(' ' * 12 + i for i in aq_tables)
        
        containers = sorted(
            'std::auto_ptr<%s_container_t> _%s_container;' % (
                i, i) for i in self._entities.keys()
        )
        containers = '\n'.join(' ' * 12 + i for i in containers)
        
        r.lines("""
        class %(dbname)s :
            public boost::enable_shared_from_this<%(dbname)s>
        {
        public:
            
            typedef boost::shared_ptr<%(dbname)s> ptr_t;
            
            %(dbname)s(const boost::python::object & klass_dict);
            
            %(aq_tables)s
            
        private:
            
            boost::python::object _klass_dict;
            %(containers)s
        };
        """, 8,
            dbname = self._name,
            aq_tables = aq_tables.lstrip(),
            containers = containers.lstrip(),
        )
        return
    
    def render_cpp_definition(self, r):
        
        r.lines("""
        %(dbname)s::%(dbname)s(const boost::python::object & klass_dict)
          :
            _klass_dict(klass_dict),""", 8,
            dbname = self._name
        ).indent()
        
        containers = sorted(
            '_%s_container( table_%s::_new_instance()),' % (
                i, i) for i in self._entities
        )
        for cont in containers:
            r.line(cont)
        
        r.unputc().deindent()
        r.line('{ }')
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

