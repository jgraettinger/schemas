
from foreign_key import ForeignKey
from index import Index
from field import Field
from ddl_core import *

class Entity(object):
    
    def __init__(self, **field_types):
        
        # set externally
        self._name = None
        # {field-name: Field}
        self._fields = {} 
        
        # {index-name: Index}
        self._indices_by_name = {}
        # {(field-name,): Index}
        self._indices_by_fields = {} 
        
        # [ForeignKey]
        #  (where local_entity == self)
        self._foreign_owned = []
        # [ForeignKey]
        #  (where foreign_entity == self)
        self._foreign_held  = []
        
        # track assigned marker bits
        cur_opt = (0, 0)
        
        for fname, ftype in field_types.iteritems():
            
            indexed    = None
            references = None
            optional   = None
            fk_policy  = None
            
            # at most one indexed constraint,
            #  and it must be outermost
            if isinstance(ftype, BaseIndexed):
                indexed = ftype.__class__
                ftype   = ftype.inner
            
            # ...then an optional declaration
            if isinstance(ftype, Optional):
                optional = cur_opt
                cur_opt = (
                    cur_opt[0] + 1 if cur_opt[1] == 7 else 0,
                    (cur_opt[1] + 1) % 8,
                )
                ftype = ftype.inner
            
            # ...then a BaseForeignPolicy declaration
            if isinstance(ftype, BaseForeignPolicy):
                fk_policy = ftype.__class__
                ftype     = ftype.inner
            
            # ...then a Field, iff a foreign key
            if isinstance(ftype, Field):
                references = ftype
                ftype = ftype.type
                
                # default FK policy
                if not fk_policy:
                    fk_policy = Cascaded
            
            assert isinstance(ftype, BaseType)
            
            field = Field()
            field.entity = self
            field.name   = fname
            field.type   = ftype
            field.opt    = optional
            
            self._fields[fname]  = field
            # expose field as attribute
            self.__dict__[fname] = field
            
            # declare any field index
            if indexed:
                self.index(fname, indexed, [fname])
            
            # declare any foreign key relation
            if references:
                self.foreign_key(fk_policy, [fname], [references])
        
        self._opt_bytes = cur_opt[0] + 1 if cur_opt[1] else cur_opt[0]
        return
    
    def index(self, name, type, field_names):
        field_names = tuple(field_names)
        
        # Consistency checks:
        assert field_names
        # valid index type
        assert isinstance(type(None), BaseIndexed)
        # index names are unique by-entity
        assert name not in self._indices_by_name
        # field names must exist
        assert all(n in self._fields for n in field_names)
        # no duplication of indexed fields
        assert field_names not in self._indices_by_fields
        
        index = Index()
        index.entity = self
        index.name   = name
        index.type   = type
        index.field_names = field_names
        
        self._indices_by_name[name] = index
        self._indices_by_fields[field_names] = index
        return self
    
    def foreign_key(self, fk_policy, local_names, foreign_fields):
        local_names    = tuple(local_names)
        foreign_fields = tuple(foreign_fields)
        
        # Consistency checks
        assert local_names
        # valid policy type
        assert isinstance(fk_policy(None), BaseForeignPolicy)
        # equal parity
        assert len(local_names) == len(foreign_fields)
        # fields are present
        assert all(n in self._fields for n in local_names)
        # all reference the same entity
        assert len(set(f.entity for f in foreign_fields)) == 1
        
        local_fields  = [self._fields[i] for i in local_names]
        for lfld, ffld in zip(local_fields, foreign_fields):
            assert lfld.type.__class__ == ffld.type.__class__
        
        foreign_ent   = foreign_fields[0].entity
        foreign_names = tuple(f.name for f in foreign_fields)
        
        # foreign keys must have foreign unique index
        assert foreign_names in foreign_ent._indices_by_fields
        foreign_index = foreign_ent._indices_by_fields[foreign_names]
        assert foreign_index.is_unique
        
        # obtain or create a local Index for field_names
        if local_names not in self._indices_by_fields:
            self.index('_and_'.join(local_names), Ordered, local_names)
        local_index = self._indices_by_fields[local_names]
        
        foreign_key = ForeignKey()
        foreign_key.local_fields   = local_fields
        foreign_key.foreign_fields = foreign_fields
        foreign_key.local_index    = local_index
        foreign_key.foreign_index  = foreign_index
        foreign_key.local_entity   = self
        foreign_key.foreign_entity = foreign_ent
        foreign_key.policy = fk_policy
        
        # track suitable indices for relational integrity
        #  w/ both this entity (for write consistency), and
        #  the foreign one (for deletion consistency)
        self._foreign_owned.append( foreign_key)
        foreign_ent._foreign_held.append( foreign_key)
        return self
    
    def render_python_class(self, r):
        
        # Class name & slots
        r.line('class %s(object):' % self._name).indent()
        r.line('__slots__ = [').indent()
        for fname, ftype in self._fields.iteritems():
            r.line("'%s'," % fname)
        r.deindent().line(']')
        
        # Constructor / assignment
        r.line('def __init__(').indent()
        r.line('self,')
        for fname, ftype in self._fields.iteritems():
            r.line('%s,' % fname)
        r.deindent().line('):').indent()
        for fname, ftype in self._fields.iteritems():
            r.line('self.%s = %s' % (fname, fname))
        r.line('return').deindent()

        # Repr
        r.line('def __repr__(self):').indent()
        r.line('parts = []')
        for fname, ftype in self._fields.iteritems():
            r.line('parts.append("%s = %%r" %% self.%s)' % (fname, fname))
        r.line('return "%s(%%s)" %% ", ".join(parts)' % self._name).deindent()
        
        r.line().deindent()
        return
    
    def render_cpp_fwd_declaration(self, r):
        r.line('class table_%s;' % self._name)
        r.line('typedef boost::shared_ptr<table_%s> table_%s_ptr_t;' % (
            self._name, self._name))
        r.line('struct %s_container_t;' % self._name)
        return
    
    def render_cpp_row_storage(self, r):
        
        # field storage declarations & initializer list
        field_decl = []
        for fname, ftype in self._fields.iteritems():
            field_decl.append('%s %s;' % (
                ftype.type.cpp_type.ljust(10), fname))
            
            if not ftype.opt: continue
            field_decl[-1] += ' // _opt[%d] & %d' % (
                ftype.opt[0], 1 << ftype.opt[1])
        
        field_decl = '\n'.join(' ' * 12 + i for i in field_decl)
        
        # storage for optional marker bits
        opt_decl = 'char _opt[%d];' % self._opt_bytes \
            if self._opt_bytes else ''
        
        r.lines("""
        class %(name)s {
        public:
            
            %(field_decl)s
            
            %(opt_decl)s
        """, 8,
        name = self._name,
        field_decl = field_decl.lstrip(),
        opt_decl   = opt_decl,
        ).indent()
        
        # tags for indicies over entity
        r.line('// index name tags')
        for ind_name, ind in self._indices_by_name.iteritems():
            r.line('struct tag_%s{};' % ind_name)
        
        # typedefs for index key types
        r.line().line('// index key typedefs')
        for ind_name, index in self._indices_by_name.iteritems():
            type_list = []
            
            for f in [self._fields[n] for n in index.field_names]:
                if f.opt:
                    type_list.append(Boolean())
                type_list.append(f.type)
            
            if len(type_list) == 1:
                key_type = type_list[0].cpp_type
            else:
                key_type = 'boost::tuple<%s>' % (
                    ','.join(i.cpp_crtype for i in type_list),)
            
            r.line('typedef %s key_%s_t;' % (key_type, ind_name))
        
        # key extractors for complex key types
        if [i for i in self._indices_by_name.values() if i.is_complex]:
            r.line().line('// index key extractors for complex types')
        
        for ind_name, index in self._indices_by_name.iteritems():
            if not index.is_complex: continue
            
            # extractor decl
            r.line('key_%s_t extract_key_%s() const {' % (
                ind_name, ind_name,)).indent()
            
            # compute member fields => extracted key
            ex_list = []
            for f in [self._fields[n] for n in index.field_names]:
                if f.opt:
                    ex_list.append('_opt[%d] & %d' % (f.opt[0], 1 << f.opt[1]))
                ex_list.append(f.name)
            
            # generate returned key
            r.line('return key_%s_t(%s);' % (
                ind_name, ', '.join(ex_list))) 
            # close extractor
            r.deindent().line('}')
        
        # end class
        r.deindent().line('};').line()
        return
    
    def render_cpp_container(self, r):
        
        r.line('struct %s_container_t : public' % self._name).indent()
        r.line('boost::multi_index_container<')
        # class of storage
        r.line('%s,' % (self._name,))
        
        # enumerate indicies
        r.line('boost::multi_index::indexed_by<').indent()
        for ind_name, index in self._indices_by_name.iteritems():
            
            is_hashed = False
            if   index.type == Ordered:
                r.line('boost::multi_index::ordered_non_unique<').indent()
            elif index.type == OrderedUnique:
                r.line('boost::multi_index::ordered_unique<').indent()
            elif index.type == Hashed:
                r.line('boost::multi_index::hashed_non_unique<').indent()
                is_hashed = True
            elif index.type == HashedUnique:
                r.line('boost::multi_index::hashed_unique<').indent()
                is_hashed = True
            
            # declare index name
            r.line('boost::multi_index::tag<%s::tag_%s>,' % (self._name, ind_name))
            
            # index instrumentation
            if index.is_complex:
                # complex index
                r.line('boost::multi_index::const_mem_fun<%s, %s::key_%s_t,' % (
                    self._name, self._name, ind_name)).indent()
                r.line('&%s::extract_key_%s>' % (
                    self._name, ind_name)).deindent()
                
                if is_hashed:
                    # apply custom hash predicate
                    r.line_append(',').line('_tuple_hash<%s::key_%s_t>' % (
                        self._name, ind_name))
            else:
                # simple index
                r.line('boost::multi_index::member<%s, %s::key_%s_t,' % (
                    self._name, self._name, ind_name)).indent()
                r.line('&%s::%s>' % (
                    self._name, index.field_names[0])).deindent()
            
            # close index decl
            r.deindent().line('>,')
         
        # unput last comma
        r.unputc()
        
        # close indexed_by
        r.deindent().line('>')
        
        # close struct
        r.deindent().line('> { };').line()
        return
    
    def get_referenced(self):
        # set of entities referenced by this entity, excluding self
        #  (required for write-consistency)
        return sorted(
            set(i.foreign_entity for i in
                self._foreign_owned if i.foreign_entity != self),
            key = lambda i: i._name)
    
    def get_referencing(self):
        # set of entities recursively referencing this entity, excluding self
        #  (required for delete-consistency)
        
        held = set()
        stack = [i.local_entity for i in self._foreign_held]
        
        while stack:
            ent = stack.pop()
            if ent in held or ent == self: continue
            held.add(ent)
            stack.extend(i.local_entity for i in ent._foreign_held)
        
        return sorted(held, key = lambda i: i._name)
    
    def __get_referenced_names(self):
        return [i._name for i in self.get_referenced()]
    
    def __get_referencing_names(self):
        return [i._name for i in self.get_referencing()]
    
    def __get_held_names(self):
        return sorted([self._name] + self.__get_referenced_names() + \
            self.__get_referencing_names())
    
    def __default_table_index(self):
        def_tab_index = None
        for ind_name, index in self._indices_by_name.iteritems():
            if index.is_ordered:
                def_tab_index = index
        
        if not def_tab_index:
            # just pick one
            def_tab_index = self._indices_by_name.values()[0]
        
        return def_tab_index

    def render_cpp_table_declaration(self, r):
        
        # full set of table containers we'll need access to
        held = self.__get_held_names()
        
        # containers managed by this table
        containers = ['%s_container_t & _%s;' % (i,i) for i in held]
        containers = '\n'.join(' ' * 12 + i for i in containers)
        
        # containers this table will delete from
        predelete  = ['size_t _pre_delete_%s(const %s &);' % (i,i) for i in \
            sorted([self._name] + self.__get_referencing_names())]
        predelete  = '\n'.join(' ' * 12 + i for i in predelete)
        
        # constructor arguments of this table
        ctor = [
            'const %s::ptr_t & db' % self._database._name,
            'const boost::python::object & klass',
        ]
        ctor.extend('%s_container_t &' % i for i in held)
        ctor = ',\n'.join(' ' * 16 + i for i in ctor)
        
        # default iterator type for whole-table iteration
        def_tab_index = self.__default_table_index()
        
        for index in self._indices_by_name.values():
            if index != def_tab_index and not index.is_ordered: continue
            # only create iterators for ordered indicies
            index.render_iterator_declaration(r)
        
        r.lines("""
        class table_%(name)s :
            public boost::enable_shared_from_this<table_%(name)s>
        {
        public:
            
            typedef boost::shared_ptr<table_%(name)s> ptr_t;
            
            ~table_%(name)s();
            
            %(tab_iter_type)s iter();
            
            void insert(const boost::python::object &);
            
            boost::python::object to_python(const %(name)s &);
            
        """, 8,
        name = self._name,
        tab_iter_type = def_tab_index.get_iterator_type(),
        ).indent()
        
        for ind_name, index in self._indices_by_name.iteritems():
            index.render_member_declaration(r)
        
        r.deindent().lines("""
        private:
            
            table_%(name)s(
                %(ctor)s
            );
            
            %(dbname)s::ptr_t _owning_db;
            boost::python::object _klass;
            
            %(containers)s
            
            %(predelete)s
            
            static %(name)s_container_t * _new_instance();
            
            friend class %(dbname)s;
        };
        """, 8,
        name    = self._name,
        dbname  = self._database._name,
        ctor    = ctor.lstrip(),
        containers = containers.lstrip(),
        predelete  = predelete.lstrip(),
        tab_iter_type = def_tab_index.get_iterator_type(),
        )
        return
    
    def render_cpp_table_definition(self, r):
        
        # full set of table containers we'll need access to
        held = self.__get_held_names()
        
        # constructor arguments of this table
        ctor = [
            'const %s::ptr_t & db' % self._database._name,
            'const boost::python::object & klass',
        ]
        ctor.extend('%s_container_t & %s_container' % (i,i) for i in held)
        ctor = ',\n'.join(' ' * 12 + i for i in ctor)
        
        # compute initializer lists (tables to aquire)
        init_list = [
            '_owning_db(db)',
            '_klass(klass)',
        ]
        [init_list.append('_%s( %s_container)' % (i,i)) for i in held]
        init_list = ',\n'.join(' ' * 12 + i for i in init_list)
        
        # default iterator type for whole-table iteration
        def_tab_index = self.__default_table_index()
        
        for index in self._indices_by_name.values():
            if index != def_tab_index and not index.is_ordered: continue
            # only create iterators for ordered indicies
            index.render_iterator_definition(r)
        
        r.lines("""
        table_%(name)s::table_%(name)s(
            %(ctor)s
        ) :
            %(init_list)s
        { }
        
        table_%(name)s::~table_%(name)s() { }
        
        %(tab_iter_type)s table_%(name)s::iter()
        {
            return %(tab_iter_type)s(
                %(get_def_index)s.begin(),
                %(get_def_index)s.end(),
                shared_from_this()
            );
        }
        
        %(name)s_container_t * table_%(name)s::_new_instance()
        {
            return new %(name)s_container_t();
        }
        
        """, 8,
        name = self._name,
        dbname = self._database._name,
        ctor   = ctor.lstrip(),
        init_list = init_list.lstrip(),
        tab_iter_type = def_tab_index.get_iterator_type(),
        get_def_index = def_tab_index.get_index(),
        )
        
        self.render_pre_delete(self._name, r)
        for ent in self.get_referencing():
            ent.render_pre_delete(self._name, r)
        
        for ind_name, index in self._indices_by_name.iteritems():
            index.render_member_definition(r)
        
        self.render_cpp_write_definition(r)
        self.render_cpp_to_python_definition(r)
        return
    
    def render_cpp_database_members(self, r):
        
        # full set of table containers we'll need access to
        held = self.__get_held_names()
        
        # define aquire_table_$name()
        r.line('table_%(name)s::ptr_t %(dbname)s::aquire_table_%(name)s() {' % {
            'name': self._name, 'dbname': self._database._name}).indent()
        
        r.line('return table_%s::ptr_t( new table_%s(' % (
            self._name, self._name)).indent()
        r.line('shared_from_this(),')
        r.line('_klass_dict["%s"],' % self._name)
        
        # then containers
        for ent in held:
            r.line('*_%s_container,' % ent)
        
        r.unputc().deindent().line('));')
        r.deindent().line('}')
        return
    
    def render_instance_extraction(self, r, py_obj, cpp_obj):
        # python => c++ field extraction
        for fname, fld in self._fields.iteritems():
            
            if fld.opt:
                r.line('if(%s.attr("%s") == none_obj) {' % (py_obj, fname)).indent()
                # turn optional bit off
                r.line('%s._opt[%d] &= %d;' % (cpp_obj, fld.opt[0], ~(1 << fld.opt[1])))
                r.line('%s.%s = %s;' % (cpp_obj, fname, fld.type.get_def_ctor()))
                r.deindent().line('} else {').indent()
                # turn optional bit on
                r.line('%s._opt[%d] |= %d;' % (cpp_obj, fld.opt[0], 1 << fld.opt[1]))
            
            r.line('%s.%s = %s;' % (cpp_obj, fname,
                fld.type.get_extractor('%s.attr("%s")' % (py_obj, fname))))
            
            if fld.opt:
                # close optional if-block
                r.deindent().line('}')
        
        r.line()
        return
    
    def render_cpp_write_definition(self, r):
        
        r.lines("""
        void table_%(name)s::insert(const boost::python::object & o)
        {
            // extract python object into row storage
            %(name)s::%(name)s t;
        """, 8,
        name = self._name,
        ).indent()
        
        self.render_instance_extraction(r, 'o', 't')
        
        # validate foreign key constraints
        for fkey in self._foreign_owned:
            fkey.render_insertion_check(r, 'table_%s::insert' % self._name, 't')
        
        # insert row & check success
        r.deindent().lines("""
            
            if(!_%(name)s.insert(t).second)
            {
                throw std::logic_error("table_%(name)s::insert(): "
                    "element violates table uniqueness constraints");
            }
            return;
        }
        """, 8,
        name = self._name
        )
        return
    
    def render_pre_delete(self, tab_name, r):
        
        r.lines("""
        size_t table_%(ent_name)s::_pre_delete_%(name)s(
            const %(name)s & t)
        {
            size_t count = 0;
        """, 8,
        name = self._name,
        ent_name = tab_name,
        ).indent()
        
        # Satisfy non-mutating constraints first
        foreign_keys = sorted(self._foreign_held, key = lambda i: i.is_deleting)
        
        for fkey in foreign_keys: 
            fkey.render_deletion_check(r, 'table_%s::_pre_delete_%s' % (
                tab_name, self._name), 't')
        
        r.line('return count;')
        r.deindent().line('}')
        return
    
    def render_cpp_to_python_definition(self, r):
        r.line('boost::python::object table_%s::to_python(const %s & t)' % (
            self._name, self._name)).line('{').indent()
        r.line('return _klass(').indent()
        
        for fname, field in self._fields.iteritems():
            if field.opt:
                r.line('(t._opt[%d] & %d) ? %s : none_obj,' % (
                    field.opt[0], 1 << field.opt[1],
                    field.type.to_python('t.%s' % fname)
                    )
                )
            else:
                r.line(field.type.to_python('t.%s' % fname) + ',')
        
        r.unputc()
        r.deindent().line(');')
        r.deindent().line('}')
        return
    
    def render_bindings(self, r):
        
        r.lines("""
        void make_table_%(name)s_bindings() {
            boost::python::class_<
                table_%(name)s,
                table_%(name)s::ptr_t,
                boost::noncopyable
            >("table_%(name)s", boost::python::no_init)
            .def("insert",        &table_%(name)s::insert)
            .def("__iter__",      &table_%(name)s::iter)""",
        8, name = self._name,
        ).indent()
        
        for ind_name, index in self._indices_by_name.iteritems():
            index.render_member_bindings(r)
        r.line_append(';')
        
        # default iterator type for whole-table iteration
        def_tab_index = self.__default_table_index()
        
        for index in self._indices_by_name.values():
            if index != def_tab_index and not index.is_ordered: continue
            # only create iterators for ordered indicies
            index.render_iterator_binding(r)
        
        r.deindent().line('}')
        return
    
