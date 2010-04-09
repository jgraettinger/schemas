
from ddl_core import *

class Index(object):
    __slots__ = ['entity', 'name', 'type', 'field_names']
    
    @property
    def is_complex(self):
        # 'simple' indices are ordered over a single member;
        # 'complex' indices are ordered over a tuple. Note
        # that optional fields generate complex indices, as
        # the key type is tuple<bool, $field_type>
        return len(self.field_names) > 1 or \
            self.entity._fields[self.field_names[0]].opt
    
    @property
    def is_unique(self):
        return self.type in (HashedUnique, OrderedUnique)
    
    @property
    def is_ordered(self):
        return self.type in (Ordered, OrderedUnique)
    
    @property
    def has_iterator(self):
        return self.is_ordered or not self.is_unique

    def get_index_type(self):
        return '%s_container_t::index<%s::tag_%s>::type' % (
            self.entity._name, self.entity._name, self.name)
    
    def get_index(self):
        return '_%s.get<%s::tag_%s>()' % (
            self.entity._name, self.entity._name, self.name)
    
    def get_key_type(self):
        return '%s::key_%s_t' % (self.entity._name, self.name)
    
    def get_iterator_type(self):
        return '%s_container_index_%s_iterator' % (
            self.entity._name, self.name)
    
    def render_iterator_declaration(self, r):
        
        r.lines("""
        class %(iter_name)s {
        public:
            
            %(iter_name)s(
                const %(ind_type)s::const_iterator & beg,
                const %(ind_type)s::const_iterator & end,
                const table_%(ent_name)s_ptr_t     & tab
            ) : _it(beg), _end(end), _table(tab)
            { }
            
            boost::python::object next();
            
            %(iter_name)s iter() {return *this;}
            
        private:
            
            %(ind_type)s::const_iterator _it, _end;
            table_%(ent_name)s_ptr_t _table;
        };
        """, 8,
        iter_name = self.get_iterator_type(),
        ind_type = self.get_index_type(),
        ent_name = self.entity._name,
        )
        return
    
    def render_iterator_definition(self, r):
        
        r.lines("""
        boost::python::object %(iter_name)s::next() {
            if(_it == _end)
            {
                PyErr_SetObject(PyExc_StopIteration, Py_None);
                boost::python::throw_error_already_set();
            }
            return _table->to_python(*(_it++));
        }
        """, 8,
        iter_name = self.get_iterator_type(),
        ind_type = self.get_index_type(),
        ent_name = self.entity._name,
        )
        return
    
    def render_iterator_binding(self, r):
        
        r.lines("""
        boost::python::class_<%(iter_name)s>(
            "%(iter_name)s", boost::python::no_init)
        .def("next",     &%(iter_name)s::next)
        .def("__iter__", &%(iter_name)s::iter);
        """, 8,
        iter_name = self.get_iterator_type(),
        )
        return
    
    def render_member_declaration(self, r):
        
        if self.is_unique:
            r.line('boost::python::object get_%s(' % self.name).indent()
            r.line('const boost::python::object &);').deindent()
            
            r.line('void replace_%s(' % self.name).indent()
            r.line('const boost::python::object & %s,' % self.name)
            r.line('const boost::python::object &);').deindent()
            
        else:
            r.line('boost::python::list with_%s(' % self.name).indent()
            r.line('const boost::python::object &);').deindent()
        
        if self.is_ordered:
            r.line('%s by_%s();' % (self.get_iterator_type(), self.name))
            
            r.line('boost::python::list range_%s(' % self.name).indent()
            r.line('const boost::python::object & lower, '
                'const boost::python::object & upper);').deindent()
            
            r.line('size_t delete_range_%s(' % self.name).indent()
            r.line('const boost::python::object & lower, '
                'const boost::python::object & upper);').deindent()
        
        r.line('size_t delete_%s(' % self.name).indent()
        r.line('const boost::python::object &);').deindent()
        return
    
    def render_member_bindings(self, r):
        if self.is_unique:
            r.line('.def("get_%s", &table_%s::get_%s)' % (
                self.name, self.entity._name, self.name))
            r.line('.def("replace_%s", &table_%s::replace_%s)' % (
                self.name, self.entity._name, self.name))
        else:
            r.line('.def("with_%s", &table_%s::with_%s)' % (
                self.name, self.entity._name, self.name))
        
        if self.is_ordered:
            r.line('.def("by_%s", &table_%s::by_%s)' % (
                self.name, self.entity._name, self.name))
            
            r.line('.def("range_%s", &table_%s::range_%s)' % (
                self.name, self.entity._name, self.name))
            
            r.line('.def("delete_range_%s", &table_%s::delete_range_%s)' % (
                self.name, self.entity._name, self.name))
        
        r.line('.def("delete_%s", &table_%s::delete_%s)' % (
            self.name, self.entity._name, self.name))
        return
    
    def __py_key_parts(self, py_obj):
        """
        Produces a sequence of extraction statements for deriving keys
        from a python object or sequence.
        """
        extractors = []
        
        if len(self.field_names) == 1:
            # Arguments will be passed directly
            field = self.entity._fields[self.field_names[0]]
            
            if field.opt:
                extractors.append('(%s != none_obj) ? 1 : 0' % py_obj)
                extractors.append('(%s != none_obj) ? %s : %s' % (
                    py_obj,
                    field.type.get_extractor(py_obj),
                    field.type.get_def_ctor(),
                ))
            else:
                extractors.append(field.type.get_extractor(py_obj))
        else:
            # Arguments are in sequences
            for f_ind, fname in enumerate(self.field_names):
                field = self.entity._fields[fname]
                
                if field.opt:
                    extractors.append('(%s[%d] != none_obj) ? 1 : 0' % (
                        py_obj, f_ind))
                    
                    extractors.append('(%s[%d] != none_obj) ? %s : %s' % (
                        py_obj,
                        f_ind,
                        field.type.get_extractor('%s[%d]' % (py_obj, f_ind)),
                        field.type.get_def_ctor(),
                    ))
                
                else:
                    extractors.append(
                        field.type.get_extractor('%s[%d]' % (py_obj, f_ind)))
        
        return ', '.join(extractors)
    
    def render_deletion(self, r, key_ex):
        if self.is_unique:
            return self.__render_deletion_unique(r, key_ex)
        else:
            return self.__render_deletion_non_unique(r, key_ex)
    
    def __render_deletion_unique(self, r, key_ex):
        
        if self.is_complex:
            key_ex = '%s(%s)' % (self.get_key_type(), key_ex)
        
        r.lines("""
        {
            %(ind_type)s::iterator it(
                %(ind_get)s.find(
                    %(key_ex)s));
            
            if(it != %(ind_get)s.end())
            {
                count += _pre_delete_%(ent_name)s(*it);
                %(ind_get)s.erase(it);
                ++count;
            }
        }
        """, 8,
        ind_type  = self.get_index_type(),
        ind_get   = self.get_index(),
        key_ex = key_ex,
        ent_name  = self.entity._name,
        )
        return
    
    def __render_deletion_non_unique(self, r, key_ex):
        """
        Foreign key dependencies form an acyclic graphic, which implies
        that we cannot use a single range query to identify nodes to
        remove (the recursive satisfaction of foreign constraints may
        delete out from under us some of the selected entities, and
        invalidate our iterators).
        
        By virtue of being acyclic, cascaded constraint satisfaction
        can never delete the node we're currently examining, so we use
        a single iterator here to incrementally walk from the lower to
        upper bound of entities which which must be removed.
        """
        
        if self.is_complex:
            ind_key_ex = 'extract_key_%s()' % self.name
        else:
            ind_key_ex = self.field_names[0]
        
        if self.is_ordered:
            lookup = 'lower_bound'
        else:
            # For hashed indicies, the interface doesn't guarentee that
            # find returns the first of a range of elements. However,
            # from reading the code this is the case, and is very
            # unlikely to change.
            lookup = 'find'
        
        r.lines("""
        {
            %(key_type)s key = %(key_type)s(
                %(key_ex)s);
            
            %(ind_type)s::iterator it(
                %(ind_get)s.%(lookup)s(key));
            
            while(it != %(ind_get)s.end() && \\
                (*it).%(ind_key_ex)s == key)
            {
                count += _pre_delete_%(ent_name)s(*it);
                %(ind_get)s.erase(it++);
                ++count;
            }
        }
        """, 8,
        ent_name    = self.entity._name,
        ind_get     = self.get_index(),
        ind_key_ex  = ind_key_ex,
        ind_type    = self.get_index_type(),
        key_ex      = key_ex,
        key_type    = self.get_key_type(),
        lookup      = lookup,
        )
        return
    
    def render_member_definition(self, r):
        self.render_member_lookups(r)
        self.render_member_deletion(r)
        if self.is_unique:
            self.render_member_replace(r)
        return
    
    def render_member_deletion(self, r):
        
        if self.is_ordered:
            # ordered indices additionally support range deletes,
            #  only as a top-level call (never used for constraint
            #  satisfaction)
            
            # See self.__render_deletion_non_unique for notes on
            #  iteration logic here (eg, why we can't use one call
            #  of lower_bound & upper_bound to establish a range)
            
            if self.is_complex:
                ind_key_ex = 'extract_key_%s()' % self.name
            else:
                ind_key_ex = self.field_names[0]
            
            r.lines("""
            size_t table_%(ent_name)s::delete_range_%(ind_name)s(
                const boost::python::object & lower,
                const boost::python::object & upper)
            {
                %(key_type)s lkey = %(key_type)s(
                    %(lkey_ex)s);
                %(key_type)s ukey = %(key_type)s(
                    %(ukey_ex)s);
                
                %(ind_type)s::iterator it(
                    %(ind_get)s.lower_bound(lkey));
                
                size_t count = 0;
                while(it != %(ind_get)s.end() && \\
                    (*it).%(ind_key_ex)s <= ukey)
                {
                    count += _pre_delete_%(ent_name)s(*it);
                    %(ind_get)s.erase(it++);
                    ++count;
                }
                return count;
            }
            """, 12,
            ent_name  = self.entity._name,
            ind_get   = self.get_index(),
            ind_key_ex = ind_key_ex,
            ind_name  = self.name,
            ind_type  = self.get_index_type(),
            key_type  = self.get_key_type(),
            lkey_ex   = self.__py_key_parts('lower'),
            ukey_ex   = self.__py_key_parts('upper'),
            )
        
        # Generalized deletion
        r.lines("""
        size_t table_%(ent_name)s::delete_%(ind_name)s(
            const boost::python::object & o)
        {
            size_t count = 0;
        """, 8,
        ent_name = self.entity._name,
        ind_name = self.name,
        ).indent()
        
        self.render_deletion(r, self.__py_key_parts('o'))
        r.line('return count;').deindent().line('}')
        return; 
    
    def render_member_lookups(self, r):
        
        kw_exp = {
            'ent_name':  self.entity._name,
            'ind_name':  self.name,
            'ind_type':  self.get_index_type(),
            'ind_get':   self.get_index(),
            'iter_name': self.get_iterator_type(),
            # inline key extractors from python objects
            'okey_ex':   self.__py_key_parts('o'),
            'lkey_ex':   self.__py_key_parts('lower'),
            'ukey_ex':   self.__py_key_parts('upper'),
        }
        
        if self.is_complex:
            kw_exp['okey_ex'] = '%s(%s)' % (self.get_key_type(), kw_exp['okey_ex'])
            kw_exp['lkey_ex'] = '%s(%s)' % (self.get_key_type(), kw_exp['lkey_ex'])
            kw_exp['ukey_ex'] = '%s(%s)' % (self.get_key_type(), kw_exp['ukey_ex'])
        
        if self.is_unique:
            r.lines("""
            boost::python::object table_%(ent_name)s::get_%(ind_name)s(
                const boost::python::object & o)
            {
                %(ind_type)s::const_iterator it(
                    %(ind_get)s.find(
                        %(okey_ex)s)
                );
                
                if(it == %(ind_get)s.end())
                { return none_obj; }
                
                return to_python(*it);
            }
            """, 12, **kw_exp)
        
        else:
            # Non-unique index; minimally supports equal_range
            
            r.lines("""
            boost::python::list table_%(ent_name)s::with_%(ind_name)s(
                const boost::python::object & o)
            {
                std::pair<
                    %(ind_type)s::const_iterator,
                    %(ind_type)s::const_iterator
                > it_range(
                    %(ind_get)s.equal_range(
                        %(okey_ex)s));
                
                boost::python::list lst;
                for(; it_range.first != it_range.second; ++it_range.first)
                    lst.append( to_python(*it_range.first));
                
                return lst;
            }
            """, 12, **kw_exp)
        
        if self.is_ordered:
            # Supports range queries & ordered, whole-table enumeration
            
            r.lines("""
            %(iter_name)s table_%(ent_name)s::by_%(ind_name)s()
            {
                return %(iter_name)s(
                    %(ind_get)s.begin(),
                    %(ind_get)s.end(),
                    shared_from_this()
                );
            }
            
            boost::python::list table_%(ent_name)s::range_%(ind_name)s(
                const boost::python::object & lower,
                const boost::python::object & upper)
            {
                %(ind_type)s::const_iterator cur(
                    %(ind_get)s.lower_bound(
                        %(lkey_ex)s));
                %(ind_type)s::const_iterator end(
                    %(ind_get)s.upper_bound(
                        %(ukey_ex)s));
                
                boost::python::list lst;
                for(; cur != end; ++cur)
                    lst.append( to_python(*cur));
                
                return lst;
            }
            """, 12, **kw_exp)
        
        return
    
    def render_member_replace(self, r):
        """
        Updating by a unique index allows for an existing instance
        to be mutated, without the potential for cascaded deletetion
        imposed by a delete-and-insert cycle.
        
        In addition to validation of foreign-key constraints,
        the implementation also requires that all foreign-held keys
        remain equal between instances.
        
        Key logic here:
        
            * A pre-existing instance is queried for;
                if none exists, insert() is called
            * A storage instance is extracted from the python object
            * For all foreign-held keys, key equality is validated
                between old & new instances
            * For all foreign-owned keys, referential integrity is
                validated (eg, insertion check)
            * multiindex.replace() is called; on failure, an exception is
                thrown & container is unchanged
        """
        
        assert self.is_unique
        
        key_ex = self.__py_key_parts('key')
        if self.is_complex:
            key_ex = '%s(%s)' % (self.get_key_type(), key_ex)
        
        r.lines("""
        void table_%(ent_name)s::replace_%(ind_name)s(
            const boost::python::object & key,
            const boost::python::object & o)
        {
            %(ind_type)s::iterator it(
                %(ind_get)s.find(
                    %(key_ex)s));
            
            if(it == %(ind_get)s.end())
            {
                // key DNE; insert as new
                insert(o);
                return;
            }
            
            // extract python object into row storage
            %(ent_name)s::%(ent_name)s t;
        """, 8,
        ind_type  = self.get_index_type(),
        ind_get   = self.get_index(),
        ind_name  = self.name,
        key_ex    = key_ex,
        ent_name  = self.entity._name,
        ).indent()
        
        # Extract python object to row storage
        self.entity.render_instance_extraction(r, 'o', 't')
        
        r.line('// Validate equality of foreign-held keys')
        for fkey in self.entity._foreign_held:
            
            if fkey.foreign_index.is_complex:
                r.line('if(it->extract_key_%s() != t.extract_key_%s())' % (
                    fkey.foreign_index.name, fkey.foreign_index.name))
            else:
                r.line('if(it->%(fld)s != t.%(fld)s)' % {
                    'fld': fkey.foreign_fields[0].name})
            
            r.lines("""
            {
                throw std::logic_error("replace_%(ind_name)s(): "
                    "replacement cannot alter foreign-held constraint "
                    "%(debug_name)s");
            }
            """, 12,
            ind_name = self.name,
            debug_name = fkey.get_debug_name(),
            )
        
        r.line('// Validate foreign key constraints')
        for fkey in self.entity._foreign_owned:
            fkey.render_insertion_check(r, 'table_%s::replace_%s' % (
                self.entity._name, self.name), 't')
        
        r.lines("""
        if(!%(ind_get)s.replace(it, t))
        {
            throw std::logic_error("table_%(ent_name)s::replace_%(ind_name)s(): "
                "element violates table uniqueness constraints; table unchanged");
        }
        return;
        """, 8,
        ind_get = self.get_index(),
        ind_name = self.name,
        ent_name = self.entity._name,
        ).deindent().line('}')
        return
        
