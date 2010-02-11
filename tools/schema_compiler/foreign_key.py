
from ddl_core import *

class ForeignKey(object):
    __slots__ = [
        'local_fields',
        'foreign_fields',
        'local_index',
        'foreign_index',
        'local_entity',
        'foreign_entity',
        'policy',
    ]
    
    @property
    def is_deleting(self):
        return self.policy == Cascaded
    
    def get_debug_name(self):
        lnames = [i.name for i in self.local_fields]
        fnames = [i.name for i in self.foreign_fields]
        
        debug_name = "%s(%s) in %s(%s) {%s}" % (
            self.local_index.entity._name,
            ', '.join(lnames),
            self.foreign_index.entity._name,
            ', '.join(fnames),
            self.policy.name,
        )
        return debug_name
    
    def get_local_opt_bits(self, row):
        """Gathers the set of optional bits on the local entity which
        must all be true for the foreign key constraint to be applied"""
        
        opt_bits = []
        for fld in self.local_fields:
            if not fld.opt: continue
            
            opt_bits.append('(%s._opt[%d] & %d)' % (
                row, fld.opt[0], 1 << fld.opt[1]))
        
        return ' && '.join(opt_bits)
    
    def project_key_local_to_foreign(self, row):
        """From a local entity row instance, extracts a key suitable for
        querying the foreign index. (used for insertion consistency)"""
        
        ex_key = []
        for lfld, ffld in zip(self.local_fields, self.foreign_fields):
            if ffld.opt:
                ex_key.append('1')
            ex_key.append('%s.%s' % (row, lfld.name))
        
        return ', '.join(ex_key)
    
    def project_key_foreign_to_local(self, row):
        """From a foreign entity row instance, extracts a key suitable for
        querying the local index. (used for deletion consistency)"""
        
        ex_key = []
        for lfld, ffld in zip(self.local_fields, self.foreign_fields):
            if lfld.opt:
                ex_key.append('1')
            ex_key.append('%s.%s' % (row, ffld.name))
        
        return ', '.join(ex_key)
    
    def render_insertion_check(self, r, method, row):
        """Renders a check that the foreign key constraint is satisfied (eg,
        foreign key exists in foreign table) by this local entity instance"""
        
        opt_bits = self.get_local_opt_bits(row)
        if opt_bits:
            r.line('if(%s)' % opt_bits)
        
        key_ex = self.project_key_local_to_foreign(row)
        if self.foreign_index.is_complex:
            key_ex = '%s(%s)' % (self.foreign_index.get_key_type(), key_ex)
        
        r.lines("""
        {
            %(find_type)s::const_iterator it(
                %(find_get)s.find(
                    %(key_ex)s));
            
            if(it == %(find_get)s.end())
            {
                throw std::logic_error("%(method)s(): "
                    "insertion would violate foreign constraint "
                    "%(debug_name)s");
            }
        }
        """, 8,
        find_type  = self.foreign_index.get_index_type(),
        find_get   = self.foreign_index.get_index(),
        key_ex     = key_ex,
        method     = method,
        debug_name = self.get_debug_name(),
        )
        return
    
    def render_deletion_check(self, r, method, row):
        if   self.policy == Cascaded:
            self.__render_deletion_cascade(r, method, row)
        elif self.policy == Restricted:
            self.__render_deletion_restrict(r, method, row)
        else:
            assert False
        return
    
    def __render_deletion_restrict(self, r, method, row):
        """Renders a check that asserts row is not being held as a
        foreign key by the referencing table"""
        
        key_ex = self.project_key_foreign_to_local(row)
        if self.local_index.is_complex:
            key_ex = '%s(%s)' % (self.local_index.get_key_type(), key_ex)
        
        r.line('// %s' % self.get_debug_name())
        r.lines("""
        {
            %(lind_type)s::const_iterator it(
                %(lind_get)s.find(
                    %(key_ex)s));
            
            if(it != %(lind_get)s.end())
            {
                throw std::logic_error("%(method)s(): "
                    "deletion would violate foreign constraint "
                    "%(debug_name)s");
            }
        }
        """, 8,
        lind_type  = self.local_index.get_index_type(),
        lind_get   = self.local_index.get_index(),
        key_ex     = key_ex,
        method     = method,
        debug_name = self.get_debug_name(),
        )
        return
    
    def __render_deletion_cascade(self, r, method, row):
        """Renders a check which recursively deletes local rows
        which would be violated by deletion of the foreign row"""
        
        r.line('// %s' % self.get_debug_name())
        self.local_index.render_deletion(r,
            self.project_key_foreign_to_local(row))
        return
    
