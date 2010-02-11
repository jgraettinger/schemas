
# Core types
class BaseType(object):
    
    def get_extractor(self, py_obj):
        return 'boost::python::extract<%s>(%s)()' % (self.cpp_type, py_obj)
    def get_def_ctor(self):
        return '%s()' % self.cpp_type
    def to_python(self, inst):
        return 'boost::python::object(%s)' % inst

class Integer(BaseType):
    # 4 byte signed int
    cpp_type   = 'int'
    cpp_crtype = 'int'
class Long(BaseType):
    # 8 byte signed int
    cpp_type   = '_int64'
    cpp_crtype = 'const _int64 &'
class Float(BaseType):
    # 4 byte floating-point
    cpp_type   = 'float'
    cpp_crtype = 'float'
class Double(BaseType):
    # 8 bytes floating-point
    cpp_type   = 'double'
    cpp_crtype = 'const double &'
class Boolean(BaseType):
    # 1 byte signed int
    cpp_type   = 'bool'
    cpp_crtype = 'bool'
class String(BaseType):
    # Unique narrow string
    cpp_type   = 'string_t'
    cpp_crtype = 'string_t const &'
    
    def get_extractor(self, py_obj):
        return 'extract_string_t(%s, _alloc)' % py_obj
    def get_def_ctor(self):
        return 'string_t(_alloc)'
    def to_python(self, inst):
        return 'boost::python::str(%s.c_str(), %s.size())' % (
            inst, inst)

## Base for field wrappers which
# declare additional properties
class Chained(object):
    def __init__(self, inner):
        self.inner = inner

## Foreign key policies
class BaseForeignPolicy(Chained):
    pass
class Restricted(BaseForeignPolicy):
    name = 'restricted'
class Cascaded(BaseForeignPolicy):
    name = 'cascaded'

# DDL declarations
class Optional(Chained):
    pass

# DDL Index declarations
class BaseIndexed(Chained):
    pass
class Ordered(BaseIndexed):
    pass
class Hashed(BaseIndexed):
    pass
class OrderedUnique(BaseIndexed):
    pass
class HashedUnique(BaseIndexed):
    pass


