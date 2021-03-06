
import os
import sys
import subprocess

from setuptools import setup, find_packages, Extension

VERSION = '0.1.0'

schemas = [
    'testdb',
    'datastore',
]
dependencies = []

def invoke_schema_compiler(schema):
    args = (
        sys.executable,
        "tools/schema_compiler/schema_compiler.py",
        'schema_defs/%s.schema' % schema,
        "schemas/%s" % schema,
    )
    
    print 'Running: %s' % ' '.join(args)
    proc = subprocess.Popen(
        args, executable = sys.executable, shell = False,
        stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    
    if proc.wait():
        print proc.stderr.read()
        sys.exit(-1)
    
    print proc.stderr.read()
    
    # Generated python & cpp sources are returned via stdout
    sources     = proc.stdout.read().strip().split('\n')
    py_sources  = filter(lambda i: i.endswith('.py'), sources)
    cpp_sources = filter(lambda i: i.endswith('.cpp'), sources)
    return cpp_sources

# Use to (re)-generate cpp sources; cannot be used within a source distribution
if '--compile-schemas' in sys.argv:
    for schema in schemas:
        sources = invoke_schema_compiler(schema)
        open('schemas/%s/generated_sources.py' % schema, 'w').write(repr(sources))
    sys.argv.remove('--compile-schemas')


packages = ['schemas']
extensions = []

for schema in schemas:
    
    packages.append('schemas.%s' % schema)
    extensions.append(
        Extension(
            'schemas.%s._%s' % (schema, schema),
            eval(open('schemas/%s/generated_sources.py' % schema).read()),
            libraries = ['boost_python'],
            extra_compile_args = ['-O3'], # optimize
            extra_link_args = ['-s'], # strip debugging info
        ))

params = {
    'name':             'schemas',
    'version':          VERSION,
    'packages':         packages,
    'ext_modules':      extensions,
    'zip_safe':         False,
    'install_requires': dependencies,
    'test_suite':       'nose.collector',
    'tests_require':    dependencies + ['nose'],
}

setup(**params)
