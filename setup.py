
import os
import sys
import subprocess

from setuptools import setup, find_packages, Extension

VERSION = '0.1.0'

schemas = [
    'testdb',
    'datastore',
]

dependencies = [
]

def invoke_schema_compiler(schema):
    
    args = (
        sys.executable,
        "tools/schema_compiler/schema_compiler.py",
        'schemas/%s.schema' % schema,
        "build/schemas/%s" % schema,
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
    return (py_sources, cpp_sources)

package_dir = {'': 'py_src'}
extensions = []
packages = ['schemas', 'tests']

for schema in schemas:
    
    py_sources, cpp_sources = invoke_schema_compiler(schema)
    build_base = py_sources[0][:py_sources[0].rindex('/')]
    
    # mapping from schema package => build location
    package_dir['schemas.%s' % schema] = py_sources[0][:py_sources[0].rindex('/')]
    
    packages.append('schemas.%s' % schema)
    extensions.append(
        Extension(
            'schemas.%s._%s' % (schema, schema),
            cpp_sources,
            libraries = ['boost_python'],
        ))

params = {
    'name':             'schemas',
    'version':          VERSION,
    'package_dir':      package_dir,
    'packages':         packages,
    'ext_modules':      extensions,
    'zip_safe':         False,
    'install_requires': dependencies,
    'test_suite':       'nose.collector',
    'tests_require':    dependencies + ['nose'],
}

setup(**params)
