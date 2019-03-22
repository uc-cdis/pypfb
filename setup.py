try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup
    from pkgutil import walk_packages


    def find_packages(path=__path__, prefix=""):
        yield prefix
        prefix = prefix + "."
        for _, name, ispkg in walk_packages(path, prefix):
            if ispkg:
                yield name

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='python_pfb_sdk',
    version='0.0.1',
    description='Python SDK for PFB format',
    long_description=open('README_old.md').read(),
    author='',
    author_email='',
    license='MIT',
    url='https://github.com/uc-cdis/python_pfb_sdk',
    packages=['python_pfb_sdk'] + find_packages(),
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'pfb = python_pfb_sdk.__main__:main',
        ]
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Software Development :: Libraries',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
    ],
)
