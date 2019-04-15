"""
This is a file to describe the Python module distribution and
helps with installation.

More info on various arguments here:
https://setuptools.readthedocs.io/en/latest/setuptools.html
"""
from subprocess import check_output

from setuptools import setup, find_packages


def get_version():
    # https://github.com/uc-cdis/dictionaryutils/pull/37#discussion_r257898408
    try:
        tag = check_output(
            ["git", "describe", "--tags", "--abbrev=0", "--match=[0-9]*"]
        )
        return tag.decode('utf-8').strip("\n")
    except Exception:
        raise RuntimeError(
            "The version number cannot be extracted from git tag in this source "
            "distribution; please either download the source from PyPI, or check out "
            "from GitHub and make sure that the git CLI is available."
        )


setup(
    name='pypfb',
    version=get_version(),
    description='Python SDK for PFB format',
    long_description=open('README.md').read(),
    author='',
    author_email='',
    license='MIT',
    url='https://github.com/uc-cdis/pypfb',
    packages=find_packages(),
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'pfb = pypfb.__main__:main',
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
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Software Development :: Libraries',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
    ],
    install_requires=[
        "cdiserrors~=0.1",
    ],
)
