#!/usr/bin/env python3

from distutils.core import setup

setup(name='BasObserve',
      version='0.1.0',
      description='Toolbox to perform IDS/Anomaly detection on KNX Building Automation Systems',
      author='Martin Peters',
      author_email='',
      url='https://github.com/FreakyBytes/',
      packages=['bas_observe', ],
      install_requires=[
          'click>=6.0,<7',
          'attrs>=17.0.0,<18.0.0',
      ],
      dependency_links=[
          'git+https://github.com/FreakyBytes/BaosKnxParser.git',
      ],
      entry_points='''
        [console_scripts]
        bob=bas_observe.cli:cli
      ''',
      )
