#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.md', encoding='utf-8') as readme_file:
    readme = readme_file.read()

with open('requirements.txt') as req:
    requirements = req.read().splitlines()

setup(name='burrow_prometheus_exporter',
      version='0.0.1',
      author='Rafik Saad',
      description='Prometheus exporter for Burrow kafka lag metrics',
      long_description=readme,
      long_description_content_type='text/markdown',
      install_requires=requirements,
      keywords='burrow_prometheus_exporter',
      classifiers=[
          'Intended Audience :: Developers',
          'Natural Language :: English',
          'Programming Language :: Python :: 3 :: Only',
          'Programming Language :: Python :: 3.7',
      ],
      packages=find_packages(),
      entry_points={
          'console_scripts': [
              'burrow_prometheus_exporter=burrow_prometheus_exporter.__main__:main',
          ],
      }
      )
