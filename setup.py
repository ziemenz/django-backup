from distutils.core import setup
from setuptools import find_packages

setup(
    name='django-backup',
    version     = '2.0.0',
    description    = 'A backup script for the Django admin',
    author = 'Dmitriy Kovalev, Michael Huynh, msaelices, Andy Baker, Chen Zhe, Chris Cohoat',
    author_email = 'andy@ixxy.co.uk',
    url = 'http://github.com/django-backup/django-backup',
    packages = find_packages(exclude=('test_project',)),
    include_package_data=True,
    classifiers=[
        'Programming Language :: Python', 
        'Framework :: Django', 
        'License :: OSI Approved :: BSD License',
    ]
)

