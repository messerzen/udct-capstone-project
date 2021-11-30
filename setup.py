from setuptools import find_packages
from setuptools import setup

setup(
    name='etl',
    version='1.0.0',
    description='this package executes de etl for receita federal data',
    author='Paulo Henrique Zen Messerschmidt',
    author_email='paulozenm@gmail.com',
    packages=find_packages(include=['etl', 'etl.*']),
    install_requires =[
        'configparser == 5.1.0',
        'psycopg2-binary == 2.9.2',
        'pytest == 6.2.5'
    ]
)