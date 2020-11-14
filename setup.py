import pathlib
from setuptools import setup, find_packages

# The directory containing this file
HERE = pathlib.Path(__file__).parent
# The text of the README file
README = (HERE / "README.md").read_text()


setup(
    name='csv2pg',
    url='https://github.com/DavidLacroix/csv2pg',
    description='A simple and fast cli application to load a csv into postgres',
    long_description=README,
    long_description_content_type="text/markdown",
    author='David Lacroix',

    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 3',
    ],
    keywords='postgres csv',
    packages=find_packages(exclude=['tests', 'test']),
    include_package_data=True,
    install_requires=[
        'Click',
        'psycopg2-binary>=2.0.6',
        'tqdm',
    ],
    python_requires='>=3.5',
    entry_points={'console_scripts': ['csv2pg=csv2pg.cli:cli'], },
    #entry_points='''
    #    [console_scripts]
    #    csv2pg=csv2pg.cli:cli
    #''',
)
