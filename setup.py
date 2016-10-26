from setuptools import setup, find_packages

setup(
    name='ffodbc',
    version='0.1',
    packages=find_packages(exclude=['tests', 'ppvenv', 'venv3']),
    package_data={'ffodbc': ['ffodbc.c']},
    setup_requires=["cffi>=1.8.0"],
    cffi_modules=["./ffodbc/buildlib.py:ffi"],
    install_requires=["cffi>=1.8.0"],
    include_dirs=['/usr/local/include'],
)
