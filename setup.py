from setuptools import setup, find_packages

setup(
    name='lingorm',
    version='1.0.0',
    url='https://www.xxling.com',
    description='A python orm',
    author='xiaolingzi',
    author_email='xlne@foxmail.com',
    license='LGPL',
    packages=find_packages(exclude=('*.sample', '*.sample.*','sample')),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'pymysql'
    ]
)
