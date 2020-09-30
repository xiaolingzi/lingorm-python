from setuptools import setup, find_packages

setup(
    name='lingorm',
    version='0.0.2',
    url='https://lingorm.com',
    description='orm framework for python',
    author='xiaolingzi',
    author_email='xlne@foxmail.com',
    license='MIT',
    packages=find_packages(exclude=('*.sample', '*.sample.*','sample')),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'pymysql'
    ]
)
