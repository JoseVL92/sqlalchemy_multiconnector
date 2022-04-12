from setuptools import setup, find_packages

setup(
    name="sqlalchemy_multiconnector",
    version=0.1,
    author="Jose Alberto Varona Labrada",
    author_email="jovalab92@gmail.com",
    description=("Easy python SQLAlchemy connector for SQL Databases (sqlite, postgresql and mysql). "
                 "Support multi-tenancy with multiple databases and/or multiple schemas"),
    python_requires=">=3.6",
    url="https://github.com/JoseVL92/sqlalchemy_multiconnector",
    download_url="https://github.com/JoseVL92/sqlalchemy_multiconnector/archive/v_01.tar.gz",
    packages=find_packages(),
    data_files=[
        ("", ["LICENSE.txt", "README.md"])
    ],
    install_requires=['sqlalchemy'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    license='MIT',
    keywords=['database', 'ORM', 'sqlalchemy', 'SQL']
)
