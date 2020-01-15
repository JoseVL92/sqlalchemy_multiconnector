from setuptools import setup, find_packages

setup(
    name="sqlalchemy_connector",
    version=0.1,
    author="Jose Alberto Varona Labrada",
    author_email="jovalab92@gmail.com",
    description="Easy sqlalchemy connector",
    python_requires=">=3.5",
    url="https://github.com/JoseVL92/sqlalchemy-connector",
    download_url="https://github.com/JoseVL92/sqlalchemy-connector/archive/v_01.tar.gz",
    packages=find_packages(),
    data_files=[
        ("", ["LICENSE.txt", "README.md"])
    ],
    install_requires=['sqlalchemy'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    license='MIT',
    keywords=['database', 'ORM', 'sqlalchemy']
)
