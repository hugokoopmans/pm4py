from setuptools import setup, find_packages

# Run with 'pip install -e .' for new users
setup(
    name="ExaExact",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "cryptography>=3.4.4",
        "pyodata==1.7.0",
        "oauthlib>=3.1.0", 
        "requests_oauthlib>=1.3.0"])