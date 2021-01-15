import ast
import re
import sys
from os import getenv, path

from setuptools import find_packages, setup
from setuptools.command.install import install

_version_re = re.compile(r"__version__\s*=\s*(.*)")

with open("dbcat/__init__.py", "rb") as f:
    __version__ = str(
        ast.literal_eval(_version_re.search(f.read().decode("utf-8")).group(1))
    )


class VerifyVersionCommand(install):
    """Custom command to verify that the git tag matches our version"""

    description = "verify that the git tag matches our version"

    def run(self):
        tag = getenv("CIRCLE_TAG")

        if tag != ("v%s" % __version__):
            info = "Git tag: {0} does not match the version of this app: {1}".format(
                tag, __version__
            )
            sys.exit(info)


here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="dbcat",
    version=__version__,
    packages=find_packages(exclude=["docs", "test*"]),
    url="https://tokern.io/data-dictionary",
    license="MIT",
    author="Tokern",
    author_email="info@tokern.io",
    description="Open Source Data Dictionary For Snowflake, BigQuery, AWS Redshift and AWS Glue",
    long_description=long_description,
    long_description_content_type="text/markdown",
    download_url="https://github.com/tokern/dbcat/tarball/" + __version__,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Database",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    keywords="data-dictionary databases postgres snowflake redshift glue",
    install_requires=[
        "amundsen-databuilder==4.0.4",
        "boto3==1.16.55",
        "botocore==1.19.55",
        "cachetools==4.2.0",
        "certifi==2020.12.5",
        "cffi==1.14.4",
        "chardet==4.0.0",
        "click==7.1.2",
        "cryptography==3.3.1",
        "elasticsearch==6.8.1",
        "google-api-core==1.25.0",
        "google-api-python-client==1.12.8",
        "google-auth==1.24.0",
        "google-auth-httplib2==0.0.4",
        "googleapis-common-protos==1.52.0",
        "httplib2==0.18.1",
        "idna==2.10",
        "jinja2==2.11.2",
        "jmespath==0.10.0",
        "markupsafe==1.1.1",
        "neo4j-driver==1.7.6",
        "neobolt==1.7.17",
        "neotime==1.7.4",
        "numpy==1.19.5",
        "pandas==1.1.5",
        "protobuf==3.14.0",
        "psycopg2-binary==2.8.6",
        "pyasn1==0.4.8",
        "pyasn1-modules==0.2.8",
        "pycparser==2.20",
        "pyhocon==0.3.57",
        "pymysql==1.0.2",
        "pyparsing==2.4.7",
        "python-dateutil==2.8.1",
        "pytz==2020.5",
        "pyyaml==5.3.1",
        "requests==2.25.1",
        "retrying==1.3.3",
        "rsa==4.7; python_version >= '3.6'",
        "s3transfer==0.3.4",
        "six==1.15.0",
        "sqlalchemy==1.3.22",
        "statsd==3.3.0",
        "unidecode==1.1.2",
        "uritemplate==3.0.1",
        "urllib3==1.26.2",
    ],
    extra_requires=[],
    dependency_links=[],
    cmdclass={"verify": VerifyVersionCommand},
    entry_points={"console_scripts": ["dbcat = dbcat.__main__:main"]},
    include_package_data=True,
)
