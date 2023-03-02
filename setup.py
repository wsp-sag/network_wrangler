from setuptools import setup

classifiers = [
    "Development Status :: 1 - Planning",
    "License :: OSI Approved :: Apache Software License",
    "Natural Language :: English",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
]

with open("README.md") as f:
    long_description = f.read()

with open("requirements.txt") as f:
    install_requires = [r.strip() for r in f.readlines()]

EXTRAS = ["tests"]
extras_require = {}
for e in EXTRAS:
    with open("requirements.{e}.txt") as f:
        extras_require[e]=[r.strip() for r in f.readlines()]


setup(
    name="network_wrangler",
    version="0.2.0",
    description="",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/wsp-sag/network_wrangler",
    license="Apache 2",
    platforms="any",
    packages=["network_wrangler"],
    include_package_data=True,
    install_requires=install_requires,
    extras_require = extras_require,
)
