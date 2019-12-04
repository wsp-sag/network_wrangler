[![Build Status](https://travis-ci.org/wsp-sag/network_wrangler.svg?branch=master)](https://travis-ci.org/wsp-sag/network_wrangler)

# network_wrangler

Network Wrangler is a Python library for managing travel model network scenarios.

## System Requirements
Network Wrangler should be operating system agonistic and has been tested on Ubuntu and Mac OS.

In order to assist in installation, its helpful to have either [miniconda](https://docs.conda.io/en/latest/miniconda.html), [anaconda](https://docs.conda.io/projects/continuumio-conda/en/latest/user-guide/install/index.html#regular-installation) or [Docker CE](https://docs.docker.com/install/) installed.  If you don't have any of these already, we reommend starting with Miniconda for Python 3.7 as it has the smallest footprint. `conda` is the environment manager that is contained within both the Anaconda and mini-conda applications.

Network Wrangler does require Python 3.6+.  If you have a different version of Python installed, `conda` will take care of installing it for you in the installation instructions below.

## Installation
Network Wrangler uses Python 3.6 and above.  Requirements are stored in `requirements.txt` but are automatically installed when using `pip`.

If you are managing multiple python versions, we suggest using [`virtualenv`](https://virtualenv.pypa.io/en/latest/) or [`conda`](https://conda.io/en/latest/) virtual environments. `conda` is the environment manager that is contained within both the Anaconda and mini-conda applications.

Example using conda in the command line:

```bash
conda create python=3.7 -n wrangler_env
source activate wrangler_env
pip install git+https://github.com/wsp-sag/network_wrangler.git@master#egg=network_wrangler
```

### From GitHub
Use the package manager [pip](https://pip.pypa.io/en/stable/) to install Network Wrangler from the source on GitHub.

```bash
pip install git+https://github.com/wsp-sag/network_wrangler.git@master#egg=network_wrangler
```

Note: if you wanted to install from a specific tag/version number or branch, replace `@master` with `@<branchname>`  or `@tag`

### From Clone
If you are going to be working on Network Wrangler locally, you might want to clone it to your local machine and install it from the clone.  The -e will install it in [editable mode](https://pip.pypa.io/en/stable/reference/pip_install/?highlight=editable#editable-installs).

This is also useful if you want to continue to update your Network Wrangler as it is developed on GitHub.

**1. Open a terminal to get a command prompt.**

**2. Consider using a virtual environment manager like conda.**

Create a new environment by typing the following commands into the command prompt (it might take a few minutes).  

```bash
conda create python=3.7 -n wrangler_env
conda activate wrangler_env
```

I chose `wrangler_env` as the name of my environment, but you could choose something else...just remember it so that you can access it later.

**NOTE** in order to get back to this "conda" environment (i.e. after you close this command prompt), you will need to access it from the command line by using the following command:

```bash
conda activate wrangler_env
```

**3. "Clone" (aka download) network wrangler from Github on to your machine**

If you have [GitHub desktop](https://desktop.github.com/) installed, you can either do this by using the GitHub user interface by clicking on the green button "clone or download" in the [main network wrangler repository page](https://github.com/wsp-sag/network_wrangler).

Otherwise, you can use the command prompt to navigate to the directory that you would like to store your network wrangler clone and then using a [git command](https://git-scm.com/downloads) to clone it.

```bash
cd path to where you want to put wrangler
git clone https://github.com/wsp-sag/network_wrangler
```

Expected output:

```bash
cloning into network_wrangler...
remote: Enumerating objects: 53, done.
remote: Counting objects: 100% (53/53), done.
remote: Compressing objects: 100% (34/34), done.
remote: Total 307 (delta 28), reused 29 (delta 19), pack-reused 254
Receiving objects: 100% (307/307), 15.94 MiB | 10.49 MiB/s, done.
Resolving deltas: 100% (140/140), done.
```
**4a. If you aren't using linux, try to install these packages before network wrangler**

Some packages are very finicky and don't like being installed from their version on the python package index on windows or macosx, so it is often necessary to install them ahead of network_wrangler.

```bash
conda install shapely
conda install rtree
conda install Fiona
```


**4b. Install Network Wrangler in "develop" mode.**

Navigate your command prompt into the network wrangler folder and then install network wrangler in editable mode.
This will take a few minutes because it is also installing all the prerequisites.

```bash
cd network_wrangler
pip install -e .
```

There will be a lot of messy output, but it should end with something like:

```bash
Running setup.py develop for network-wrangler
Successfully installed Rtree-0.8.3 attrs-19.1.0 cchardet-2.1.4 chardet-3.0.4 click-7.0 click-plugins-1.1.1 cligj-0.5.0 cycler-0.10.0 decorator-4.4.0 descartes-1.1.0 fiona-1.8.6 geojson-2.4.1 geopandas-0.5.1 idna-2.8 isoweek-1.3.3 jsonschema-3.0.2 kiwisolver-1.1.0 matplotlib-3.1.1 munch-2.3.2 network-wrangler networkx-2.3 numpy-1.17.0 osmnx-0.10 pandas-0.25.0 partridge-1.1.0 pyparsing-2.4.2 pyproj-2.2.1 pyrsistent-0.15.4 python-dateutil-2.8.0 pytz-2019.2 pyyaml-5.1.2 requests-2.22.0 shapely-1.6.4.post2 six-1.12.0 urllib3-1.25.3
```

**5. Test the Installation**

You can test that network wrangler was properly installed by running python from the command prompt and importing network wrangler as follows:

```bash
python
import network_wrangler
exit
```

**6. [optional] run all the tests**

Running the official tests on your machine requires installing the test packages and can be accomplished as follows from the command line:

```bash
pip install -r dev-requirements.txt
pytest -s -m basic
```

Using the `-s` flag will run all the tests in "noisy" mode.
The `-m basic` flag runs only tests that are marked as "basic"


Note: if you are not part of the project team and want to contribute code back to the project, please fork before you clone and then add the original repository to your upstream origin list per [these directions on github](https://help.github.com/en/articles/fork-a-repo).

### Using Docker

1. Install [Docker](https://docs.docker.com/install/)
2. Clone git repository (see instructions above) *NOTE: this is easiest way right now since repo is private. When it is public we can clone right from github without having to muck around with logins or keys*
3. From the cloned repository, open a terminal from the `/docker` folder and build and run the docker container corresponding to what you want to do by running `docker-compose run <container name> <entry point (optional)> --build`
4. Command to exit container: `exit`

Containers:
 - `wrangler-jupyter` started by running `docker-compose run wrangler-jupyter --build` is appropriate for running and testing wrangler.
   - Default action is to start [jupyter notebook](https://jupyter.org/) which can be found at http://127.0.0.1:8888
   - Safe: It creates an empty folder to store jupyter notebooks within the container but wont overwrite the source files on your actual machine.
   - Starting Bash: You can also start the container with a command line using `docker-compose run wrangler-jupyter /bin/bash --build`.  
   - Doesn't install development dependencies (although they can be installed from within the container)
 - `wrangler-ci` is a small image without extras meant for running tests and deploying to continuous integration server.
   - default command is to run [pytest](https://docs.pytest.org/en/latest/).
   - contains development dependencies so that it can run tests and build docs.
 - `wrangler-dev` is the most powerful but dangerous container `docker-compose run wrangler-dev /bin/bash --build`
   - Warning: It will synchronize code edited from the container to your wrangler clone.  This is great for developing within an IDE, but please take this into account.

### Common Installation Issues

**Issue: `clang: warning: libstdc++ is deprecated; move to libc++ with a minimum deployment target of OS X 10.9 [-Wdeprecated]`**
If you are using MacOS, you might need to update your [xcode command line tools and headers](https://developer.apple.com/downloads/)

**Issue: `OSError: Could not find libspatialindex_c library file`***
Try installing rtree on its own from the Anaconda cloud

```bash
conda install rtree
```
**Issue: Shapely, a pre-requisite, doesn't install propertly because it is missing GEOS module**
Try installing shapely on its own from the Anaconda cloud

```bash
conda install shapely
```
## Quickstart

To get a feel for the API and using project cards, please refer to the "Wrangler Quickstart" jupyter notebook.

To start the notebook, open a command line in the network_wrangler top-level directory and type:

`jupyter notebook`


## Documentation
Documentation can be built from the `/docs` folder using the command: `make html`

## Usage
```python
import network_wrangler

##todo this is just an example for now

## Network Manipulation
my_network = network_wrangler.read_roadway_network(...) # returns
my_network.apply_project_card(...) # returns
my_network.write_roadway_network(...) # returns

## Scenario Building
my_scenario = scenario_from_network(roadway_network, transit_network)
my_scenario.add_projects(directory, keyword)
my_scenario.write_networks(directory, format)

```
## Attribution  
This project is built upon the ideas and concepts implemented in the [network wrangler project](https://github.com/sfcta/networkwrangler) by the [San Francisco County Transportation Authority](http://github.com/sfcta) and expanded upon by the [Metropolitan Transportation Commission](https://github.com/BayAreaMetro/NetworkWrangler).

While Network Wrangler as written here is based on these concepts, the code is distinct and builds upon other packages such as `geopandas` and `partridge` which hadn't been implemented when networkwrangler 1.0 was developed.

## Contributing
Pull requests are welcome. Please open an issue first to discuss what you would like to change.
Please make sure to update tests as appropriate.



## License
[Apache-2.0](https://choosealicense.com/licenses/apache-2.0/)
