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

#### From GitHub
Use the package manager [pip](https://pip.pypa.io/en/stable/) to install Network Wrangler from the source on GitHub.

```bash
pip install git+https://github.com/wsp-sag/network_wrangler.git@master#egg=network_wrangler
```

Note: if you wanted to install from a specific tag/version number or branch, replace `@master` with `@<branchname>`  or `@tag`

#### From Clone
If you are going to be working on Network Wrangler locally, you might want to clone it to your local machine and install it from the clone.  The -e will install it in [editable mode](https://pip.pypa.io/en/stable/reference/pip_install/?highlight=editable#editable-installs).

```bash
git clone https://github.com/wsp-sag/network_wrangler
cd network_wrangler
pip install -e .
```

Note: if you are not part of the project team and want to contribute code back to the project, please fork before you clone and then add the original repository to your upstream origin list per [these directions on github](https://help.github.com/en/articles/fork-a-repo).

#### Using Docker

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

#### Common Installation Issues

**Issue: `clang: warning: libstdc++ is deprecated; move to libc++ with a minimum deployment target of OS X 10.9 [-Wdeprecated]`**
If you are using MacOS, you might need to update your [xcode command line tools and headers](https://developer.apple.com/downloads/)

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
