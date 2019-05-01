# network_wrangler

Network Wrangler is a Python library for managing travel model network scenarios.

## Installation
Network Wrangler uses Python 3.6 and above.  Requirements are stored in `requiements.txt` but are automatically installed when using `pip`.

If you are managing multiple python versions, we suggest using [`virtualenv`](https://virtualenv.pypa.io/en/latest/) or [`conda`](https://conda.io/en/latest/) virtual environments.

Example using conda:

```bash
conda create python=3.7 -n <my_network_wrangler_environment>
source activate <my_network_wrangler_environment>
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

Forthcoming...

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
