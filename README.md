# Simple neo4j / networks demo Using:
* Neo4j (https://neo4j.com)


## Getting started (first time use)
- Download repository and place into own project folder

```
Project_folder
  networks/
  ├── venv_create.sh                  
  ├── venv_packages.sh
  ├── download_neo4j.py
  ├── download_neo4j.sh
  ├── neo4j networks.ipynb
  ├── neo4j networks.py
  ├── README.md
  ├── requirements.txt
  │   
  ├── neo4j_plugins/                         # Neo4j algorithms. script will move these to relevant directory
  │   ├── apoc-5.6.0-extended.jar              
  │   ├── neo4j-graph-data-science-2.3.1.jar         
  │   
  ├── test_data/                             # relationship files should be placed in neo4j/import directory
  │   ├── claims_edgelist.csv        
  │   ├── projected_edgelist.csv    
  neo4j/                                     # created after running download_neo4j.sh file. Can move these to the import directory of neo4j
  │   
```

### - Window 1

 ```console
 chmod +x ./download_neo4j.sh
./download_neo4j.sh
 ```
 * automatically downloads neo4j, updates neo4j config, launch server
   * accessible on http://localhost:7474/browser/ in a chrome/safari browser
   * Should be able to connect without a password

![Alt text](/screenshots/neo4j_browser1.png?raw=true "neo4j browser")

### - Window 2
```console
chmod +x venv_create.sh
venv_create.sh
```
* install virtualenv package and creates virtual environment folder "neo4j_venv"

```console
source neo4j_venv/bin/activate
```
* activate virtual environment


```console
chmod +x venv_packages.sh
venv_packages.sh
```
* install packages and add your kernel to jupyter


```console
jupyter notebook
```
* launches jupyter on your browser. accessible via http://localhost:8888/
* can directly work in the neo4j_networks.ipynb to execute relevant commands (generate data, upload to database, generate graphs, etc)
* when you execute above steps, interact with your graph via the browser
* OR.... .py script option


```console
python3 neo4j_networks.py
```
* python script that will generate fake data, upload to the neo4j database, build the network
* after executing script, interact with your graph via the browser





## Subsequent executions

* assumes neo4j directory exists, config file edited, algorithm jars added to plugins/
*  environment has been created and packages installed

### Window 1
```console
../neo4j/bin/neo4j console
```
*  starts neo4j db server with already existing data


* then interface with neo4j using your favorite platform (atom, terminal/command line, jupyter, etc) or directly via the browser



### - data format
![Alt text](/screenshots/edgelist_example.png?raw=true "edgelist example")
* In your favorite language a platform, format your data where every row represents a relationship.
* More efficient to just have Source, Weight, Target (3 column csv file) + a separate file for demographic / additional properties of your nodes to avoid duplicating data over rows. (ex relationships.csv, properties.csv)


### - bipartite Patient - Provider network
![Alt text](/screenshots/patient_provider_graph.png?raw=true "patient provider example")


### - projected Provider - Provider network
![Alt text](/screenshots/provider_provider_graph.png?raw=true "patient provider example")
