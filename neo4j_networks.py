import pandas as pd
import re
import json
import os
from py2neo import Graph, Node, Relationship
from datetime import datetime
import networkx as nx
from networkx.algorithms import bipartite
import random

#when running queries to upload data from file to neo4j database, file must be located in this directory
neo_import_directory = "../neo4j/import/{}"


def gen_relationship(row):
    """
    generate a dataframe row (relationship between a patient and a provider)

    optionally generate 3 files to avoid duplication of data across rows:
    relationships.csv: [patientid, weight, providerid]
    patient_demographic.csv: [patientid, age, gender, etc]
    provider_details.csv: [providerid, specialty, etc]
    """
    row['gender'] = random.choices(['F','M'], weights=(.55,.45), k=1)[0]
    row['age'] = random.randint(20,80)
    row['weight'] = random.choices([1,2,3,4,5,6,10,20], weights=(.3, .2, .1, .1,.05,.05,.025, .01 ), k=1)[0]
    row['specialty'] = random.choices(['RN','PA', 'PCP', 'Psych', 'cardiologist', 'radiologist'], weights=(.3, .1, .3, .1, .1,.1), k=1)[0]
    row['patient_id'] = random.randint(0,2000)
    row['provider_id'] = random.randint(2001,2050)
    #row['icd'] = random.choices([list of icd codes], weights=(.55,.45), k=1)[random.randint(0,4)]
    #cost if desired
    return row

def gen_relationships_data(nrelationships):
    """
    generate a dataframe of nrelationships rows
    where each row is a pairing between patient and provider
    """
    columns = ['patient_id','gender', 'provider_id', 'weight','specialty']
    df_claims = pd.DataFrame(index=range(nrelationships), columns=columns)
    df_claims = df_claims.apply(gen_relationship, axis=1)
    return df_claims
# ----------------------------------------------------------------------------------------------------------------
print("generate patient provider relationship df...")
df_claims = gen_relationships_data(5000)
print(df_claims.head())

#save file to neo4j import directory
df_claims.to_csv('../neo4j/import/claims_edgelist.csv', index=0)

# ----------------------------------------------------------------------------------------------------------------
#project patient-provider to provider-provider
#REMOVE NODES - REDUCE GRAPH
def remove_zero_deg_nodes(G):
    df_degr_w=pd.DataFrame.from_dict(dict(G.degree(weight='weight')),orient='index')
    df_degr_w_zeros=df_degr_w.loc[df_degr_w[0] == 0]
    G.remove_nodes_from(list(df_degr_w_zeros.index))
    df_degr_w_nzeros=df_degr_w.loc[df_degr_w[0]>0]
    return G, df_degr_w_nzeros

def get_bipartite_graph_from_df(df,columns):  # generate bipartite network:
    # I need to check the impact of the function on the graph directionality
    # columns format: columns=[colname1, colname2, weightname] %weight is not necessary
    start = datetime.now()
    if len(columns)==2:
        B=nx.from_pandas_edgelist(df, columns[0], columns[1])
    elif len(columns)==3:
        B=nx.from_pandas_edgelist(df, columns[0], columns[1], edge_attr=columns[2]) # check if directed or undirected
    proj_nodes = set(df[columns[1]])
    difference=datetime.now()-start
    print ('time to generate bi-graph: ' + str(difference.seconds) + ' seconds')
    return B, proj_nodes

def get_projected_graph(B,proj_nodes,remove_zeros=True): #using nx
    start = datetime.now()
    G=bipartite.weighted_projected_graph(B, proj_nodes)
    if remove_zeros:
        G, df_degr_w=remove_zero_deg_nodes(G)
    diff_graph=datetime.now()-start
    print ('time to generate projected graph: ' + str(diff_graph.seconds) + ' seconds')
    return G

def project_edgelist(df_edgelist):
    print('Projecting...')

    projected_file = 'projected_edgelist.csv'
    provider_details = df_edgelist.set_index('provider_id')['specialty'].to_dict()

    def add_specialties(row):
        row['sourcespec'] = provider_details[row['source']]
        row['targetspec'] = provider_details[row['target']]
        return row

    cols = ['patient_id', 'provider_id']
    bipartite_graph, proj_nodes = get_bipartite_graph_from_df(df_edgelist,cols)
    G = get_projected_graph(bipartite_graph,proj_nodes)

    nx.write_weighted_edgelist(G, neo_import_directory.format('projected_edgelist_temp.csv')) #can load in current dir and delete after
    claims_projected_el = pd.read_csv(neo_import_directory.format('projected_edgelist_temp.csv'), names = ['source', 'target', 'weight'], sep=' ')
    claims_projected_el = claims_projected_el[claims_projected_el['target'].isin(proj_nodes)]
    claims_projected_el = claims_projected_el.apply(add_specialties, axis=1)
    claims_projected_el['normweight'] = ((claims_projected_el['weight'] - claims_projected_el["weight"].min() ) /(claims_projected_el["weight"].max()-claims_projected_el["weight"].min()))*10
    claims_projected_el.to_csv(neo_import_directory.format(projected_file), index=False)

    return claims_projected_el


# ----------------------------------------------------------------------------------------------------------------
df_projected_graph = project_edgelist(df_claims)
print(df_projected_graph.head())
projected = pd.read_csv('../neo4j/import/projected_edgelist.csv')
print("generate provider provider relationship df...")

# ----------------------------------------------------------------------------------------------------------------
# NEO4J QUERIES
def load_data(projected):

    load_basic_query = """
    LOAD CSV WITH HEADERS FROM 'file:///claims_edgelist.csv' AS line
    WITH line
    MERGE (patient:Patient{name:line.patient_id, gender:line.gender, age:line.age, type:'patient'})
    MERGE (provider:Provider{name:line.provider_id, specialty:line.specialty, type:'provider'})
    MERGE (provider)-[:TREATED {weight:toInteger(line.weight) }]-> (patient)
    """

    #TREATED edge can contain multiple properties: ex number of visits, costs, etc
    #MERGE (provider)-[:treated {weight:line.normweight,cost:line.pay }]-> (patient)

    load_projected_query = """
    LOAD CSV WITH HEADERS FROM 'file:///projected_edgelist.csv' AS line
    WITH line
    MERGE (providera:Provider{name:line.source, specialty:line.sourcespec})
    MERGE (providerb:Provider{name:line.target, specialty:line.targetspec})
    MERGE (providera)-[:SHARED_WITH {weight:toInteger(line.normweight)}]-> (providerb)
    """
    if projected:
        graph.run(load_projected_query)
    else:
        graph.run(load_basic_query)


def list_graphs():
    query = """
    CALL gds.graph.list()
    YIELD graphName, nodeCount, relationshipCount
    RETURN graphName, nodeCount, relationshipCount
    ORDER BY graphName ASC
    """
    graphs_dict_list = graph.run(query).data()
    graphs_list = [graph['graphName'] for graph in graphs_dict_list]
    return graphs_list

def delete_elements():
    """
    query & delete all elements
    """
    query = """
    MATCH (n)
    DETACH DELETE n
    """
    graph.run(query)

def remove_graph(graphs):
    """
    graphs: list of existing graphs
    delete each
    """
    for g in graphs:
        query = """
        CALL gds.graph.drop('{}') YIELD graphName;
        """.format(g)
        graph.run(query)

def clean_up():
    """
    delete all existing nodes, relationships, and graphs
    """
    graphs = list_graphs()
    delete_elements()
    remove_graph(graphs)


def create_graph_projection(gname):
    """
    formally creates a graph that can be referenced using the graph's name
    provide:
    graphname
    nodes to include in graph
    relationship (edge connecting the nodes) & direction of relationship, and value (property)

    direction:
    Provider1 - treated > Patient1
    natural: Provider1: 1 Patient1: 0
    reversed: Provider1: 0 Patient1: 1
    undirected: Provider1: 1 Patient1: 1
    """
    query = """
        CALL gds.graph.project(
        "{}",
        ["Patient", "Provider"],
        {TREATED: {orientation: "UNDIRECTED",properties: ["weight"]}}
        )
    """.format(gname)

    #natural, reversed, undirected
    graph.run(query)

def write_degrees_gds(gname):
    """
    Given graph, write degree property for each node based on direction of relationship
    use GDS library
    """
    query = """
    CALL gds.degree.write('{}', { writeProperty: 'degree' })
    """.format(gname)
    graph.run(query)

def write_degrees(patient):
    """
    Cypher/query to manually write a node's undirected degree (same as GDS implementation)
    can rewrite to be more flexible - pass in node types, relationships
    to generate specific degrees based on nodes involved
    example:
    patient can have specialty_degree, nurse_degree, hospital_degree indicating
    number of specialists, nurses, hospitals visited

    Or degree as number of similar patients patientx's PCP has seen
    """
    patient_query ="""
      MATCH (provider : Provider)-[: TREATED]-> (patient : Patient)
      with patient, COUNT(DISTINCT provider) AS pat_degree
      SET patient.pat_degree = pat_degree
    """

    #treated degree - optionally, can have a shared_with degree
    provider_query ="""
      MATCH (provider : Provider)-[: TREATED]-> (patient : Patient)
      with provider, COUNT(DISTINCT patient) AS prov_degree
      SET provider.prov_degree = prov_degree
    """
    graph.run(patient_query) if patient else graph.run(provider_query)
    #graph.run(query)
# ----------------------------------------------------------------------------------------------------------------
print("load relationshops to neo4j...")
graph = Graph()

#query for any existing graphs
graphs_list =list_graphs()
print(graphs_list)

#erase contents of the database
clean_up()

print("load patient-provider relationsips to neo4j...")
#load patient-provider relationsips
load_data(projected = 0)

print("load provider-provider relationsips to neo4j...")
#load provider-provider relationships
load_data(projected = 1)


#Create graph called patient_provider_graphmygraph. execute in browser
"""
CALL gds.graph.project(
    "patient_provider_graph",
    ["Patient", "Provider"],
    {TREATED: {orientation: "UNDIRECTED",properties: ["weight"]}}
)
"""

"""
CALL gds.graph.project(
    "providers_graph",
    ["Provider"],
    {SHARED_WITH: {orientation: "UNDIRECTED",properties: ["weight"]}}
)
"""

print(list_graphs())


#write degrees: execute in browser
"""
CALL gds.degree.write('patient_provider_graph', { writeProperty: 'degree' })
"""

"""
CALL gds.degree.write('providers_graph', { writeProperty: 'degree' })
"""

#manual query to write degrees (more flexible)
write_degrees(patient=1)
write_degrees(patient=0)
