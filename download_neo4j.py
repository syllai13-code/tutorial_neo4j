import os, tarfile
from urllib.request import urlretrieve

base_directory, current_directory = os.path.split(os.getcwd())
print('base_directory:{} \ncurrent_directory:{}'.format(base_directory, current_directory))

#download neo4j community zip from url
def download_zip(zip_file_url):

    file_tmp = urlretrieve(zip_file_url, filename=None)[0]
    tar = tarfile.open(file_tmp)
    tar.extractall(base_directory)
    filename = tar.getnames()[0]

    print(filename) #'https://neo4j.com/artifact.php?name=neo4j-community-3.5.12-unix.tar.gz'
    os.rename('{}/{}'.format(base_directory,tar.getnames()[0]), base_directory+'/neo4j/')

#add network/graph algorithm plugins to neo4j directory
def add_plugins():
    print('adding algorithm plugins...')
    for jar in os.listdir('neo4j_plugins'):

        jar_path = os.getcwd() + '/neo4j_plugins/'+jar
        neo4j_path = base_directory+'/neo4j/plugins/'+jar
        print(jar_path)
        print(neo4j_path)
        os.rename(jar_path, neo4j_path)

#change configuration file to make use of algorithm plugins and require passwordless access***
def update_conf():
    print('\nupdating neo4j config file...\n')
    conf_file = base_directory+'/neo4j/conf/neo4j.conf'
    print(conf_file)
    f = open(conf_file, 'r')

    lines = f.readlines()

    newConf = ""

    for line in lines:
        if line.strip() == '#dbms.security.procedures.unrestricted=my.extensions.example,my.procedures.*':
            line = 'dbms.security.procedures.unrestricted=gds.*'
            print(line)

        elif line.strip() == '#dbms.security.auth_enabled=false':
            line = 'dbms.security.auth_enabled=false'
            print(line)

        newConf += line

    new_conf_file = open(conf_file,'w')
    new_conf_file.write(newConf)
    new_conf_file.close()

if not os.path.exists(base_directory+'/neo4j/'):
    print('Beginning file download with requests \n',base_directory)
    #zip_url = 'https://neo4j.com/artifact.php?name=neo4j-community-3.5.12-unix.tar.gz'
    zip_url = 'https://neo4j.com/artifact.php?name=neo4j-community-5.5.0-unix.tar.gz'
    download_zip(zip_url)
else:
    print('Neo4j ready \n')

update_conf()
add_plugins()
