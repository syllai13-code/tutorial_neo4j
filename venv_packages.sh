#if you do not have jupyter involved
# python3 -m pip install jupyter

python3 -m pip install ipykernel
python3 -m pip install -r requirements.txt
python3 -m ipykernel install --user --name=neo4j_venv
