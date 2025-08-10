#! /bin/bash

# Cria o diretório data se não existir
mkdir -p data

# Download the datasets
# pedidos
curl -L -o ./data/pedidos.gz https://raw.githubusercontent.com/infobarbosa/datasets-csv-pedidos/main/data/pedidos/pedidos-2024-01.csv.gz

# clientes
curl -L -o ./data/clientes.gz https://raw.githubusercontent.com/infobarbosa/dataset-json-clientes/main/data/clientes.json.gz
