import requests
import os
import json

api_url = 'http://127.0.0.1:5000/load_data'

transactions = []
batch_size = 1000

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

data_to_send = {
    'departments': {'file_name': 'departments.csv', 'file_path': './load_data/departments.csv'},
    'jobs': {'file_name': 'jobs.csv', 'file_path': './load_data/jobs.csv'},
    'hired_employees': {'file_name': 'hired_employees.csv', 'file_path': './load_data/hired_employees.csv'}
}

for table_name, file_info in data_to_send.items():
    file_name = file_info['file_name']
    file_path = file_info['file_path']

    if os.path.exists(file_path):
        with open(file_path, 'rb') as file:
            file_content = file.read().decode('utf-8')

        for line in file_content.splitlines():
            row_data = line.split(',')
            transaction = {
                'table_name': table_name,
                'data': row_data
            }
            transactions.append(transaction)

transaction_batches = list(chunks(transactions, batch_size))

for i, batch in enumerate(transaction_batches):
    payload = {'transactions': batch}
    response = requests.post(api_url, json=payload)

    if response.status_code == 200:
        print(f"Lote de datos enviado con éxito #{i+1}")
    else:
        print(f"Error al enviar el lote de datos {i+1}. Código de estado: {response.status_code}")
        print(response.text)