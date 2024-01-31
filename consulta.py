import requests
import os

data_to_send = {
    'departments': {'file_name': 'departments.csv', 'file_path': 'C:/Users/X11924/Desktop/API REST/load_data/departments.csv'},
    'jobs': {'file_name': 'jobs.csv', 'file_path': 'C:/Users/X11924/Desktop/API REST/load_data/jobs.csv'},
    'hired_employees': {'file_name': 'hired_employees.csv', 'file_path': 'C:/Users/X11924/Desktop/API REST/load_data/hired_employees.csv'}
}

api_url = 'http://127.0.0.1:5000/load_data'  # Reemplaza con la URL de tu API

for table_name, file_info in data_to_send.items():
    file_name = file_info['file_name']
    file_path = file_info['file_path']

    if os.path.exists(file_path):
        files = {'files': (file_name, open(file_path, 'rb'))}
        response = requests.post(api_url, files=files)

        if response.status_code == 200:
            print(f"Archivo {file_name} enviado con éxito.")
        else:
            print(f"Error al enviar el archivo {file_name}. Código de estado: {response.status_code}")
    else:
        print(f"El archivo {file_name} no existe en la ruta especificada: {file_path}")
