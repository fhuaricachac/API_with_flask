import requests

api_url = 'http://127.0.0.1:5000/load_data'

data_to_send = {
    'departments': {'file_path': './load_data/departments.csv'},
    'jobs': {'file_path': './load_data/jobs.csv'},
    'hired_employees': {'file_path': './load_data/hired_employees.csv'}
}

payload = {'files': data_to_send}
response = requests.post(api_url, json=payload)

if response.status_code == 200:
    print(f"files send. inserts: {response.text}")
else:
    print(f"Error with files. cod: {response.status_code}")
    print(response.text)