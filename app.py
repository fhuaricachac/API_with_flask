from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import os, sys

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///globant.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

class Department(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    department = db.Column(db.String(50), unique=True, nullable=False)

class Job(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    job = db.Column(db.String(50), unique=True, nullable=False)

class Employee(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=True)
    datetime = db.Column(db.String(50))
    department_id = db.Column(db.Integer, db.ForeignKey('department.id'), nullable=True)
    job_id = db.Column(db.Integer, db.ForeignKey('job.id'), nullable=True)

transactions = []
batch_size = 1000

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

@app.route('/load_data', methods=['POST'])
def load_data():
    try:
        if not request.is_json:
            return jsonify({'error': 'Invalid JSON payload'}), 400

        data_to_send = request.json.get('files')

        for table_name, file_info in data_to_send.items():
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

        for i, listtran in enumerate(transaction_batches):
            if not isinstance(listtran, list):
                return jsonify({'error': 'Invalid transactions format'}), 400

            for transaction in listtran:
                if 'table_name' not in transaction or 'data' not in transaction:
                    return jsonify({'error': 'Invalid transaction format'}), 400
                
                table_name = transaction['table_name'].lower()
                data = transaction['data']
                if not isinstance(data, list):
                    return jsonify({'error': 'Invalid data format'}), 400

                if table_name == 'departments':
                    department = Department(
                        id = data[0],
                        department=data[1]
                    )
                    db.session.add(department)
                elif table_name == 'jobs':
                    job = Job(
                        id=data[0], 
                        job=data[1]
                    )
                    db.session.add(job)
                elif table_name == 'hired_employees':
                    employee = Employee(
                        id=data[0],
                        name=data[1],
                        datetime=data[2],
                        department_id=data[3],
                        job_id=data[4]
                    )
                    db.session.add(employee)
                else:
                    return jsonify({'error': f'Unsupported table name: {table_name}'}), 400

            db.session.commit()
            print(f"Lote número {i+1} enviado", file=sys.stdout)

        return jsonify({'message': 'Data loaded successfully'}), 200

    except Exception as e:
        print(f"Error during data loading: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
        print("Tables in the database:")
        print(db.engine)
    app.run(debug=True)
