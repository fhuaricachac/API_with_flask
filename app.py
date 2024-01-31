from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
import os

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

@app.route('/load_data', methods=['POST'])
def load_data():
    try:
        # Asegúrate de que la solicitud contiene archivos adjuntos
        if 'files' not in request.files:
            return jsonify({'error': 'No files provided'}), 400

        files = request.files.getlist('files')

        for uploaded_file in files:
            if uploaded_file.filename == '':
                return jsonify({'error': 'No selected file'}), 400

            table_name = os.path.splitext(uploaded_file.filename)[0].lower()
            df = pd.read_csv(uploaded_file, header=None)

            if table_name == 'departments':
                for _, row in df.iterrows():
                    department = Department(id=int(row[0]), department=row[1])
                    db.session.add(department)

            elif table_name == 'jobs':
                for _, row in df.iterrows():
                    job = Job(id=int(row[0]), job=row[1])
                    db.session.add(job)

            elif table_name == 'hired_employees':
                for _, row in df.iterrows():
                    employee = Employee(id=int(row[0]), name=row[1], datetime=row[2], department_id=row[3], job_id=row[4])
                    db.session.add(employee)

            else:
                return jsonify({'error': f'Unsupported table name: {table_name}'}), 400

        db.session.commit()

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
