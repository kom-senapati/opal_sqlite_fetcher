import sqlite3
from datetime import datetime, timedelta
import random
import uuid
from faker import Faker

# Initialize Faker for generating random data
fake = Faker()

# Connect to the SQLite database
conn = sqlite3.connect("example.db")
cursor = conn.cursor()

# create a student table and fill random data
cursor.execute(
    """CREATE TABLE IF NOT EXISTS students
        (id text, 
        first_name text, 
        last_name text, 
        dob text, 
        email text, 
        phone text, 
        address text)"""
)

for i in range(100):
    id = str(uuid.uuid4())
    first_name = fake.first_name()
    last_name = fake.last_name()
    dob = fake.date_of_birth(minimum_age=18, maximum_age=22).strftime("%Y-%m-%d")
    email = fake.email()
    phone = fake.phone_number()
    address = fake.address()
    cursor.execute(
        "INSERT INTO students VALUES (?, ?, ?, ?, ?, ?, ?)",
        (id, first_name, last_name, dob, email, phone, address),
    )


# Commit the transaction and close the connection
conn.commit()
conn.close()
