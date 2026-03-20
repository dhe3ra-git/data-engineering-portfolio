import csv
import random
from datetime import datetime, timedelta

cities = [
    ('Delhi', 'Delhi', 1),
    ('Noida', 'Uttar Pradesh', 1),
    ('Gurgaon', 'Haryana', 1),
    ('Chennai', 'Tamil Nadu', 2),
    ('Bangalore', 'Karnataka', 2),
    ('Hyderabad', 'Telangana', 2),
    ('Kolkata', 'West Bengal', 3),
    ('Bhubaneswar', 'Odisha', 3),
    ('Mumbai', 'Maharashtra', 4),
    ('Pune', 'Maharashtra', 4),
    ('Ahmedabad', 'Gujarat', 4),
    ('Nagpur', 'Maharashtra', 5),
    ('Bhopal', 'Madhya Pradesh', 5),
    ('Indore', 'Madhya Pradesh', 5)
]

segments = ['Premium', 'Regular', 'Budget']
genders = ['M', 'F']

first_names = ['Aarav','Aditya','Akash','Arjun','Dheeraj','Kiran',
               'Priya','Sneha','Divya','Pooja','Rahul','Rohit',
               'Suresh','Ramesh','Vikram','Neha','Ananya','Kavya',
               'Ravi','Sanjay','Meera','Lakshmi','Swathi','Harsha']

last_names = ['Reddy','Sharma','Kumar','Singh','Patel','Rao',
              'Nair','Pillai','Iyer','Verma','Gupta','Joshi',
              'Mehta','Shah','Kapoor','Mishra','Pandey','Tiwari']

with open('data/raw/customers.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['customer_id','first_name','last_name',
                     'email','phone','gender','age',
                     'city','state','region_id',
                     'segment','registration_date','is_active'])

    for i in range(1, 2001):
        fn = random.choice(first_names)
        ln = random.choice(last_names)
        city, state, region_id = random.choice(cities)
        age = random.randint(18, 65)
        segment = random.choice(segments)
        gender = random.choice(genders)
        reg_date = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1460))
        is_active = random.choice(['Y', 'Y', 'Y', 'N'])

        writer.writerow([
            i, fn, ln,
            f"{fn.lower()}.{ln.lower()}{i}@gmail.com",
            f"9{random.randint(100000000, 999999999)}",
            gender, age, city, state, region_id,
            segment, reg_date.strftime('%Y-%m-%d'), is_active
        ])

print("customers.csv created — 2000 rows!")