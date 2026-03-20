# save as generate_returns.py and run:
# python3 generate_returns.py

import csv
import random
from datetime import datetime, timedelta

random.seed(42)

with open('data/raw/returns.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow([
        'return_id','sale_id','customer_id',
        'product_id','return_date','return_reason',
        'refund_amount','return_status'
    ])

    reasons = [
        'Defective product','Wrong item delivered',
        'Size mismatch','Not as described',
        'Changed mind','Damaged packaging'
    ]
    statuses = ['Approved','Approved','Pending','Rejected']

    for i in range(1, 1001):
        sale_id = random.randint(1, 10000)
        customer_id = random.randint(1, 2000)
        product_id = random.randint(1, 25)
        return_date = datetime(2024, 1, 15) + timedelta(days=random.randint(0, 350))
        reason = random.choice(reasons)
        refund = round(random.uniform(500, 50000), 2)
        status = random.choice(statuses)

        writer.writerow([
            i, sale_id, customer_id, product_id,
            return_date.strftime('%Y-%m-%d'),
            reason, refund, status
        ])

print("returns.csv created — 1000 rows!")