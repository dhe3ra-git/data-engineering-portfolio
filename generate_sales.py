# save as generate_sales.py and run:
# python3 generate_sales.py

import csv
import random
from datetime import datetime, timedelta

random.seed(42)

with open('data/raw/sales_transactions.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow([
        'sale_id','customer_id','product_id','store_id',
        'promo_id','quantity','unit_price','discount_amount',
        'tax_amount','final_amount','sale_date',
        'payment_method','sale_status'
    ])

    products = {
        1:129999, 2:89999, 3:114999, 4:129999, 5:29999,
        6:15999, 7:12999, 8:14999, 9:4999, 10:2999,
        11:1999, 12:8999, 13:12999, 14:3999, 15:7999,
        16:5999, 17:8999, 18:24999, 19:4999, 20:3999,
        21:599, 22:299, 23:449, 24:28, 25:199
    }

    payment_methods = ['UPI','Credit Card','Debit Card','Net Banking','Cash','EMI']
    statuses = ['Completed','Completed','Completed','Completed','Returned','Cancelled']

    for i in range(1, 10001):
        customer_id = random.randint(1, 2000)
        product_id = random.randint(1, 25)
        store_id = random.randint(1, 15)
        promo_id = random.choice([1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,15,15])
        quantity = random.randint(1, 5)
        unit_price = products[product_id]
        discount_pct = random.choice([0,0,0,5,10,15,20,25,30])
        discount_amount = round(unit_price * quantity * discount_pct / 100, 2)
        subtotal = unit_price * quantity - discount_amount
        tax_amount = round(subtotal * 0.18, 2)
        final_amount = round(subtotal + tax_amount, 2)
        sale_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))
        payment = random.choice(payment_methods)
        status = random.choice(statuses)

        writer.writerow([
            i, customer_id, product_id, store_id,
            promo_id, quantity, unit_price, discount_amount,
            tax_amount, final_amount,
            sale_date.strftime('%Y-%m-%d'),
            payment, status
        ])

print("sales_transactions.csv created — 10,000 rows!")