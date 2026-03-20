import csv
from datetime import datetime, timedelta

with open('data/raw/date_dim.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow([
        'date_id','full_date','year','month',
        'month_name','quarter','day','day_name',
        'week_number','is_weekend','is_holiday'
    ])

    start = datetime(2024, 1, 1)
    holidays = [
        '2024-01-26','2024-03-25','2024-04-14',
        '2024-08-15','2024-10-02','2024-11-01',
        '2024-12-25'
    ]

    for i in range(366):
        d = start + timedelta(days=i)
        quarter = f"Q{(d.month-1)//3 + 1}"
        is_weekend = 'Y' if d.weekday() >= 5 else 'N'
        is_holiday = 'Y' if d.strftime('%Y-%m-%d') in holidays else 'N'

        writer.writerow([
            i+1,
            d.strftime('%Y-%m-%d'),
            d.year, d.month,
            d.strftime('%B'),
            quarter, d.day,
            d.strftime('%A'),
            d.isocalendar()[1],
            is_weekend, is_holiday
        ])

print("date_dim.csv created — 366 rows!")