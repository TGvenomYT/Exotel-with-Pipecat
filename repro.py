
import csv
import os
from datetime import datetime

CSV_FILE = 'calls.csv'

def test_csv_read():
    try:
        with open(CSV_FILE, mode='r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                print(f"Row: {row}")
                call_data = {
                    'id': row['id'],
                    'phone_number': row['phone_number'].strip(),
                    'booking_date': row['booking_date'].strip(),
                    'booking_time': row['booking_time'].strip(),
                    'day_of_week': row['day_of_week'].strip()
                }
                print(f"Processed: {call_data}")
    except Exception as e:
        print(f"CRASH: {e}")

if __name__ == "__main__":
    test_csv_read()
