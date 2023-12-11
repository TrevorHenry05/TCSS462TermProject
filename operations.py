import csv
import os
import sqlite3
from datetime import datetime


def transform(file_path):
    transformed_rows = []
    seen_order_ids = set()
    with open(file_path, newline='', mode='r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Check if the row is unique
            if row['Order ID'] in seen_order_ids:
                continue
            
            # Add the order id to the seen order id list
            seen_order_ids.add(row['Order ID'])
            
            # Calculate Order Processing Time
            order_date = datetime.strptime(row['Order Date'], '%m/%d/%Y')
            ship_date = datetime.strptime(row['Ship Date'], '%m/%d/%Y')
            row['Order Processing Time'] = (ship_date - order_date).days

            # Transform Order Priority
            order_priority_mapping = {'H': 'High', 'C': 'Critical', 'L': 'Low', 'M': 'Medium'}
            row['Order Priority'] = order_priority_mapping.get(row['Order Priority'])

            # Calculate Gross Margin
            total_profit = float(row['Total Profit'])
            total_revenue = float(row['Total Revenue'])
            row['Gross Margin'] = total_profit / total_revenue if total_revenue else 0

            transformed_rows.append(row)

    transformed_file_path = '/tmp/transformed_' + os.path.basename(file_path)
    with open(transformed_file_path, mode='w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=reader.fieldnames + ['Order Processing Time', 'Gross Margin'])
        writer.writeheader()
        writer.writerows(transformed_rows)
    
    return transformed_file_path


def load(file_path):
    db_file_path = '/tmp/data.db'

    # Check if SQLite DB file exists, if not create it and initialize a table
    db_exists = os.path.isfile(db_file_path)
    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()

    if not db_exists:
        create_table_query = '''
            CREATE TABLE orders (
                OrderID INTEGER PRIMARY KEY,
                Region TEXT,
                Country TEXT,
                ItemType TEXT,
                SalesChannel TEXT,
                OrderPriority TEXT,
                OrderDate DATE,
                ShipDate DATE,
                UnitsSold INTEGER,
                UnitPrice REAL,
                UnitCost REAL,
                TotalRevenue REAL,
                TotalCost REAL,
                TotalProfit REAL,
                GrossMargin REAL,
                OrderProcessingTime INTEGER
            );
        '''
        cursor.execute(create_table_query)
    
    # Read CSV and prepare data for batch insert
    insert_query = '''
        INSERT OR REPLACE INTO orders (
            Region, 
            Country, 
            ItemType, 
            SalesChannel, 
            OrderPriority, 
            OrderDate, 
            OrderID, 
            ShipDate, 
            UnitsSold, 
            UnitPrice, 
            UnitCost, 
            TotalRevenue, 
            TotalCost, 
            TotalProfit, 
            GrossMargin, 
            OrderProcessingTime
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    '''
    
    data_to_insert = []
    with open(file_path, newline='', mode='r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data_to_insert.append((
                row['Region'], 
                row['Country'], 
                row['Item Type'], 
                row['Sales Channel'], 
                row['Order Priority'], 
                row['Order Date'], 
                row['Order ID'], 
                row['Ship Date'], 
                row['Units Sold'], 
                row['Unit Price'], 
                row['Unit Cost'], 
                row['Total Revenue'], 
                row['Total Cost'], 
                row['Total Profit'],
                row['Gross Margin'],
                row['Order Processing Time']
            ))

    # Perform batch insert
    cursor.executemany(insert_query, data_to_insert)
    
    # Commit changes and close connection
    conn.commit()
    conn.close()
    
    return db_file_path