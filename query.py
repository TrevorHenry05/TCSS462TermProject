import sqlite3

def query(requests:dict, db_file_path):
    
    # Check if SQLite DB file exists, if not create it and initialize a table
    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()


    #query for aggregate data
    sql_query_agg = '''
    SELECT 
        AVG(OrderProcessingTime), 
        AVG(GrossMargin), 
        AVG(UnitsSold),  
        MAX(UnitsSold), 
        MIN(UnitsSold),
        SUM(UnitsSold),
        SUM(TotalRevenue),
        SUM(TotalProfit),
        COUNT(distinct OrderId)
    FROM 
        orders
     '''
    

    #query for filters
    sql_query_filters = 'SELECT * FROM orders'

    query_filters =[]

    if requests.get("Region"):
        query_filters.append("Region = \'" + requests["Region"] + "\'")

    if requests.get("Item Type"):
        query_filters.append("ItemType = \'" + requests["Item Type"] + "\'")

    if requests.get("Sales Channel"):
        query_filters.append("SalesChannel = \'" + requests["Sales Channel"] + "\'")

    if requests.get("Order Priority"):
        query_filters.append("OrderPriority = \'" + requests["Order Priority"] + "\'")

    if requests.get("Country"):
        query_filters.append("Country = \'" + requests["Country"] + "\'")


    #add filters
    if query_filters:
        sql_query_agg += " WHERE " + ' AND '.join(query_filters)
        sql_query_filters += " WHERE " + ' AND '.join(query_filters)
        
    #add group by 
    sql_query_agg += " GROUP BY " + ' , '.join(requests["Group By"]) 
    

    #collect filters
    cursor.execute(sql_query_filters)
    filter_result = cursor.fetchall()

    #collect aggegate data
    cursor.execute(sql_query_agg)
    agg_result = cursor.fetchall()

    #close connection
    conn.close()

    #list of dictionaries
    result = []

    #add tuples to result
    for row in filter_result:
        result.append({
            "OrderID": row[0],
            "Region": row[1], 
            "Country": row[2],
            "ItemType": row[3], 
            "SalesChannel" : row[4],
            "OrderPriority": row[5],
            "OrderDate": row[6],
            "ShipDate": row[7],
            "UnitsSold" :row[8],
            "UnitPrice" :row[9],
            "UnitCost" :row[10],
            "TotalRevenue" : row[11],
            "TotalCost" :row[12],
            "TotalProfit" :row[13],
            "GrossMargin" :row[14],
            "OrderProcessingTime" :row[15]
    })


    for row in agg_result:
        result.append({
            "Average Order Processing Time in days": row[0],
            "Average Gross Margin in percent": row[1], 
            "Average Units Sold": row[2],
            "Max Units Sold": row[3],
            "Min Units Sold": row[4], 
            "Total Units Sold" : row[5],
            "Total Total Revenue": row[6],
            "Total Profit": row[7],
            "Number of Orders": row[8],
    })
        
    return result
