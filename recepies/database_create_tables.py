import mysql.connector

def populate():
    """ create DDL for tables and users """
    print("This script will drop and recreate the stock_price table, and the web_user user.")
    print("")

    stock_sql_1 = "DROP TABLE IF EXISTS stock_price;"
    stock_sql_2 = "create table stock_price (company nvarchar(20) not null primary key, date_stock date DEFAULT NULL, price FLOAT(4,4), created_datetime DATETIME DEFAULT now());"

    user_sql_1 = "DROP USER 'web_user';"
    user_sql_2 = "CREATE USER 'web_user' IDENTIFIED BY %s;"
    user_sql_3 = "GRANT SELECT, INSERT, UPDATE, DELETE on stock_price to 'web_user';"

    #rds settings
    rds_host = input("Database host> ")
    db_user = input("Database user> ")
    password = input("Database password> ")
    db_name = input("Database name> ")
    app_password = input("web_user password> ")

    conn = mysql.connector.connect(user=db_user, password=password,
                                   host=rds_host,
                                   database=db_name)
    cursor = conn.cursor()
    print("Dropping / creating stock_price table")
    cursor.execute(stock_sql_1)
    conn.commit()
    cursor.execute(stock_sql_2)
    conn.commit()

    # this would be nicer in mysql 5.7, i.e "IF EXISTS"
    # https://dev.mysql.com/doc/refman/5.7/en/drop-user.html
    cursor.execute("SELECT 1 FROM mysql.user WHERE user = 'web_user'")
    result = cursor.fetchone()
    if result:
        print("Dropping web_user")
        cursor.execute(user_sql_1)
        conn.commit()

    print("Creating the web_user")
    cursor.execute(user_sql_2, (app_password,))
    conn.commit()
    print("Granting access to web_user")
    cursor.execute(user_sql_3)
    conn.commit()

    conn.close()


populate()
