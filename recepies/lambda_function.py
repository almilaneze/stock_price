import stock_trend

def lambda_handler():
    stock_trend.last_20(os.environ['region'], os.environ['table_name'])

if __name__ == "__main__":
    lambda_handler()

