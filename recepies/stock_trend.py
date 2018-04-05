import boto3
import datetime
import os
import yaml
import json
from boto3.dynamodb.conditions import Key


def last_20(region, tbl_name):
    """
        Gets content from DynamoDB
        region: 'us-east-1'
        Table_name: 'stock_price'
        data: [company, date, price]
    """
    company = ['DXC', 'HPE', 'HPQ', 'MFGP']
    company_data = []
    dyndb = boto3.resource('dynamodb', region_name=region)
    dyn_table = dyndb.Table(tbl_name)

    for c in company:
        company_data.append(dyn_table.query(KeyConditionExpression=Key('company').eq(c) & Key('date').between(
        str(datetime.date.today() - datetime.timedelta(days=15)), str(datetime.date.today()))))

    items = len(company_data[0]['Items'])

    """
        YAML or JSON file generation and coping to S3
    """
    s3_client = boto3.client('s3')
    for c in company_data:
        s3_key = '{}/{}_{}.yml'.format(c['Items'][-1]['company'], c['Items'][-1]['company'], c['Items'][-1]['date'])
#        file_data = yaml.safe_dump(c['Items'], stream=None)
        file_data = json.dumps(c['Items'])
        s3_client.put_object(Bucket='kofa-za-lambda', Key=s3_key, Body=file_data)
        #    returns = s3_client.put_object(Bucket=os.environ['s3_bucket'], Key=s3_key, Body=file_data)

#    dest = open('c:\\temp\\yaml\\test_yaml_from_DynamoDB_new.yml', 'w')
#    yaml.safe_dump(dload_dxc['Items'], dest, default_flow_style=False)
#    dest.close()

    """ SNS Message header generation"""

    sns_message = 'Stock price trend for the period: ' + company_data[0]['Items'][0]['date'] + ' - ' + company_data[0]['Items'][items-1]['date'] + '\n'
    sns_message += '{:<14}'.format('') + '{:^10}'.format(company[0]) + '{:^10}'.format(company[1]) + '{:^10}'.format(company[2]) + '{:^10}'.format(company[3]) + '\n'

    for i in range(items):
        sns_message += '{:<14}'.format(company_data[0]['Items'][i]['date']) + '{:<10}'.format(company_data[0]['Items'][i]['price']) + \
                       '{:<10}'.format(company_data[1]['Items'][i]['price']) + '{:<10}'.format(company_data[2]['Items'][i]['price']) + '{:<10}'.format(company_data[3]['Items'][i]['price']) + '\n'

    sns_message += 'Stock trend ' + '{:^10}'.format(str(calculate_proc(company_data[0],items))+ "%") + '{:^10}'.format(str(calculate_proc(company_data[1],items))+ "%") + \
                   '{:^10}'.format(str(calculate_proc(company_data[2],items))+ "%") + '{:^10}'.format(str(calculate_proc(company_data[3],items))+ "%")

    print(sns_message)
    send_price_trend(region, sns_message)

def calculate_proc(db_data,items):
    return round((-100 + (float(db_data['Items'][items - 1]['price']) / (float(db_data['Items'][0]['price'])/100))),3)

def send_price_trend(region, sns_message):
    sns_msg = boto3.resource('sns', region_name=region)
    msg = sns_msg.Topic(os.environ['sns_arn_dynamodb'])
# moje bi shte raboti i s: config.sns_arn_dynamodb
#    msg = sns_msg.Topic('arn:aws:sns:us-east-1:589119646023:dynamodb')
    sns_response = msg.publish(Message = sns_message, Subject = 'Stock price trend for the last 15 days')


if __name__ == '__main__':
    try:
#        last_20('us-east-1','stock_price')
        last_20(os.environ['region'], os.environ['table_name'])
    except:
        print()
