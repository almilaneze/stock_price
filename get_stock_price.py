import requests
import json
import sys
import http.client
import db_ops


api_key = 'K7PWPA2GGI91N9OX'
base_url = 'https://www.alphavantage.co/query?apikey=' + api_key


def get_daily_prices(function, symbols):
    """
        Gets daily info for several stock prices via REST and uploads the parsed data to AWS
    """
    url = base_url + "&function=" + function + "&symbols=" + symbols
    r = requests.get(url, verify=False)

    if r.status_code != http.client.OK:
        raise requests.HTTPError(r)

    print('Type of r is: ', type(r))                # Type of r is:  <class 'requests.models.Response'>
    print('Type of r.json is: ', type(r.json()))    # Type of r.json is:  <class 'dict'>

    for dic in r.json()["Stock Quotes"]:
        db_ops.db_upload('us-east-1', 'stock_price', [dic["1. symbol"], dic["4. timestamp"][0:10], dic["2. price"]])


def get_historica_prices(function, symbols):
    """
        Gets historical info once for several stock prices via GET REST and uploads the parsed data to AWS to fill the DynamoDB
        headers = {"content-type": "application/json",
                   "accept": "application/json"}
        auth = USER_CREDENTIAL
    """
    url = base_url + "&function=" + function + "&symbol=" + symbols
    r = requests.get(url, verify=False)

    if r.status_code != http.client.OK:
        raise requests.HTTPError(r)

#    print(type(r))         #  <class 'requests.models.Response'>

    ### To get the historic data into a file ###
        #    f1 = open("c:\\temp\yaml\stock_history_DXC.json", 'w')
        #    json.dump(r.json(), f1)
        #    f1.close()
    for key in sorted(r.json()["Time Series (Daily)"].keys(), reverse=True):
        db_ops.db_upload('us-east-1', 'stock_price', [r.json()["Meta Data"]["2. Symbol"],key,r.json()["Time Series (Daily)"][key]["4. close"]])


if __name__ == '__main__':
    try:
#        get_historica_prices('TIME_SERIES_DAILY_ADJUSTED', 'DXC')
        get_daily_prices('BATCH_STOCK_QUOTES', 'DXC,HPQ,HPE,MFGP')

    except requests.ConnectionError:
        sys.stderr.write("Connection Error!\n")
        sys.stderr.write(traceback.format_exc())
    except requests.HTTPError as he:
        sys.stderr.write("HTTP Error! status code : ")
        sys.stderr.write(str(he.args[0].status_code) + "\n")
        sys.stderr.write(he.args[0].text + "\n")
    except Exception as e:
        sys.stderr.write(traceback.format_exc())
        for msg in e.args:
            sys.stderr.write(str(msg) + "\n")
        """
    finally:
        # ----step10 Discard the session----#

        print("Discard the session")
        url = block_storage_api.discard_session(session_id)
        r = requests.delete(url, headers=headers, verify=False)
        try:
            if r.status_code != http.client.OK:
                raise requests.HTTPError(r)
        except requests.HTTPError as he:
            sys.stderr.write("HTTP Error! status code : ")
            sys.stderr.write(str(he.args[0].status_code) + "\n")
            sys.stderr.write(he.args[0].text + "\n")

        print("Operation was completed.")
        sys.exit()
        """
