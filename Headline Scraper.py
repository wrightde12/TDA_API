import tdameritrade
import urllib
import json
import requests
import dateutil.parser
from datetime import datetime
import websockets
import asyncio
import pyodbc
import nest_asyncio
import time

client_id = 'KTRUVRCFBZSTBCKHWLFBUGCXFM0D59QV'
access_token =
refresh_token = 'CSd5mPV+HWovaY94z6vDMFHoC8aoi+3yBjS37Y9CMRSMSOUb57J7lkgZgjD7t57yGDUO8Ckk5GVfb1D9O76OEcnsVuVIkZ3XxbEdT5zJy02eIbzQSZLmuF0BQIIAFcg+mRoiKmCNYmqcMqy3bfLlPo89GkH3qAMNsu6l7mhdgmGOaxT7IwvVoxqxunftaZQTqx9zTBu5DHWESCcrzDV4nS59/gY/YrdOnPNp6dP8kDjsAkFsC/yuUH09xgS94kn/n4mdkZdSO4zSIZVUlDB3AIgM+kEe85hXT7OOhhwhjvQMnpQrEJf5MLtlSUigSI3P126+DIitNRZRUpX1MQxE5TGFyY0cGcYQeJdOrHZIIaaqh3X9Ua4OxXNyiEC7GU8jQxHaKqUh5uQDLEBGAGLXa2IqR4vj15cAPS5RndGYbfC3Rl4nBftEn8bup6Z100MQuG4LYrgoVi/JHHvluin610Gn54h9Z4t2f55wjJpbbLVpQFYEcf3ohu7lEo2bYRadcxOcYRgFF7tn4oCLrqGexOzLR1SqszeDxl/hzUS4OEX62EeHhJJwtJ5B8VPZSf9CLmWo8FSbBS1l6W1lGs4nug30YS64ifX9cHqJgC6fH57C9dtYQETDK0radNr93rMYOHdkSbq2d7dIpzkrHUePcw5bWnmhu4m9cu5IvhgXLV4EyYkZ3viQCre2tATMpuJqbTXSeplP3yIM8JyfxfcsNnUp8b99dx9nD2cza7pyKGjdUz6yhthBUk+jU6BtFWcnUuqCQJOiKpz+FmWwXEO3XADQQwAZbgNoon/HAXlCZ6a36jDy3V1tDe5m9vgDo/zc0b+nR5yjP26yUBbHE9pawwINS2xS7N8Hs3N8n2Ec9GtfxbXopTg41d25vATrf273y1ZoFU1FQs4=212FD3x19z9sWBHDJACbC00B75E'
account_number = 
password =

def unix_time_millis(dt):
    epoch = datetime.utcfromtimestamp(0)
    return (dt - epoch).total_seconds() * 1000.0

# define our the user principals endpoint
endpoint = 'https://api.tdameritrade.com/v1/userprincipals'
headers = {'Authorization':'Bearer {}'.format(access_token)}

#define parameter for endpoint
params = {'fields': 'streamerSubscriptionKeys,streamerConnectionInfo'}

#make request
content = requests.get(url = endpoint, params = params, headers = headers)
userPrincipalsResponse = content.json()

# grab the timestamp and convert it to milliseconds
tokenTimeStamp = userPrincipalsResponse['streamerInfo']['tokenTimestamp']
date = dateutil.parser.parse(tokenTimeStamp, ignoretz = True)
tokenTimeStampAsMs = unix_time_millis(date)
print(tokenTimeStampAsMs)

# define the items we need in order to make a request to login
credentials = {'userid':userPrincipalsResponse['accounts'][0]['accountId'],
               'token':userPrincipalsResponse['streamerInfo']['token'],
               'company':userPrincipalsResponse['accounts'][0]['company'],
               'segment':userPrincipalsResponse['accounts'][0]['segment'],
               'cddomain':userPrincipalsResponse['accounts'][0]['accountCdDomainId'],
               'usergroup':userPrincipalsResponse['streamerInfo']['userGroup'],
               'accesslevel':userPrincipalsResponse['streamerInfo']['accessLevel'],
               'authroized':"Y",
               'timestamp':int(tokenTimeStampAsMs),
               'appid':userPrincipalsResponse['streamerInfo']['appId'],
               'acl':userPrincipalsResponse['streamerInfo']['acl']}

#defining my login request
login_request = {"requests": [{"service": "ADMIN",
                               "command": "LOGIN",
                               "requestid": "0",
                               "account": userPrincipalsResponse['accounts'][0]['accountId'],
                               "source": userPrincipalsResponse['streamerInfo']['appId'],
                               "parameters": {"credential": urllib.parse.urlencode(credentials),
                                              "token": userPrincipalsResponse['streamerInfo']['token'],
                                              "version": "1.0"}}]}

#defining a request for different data sources
data_request = {"requests": [{"service": "ACTIVES_NASDAQ",
                              "requestid": "3",
                              "command": "SUBS",
                              "account": "your_account",
                              "source": "your_source_id",
                              "parameters": {"keys": "NASDAQ-60",
                                             "fields": "0,1"}},
                            {"service": "ACTIVES_OTCBB",
                             "requestid": "5",
                             "command": "SUBS",
                             "account": "your_account",
                             "source": "your_source_id",
                             "parameters": {"keys": "OTCBB-1800",
                                            "fields": "0,1"}},
                            {"service": "ACTIVES_NYSE",
                             "requestid": "2",
                             "command": "SUBS",
                             "account": "your_account",
                             "source": "your_source_id",
                             "parameters": {"keys": "NYSE-ALL",
                                            "fields": "0,1"}},
                            {"service": "ACTIVES_OPTIONS",
                             "requestid": "4",
                             "command": "SUBS",
                             "account": "your_account",
                             "source": "your_source_id",
                             "parameters": {"keys": "OPTS-DESC-ALL",
                                            "fields": "0,1"}}]}

# turn the requests into a json string
login_encoded = json.dumps(login_request)
data_encoded = json.dumps(data_request)

class WebSocketClient():
    def __init__(self):
        self.cnxn = None
        self.crsr = None

    def database_connect(self):
        #define the server and the database
        server = 'DESKTOP-0J2DPTF\SQLEXPRESS'
        database = 'stock_database'
        sql_driver = '{ODBC Driver 17 for SQL Server}'

        # define our connection to the database
        self.cnxn = pyodbc.connect(driver = sql_driver,
                                   server = server,
                                   database = database,
                                   trusted_connection = 'yes')
        self.crsr = self.cnxn.cursor()

    def database_insert(self, query, data_tuple):
        self.crsr.execute(query, data_tuple)
        self.cnxn.commit()
        self.cnxn.close()
        print('Data has been successfully inserted into the database')

    async def connect(self):
        uri = "wss://" + userPrincipalsResponse['streamerInfo']['streamerSocketUrl'] + "/ws"
        self.connection = await websockets.client.connect(uri)
        if self.connection.open:
            print("Connection established. Client correctly connected.")
            return self.connection

    async def sendMessage(self, message):
        await self.connection.send(message)

    async def receiveMessage(self, connection):
        while True:
            try:
                # grab and decode the message
                time.sleep(1)
                message = await connection.recv()
                message_decoded = json.loads(message)

                #prepare data for insertion, connect to database
                query = "INSERT INTO td_service_data (service, timestamp, command) VALUES (?,?,?);"
                self.database_connect()

                if 'data' in message_decoded.keys():
                    # grab the data
                    date = message_decoded['data'][0]
                    data_tuple = (data['service'], data['timestamp'], data['command'])

                    # insert the data
                    self.database_insert(query, data_tuple)

                print('-'*20)
                print('Received message from server ' + str(message))

            except websockets.exceptions.ConnectionClosed:
                print('Connection with server closed')
                break

    async def heartbeat(self, connection):
        while True:
            try:
                await connection.send('ping')
                await asyncio.sleep(5)
            except websockets.exceptions.ConnectionClosed:
                print('Connection with server closed')
                break


nest_asyncio.apply()

if __name__ == '__main__':
    # create the client object
    client = WebSocketClient()

    #define an event loop
    loop = asyncio.get_event_loop()

    #start a connection to the websocket
    connection = loop.run_until_complete(client.connect())

    # define the tasks that we want to run
    tasks = [asyncio.ensure_future(client.receiveMessage(connection)),
             asyncio.ensure_future(client.sendMessage(login_encoded)),
             asyncio.ensure_future(client.receiveMessage(connection)),
             asyncio.ensure_future(client.sendMessage(data_encoded)),
             asyncio.ensure_future(client.receiveMessage(connection))]

    # run your tasks
    loop.run_until_complete(asyncio.wait(tasks))
