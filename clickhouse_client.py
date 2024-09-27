from clickhouse_driver import Client


class ClickhouseClient :
    def __init__ (self, host='clickhouse', port=9000, user='default', password='') :
        self.client = Client(host=host, port=port, user=user, password=password)

    def execute (self, query, params=None) :
        return self.client.execute(query, params)

    def close (self) :
        self.client.disconnect()