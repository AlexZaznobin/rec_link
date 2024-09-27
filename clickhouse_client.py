from clickhouse_driver import Client


class ClickhouseClient :
    def __init__ (self, host, user, password) :
        self.client = Client(host=host, user=user, password=password)

    def execute (self, query, params=None) :
        return self.client.execute(query, params)

    def close (self) :
        self.client.disconnect()