import psycopg2

class PostgresOperator(object):
    """
    Uses psycopg2 library to do things
    
    """
    def __init__(self, connection_dict={}, *args, **kwargs):
        self.connection_dict = connection_dict
        self.connection_string = self.get_connection_string()
    
    def get_connection_string(self):
        connection_string = 'postgresql://' + self.connection_dict['username'] + \
                            ':' + self.connection_dict['password'] + \
                            '@' + self.connection_dict['hostname'] + \
                            ':' + self.connection_dict['port']
                            #'/' + self.connection_dict['hostname']
        return connection_string
    
    def connect(self):
        connection = psycopg2.connect(self.connection_string)
        return connection
    
    def run_query(self, query):
        connection = self.connect()
        cursor = connection.cursor()
        cursor.execute(query)
        cursor.commit()
        return connection
        
