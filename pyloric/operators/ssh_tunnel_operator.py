from sshtunnel import SSHTunnelForwarder

class SSHTunnelOperator(object):
    """
    Uses SSHTunnel library to build tunnel and return a server that can _.start() and _.stop()
    
    """
    
    def __init__(self, ssh_tunnel_dict={}, local_port=5432, ssh_port=22, *args, **kwargs):
        self.ssh_tunnel_dict = ssh_tunnel_dict
        self.local_port = local_port
        try:
            self.ssh_port = self.ssh_tunnel_dict['ssh_port']
        except:
            self.ssh_port = ssh_port
        
    def build_server(self):
        server = SSHTunnelForwarder(
            (self.ssh_tunnel_dict['ssh_server'], 
             int(self.ssh_port)
            ),
            ssh_password = self.ssh_tunnel_dict['ssh_password'],
            ssh_username = self.ssh_tunnel_dict['ssh_username'],
            remote_bind_address = (self.ssh_tunnel_dict['localhost'], int(self.local_port)),
            local_bind_address = (self.ssh_tunnel_dict['localhost'], int(self.local_port))
        )
        
        return server
        