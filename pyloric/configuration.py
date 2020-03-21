import configparser

class ConfigurationData(dict):
    """
    Basically just a wrapper for a dictionary populated with configuration data.
    
    Can be called upon by using: 
        cfg = ConfigurationData(config_dict)
    
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config_filepath = config_filepath

    def _get_config_data(self):
        return self['config_db']
    
    def _get_ssh_tunnel_data(self):
        return self['ssh_tunnel']
    
    def _get_connections(self):
        pass
