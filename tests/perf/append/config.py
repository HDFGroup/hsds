import os
import json

class Config:
    """
    User Config state
    """
    def __init__(self, config_file=None, **kwargs):
        self._cfg = {}
        if config_file:
            self._config_file = config_file
        elif os.path.isfile(".hscfg"):
            self._config_file = ".hscfg"
        else:
            self._config_file = os.path.expanduser("~/.hscfg")
        # process config file if found
        if os.path.isfile(self._config_file):
            line_number = 0
            with open(self._config_file) as f:
                for line in f:
                    line_number += 1
                    s = line.strip()
                    if not s:
                        continue
                    if s[0] == '#':
                        # comment line
                        continue
                    index = line.find('=')
                    if index <= 0:
                        print("config file: {} line: {} is not valid".format(self._config_file, line_number))
                        continue
                    k = line[:index].strip()
                    v = line[(index+1):].strip()
                    if v and v.upper() != "NONE":
                        self._cfg[k] = v
        # override any config values with environment variable if found
        for k in self._cfg.keys():
            if k.upper() in os.environ:
                self._cfg[k] = os.environ[k.upper()]

        # finally update any values that are passed in to the constructor
        for k in kwargs.keys():
            self._cfg[k] = kwargs[k]

    def __getitem__(self, name):
        """ Get a config item  """

        # Load a variable from environment. It would have only been loaded in
        # __init__ if it was also specified in the config file.
        env_name = name.upper()
        if name not in self._cfg and env_name in os.environ:
            self._cfg[name] = os.environ[env_name]

        return self._cfg[name]

    def __setitem__(self, name, obj):
        """ set config item """
        self._cfg[name] = obj

    def __delitem__(self, name):
        """ Delete option. """
        del self._cfg[name]

    def __len__(self):
        return len(self._cfg)

    def __iter__(self):
        """ Iterate over config names """
        keys = self._cfg.keys()
        for key in keys:
            yield key

    def __contains__(self, name):
        return name in self._cfg or name.upper() in os.environ

    def __repr__(self):
        return json.dumps(self._cfg)

    def keys(self):
        return self._cfg.keys()

    def get(self, name, default=None):
        if name in self:
            return self[name]
        else:
            return default
            
