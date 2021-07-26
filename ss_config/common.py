import time
import json
import os
import zmq

import logging

logger = logging.getLogger(__name__)

class ConfigNotFoundError(Exception):
    def __init__(self, config, path, key):
        self.message = f'Mandatory entry for path "{path}" was not found in config {config}. Failed at key "{key}"'
        super().__init__(self.message)


class ConfigManagerClient:
    def __init__(self):
        self.endpoint = None

    def __get_manager_endpoint(self,timeout=None):
        if self.endpoint:
            return self.endpoint

        timestamp = time.time()
        endpoint = None
        while endpoint is None and (timeout is None or time.time() < timestamp + timeout):
            logger.debug("looking for config manager...")
            endpoint = os.environ.get('SS_CONFIG_MANAGER_ENDPOINT')
            try:
                if endpoint is None:
                    with open('/tmp/ss_config_manager_endpoint','r') as fd:
                        endpoint = fd.read()
            except:
                pass
            logger.debug(f"endpoint is: {'None' if endpoint is None else endpoint}")
            time.sleep(1)
        if endpoint is None:
            raise TimeoutError("Timed out while trying to find config manager endpoint")
        logger.info(f"config manager endpoint found: {endpoint}")
        self.endpoint = endpoint
        return endpoint


    def get_config(self, path, timeout=None):
        ctx = zmq.Context()
        ep = self.__get_manager_endpoint(timeout)
        sock = ctx.socket(zmq.REQ)
        logger.debug("Connecting to config manager endpoint {ep}")
        sock.connect(ep)
        logger.debug("Connected to config manager")
        sock.send_multipart([path.encode()])
        logger.debuf("Requested config at {path}")
        res = sock.poll(timeout,zmq.POLLIN)
        if res == 0:
            #error
            self.endpoint = None # reset endpoint in case it is incorrect
            logger.warning(f"Config request for {path} timed out") 
            raise TimeoutError(f"Timed out while waiting for response from config manager at {self.endpoint}")
        msg = sock.recv_multipart()
        if msg[-2] == b'0':
            result = config.Config.deserialise(msg[-1].decode())
            logger.info(f"Config request for {path} returned result: {result}") 
            return result
        else:
            error_msg = msg[-1].decode()
            logger.error(f"Config request for {path} returned ERROR: {error_msg}") 
            raise Exception(error_msg)

class Config(dict):
    def __init__(self, contents=None, parental_path=None):
        self.parental_path = parental_path
        if contents is None:
            super().__init__()
        elif isinstance(contents, dict):
            super().__init__()
            for key, val in contents.items():
                self[key] = val
        else:
            super().__init__(contents)


    def __setitem__(self, raw_key, value):
        #print(f"set {raw_key},{value}")
        key_parts = raw_key.split('/', 1)
        key = key_parts[0]
        key_tail = key_parts[1] if len(key_parts) > 1 else None
        
        if not key and key_tail:
            #if key is empty string and there are still entries in tail
            return self.__setitem__(key_tail,value)
        
        if not key_tail:
            val = ConfigValue.get(value, f"{self.parental_path}/{key}" if self.parental_path else key)
            dict.__setitem__(self, key, val)
        else:
            if key not in self:
                dict.__setitem__(self, key, Config())
            self[key][key_tail] = value


    def __getitem__(self, raw_key):
        #print(f"get {raw_key}")
        key_parts = raw_key.split('/', 1)
        key = key_parts[0]
        key_tail = key_parts[1] if len(key_parts) > 1 else None
        
        if not key and key_tail:
            #if key is empty string and there are still entries in tail
            return self.__getitem__(key_tail)

        val = dict.__getitem__(self, key)

        if key_tail:
            return val[key_tail]
        else:
            #print(f"got {val}")
            return val

    def __str__(self):
        return f"{type(self)} parents: {self.parental_path} subtree: {super().__str__()}"
    
    def deep_items(self):
        items = []
        for k,v in dict.items(self):
            if isinstance(v,Config):
                items.extend([(f"{k}/{subk}",subv) for (subk,subv) in v.items()])
            else:
                items.append((k,v))
        return items

    def keys(self):
        keys = []
        for k,v in self.items():
            if isinstance(v,Config):
                keys.extend([f"{k}/{sub}" for sub in v.keys()])
            else:
                keys.append(k)
        return keys

    def serialise(self):
        return f"{self.parental_path}:{json.dumps(self)}"

    @staticmethod
    def deserialise(raw_string):
        [parental_path, content_string] = raw_string.split(':', 1)
        decoded_contents = json.loads(content_string)
        return ConfigValue.get(decoded_contents, parental_path)

    def must_get(self, path):
        if type(path) == str:
            path = path.split('/')
        current = self
        for element in path:
            if not type(current) == dict and dict not in type(
                    current).__bases__:  # if current is not a subclass of dict
                logger.error(f"{current} is not a dict subclass")
                raise ConfigNotFoundError(self, '/'.join(path), element)
            current = current.get(element)
            if current is None:
                raise ConfigNotFoundError(self, '/'.join(path), element)

        return current
    
    @classmethod
    def from_kvlist(cls,kv_list):
        conf = cls()
        for k,v in kv_list:
            conf[k]=v
        return conf

    def diff(self,updated_config):
        diffs = []
        for k in self.keys():
            try:
                new_value = updated_config[k]
                old_value = self[k]
                if new_value != old_value:
                    #handle type mismatches ('12' != 12)
                    diffs.append((k,old_value,new_value))
            except KeyError:
                #not present
                pass
        return diffs

    def apply_diff(self,diff):
        for item in diff:
            key = None
            if len(item) == 2:
                (key,new) = item
            elif len(item) == 3:
                (key,_old,new) = item
            
            if key:
                self[key] = new

class ConfigValue:
    @staticmethod
    def get(value, parental_path=None):
        if isinstance(value,dict):
            return Config(value, parental_path)
        if isinstance(value,str):
            return ConfigString(value, parental_path)
        if isinstance(value,int):
            return ConfigInt(value, parental_path)
        if isinstance(value,float):
            return ConfigFloat(value, parental_path)
        if isinstance(value,list):
            return ConfigList(value, parental_path)
        return None


class ConfigString(str):
    def __new__(cls, value, parental_path=None):
        args = (value,)
        obj = str.__new__(cls, *args)
        obj.parental_path = parental_path
        return obj

    def serialise(self):
        return f"{self.parental_path}:{json.dumps(self)}"


class ConfigInt(int):
    def __new__(cls, value, parental_path=None):
        args = (value,)
        obj = int.__new__(cls, *args)
        obj.parental_path = parental_path
        return obj

    def serialise(self):
        return f"{self.parental_path}:{json.dumps(self)}"


class ConfigFloat(float):
    def __new__(cls, value, parental_path=None):
        args = (value,)
        obj = float.__new__(cls, *args)
        obj.parental_path = parental_path
        return obj

    def serialise(self):
        return f"{self.parental_path}:{json.dumps(self)}"

class ConfigList(list):
    def __init__(self, value, parental_path=None):
        super().__init__(value)
        self.parental_path = parental_path

    def serialise(self):
        return f"{self.parental_path}:{json.dumps(self)}"

