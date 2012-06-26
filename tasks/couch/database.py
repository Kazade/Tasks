import uuid

class Database(object):
    def __init__(self, database_name):
        self._db = u1db.open(name, create=True)        
                    
database = None

def connect(name):
    global database
    
    database = Database(name)

class Field(object):
    def __init__(self):
        self._value = None
        
    def get_value(self):
        raise NotImplementedError()
    
    def set_value(self, value):
        raise NotImplementedError()

class IntegerField(Field):
    def get_value(self):
        return int(self._value)
    
    def set_value(self, value):
        self._value = int(value)


class Model(object):
    def __init__(self, **kwargs):
        global database
        
        self._database = database
        self._doc = None
        self._pk = None

    def __getattr__(self, name):
        result = object.__getattr__(self, name)
        if issubclass(result, Field): #If this is a field
            return result.get_value()
        return result

    def __setattr__(self, name, value):
        result = object.__getattr__(name)
        if issubclass(result, Field):
            result.set_value(value)
        object.__setattr__(self, name, value)
    
    def save(self):            
        if self._pk:
            for attr in dir(self):
                if issubclass(getattr(self, attr, None), Field):
                    setattr(self._doc, attr, getattr(self, attr))
            self._database.put_doc(self._doc)
        else:
            self._pk = str(uuid.uuid4()) #Create a new id
            
            properties = {}
            for attr in dir(self):
                if issubclass(getattr(self, attr, None), Field):
                    properties[attr] = getattr(self, attr)
            
            self._doc = self._database.create_doc(json.dumps(properties), doc_id=self._pk)

    @classmethod
    def create(cls, *args, **kwargs):
        instance = cls()
        for k, v in kwargs.items():
            setattr(instance, k, v)            
        instance.save()
        return instance
        
        
    def delete(self, *args, **kwargs):
        raise NotImplementedError()

    objects = Query(
    
    
    
class Query(object):
    
    
