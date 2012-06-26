import sys    
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
import u1db
import json
import uuid

import time
import datetime

from gi.repository import GObject

class Importance:
    VERY_LOW = 0
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4

class Field(object):
    def __init__(self, name, data_type, null=False, default=None):
        self.name = name
        self.data_type = data_type
        self.nullable = null
        self.default = default
        
    def __unicode__(self):
        return u"<%s, %s>" % (self.name, self.data_type.__name__)
    
    def __repr__(self):
        return "<%s, %s>" % (self.name, self.data_type.__name__)

class Manager(object):
    def exists(self):
        try:
            self.get_queryset()[0]
            return True
        except IndexError:
            return False
        
    def count(self):
        return len(self.get_queryset())
        
    def all(self):
        return list(self.get_queryset())
        
    def get_queryset(self):
        raise NotImplementedError()
        
class TaskObjectsManager(Manager):
    def get_queryset(self):
        return [ self._store._json_to_instance(Task, x) for x in self._store._connection.get_from_index("idx_complete", "*") ]


class Index(object):
    def __init__(self, name, fields=[]):
        self.name = name
        self.fields = fields
        
    def expression(self):
        parts = []
        for field_name in self.fields:
            #Get the field with this name, throw an indexerror if it doesn't exist
            field = [ x for x in self._model_class.fields() if x.name == field_name ][0]
            
            if field.data_type == bool:
                parts.append("bool(%s)" % field.name)
            else: 
                parts.append(field.name)
                
        return parts
    
class Model(object):
    objects = Manager()

    def __init__(self, *args, **kwargs):
        self.pk = None

        for field in self.fields():
            if field.name not in kwargs:
                setattr(self, field.name, field.default)

        for k, v in kwargs.items():
            setattr(self, k, v)

    def validate(self):
        for field in self.fields():
            if field.nullable:
                assert getattr(self, field.name) is None or isinstance(getattr(self, field.name), field.data_type)
            else:
                assert isinstance(getattr(self, field.name), field.data_type)
            
    @classmethod
    def fields(cls):
        if cls.__name__ == "Model":
            return Model.Meta.fields
            
        return getattr(getattr(cls, "Meta", None), "fields", []) + Model.fields()

    @classmethod
    def indexes(cls):
        if cls.__name__ == "Model":
            return Model.Meta.indexes
            
        return getattr(getattr(cls, "Meta", None), "indexes", []) + Model.indexes()
        
    class Meta:
        fields = [
            Field("pk", str),
            Field("modified", datetime.datetime, null=True),
            Field("created", datetime.datetime, null=True)
        ]   
        
        indexes = [
            Index("idx_pk", ["pk"])
        ]
        
class Task(Model):        
    class Meta:
        fields = [
            Field("complete", bool, default=False),
            Field("archived", bool, default=False),
            Field("importance", int, default=Importance.NORMAL),
            Field("summary", basestring, default=""),
            Field("due_date", datetime.date, null=True),
            Field("due_time", time.time, null=True),
            Field("details", basestring, default="")            
        ]
        
        indexes = [
            Index("idx_complete", [ "complete" ]) 
        ]
        
    objects = TaskObjectsManager()
         
class Store(object):
    """
        All database operations should go through this.
        Inherits GObject so we have access to signal magic.
    """
    
    __gtypename__ = "Store"
    
    __gsignals__ = {
        "post-save" : (GObject.SIGNAL_RUN_FIRST, None, (object, )),
        "pre-save" : (GObject.SIGNAL_RUN_FIRST, None, (object, ))
    }
    
    def __init__(self, *args, **kwargs):
        #GObject.__init__(self, *args, **kwargs)
        
        self._connection = u1db.open(":memory:", create=True)
                      
    def register_model(self, model_class):
        existing_indexes = dict(self._connection.list_indexes())
        
        for index in model_class.indexes():
            index._model_class = model_class
        
            if index.name in existing_indexes:
                if index.expression() == existing_indexes[index.name]:
                    continue
                else:
                    #The index exists, but it's wrong in'it so kill the bugger
                    self._connection.delete_index(index.name)

            #Create the index we need
            self._connection.create_index(index.name, *index.expression())
                                   
        #Set the store and the model class on the managers
        for attr_name in dir(model_class):
            attr = getattr(model_class, attr_name)
            if isinstance(attr, Manager):
                attr._store = self 
                attr._model_class = model_class                
                                              
    def get(self, task_id):
        return self._json_to_instance(self._connection.get_doc(task_id))
                
    def _json_to_instance(self, cls, data):
        dic = json.loads(data)        
        instance = cls(**dic)        
        return instance

    def _instance_to_json(self, instance):            
        instance.validate() #Make sure the data is valid
    
        result = {}
        for field in instance.fields():            
            attr = getattr(instance, field.name)
            if isinstance(attr, datetime.datetime):
                result[field.name] = attr.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(attr, datetime.date):
                result[field.name] = attr.strftime("%Y-%m-%d")
            elif isinstance(attr, unicode):
                result[field.name] = attr.encode("utf-8")
            else:                
                result[field.name] = attr

        return json.dumps(result)

    def save(self, task):
#        self.emit("pre-save", task)

        task.modified = datetime.datetime.now() 

        if task.pk is None:
            task.pk = str(uuid.uuid4()) 
            task.created = datetime.datetime.now()
            json = self._instance_to_json(task)
            self._connection.create_doc(json)
        else:
            json = self._instance_to_json(task)
            self._connection.put_doc(json)

#        self.emit("post-save", task)

if __name__ == "__main__":
    store = Store()
    store.register_model(Task)
    
    assert hasattr(Task, "objects")
    
    t = Task()
    assert t.pk is None
    store.save(t)
    assert t.pk is not None
    
    assert Task.objects.count() == 1
    
    


