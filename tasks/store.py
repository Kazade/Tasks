"""
    The beginnings of a sort of Django-esque ORM (although there's no relational stuff yet so I guess it's
    more O/M :p.
    
    You define your model with fields, indexes and managers. Each manager represents a query and has the following
    methods:
     * exists() -> True if there are any results
     * count() -> Returns the number of results
     * all() -> Returns a list of results
     
    Each model has the following built-in fields:
    
    * pk -> This is the doc_id for U1DB, and is generated using uuid.uuid4() on creation
    * rev -> This is the revision according to U1DB (you shouldn't change this!)
    * modified -> The last time the instance was saved
    * created -> When the instance was created
    
    It also has a build-in index for "pk".
    
    This does just enough for me to swap out Django's ORM with U1DB, but I think it's totally feasable to
    mimic even more of django's ORM, including related fields, and filter(). And to even automatically 
    generate indexes when they are used for the first time. But, I can't do that now, I've got a
    week to finish my U1DB entry!
    
    Usage:
    
    class MyModel(Model):
        class Meta:
            fields = [
                Field("myfield", int, default=0)
            ]
            
            indexes = [
                Index("myindex", "myfield")
            ]

    store = Store()
    store.register_model(MyModel) #important!            
    
    MyModel.objects.exists() -> False
    
    instance = MyModel()
    store.save(instance)
    
    MyModel.objects.count() -> 1
    MyModel.objects.get(pk=instance.pk) -> Returns a freshly loaded instance
    MyModel.objects.exists() -> True    
    MyModel.objects.all() -> [ <MyModel> ]
"""

import sys    
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
import u1db
import json
import uuid

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
        return self
        
    def get_queryset(self):
        raise NotImplementedError()
        
    def __iter__(self):
        return iter(self.get_queryset())

    def __len__(self):
        return len(self.get_queryset())
        
    def __getitem__(self, key):
        return self.get_queryset().__getitem__(key)
        
class ObjectsManager(Manager):
    def get_queryset(self):
        return [ self._store._document_to_instance(self._model_class, x) for x in self._store._connection.get_from_index("idx_pk", "*") ]

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
    
class ValidationError(Exception):
    pass
        
class Model(object):
    objects = ObjectsManager()

    def __init__(self, *args, **kwargs):
        self.pk = None
        self._store = None

        for field in self.fields():
            if field.name not in kwargs:
                #Ensure that we always work with unicode
                if field.data_type == unicode and isinstance(field.default, str):
                    field.default = field.default.decode("utf-8")
                    
                setattr(self, field.name, field.default)

        for k, v in kwargs.items():
            setattr(self, k, v)

    def has_store(self):
        return self._store is not None

    def save(self):
        assert self.has_store()
        
        self._store.save(self)

    def reload(self):
        assert self.has_store()
        assert self.pk
        
        return self._store.get(self.__class__, self.pk)

    def validate(self):
        for field in self.fields():
            try:
                attr = getattr(self, field.name)
                if field.nullable:                
                    if (attr is not None) and (not isinstance(attr, field.data_type)):
                        raise ValidationError("Expected %s or None, but got %s" % (field.data_type, attr.__class__))
                else:
                    if not isinstance(attr, field.data_type):
                        raise ValidationError("Expected %s but got %s" % (
                            field.data_type, attr.__class__)
                        )
            except TypeError:
                print attr.__class__, " != ", field.data_type
                raise
            
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
       
    def __eq__(self, other):
        return self.pk == other.pk
               
    class Meta:
        fields = [
            Field("pk", str),
            Field("rev", unicode, null=True, default=None),
            Field("modified", datetime.datetime, null=True),
            Field("created", datetime.datetime, null=True)
        ]   
        
        indexes = [
            Index("idx_pk", ["pk"])
        ]

class Store(GObject.GObject):
    """
        All database operations should go through this.
        Inherits GObject so we have access to signal magic.
    """
    
    __gtypename__ = "Store"
    
    __gsignals__ = {
        "post-save" : (GObject.SIGNAL_RUN_FIRST, None, (object, )),
        "pre-save" : (GObject.SIGNAL_RUN_FIRST, None, (object, ))
    }
    
    def __init__(self, location=":memory:", *args, **kwargs):
        super(Store, self).__init__()
                
        self._connection = u1db.open(location, create=True)
                             
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
           
    def create(self, cls, **kwargs):
        instance = cls(**kwargs)
        instance._store = self
        self.save(instance)
        return instance
    
    def get(self, cls, doc_id):
        instance = self._document_to_instance(cls, self._connection.get_doc(doc_id))        
        return instance
                
    def _document_to_instance(self, cls, data):
        assert data
        
        final_data = {}
                        
        for field in cls.fields():
            value = data.content[field.name]
            if value is not None:                
                if field.data_type == unicode:
                    final_data[field.name] = data.content[field.name].decode("utf-8")
                elif field.data_type == datetime.datetime:
                    final_data[field.name] = datetime.datetime.strptime(
                        data.content[field.name], "%Y-%m-%d %H:%M:%S"
                    )
                elif field.data_type == datetime.date:
                    final_data[field.name] = datetime.datetime.strptime(
                        data.content[field.name], "%Y-%m-%d"
                    ).date()
                else:
                    final_data[field.name] = field.data_type(value)
            else:
                if field.nullable:
                    final_data[field.name] = None
                else:
                    raise ValueError("%s was returned as None from the datastore, but is not nullable" % field.name)

        final_data["rev"] = data.rev
        
        instance = cls(**final_data) #Construct the instance using the U1DB Document.content 
        instance._store = self
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

    def save(self, instance):
        if instance.has_store():
            if instance._store != self:
                raise ValueError("Instance is registered to another store")
                
        self.emit("pre-save", instance)

        instance.modified = datetime.datetime.now() 
        instance._store = self
        
        if instance.pk is None:
            instance.pk = str(uuid.uuid4()) 
            instance.created = datetime.datetime.now()
            json = self._instance_to_json(instance)
            doc = self._connection.create_doc(json, doc_id=instance.pk)
            instance.rev = doc.rev            
        else:
            json = self._instance_to_json(instance)
            doc = u1db.Document(instance.pk, instance.rev, json)
            instance.rev = self._connection.put_doc(doc)

        self.emit("post-save", instance)

##========================= Task manager specific (not related to the ORM stuff) ===

class TaskCompleteManager(Manager):
    def get_queryset(self):
        return [ 
            self._store._document_to_instance(self._model_class, x) 
            for x in self._store._connection.get_from_index("idx_complete", "1") 
        ]
   
class TaskUncompleteManager(Manager):
    def get_queryset(self):
        return [ 
            self._store._document_to_instance(self._model_class, x) 
            for x in self._store._connection.get_from_index("idx_complete", "0") 
        ]

class TaskOverdueManager(Manager):
    def get_queryset(self):
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        
        all_overdue_or_due = [ 
            self._store._document_to_instance(self._model_class, x) 
            for x in self._store._connection.get_range_from_index(
                "idx_due_date", current_date, None
            ) 
        ]
        print all_overdue_or_due
        return all_overdue_or_due
            
        #Return all that have no due_time or the due_time has already passed     
        return [ x for x in all_overdue_or_due 
            if x.due_time is None or x.due_time < datetime.datetime.now().strftime("%H:%M:%S") 
        ]
            
class Task(Model):        
    class Meta:
        fields = [
            Field("complete", bool, default=False),
            Field("archived", bool, default=False),
            Field("importance", int, default=Importance.NORMAL),
            Field("summary", unicode, default=""),
            Field("due_date", datetime.date, null=True),
            Field("due_time", datetime.datetime, null=True),
            Field("details", unicode, default="")            
        ]
        
        indexes = [
            Index("idx_complete", [ "complete" ]),
            Index("idx_due_date", [ "due_date" ])
        ]
        
    def __repr__(self):
        return "<Task: '" + self.summary + "'>"
        
    objects_completed = TaskCompleteManager()
    objects_uncompleted = TaskUncompleteManager()        
    objects_overdue = TaskOverdueManager()
         
if __name__ == "__main__":
    store = Store()
    store.register_model(Task)
    
    assert hasattr(Task, "objects")
    
    t = Task()
    assert t.pk is None
    store.save(t)
    assert t.pk is not None
    
    assert Task.objects.count() == 1
    assert Task.objects.all()[0] == t
    
    t = store.get(Task, t.pk)
    print t.pk
       
    assert Task.objects_complete.count() == 0
    
    t.complete = True
    t.summary = u"This is a test task"
    store.save(t)
    
    print Task.objects_complete.all()
    assert Task.objects_complete.count() == 1
    

