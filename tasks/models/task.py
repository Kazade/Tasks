from django.db import models
import uuid
import datetime

from .project import Project

POSSIBLE_IMPORTANCE_CHOICES = [
    ("VERY_LOW", "Very Low"),
    ("LOW", "Low"),
    ("NORMAL", "Normal"),
    ("HIGH", "High"),    
    ("CRITICAL", "Critical")    
]

class ImportantManager(models.Manager):
    def get_query_set(self):
        return super(ImportantManager, self).get_query_set().filter(importance__in=["HIGH", "CRITICAL"])

class CompletedManager(models.Manager):
    def get_query_set(self):
        return super(CompletedManager, self).get_query_set().filter(complete=True)

class UncompletedManager(models.Manager):
    def get_query_set(self):
        return super(UncompletedManager, self).get_query_set().filter(complete=False)

class DueTodayManager(models.Manager):
    def get_query_set(self):
        return super(DueTodayManager, self).get_query_set().filter(due_date=datetime.datetime.today())
        
class OverdueManager(models.Manager):
    def get_query_set(self):
        return super(OverdueManager, self).get_query_set().filter(due_date__lt=datetime.datetime.today())
                    
def make_uuid():
    return str(uuid.uuid4()).replace('-','')

def default_project():
    return Project.objects.get_or_create(name="Default")[0]
    
class Task(models.Model):
    project = models.ForeignKey(Project, default=default_project)

    uuid = models.CharField(max_length=255, unique=True, default=make_uuid)
    complete = models.BooleanField(default=False)
    importance = models.CharField(max_length=64, choices=POSSIBLE_IMPORTANCE_CHOICES, default="NORMAL")
    summary = models.CharField(max_length=512)    
    due_date = models.DateField(null=True)
    
    class Meta:
        db_table = "tasks_task"
        app_label = "tasks"
        
    objects = models.Manager()
    objects_important = ImportantManager()
    objects_completed = CompletedManager()
    objects_uncompleted = UncompletedManager()
    objects_due_today = DueTodayManager()
    objects_overdue = OverdueManager()
    
    def __unicode__(self):
        return self.summary

