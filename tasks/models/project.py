from django.db import models

class Project(models.Model):
    name = models.CharField(max_length=128, unique=True)
    
    class Meta:
        db_table = "tasks_project"
        app_label = "tasks"
