from django.db import models

class Tag(models.Model):
    value = models.CharField(max_length=32)
    colour = models.CharField(max_length=7) #e.g. #003366
    
    class Meta:
        db_table = "tasks_tag"
        app_label = "tasks"
