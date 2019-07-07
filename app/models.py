from django.db import models

# Create your models here.
class Tool(models.Model):
    name = models.CharField(max_length=255)
    pub_date = models.DateTimeField('date published')
    owner = models.CharField(max_length=255)
    url_name = models.CharField(max_length=50)

    def __str__(self):
        return 'AST Tool {0} owned by {1}'.format(self.name,self.owner)
