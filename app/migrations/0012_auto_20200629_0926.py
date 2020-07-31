# Generated by Django 2.1.13 on 2020-06-29 13:26

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('app', '0011_auto_20200617_1049'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='brdloadinfo',
            name='survey_answer',
        ),
        migrations.RemoveField(
            model_name='brdloadinfo',
            name='surveygizmo_id',
        ),
        migrations.AddField(
            model_name='brdloadattempts',
            name='survey_id',
            field=models.CharField(choices=[('BRD', '4623162'), ('New BRD', '1234567')], default='BRD', max_length=10),
        ),
    ]