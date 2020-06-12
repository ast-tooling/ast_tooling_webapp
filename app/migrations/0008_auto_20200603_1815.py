# Generated by Django 2.1.13 on 2020-06-03 22:15

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('app', '0007_auto_20200603_1658'),
    ]

    operations = [
        migrations.CreateModel(
            name='CSRMappings',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('csr_tab', models.CharField(max_length=20)),
                ('csr_setting', models.CharField(max_length=50)),
                ('table_ref', models.CharField(max_length=100)),
                ('col_name', models.CharField(max_length=100)),
                ('map_parents', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='has_mappings', to='app.BRDQuestions')),
            ],
        ),
        migrations.AlterField(
            model_name='answers',
            name='ans_parent',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='has_answers', to='app.BRDQuestions'),
        ),
    ]
