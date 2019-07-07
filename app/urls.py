from django.urls import path

from . import views

app_name = 'app'

urlpatterns = [
    path('', views.index, name='index'),
    path('vft', views.vft, name='vft'),
    path('prepost', views.prepost, name='prepost'),
    path('cazar', views.cazar, name='cazar'),
]
