from django.urls import path

from . import views

urlpatterns = [
    path('',views.index,name='index'),
    path('vft',views.vft,name='vft'),
    path('prepost_compare',views.prepost,name='prepost'),
    path('cazar',views.cazar,name='cazar'),
]
