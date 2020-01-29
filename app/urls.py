from django.urls import path

from . import views

app_name = 'app'

urlpatterns = [
    path('', views.index, name='index'),
    path('vft', views.vft, name='vft'),
    path('prepost', views.prepost, name='prepost'),
    path('cazar', views.cazar, name='cazar'),
    path('thanks',views.thanks,name='thanks'),
    path('no_bueno',views.no_bueno,name='no_bueno'),
    path('gmc', views.gmc,name='gmc_index'),
    path('gmc/<int:cust_id>/<int:ffd_id>', views.gmc_details, name='gmc_details'),
    path('gmc/search/', views.pull_current_uses_gmc, name='gmc_search'),

]
