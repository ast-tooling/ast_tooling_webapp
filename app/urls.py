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
    path('gmc', views.gmc_index,name='gmc_index'),
    path('confirm', views.confirm, name='confirm'),
    path('gmc/<int:cust_id>/<int:ffd_id>', views.gmc_details, name='gmc_details'),
    path('gmc/search/', views.pull_current_uses_gmc, name='gmc_index'),
# BRD Buddy Homepage
    path('brd_buddy', views.HomepageView.as_view(), name='homepage'),

# BRD Buddy - Manage the BRD to CSR Mappings that are used 
    path('brd_buddy/mappings', views.mappings,name='mappings'),
# View details about a mapping
	path('collection/<int:pk>/', views.CollectionDetailView.as_view(), name='collection_detail'),
# Create a new mapping
    path('collection/create/', views.CollectionCreate.as_view(), name='collection_create'),
# Update an existing mapping
    path('collection/update/<int:pk>/', views.CollectionUpdate.as_view(), name='collection_update'),
# Delete an existing mapping
    path('collection/delete/<int:pk>/', views.CollectionDelete.as_view(), name='collection_delete'),

# BRD Buddy - Manage the Survey Loads
    path('brd_buddy/loads', views.loads,name='loads'),
# Load a survey
    path('load/create/', views.LoadCreate.as_view(), name='load_create'),
# View details about a load
	path('load/<int:pk>/', views.LoadDetailView.as_view(), name='load_detail'),
# View answers that will be mapped
	path('answers/<int:resp_id>/',views.showAnswers, name='load_answers'),
]
