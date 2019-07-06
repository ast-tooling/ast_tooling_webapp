from django.shortcuts import render,get_object_or_404
from django.http import HttpResponse,Http404
from django.template import loader

from .models import Tool

# Create your views here.
def index(request):
    l_tools = Tool.objects.order_by('-pub_date')[:5]
    template = loader.get_template('app/index.html')
    context = {
        'l_tools': l_tools,
    }
    return render(request,'app/index.html',context)

def vft(request,tool_id):

    tool = get_object_or_404(Tool,pk=tool_id)
    return render(request,'app/vft',{'tool':tool})

def prepost(request):
    return HttpResponse('pre post time snitches')

def cazar(request):
    return HttpResponse('find this')
