from django.shortcuts import render,get_object_or_404
from django.http import HttpResponse,Http404,HttpResponseRedirect
from django.template import loader
from django.urls import reverse

from .models import Tool
from .forms import VftForm

# Create your views here.
def index(request):
    l_tools = Tool.objects.order_by('-pub_date')[:5]
    template = loader.get_template('app/index.html')
    context = {
        'l_tools': l_tools,
    }
    return render(request,'app/index.html',context)

def vft(request):
    tool = Tool.objects.filter(name='vft workflow').values()[0]
    if request.method == 'POST':
        form = VftForm(request.POST)
        if form.is_valid():
            context = {
                'tool': tool,
                'form': form
            }
            HttpResponseRedirect('/thanks/')
        elif form.is_valid == False:
            HttpResponseRedirect('/no_bueno/')
    else:
        form = VftForm()
        context = {
            'tool': tool,
            'form': form
        }
    return render(request,'app/vft.html',context)

def prepost(request):
    return HttpResponse('pre post time snitches')

def cazar(request):
    return HttpResponse('find this')

def thanks(request):
    return HttpResponse('you landed on the thanks page')

def no_bueno(request):
    return HttpResponse('this here is the no bueno page, boo')
