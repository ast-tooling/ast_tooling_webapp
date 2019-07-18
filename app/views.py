from django.shortcuts import render,get_object_or_404
from django.http import HttpResponse,Http404,HttpResponseRedirect
from django.template import loader
from django.urls import reverse

from .models import Tool
from .forms import VftForm,PrePostForm

import os

# Create your views here.
def index(request):
    l_tools = Tool.objects.order_by('-pub_date')[:5]
    template = loader.get_template('app/index.html')
    context = {
        'l_tools': l_tools,
    }
    return render(request,'app/index.html',context)

def vft(request):
    base = os.getcwd()
    fname = 'vft_optin_single.csv'
    tool = Tool.objects.filter(name='vft workflow').values()[0]
    if request.method == 'POST':
        form = VftForm(request.POST)
        if form.is_valid():
            l_vft_cust = [form.cleaned_data['prov_id'],
                          form.cleaned_data['csr_id'],
                          form.cleaned_data['name']
                          ]
            with open(base+fname,'w') as f:
                f.write('provder id,csr id,name\n')
                f.write(','.join(l_vft_cust))
                f.write('\n')
            f.close()
            context = {
                'tool': tool,
                'form': form,
                'prov_id': l_vft_cust[0],
                'csr_id': l_vft_cust[1],
                'name': l_vft_cust[2],
                'vft_list': l_vft_cust,
            }
            return render(request,'app/thanks.html',context)
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
    tool = Tool.objects.filter(name='prepost compare').values()[0]
    form = PrePostForm()
    context = {
        'form': form,
        'tool': tool,
    }
    return render(request,'app/prepost.html',context)

def cazar(request):
    return HttpResponse('find this')

def thanks(request):
    return render(request,'app/thanks.html')

def no_bueno(request):
    return HttpResponse('this here is the no bueno page, boo')
