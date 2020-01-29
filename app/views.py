from django.shortcuts import render,get_object_or_404
from django.http import HttpResponse,Http404,HttpResponseRedirect
from django.template import loader
from django.urls import reverse
from celery import task
import json

from .models import Tool,PrePostComp, GMCCustomer
from .forms import VftForm,PrePostForm,GMCForm
from .prepost import compare
from .prepost import sheet_requests


import os

API_KEY = 'AIzaSyANaCUsVD4l-ZsIh9pST5o-tAfMNJINXB0'

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
                          form.cleaned_data['csr_vft_id'],
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
        elif form.is_valid() == False:
            context = {
                'tool': tool,
                'form': form
            }
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
    if request.method == 'POST':
        form = PrePostForm(request.POST)
        if form.is_valid():
            ppc_obj = PrePostComp(int(form.cleaned_data['prechange_id']),
                                  int(form.cleaned_data['postchange_id']),
                                  int(form.cleaned_data['csr_ppc_id']),
                                  ssUrl=form.cleaned_data['spreadsheet_url'])
            url = ppc_obj.spreadsheetUrl
            context = {
                'url'       : url+'&key='+API_KEY,
                'form'      : form,
                'tool'      : tool,
            }

            ## TESTING
            prePostDocProps = compare.QueryMongo(ppc_obj.fsidocprops, ppc_obj.coversheetDocIds, ppc_obj.arguments)
            mergedData = compare.MergeToDataFrame(prePostDocProps[0], prePostDocProps[1], ppc_obj.fsiDocumentInfo, ppc_obj.arguments, ppc_obj.service)
            compare.CreateCompareTab(mergedData[0], mergedData[1], mergedData[2], ppc_obj.arguments, ppc_obj.service)

            # END TESTING

            return render(request,'app/prepost.html',context)
    else:
        form = PrePostForm()
        context = {
            'form'      : form,
            'tool'      : tool,
        }
        return render(request,'app/prepost.html',context)

def cazar(request):
    return HttpResponse('find this')

def thanks(request):
    return render(request,'app/thanks.html')

def no_bueno(request):
    return HttpResponse('this here is the no bueno page, boo')

def gmc(request):
    if request.method == "POST":
        form = GMCForm(request.POST)
        if form.is_valid():
            strname = form.cleaned_data['cust_name']
            strffdid = form.cleaned_data['ffdid']
            gmccust = GMCCustomer.objects.get(name=strname)
            context = {
                'form'      : form,
                'tool'      : tool,
                'ffdid'     : strffdid,
                'cust_name' : strname
            }
        else:
            HttpResponseRedirect('/no_bueno/')
    else:
        form = GMCForm()
        context = {
            'form'    : form
        }
    return render(request, 'app/gmc_index.html', context)

def gmc_details(request, cust_id, ffd_id):

    return render(request, 'app/gmc_details.html', context)

def pull_current_uses_gmc(request):
    if request.is_ajax():
        q = request.GET.get('term', '').capitalize()
        search_qs = GMCCustomer.objects.filter(cust_name__startswith=q)
        results = []
        # print q
        for r in search_qs:
            results.append(r.cust_name)
        data = json.dumps(results)
    else:
        data = 'fail'
    mimetype = 'application/json'
    return HttpResponse(data, mimetype)
