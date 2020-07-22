from django.shortcuts import render,get_object_or_404, redirect, render_to_response
from django.http import HttpResponse,Http404,HttpResponseRedirect
from django.template import loader
from django.urls import reverse, reverse_lazy
from django.db import transaction
from django.db.models import Q
from django.views.generic import TemplateView, ListView
from django.views.generic.detail import DetailView
from django.views.generic.edit import CreateView, UpdateView, DeleteView
from celery import task
import json
import surveygizmo as sg

from .models import Tool,PrePostComp, GMCCustomer, GMCTemplate, BRDQuestions, Answers, CSRMappings, BRDLoadAttempts
from .forms import VftForm, PrePostForm, GMCForm, QuestionForm, AnswersForm, AnswersFormSet, MappingForm, MappingFormSet, LoadForm
from .prepost import compare
from .prepost import sheet_requests

from app.brdbuddy import mapping, getSurveyData

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

def confirm(request):
    return HttpResponse('got the new mapping!')

def gmc_index(request):
    tool = Tool.objects.filter(name='GMC Transparency 3000').values()[0]
    if request.method == 'POST':
        form = GMCForm(request.POST)
        if form.is_valid():
            strname = form.cleaned_data['cust_name']
            # strffdid = form.cleaned_data['ffdid']
            gmccust = GMCCustomer.objects.get(cust_name=strname)
            gmctemp = GMCTemplate.objects.select_related('gmccustomer').filter(gmccustomer_id__exact=gmccust.id)
            # TODO test mutiple template for 1 cust, how does that return
            context = {
                'form'      : form,
                'tool'      : tool,
                'gmctemp'   : gmctemp,
                'cust_name' : strname,
                'gmccust'   : gmccust
            }
            # return HttpResponseRedirect('/gmc/{cust_id}/{ffdid}'.format(cust_id=gmccust.cust_id, ffdid=strffdid))
            return render(request, 'app/gmc_index.html', context)
        else:
            return HttpResponseRedirect('/no_bueno/')
    else:
        form = GMCForm()
        context = {
            'form'    : form,
            'tool'    : tool,
        }
    return render(request, 'app/gmc_index.html', context)

def gmc_details(request, cust_id, ffd_id):
    gmc_cust = GMCCustomer.objects.filter(cust_id = cust_id).values()[0]
    gmc_template = GMCTemplate.objects.filter(ffd_id = ffd_id).values()[0]
    props = json.loads(gmc_template['wfd_props'].replace('\'','"'))

    context = {
        'gmc_cust'    : gmc_cust,
        'gmc_template': gmc_template,
        'props'       : props,
    }
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

class HomepageView(TemplateView):
    template_name = "test.html"

##########################################################################
#                           Collection views                             #
##########################################################################

def mappings(request):
    allquestions= BRDQuestions.objects.all()
    context= {'allquestions': allquestions}
    return render(request,'app/mappings.html',context)

class CollectionDetailView(DetailView):
    model = BRDQuestions
    template_name = 'collection_detail.html'

    def get_context_data(self, **kwargs):
        context = super(CollectionDetailView, self).get_context_data(**kwargs)
        return context

class CollectionCreate(CreateView):
    model = BRDQuestions
    template_name = 'collection_create.html'
    form_class = QuestionForm
    success_url = None

    def get_context_data(self, **kwargs):
        data = super(CollectionCreate, self).get_context_data(**kwargs)
        if self.request.POST:
            data['answers'] = AnswersFormSet(self.request.POST)
            data['mappings'] = MappingFormSet(self.request.POST)
        else:
            data['answers'] = AnswersFormSet()
            data['mappings'] = MappingFormSet()
        return data

    def form_valid(self, form):
        context = self.get_context_data()
        answers = context['answers']
        mappings = context['mappings']
        with transaction.atomic():
            form.instance.created_by = self.request.user
            self.object = form.save()
            if answers.is_valid() and mappings.is_valid():
                answers.instance = self.object
                mappings.instance = self.object
                answers.save()
                mappings.save()
        return super(CollectionCreate, self).form_valid(form)

    def get_success_url(self):
        return reverse_lazy('app:collection_detail', kwargs={'pk': self.object.pk})

class CollectionUpdate(UpdateView):
    model = BRDLoadAttempts
    form_class = LoadForm
    template_name = 'collection_create.html'

    def get_context_data(self, **kwargs):
        data = super(CollectionUpdate, self).get_context_data(**kwargs)
        if self.request.POST:
            data['answers'] = AnswersFormSet(self.request.POST, instance=self.object)
            data['mappings'] = MappingFormSet(self.request.POST, instance=self.object)
        else:
            data['answers'] = AnswersFormSet(instance=self.object)
            data['mappings'] = MappingFormSet(instance=self.object)
        return data

    def form_valid(self, form):
        context = self.get_context_data()
        answers = context['answers']
        mappings = context['mappings']
        with transaction.atomic():
            form.instance.created_by = self.request.user
            self.object = form.save()
            if answers.is_valid() and mappings.is_valid():
                answers.instance = self.object
                mappings.instance = self.object
                answers.save()
                mappings.save()
        return super(CollectionUpdate, self).form_valid(form)

    def get_success_url(self):
        return reverse_lazy('app:collection_detail', kwargs={'pk': self.object.pk})
    model = BRDQuestions
    template_name = 'collection_create.html'
    form_class = QuestionForm
    success_url = None

class CollectionDelete(DeleteView):
    model = BRDQuestions
    template_name = 'collection_delete.html'
    success_url = reverse_lazy('app:homepage')

class SearchResultsView (ListView):
    model = BRDQuestions
    template_name = 'search_results.html'

    def get_queryset(self): # new
        query = self.request.GET.get('q')
        object_list = BRDQuestions.objects.filter(
            Q(question__icontains=query) 
        )
        return object_list
##########################################################################
#                           Load views                             #
##########################################################################

def loads(request):
    allloads= BRDLoadAttempts.objects.all()
    context= {'allloads': allloads}
    return render(request,'app/loads.html',context)

class LoadCreate(CreateView):
    model = BRDLoadAttempts
    template_name = 'load_create.html'
    form_class = LoadForm
    success_url = None

    def get_context_data(self, **kwargs):
        data = super(LoadCreate, self).get_context_data(**kwargs)
        if self.request.POST:
            data['loads'] = LoadForm(self.request.POST)
        else:
            data['loads'] = LoadForm()
        return data

    def form_valid(self, form):
        context = self.get_context_data()
        loads = context['loads']
        with transaction.atomic():
            form.instance.created_by = self.request.user
            self.object = form.save()
            if loads.is_valid():
                loads.instance = self.object
                loads.save()
        return super(LoadCreate, self).form_valid(form)
        
    def get_success_url(self):
        return reverse_lazy('app:load_detail', kwargs={'pk': self.object.pk})

class LoadDetailView(DetailView):
    model = BRDLoadAttempts
    template_name = 'load_detail.html'

    def get_context_data(self, **kwargs):
        context = super(LoadDetailView, self).get_context_data(**kwargs)
        return context

def showAnswers(request, pk):
    # use the pk to get the load data and save it
    load_data = BRDLoadAttempts.objects.filter(pk=pk).values()[0]
    survey_id = load_data['survey_id']
    resp_id = load_data['response_id']

    # get the answers from sg
    ans_dict = getSurveyData(survey_id, resp_id)

    # get pcase number to create link to z drive
    # pcase_num = BRDLoadAttempts.objects.filter(response_id=resp_id).values('pcase_num')[0]['pcase_num']

    # add error handling for invalid response IDs
    if ans_dict == {}:
        return render(request, 'error_page.html')
    else:
        # do the mappings
        load_info = mapping(ans_dict, resp_id, survey_id)

        # display mappings table on the page with option to download the spreadsheet
        context = {'id' : resp_id, 'status' : load_info}
        return render(request, 'load_answers.html', context)




