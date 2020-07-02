from django import forms
from django.db import models
from django import forms
from django.forms import ModelForm
from .models import BRDQuestions, Answers, CSRMappings, BRDLoadAttempts, BRDLoadInfo
from django.forms.models import inlineformset_factory
from crispy_forms.helper import FormHelper
from crispy_forms.layout import Submit, Layout, Field, Fieldset,Div, HTML, ButtonHolder
from .custom_layout_object import *

class VftForm(forms.Form):
    csr_vft_id = forms.CharField(label='CSR ID', max_length=10)
    prov_id = forms.CharField(label='Provider ID', max_length=10)
    name = forms.CharField(label='Name', max_length=255)

    def __init__(self,*args, **kwargs):
        super(VftForm, self).__init__(*args, **kwargs)


        self.fields['csr_vft_id'].widget.attrs.update({
            'placeholder': '',
        })

        self.fields['prov_id'].widget.attrs.update({
            'placeholder': '',
        })

        self.fields['name'].widget.attrs.update({
            'placeholder': '',
        })
FFDIDS = ['12345','456789','33421']
class GMCForm(forms.Form):
    cust_name = forms.CharField(label='CustomerName')
    # ffdid = forms.CharField(label='FFDId')

    def __init__(self, *args, **kwargs):
        super(GMCForm,self).__init__(*args, **kwargs)

        self.fields['cust_name'].widget.attrs.update({
            'id': 'cust_name_id'
        })
        '''
        self.fields['ffdid'].widget.attrs.update({
            'id': 'ffd_id'
        })
        '''

class PrePostForm(forms.Form):

    prechange_id = forms.CharField(
        label='Prechange ID',
        max_length=10)

    postchange_id = forms.CharField(
        label='Postchange ID',
        max_length=10)

    csr_ppc_id = forms.CharField(
        label='CSR ID',
        max_length=10)

    compare_logic = forms.ChoiceField(
        label='Compare Logic',
        choices=(('docId','Document ID'),('masterKey','master key')),
        required=False)

    pre_env = forms.ChoiceField(
        label='Pre Environment',
        choices=(('imstage','imstage'),('prod','prod')),
        required=False)

    post_env = forms.ChoiceField(
        label='Post Environment',
        choices=(('imstage','imstage'),('prod','prod')),
        required=False)

    unchanged_rows = forms.ChoiceField(
        label='Unchanged Doc Pairs',
        choices=(('hide','hide'),('show','show'),('exclude','exclude')),
        required=False)

    unchanged_cols = forms.ChoiceField(
        label='Unchanged Doc Props',
        choices=(('hide','hide'),('show','show'),('exclude','exclude')),
        required=False)

    masterkey_props = forms.MultipleChoiceField(
        label='Master Key Props',
        widget =forms.CheckboxSelectMultiple,
        choices=(('test', 'test'),('2','2')),
        required=False
        )

    spreadsheet_url = forms.CharField(
        label='Spreadsheet URL (optional)',
        required=False)

    def __init__(self,*args, **kwargs):
        self.helper = FormHelper()
        self.helper.form_class = 'blueForms'
        #self.helper.form_method = 'post'
        super(PrePostForm, self).__init__(*args, **kwargs)


        self.fields['prechange_id'].widget.attrs.update({
            'placeholder': '',
        })

        self.fields['postchange_id'].widget.attrs.update({
            'placeholder': '',
        })

        self.fields['csr_ppc_id'].widget.attrs.update({
            'placeholder': '',
        })

        self.fields['pre_env'].widget.attrs.update({
            'placeholder'   : '',
        })

        self.fields['post_env'].widget.attrs.update({
            'placeholder': '',
        })

        self.fields['spreadsheet_url'].widget.attrs.update({
            'placeholder': 'if this is left blank, a new google spreadsheet will be generated',
        })

class LoadForm(forms.ModelForm):
    
    class Meta:
        model = BRDLoadAttempts
        exclude = ['status']

    def __init__(self, *args, **kwargs):
        super(LoadForm, self).__init__(*args, **kwargs)
        self.helper = FormHelper()
        self.helper.form_tag = True
        self.helper.form_class = 'form-horizontal'
        self.helper.label_class = 'col-md-3 create-label'
        self.helper.field_class = 'col-md-9'
        self.helper.layout = Layout(
            Div(
                Field('survey_id'),
                Field('response_id'),
                Field('customer_id'),
                Field('pcase_num'),
                Field('username'),
                HTML("<br>"),
                ButtonHolder(Submit('submit', 'save')),
                )
            )

class QuestionForm(forms.ModelForm):

    class Meta:
        model = BRDQuestions
        exclude = ()

    def __init__(self, *args, **kwargs):
        super(QuestionForm, self).__init__(*args, **kwargs)
        self.helper = FormHelper()
        self.helper.form_tag = True
        self.helper.form_class = 'form-horizontal'
        self.helper.label_class = 'col-md-3 create-label'
        self.helper.field_class = 'col-md-9'
        self.helper.layout = Layout(
            Div(
                Field('survey_id'),
                Field('surveygizmo_id'),
                Field('question'),
                Fieldset('Add answers', Formset('answers')),
                Fieldset('Add mappings', Formset('mappings')),
                HTML("<br>"),
                ButtonHolder(Submit('submit', 'save')),
                )
            )

class MappingForm(forms.ModelForm):

    class Meta:
        model = CSRMappings
        exclude = ()

MappingFormSet = inlineformset_factory(BRDQuestions, CSRMappings, form=MappingForm, fields=['csr_tab', 'csr_setting', 'table_ref', 'col_name'], extra=1, can_delete=True)

class AnswersForm(forms.ModelForm):

    class Meta:
        model = Answers
        exclude = ()

AnswersFormSet = inlineformset_factory(BRDQuestions, Answers, form=AnswersForm, fields=['brd_answer', 'csr_value'], extra=1, can_delete=True)