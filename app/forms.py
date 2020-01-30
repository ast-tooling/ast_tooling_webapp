from django import forms
from crispy_forms.helper import FormHelper
from crispy_forms.layout import Submit

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
