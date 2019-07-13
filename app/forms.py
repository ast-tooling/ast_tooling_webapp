from django import forms

class VftForm(forms.Form):
    csr_id = forms.CharField(label='CSR ID', max_length=10)
    prov_id = forms.CharField(label='Provider ID', max_length=10)
    name = forms.CharField(label='Name', max_length=255)

class PrePostForm(forms.Form):
    prechange_id = forms.CharField(label='Prechange ID', max_length=10)
    postchange_id = forms.CharField(label='Postchange ID', max_length=10)
    csr_id = forms.CharField(label='CSR ID', max_length=10)
    spreadsheet_url = forms.CharField(label='Spreadsheet URL')
    
