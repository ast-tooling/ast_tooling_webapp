from django import forms

class VftForm(forms.Form):
    csr_id = forms.CharField(label='CSR ID', max_length=10)
    prov_id = forms.CharField(label='Provider ID', max_length=10)
    name = forms.CharField(label='Name', max_length=255)

    def __init__(self,*args, **kwargs):
        super(VftForm, self).__init__(*args, **kwargs)


        self.fields['csr_id'].widget.attrs.update({
            'placeholder': '',
        })

        self.fields['prov_id'].widget.attrs.update({
            'placeholder': '',
        })

        self.fields['name'].widget.attrs.update({
            'placeholder': '',
        })


class PrePostForm(forms.Form):
    prechange_id = forms.CharField(label='Prechange ID', max_length=10)
    postchange_id = forms.CharField(label='Postchange ID', max_length=10)
    csr_id = forms.CharField(label='CSR ID', max_length=10)
    spreadsheet_url = forms.CharField(label='Spreadsheet URL')

    def __init__(self,*args, **kwargs):
        super(PrePostForm, self).__init__(*args, **kwargs)


        self.fields['prechange_id'].widget.attrs.update({
            'placeholder': '',
        })

        self.fields['postchange_id'].widget.attrs.update({
            'placeholder': '',
        })

        self.fields['csr_id'].widget.attrs.update({
            'placeholder': '',
        })

        self.fields['spreadsheet_url'].widget.attrs.update({
            'placeholder': 'spreadsheets/d/1-SWPPRg2i2IsTgUA-4BvpEkMyE1TBZUvmEHZw1zpWo4/edit?usp=drive_web&ouid=116956695434029002425',
        })
