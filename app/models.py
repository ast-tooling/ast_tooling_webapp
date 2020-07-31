from django.db import models
from .prepost import compare

SURVEY_CHOICES = (
    ('4623162', 'BRD'),
    ('5671111', 'New BRD'),
)

# Create your models here.
class Tool(models.Model):
    name = models.CharField(max_length=255)
    pub_date = models.DateTimeField('date published')
    owner = models.CharField(max_length=255)
    url_name = models.CharField(max_length=50)

    def __str__(self):
        return 'AST Tool {0} owned by {1}'.format(self.name,self.owner)

class PrePostComp(object):

    prechange_id = models.CharField(max_length=255)
    postchange_id = models.CharField(max_length=255)
    csr_id = models.CharField(max_length=25)
    spreadsheet_url = models.CharField(max_length=255)

    # contructor acceptis prechange, postchange, csr ids; these are required
    def __init__(self,prechangeId,postchangeId,csrId,**kwargs):
        self.prechangeId = prechangeId
        self.postchangeId = postchangeId
        self.csrId = csrId

        # spreadsheet url is optional param, if passed grab if out of the url
        # if not passed set as empty strings, request at later date
        # TODO: do we want a default spreadsheet id, one we keep adding tabs to?
        if 'ssUrl' in kwargs:
            self.spreadsheetUrl = kwargs['ssUrl']
            self.spreadsheetId = self.spreadsheetUrl.split('/')[-2]
        else:
            self.spreadsheetUrl = ''
            self.spreadsheetId = ''

        # Optional Parameters and Default Values:
        # -preEnv and postEnv
        #   desc: environment batch was ran in
        #   values: imdb, reportdb
        # -compareLogic
        #   desc: logic used to match docs across batches
        #   values: docId, masterKey
        # -noChangeCols and noChangeRows
        #   desc: controls whether columns (doc props) and rows (pre/post pairs) that saw no change are included
        #   values: show, hide, exclude
        # -masterKeyProps
        #   desc: doc props that will be used to match docs across batches if compareLogic = docId
        #   values: props listed and any user defined properties from free-form text box
        optionalParams = {  'preEnv'            : 'imdb',
                            'postEnv'           : 'imdb',
                            'compareLogic'      : 'docId',
                            'noChangeCols'      : 'hide', # hide and show work, except has not yet been implemented
                            'noChangeRows'      : 'hide', # hide and show work, except has not yet been implemented
                            'masterKeyProps'    : ['ACCOUNT_NUMBER', 'INVOICE_NUMBER', 'TOTAL_DUE', 'BT_ROUTE', 'FFDID'],
                            'ignoredProps'      : ['FILEDATE', 'SIG_BMP', 'FILE_PREFIX', 'XML_DATA', 'BT_PRINT_FILE_NAME', 'BILLING_ADDRESS_BEG1',
                                                   'BILLING_ADDRESS_BEG2','BILLING_ADDRESS_END1', 'BILLING_ADDRESS_END2', 'BILLING_ADDRESS_ZIP4',
                                                   'BILLING_ADDRESS_ZIP5', 'BILLING_ADDRESS_CITY', 'BILLING_ADDRESS_STATE', 'ROWIMG', 'JOB_ID']}

        for param in optionalParams.keys():
            if param in kwargs:
                setattr(self, param, kwargs[param])
            else:
                setattr(self, param, optionalParams[param])

        # add google api build obj as class attr
        self.service = compare.GoogleAPIAuthorization()

        # create arguments dict to pass to funcs
        self.arguments = {
            'custId'            : self.csrId,
            'preId'             : self.prechangeId,
            'preEnv'            : self.preEnv,
            'postId'            : self.postchangeId,
            'postEnv'           : self.postEnv,
            'spreadsheetURL'    : self.spreadsheetUrl,
            'spreadsheetId'     : self.spreadsheetId,
            'compareLogic'      : self.compareLogic,
            'noChangeCols'      : self.noChangeCols,
            'noChangeRows'      : self.noChangeRows,
            'masterKeyProps'    : self.masterKeyProps,
            'ignoredProps'      : self.ignoredProps,
            'service'           : self.service
        }

        # set stacks as class attributes
        # TODO how to handle circular argument of establishing connection and
        # determining stack, only way would be to connect to billing master
        # by default or not using the conns passed back by InitSQLClient
        # maybe there's a master bool in the InitSQLClient func?
        # maybe there's a sep master conn that is done in the constructor?
        self.stack = {}
        master_mysqlClient = compare.InitSQLClient(master=True)
        for k,v in master_mysqlClient.items():
            cursor = v.cursor()
            cursor.execute("SELECT r.Schema FROM billingmaster.fsimastercustomer mc "                     \
                           "INNER JOIN billingmaster.fsibilltrustdbstack s ON mc.DbStackId=s.DbStackId "  \
                           "LEFT JOIN billingmaster.fsibilltrustdatabase r ON s.ReadDbId=r.DatabaseId "   \
                           "WHERE mc.customerid = %s;" % self.csrId)
            schema = cursor.fetchone()
            self.stack[k] = schema[0]
            cursor.close()

        print("stack check is:")
        print(self.stack)

        # as a part of constructer, connect to databases
        self.mysqlClient = compare.InitSQLClient(dStack=self.stack)
        self.fsidocprops = compare.InitMongoClient()


        # self.sql_serve_conn = compare.InitSqlServerClient()

        # grab coversheet ids
        self.coversheetDocIds = compare.GetCoversheetDocIds(self.mysqlClient,
                                                            self.arguments)
        # grab fsi doc info
        self.fsiDocumentInfo = compare.GetFSIDocumnetInfo(self.mysqlClient,
                                                          self.arguments)

    # string representation of class, useful when debugging
    def __repr__(self):
        return '<PrePost Object using pre as %s and post as %s for csr id %s>'  \
        '' % (self.prechangeId, self.postchangeId, self.csrId)

class GMCCustomer(models.Model):
    cust_name = models.CharField(max_length=50)
    cust_id = models.IntegerField()
    cust_update = models.DateField(auto_now=True)

class GMCTemplate(models.Model):
    ffd_id = models.IntegerField()
    ffd_name = models.CharField(max_length=100)
    wfd_input_type = models.CharField(max_length=50)
    wfd_input_name = models.CharField(max_length=50)
    wfd_name = models.CharField(max_length=50)
    wfd_multiple_records = models.BooleanField()
    wfd_delimiter = models.CharField(max_length=2)
    wfd_text_qualifier = models.CharField(max_length=2)
    wfd_props = models.TextField()
    gmccustomer = models.ForeignKey('GMCCustomer', on_delete = models.CASCADE)

class BRDLoadAttempts(models.Model):
    survey_id = models.CharField(max_length=10, choices=SURVEY_CHOICES, default='BRD')
    response_id = models.IntegerField(default=12345 ,blank=True)
    customer_id = models.IntegerField(default=12345,blank=True)
    pcase_num = models.CharField(max_length=20,default='PLEASE FILL IN')
    username = models.CharField(max_length=20,blank=True)
    status = models.CharField(max_length=10,default='partial')

class BRDQuestions(models.Model):
    surveygizmo_id = models.CharField(max_length=10)
    survey_id = models.CharField(max_length=10, choices=SURVEY_CHOICES, default='BRD')
    question = models.CharField(max_length=300, blank=True)

class CSRMappings(models.Model):
    map_parents = models.ForeignKey(BRDQuestions,related_name="has_mappings", on_delete=models.CASCADE)
    csr_tab = models.CharField(max_length=20,blank=True)
    csr_setting = models.CharField(max_length=50,blank=True)
    table_ref = models.CharField(max_length=100,blank=True)
    col_name = models.CharField(max_length=100,blank=True)

class TableCustomerId(models.Model):
   table_ref = models.CharField(max_length=100,blank=True)
   cust_id_name = models.CharField(max_length=100,blank=True)

class Answers(models.Model):
    ans_parent = models.ForeignKey(BRDQuestions,related_name="has_answers",on_delete=models.CASCADE)
    brd_answer = models.CharField(max_length=100)
    csr_value = models.CharField(max_length=50)
