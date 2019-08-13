from django.db import models

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

        # create arguments dict to pass to funcs
        self.arguments = {
            "custId"        : self.csrId,
            "preId"         : self.prechangeId,
            "postId"        : self.postchangeId,
            "spreadsheetURL": self.spreadsheetUrl,
            "spreadSheetId" : self.spreadsheetId,
        }
        # as a part of constructer, connect to databases
        self.mysqlClient = compare.InitSQLClient()
        self.fsidocprops = compare.InitMongoClient()
        # self.sql_serve_conn = compare.InitSqlServerClient()

        # add google api build obj as class attr
        self.service = compare.GoogleAPIAuthorization()

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
