#####################################################################################
## jmapParseXML.py
## Parse a directory of publisher XML files to grab the citation information and 
## locations needed for JournalMap. Creates a set of CSV import files for JournalMap
## Includes a log file of results.
##
## This importer works with the following XML formats:
## "-//NLM//DTD Journal Publishing DTD v2.3 20070202//EN" "journalpublishing.dtd"
## "-//NLM/DTD Journal Archiving and interchange DTD v2.2 20060430//EN
## "-//NLM//DTD Journal Publishing DTD v3.0 20080202//EN"
##
## External Dependencies
##     BeautifulSoup4
##     Pandas
#####################################################################################

import os, sys, re, io
import fnmatch
import unicodecsv, csv
import requests, json, time

from decimal import Decimal, setcontext, ExtendedContext
from datetime import datetime
from bs4 import BeautifulSoup
from bs4 import UnicodeDammit

import pandas as pd

geoparser = "spacy-lg" # Which NLP parser to use: "spacy-lg", "spacy-trf", "mordecai", "stanza", "locatext", "nltk"

startDir = 'C:/Users/bgodfrey/Documents/GitHub/Placenames/XML/PLOSOne'
outDir = 'C:/Users/bgodfrey/Documents/GitHub/Placenames/2023'
articlesFile = outDir + '/articles_' + geoparser + '_2023.csv'
locationsFile = outDir + '/locations_' + geoparser + '_2023.csv'
logFile = outDir + '/jmap_parse_' + geoparser + '_2023.log'

#articlesFile = outDir + '/articles_mordecai_2023.csv'
#locationsFile = outDir + '/locations_mordecai_2023.csv'
#logFile = outDir + '/jmap_parse_mordecai_2023.log'

collectionKeyword = "" # Add special keyword for organizing into a collection
allArticles = True  # Include all articles (True) or only articles that have parsed locations in the output (False)?


class UnicodeWriter(object):
    """
    Like UnicodeDictWriter, but takes lists rather than dictionaries.
    
    Usage example:
    
    fp = open('my-file.csv', 'wb')
    writer = UnicodeWriter(fp)
    writer.writerows([
        [u'Bob', 22, 7],
        [u'Sue', 28, 6],
        [u'Ben', 31, 8],
        # \xc3\x80 is LATIN CAPITAL LETTER A WITH MACRON
        ['\xc4\x80dam'.decode('utf8'), 11, 4],
    ])
    fp.close()
    """
    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = io.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoding = encoding
    
    def writerow(self, row):
        # Modified from original: now using unicode(s) to deal with e.g. ints
        self.writer.writerow([unicode(s).encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = data.encode(self.encoding)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)
    
    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


class ParseLog(object):
    def __init__(self):
        self.messages = []
        self.countArticles = 0
        self.countGeoTagged = 0
        self.locations = 0
        self.countErrors = 0
        self.countNoAuthors = 0
        self.countArticlesWritten = 0
    
    def add_msg(self, msg):
        self.messages.append(msg)

class Article(object):
    
    def __init__(self, doi, title, year):
        self.doi = doi
        self.title = title
        self.year = year
        
        # Set the remaining attributes to blank
        self.no_keywords = False
        self.no_abstract = False
        self.url = ''
        self.publisher_abbreviation = ''
        self.publisher_name = ''
        self.citation = ''
        self.first_author = ''
        self.volume_issue_pages = ''
        self.volume = ''
        self.issue = ''
        self.start_page = ''
        self.end_page = ''
        self.abstract = ''
        self.authors = []
        self.keywords = []
        self.body = ''
        
    def add_author(self, author):
        if not author in self.authors:
            self.authors.append(author)
    
    def add_keyword(self, keyword):
        if not keyword in self.keywords:
            self.keywords.append(keyword)

    def format_authors(self):
        author_string = ''
        for author in self.authors:
            author_string = author_string + ', ' + author
        return author_string[2:]

    def format_keywords(self):
        kw_string = ''
        for kw in self.keywords:
            kw_string = kw_string + ', ' + kw
        return kw_string[2:]

    def format_volisspg(self):
        #Must have a volume     
        # check for issue
        if self.issue: istring = "(" + str(self.issue) + ")"
        else: istring = ''
        # Check for pages
        if self.end_page: pgstring = "-"+str(self.end_page)
        else: pgstring = ""
        if self.start_page: pgstring = ":" + str(self.start_page) + pgstring
        vip = str(self.volume) + istring + pgstring
        return vip

    def build_citation(self):
        citation = self.format_authors() + ". " + str(self.year) +". " + self.title + ". " + self.publisher_name + ". "
        self.citation = citation
        return citation



def writeLocationsHeader(headerString, f):
    with open(f, "wb") as locationsCSV:
        locationWriter = unicodecsv.writer(locationsCSV)
        locationWriter.writerows(headerString)

#start logging
log = ParseLog()
lf = open(logFile,"w")
lf.write("Starting processing of "+startDir+" on "+datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')+"\n")

# delete the locations file if it exists
try:
    os.remove(locationsFile)
except OSError:
    pass

# open/create the articles file and set header rows
with open(articlesFile, 'wb') as articlesCSV:
        articleWriter = unicodecsv.writer(articlesCSV)
        articlelines = [['doi','publisher_name','publisher_abbreviation','citation','title','publish_year','first_author','authors_list','volume_issue_pages','volume','issue','start_page','end_page','keywords_list','no_keywords_list','abstract','no_abstract','url']]
        articleWriter.writerows(articlelines)    
    
    
        # Traverse the start directory structure
        for root, dirs, files in os.walk(startDir):
            for name in fnmatch.filter(files, '*.xml'):
                ###############################
                ## Grab the article XML file ##
                ############################### 
                xmlFile = os.path.join(root,name)
                print("Processing " + xmlFile)
                log.add_msg("Processing " + xmlFile)
                log.countArticles += 1
                
                ###############################
                ## Grab the article metadata ##
                ###############################        
    
                # Read the XML
                f = open(xmlFile, encoding='utf-8')
                #xmlStr = UnicodeDammit(f.read())
                tree = BeautifulSoup(f.read(),"lxml")
                #rawtext = UnicodeDammit.detwingle(f.read())
                #tree = BeautifulSoup(rawtext.decode("utf-8",'ignore'),'xml')
                f.close()
                #print(tree.prettify())

                #############################################
                ## Process NLM or JATS-formatted XML files ##
                #############################################                
                if tree.find('front'):  # NLM or JATS formatted XML
                    fmt = "NLM"
                    # Read the first three elements and create the article object
                    try: doi = tree.front.find('article-id', {'pub-id-type':'doi'}).text.strip() 
                    except: doi=''
                    try: title = tree.front.find('article-title').text.strip()
                    except: title=''
                    try: year = tree.front.find('pub-date').year.text.strip()
                    except: year = ''
        
                    article = Article(doi, title, year)
        
                    # Add the other single item attributes
                    try: article.publisher_name = tree.front.find('journal-title').text
                    except: article.publisher_name = ''            
                    
                    try: article.volume = tree.front.find('volume').text
                    except: article.volume = ''
                    
                    try: article.issue = tree.front.find('issue').text
                    except: article.issue = ''
                    
                    try: article.start_page = tree.front.find('fpage').text
                    except:
                        try: article.start_page = tree.front.find('elocation-id').text
                        except: article.start_page = ''
                        
                    try: article.end_page = tree.front.find('lpage').text
                    except: article.end_page = ''
                    
                    try:
                        for a in tree.find_all('abstract'):
                            if not a.get('abstract-type')=='precis':
                                article.abstract = a.text
                            else:
                                article.abstract = ''
                        if not article.abstract: article.no_abstract = True
                    except: 
                        article.abstract = ''
                        article.no_abstract = True
                    
                    
                    ###############################
                    ## Build authors list        ##
                    ############################### 
                    try:
                        for author in tree.find_all('contrib'):
                            article.add_author(author.find('surname').text + ", " + author.find('given-names').text)
                        if len(article.authors)==0: raise
                    except:
                        print ("No authors found for " + xmlFile + ". Skipping this article.")
                        log.add_msg("No authors found for " + xmlFile + ". Skipping this article.")
                        log.countNoAuthors += 1
                        continue
                    
                    ###############################
                    ## Build keywords list       ##
                    ############################### 
                    if tree.find('kwd'):
                        for kw in tree.find_all('kwd'):
                            article.add_keyword(kw.text)
                    if collectionKeyword: article.add_keyword(collectionKeyword)    
                    if not article.keywords: no_keywords = True

                    ###############################
                    ## Retrieve article body text #
                    ###############################
                    # this is a bit kludgy. The body text is located within <sec> tags, multiple <sec> 
                    # tags need to be assembled to make up the body. However, each tag has lots of other 
                    # tags within it, so we need to strip out all that garbage. Probably a more elegant/robust way to do this.
                    body = ''
                    for sec in tree.find_all('sec'):
                        for s in sec.stripped_strings:
                            s = s + ' '
                            body += s
                    article.body = body

                ########################################
                ## Process Elsevier XML files         ##
                ########################################                
                elif tree.find('coredata'):
                    fmt = "Elsevier"
                    meta = tree.find('coredata')
                    print ('Elsevier formatted XML for' + xmlFile)
                    # Read the first three elements and create the article object
                    try: doi = tree.coredata.find('doi').text 
                    except: doi=''
                    try: title = tree.coredata.find('title').text
                    except: title=''
                    try: year = tree.coredata.find('coverDate').text[:4]
                    except: year = ''
        
                    article = Article(doi, title, year)
                    
                    # Add the other single item attributes
                    try: article.publisher_name = tree.coredata.find('publicationName').text
                    except: article.publisher_name = ''            
                    
                    try: article.volume = tree.coredata.find('volume').text
                    except: article.volume = ''
                    
                    try: article.issue = tree.coredata.find('issueIdentifier').text
                    except: article.issue = ''
                    
                    try: article.start_page = tree.coredata.find('startingPage').text
                    except: article.start_page = ''
                        
                    try: article.end_page = tree.coredata.find('endingPage').text
                    except: article.end_page = ''                    
                    
                    try: 
                        abs = tree.coredata.find('description').text
                        if abs[:8] == "Abstract":
                            article.abstract = abs[8:]
                        else: 
                            article.abstract = abs
                        if not article.abstract: article.no_abstract = True
                    except: 
                        article.abstract = ''
                        article.no_abstract = True                    

                    ###############################
                    ## Build keywords list       ##
                    ############################### 
                    if tree.coredata.find('subject'):
                        for kw in tree.coredata.find_all('subject'):
                            article.add_keyword(kw.text)
                    if collectionKeyword: article.add_keyword(collectionKeyword)    
                    if not article.keywords: no_keywords = True                    
                    
                    ###############################
                    ## Build authors list        ##
                    ############################### 
                    try:
                        for author in tree.coredata.find_all('creator'):
                            article.add_author(author.text)
                        if len(article.authors)==0: raise
                    except:
                        print("No authors found for " + xmlFile + ". Skipping this article.")
                        log.add_msg("No authors found for " + xmlFile + ". Skipping this article.")
                        log.countNoAuthors += 1
                        continue
                    
                    ###############################
                    ## Retrieve article body text #
                    ###############################                       
                    article.body = " ".join(tree.find('originalText').stripped_strings)
                    
                else:
                    fmt = "other"
                    print('Unknown XML format...')
                    
                
                ###############################
                ## parse XML for locations   ##
                ## and write to CSV file     ##
                ###############################
                try:
                    
                    #print text
                    initlocs = log.locations
                    if geoparser == "spacy-lg":
                        print("using spacy-lg")
                        URL = "https://geolocate.nkn.uidaho.edu/api/spacy-lg"
                        if not os.path.exists(locationsFile):
                            header = [['pandas.index','filename','doi','title','level','section','nchar','status','parser',
                                   'coordinates','end_char','score','start_char','text','type']]
                            writeLocationsHeader(header,locationsFile)                           

                    elif geoparser == "spacy-trf":
                        print("using spacy-trf")
                        URL = "https://geolocate.nkn.uidaho.edu/api/spacy-trf"
                        if not os.path.exists(locationsFile):
                            header = [['pandas.index','filename','doi','title','level','section','nchar','status','parser',
                                   'coordinates','end_char','score','start_char','text','type']]
                            writeLocationsHeader(header,locationsFile)                           
                        
                    elif geoparser == 'mordecai':
                        # pass info to mordecai service
                        print("using Mordecai")
                        URL = "https://geolocate.nkn.uidaho.edu/api/mordecai"
                        if not os.path.exists(locationsFile):
                            header = [['pandas.index','filename','doi','title','level','section','nchar','status','parser',
                                       'country_conf','country_predicted','geo.admin1',
                                       'geo.country_code3', 'geo.feature_class', 'geo.feature_code',
                                       'geo.geonameid', 'geo.lat', 'geo.lon', 'geo.place_name','spans','word']]
                            writeLocationsHeader(header,locationsFile)
                                                
                    elif geoparser == 'locatext':
                        print("using LocateXT")

                    elif geoparser == 'stanza':
                        print("Using stanza")
                        URL = "https://geolocate.nkn.uidaho.edu/api/stanza"
                        if not os.path.exists(locationsFile):                        
                            header = [['pandas.index','filename','doi','title','level','section','nchar','status','parser',
                                       'coordinates','end_char','score','start_char','text','type']]
                            writeLocationsHeader(header,locationsFile)                            
                        
                    elif geoparser == 'nltk':
                        print("Using NLTK")
                        URL = "https://geolocate.nkn.uidaho.edu/api/nltk"
                        if not os.path.exists(locationsFile):                        
                            header = [['pandas.index','filename','doi','title','level','section','nchar','status','parser',
                                       'coordinates','score','text','type']]
                            writeLocationsHeader(header,locationsFile)                        
                                                
                    else: # no parsing??
                        print("I don't know what to do!!!")
                    
                    # Need initialize values in case the title and/or abstract doesn't contain any location info.
                    # Also need to reinitialize the values between each returned record so we don't get bleed over if a record doesn't have a 'geo' element
                    
                    print("Parsing title...")    
                    rjson={}; j='' #reset this to prevent bleed-over from prior article/section
                    resp = requests.post(URL, article.title.encode('utf-8'))
                    rjson = json.loads(resp.text)
                    if len(rjson) > 0:  
                        df = pd.json_normalize(rjson, max_level=1)
                        df['7_status'] = "True"
                    else:
                        df = pd.DataFrame(data={"7_status":["False"]})
                        
                    df['1_name'] =  name
                    df['2_doi'] = article.doi
                    df['3_title'] = article.title
                    df['4_level'] = "title"
                    df['5_section'] = "title"
                    df['6_nchar'] = len(article.title)
                    df['8_parser'] = geoparser

                    df = df.reindex(sorted(df.columns), axis=1)

                    hdr = False  if os.path.isfile(locationsFile) else True
                    df.to_csv(locationsFile, mode='a', header=hdr)
                    
                    # Parse Abstract
                    print("Parsing abstract...")                   
                    rjson={}; j='' #reset this to prevent bleed-over from prior article/section
                    resp = requests.post(URL, article.abstract.encode('utf-8'))
                    if len(rjson) > 0:  
                        df = pd.json_normalize(rjson, max_level=1)
                        df['7_status'] = "True"
                    else:
                        df = pd.DataFrame(data={"7_status":["False"]})
                        
                    df['1_name'] =  name
                    df['2_doi'] = article.doi
                    df['3_title'] = article.title
                    df['4_level'] = "abstract"
                    df['5_section'] = "abstract"
                    df['6_nchar'] = len(article.abstract)
                    df['8_parser'] = geoparser

                    df = df.reindex(sorted(df.columns), axis=1)

                    hdr = False  if os.path.isfile(locationsFile) else True
                    df.to_csv(locationsFile, mode='a', header=hdr)                       
                    
                    # Parse body of article
                    for sec in tree.find_all('sec'):
                        secText = " ".join(sec.stripped_strings)
                        secID = sec.attrs #["id"]
                        secTitle = sec.find("title").text
                    
                        print("Parsing "+secTitle+"...")                   
                        rjson={}; j='' #reset this to prevent bleed-over from prior article/section
                        resp = requests.post(URL, secText.encode('utf-8'))
                        rjson = json.loads(resp.text)
                        if len(rjson) > 0:  
                            df = pd.json_normalize(rjson, max_level=1)
                            df['7_status'] = "True"
                        else:
                            df = pd.DataFrame(data={"7_status":["False"]})
                            
                        df['1_name'] =  name
                        df['2_doi'] = article.doi
                        df['3_title'] = article.title
                        df['4_level'] = "body"
                        df['5_section'] = secTitle
                        df['6_nchar'] = len(secText)
                        df['8_parser'] = geoparser
    
                        df = df.reindex(sorted(df.columns), axis=1)
    
                        hdr = False  if os.path.isfile(locationsFile) else True
                        df.to_csv(locationsFile, mode='a', header=hdr)                        
                        
                        
                                                  
                    
                except Exception as inst:
                    print(type(inst))
                    print(inst)
                    print ("Error in parsing article text for place names: " + xmlFile)
                    log.add_msg("Error in parsing article text for place names: " + xmlFile)
                    continue
                
                
                ###############################
                ## Write article to output   ##
                ###############################
                if (allArticles or articlelocs>0):
                    try:
                        articleLine = [[article.doi,article.publisher_name,'',article.build_citation(),article.title,str(article.year),article.authors[0],article.format_authors(),article.format_volisspg(),article.volume,article.issue,article.start_page,article.end_page,article.format_keywords(),article.no_keywords,article.abstract,article.no_abstract,article.url]]
                        articleWriter.writerows(articleLine)            
                        log.countArticlesWritten += 1
                    except: 
                        print ("Error writing record for " + xmlFile + " - " + article.title)
                        log.add_msg("Error writing record for " + xmlFile + " - " + article.title)
                        log.countErrors += 1
                        continue                
                
                # Pause for a second to keep the API happy.
                time.sleep(1)
                
        ###############################
        ## Clean up and log errors   ##
        ############################### 
        
        print ("")
        print ("Finished!!")
        print ("Processed " + str(log.countArticles) + " articles.")
        print ("Errors encountered in " + str(log.countErrors) + " articles.")
        print (str(log.countNoAuthors) + " articles had no authors and were skipped.")
        print (str(log.countArticlesWritten) + " articles written to the CSV file")
        print (str(log.countGeoTagged) + " articles had parsed coordinates.")
        print (str(log.locations) + " total locations found.")
        
        for msg in log.messages:
            lf.write("\n"+msg)
        lf.write("\n".join(["","","Finished processing directory "+startDir+" at "+datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'),"Processed " + str(log.countArticles) + " articles.",
                           "Errors encountered in " + str(log.countErrors) + str(log.countNoAuthors) + " articles had no authors and were skipped." + str(log.countArticlesWritten) + " articles written to the CSV file" + " articles.", str(log.countGeoTagged) + " articles had parsed coordinates.",str(log.locations) + " total locations found.",
                           "Created output files:",articlesFile,locationsFile,logFile]))
        lf.close()  
        
