import subprocess
import requests
import re
import logging
import sys
import json
from datetime import datetime, timedelta, date
from airflow.hooks.druid_hook import DruidHook
import softBugConfig
from HdfsUtils import pullXcom, run_cmd

SOPHIA_CONFIG = softBugConfig.SOFTBUG_CONFIGS

druidLookbackDays = SOPHIA_CONFIG['druidLookbackDays']
log = logging.getLogger(__name__)
xcom_keys = 'fs_path,hdfs_path,hdfs_path_month,log_enrich_path,log_enrich_path_phoenix,log_iplookup_path,glo_path,fs_pattern,params,fs_file,index_template,source'


def druid_indexing(task_id, **kwargs):
    task = 'init_task_{}'.format(task_id)
    ti = kwargs['ti']
    xcom_values = pullXcom(ti, task, xcom_keys)  # pull data from xcom object
    index_template = xcom_values['index_template']

    # index_template = kwargs.get('templates_dict').get('index_template', None)
    # site = kwargs.get('templates_dict').get('site', None)
    tmp_index_template = index_template.replace("_template.json", "_template_{}.json".format(task_id))
    print(tmp_index_template)
    hook = DruidHook(
        druid_ingest_conn_id='druid_ingest_default',
        max_ingestion_time=None
    )

    with open(tmp_index_template) as data_file:
        index_spec = json.load(data_file)
    index_spec_str = json.dumps(
        index_spec,
        sort_keys=True,
        indent=4,
        separators=(',', ': ')
    )
    log.info("Sumitting %s", index_spec_str)
    hook.submit_indexing_job(index_spec_str)


def DruidIndexTask(**kwargs):
    task = 'boot_task'
    ti = kwargs['ti']
    xcom_keys = "index_template,source,date,log_enrich_path"
    xcom_values = pullXcom(ti, task, xcom_keys)  # pull data from xcom object
    index_template = xcom_values['index_template']
    source = xcom_values['source']
    date = xcom_values['date']
    enrichDataPath = xcom_values['log_enrich_path']
    # read arguments
    # index_template = kwargs.get('templates_dict').get('index_template', None)
    # source = kwargs.get('templates_dict').get('source', None)

    # date = kwargs.get('templates_dict').get('date', None)
    # enrichDataPath = kwargs.get('templates_dict').get('enrichedDir', None)

    hdfs_path = buildDates(date, enrichDataPath)
    jReturn = json.loads(hdfs_path)

    interval = jReturn['interval']
    hdfs_path = jReturn['path'][0]

    hook = DruidHook(
        druid_ingest_conn_id='druid_ingest_default',
        max_ingestion_time=None
    )

    # Open the JSON file for reading
    jsonFile = open(index_template, "r")

    # Read the JSON into the buffer
    data = json.load(jsonFile)

    # Close the JSON file
    jsonFile.close()

    ## Working with buffered content
    jData = data['spec']['dataSchema']['dataSource']
    data['spec']['dataSchema']['dataSource'] = str(jData).replace(jData, source)
    jData = data['spec']['dataSchema']['granularitySpec']['intervals'][0]
    data['spec']['dataSchema']['granularitySpec']['intervals'] = [str(jData).replace(jData, interval)]
    jData = data['spec']['ioConfig']['inputSpec']['paths']
    data['spec']['ioConfig']['inputSpec']['paths'] = str(jData).replace(jData, hdfs_path)

    index_spec_str = json.dumps(
        data,
        sort_keys=True,
        indent=4,
        separators=(',', ': ')
    )
    log.info("Sumitting %s", index_spec_str)
    hook.submit_indexing_job(index_spec_str)

    ## Save our changes to JSON file
    jsonFile = open(index_template, "w+")
    jsonFile.write(json.dumps(data, indent=4, sort_keys=True))
    jsonFile.close()
    return index_template


def updateDruidIndexJsonFile(**kwargs):
    # read arguments
    index_template = kwargs.get('templates_dict').get('index_template', None)
    source = kwargs.get('templates_dict').get('source', None)

    date = kwargs.get('templates_dict').get('date', None)
    enrichDataPath = kwargs.get('templates_dict').get('enrichDataPath', None)

    hdfs_path = buildDates(date, enrichDataPath)
    jReturn = json.loads(hdfs_path)

    interval = jReturn['interval']
    hdfs_path = jReturn['path'][0]

    # Open the JSON file for reading
    jsonFile = open(index_template, "r")

    # Read the JSON into the buffer
    data = json.load(jsonFile)

    # Close the JSON file
    jsonFile.close()

    ## Working with buffered content
    jData = data['spec']['dataSchema']['dataSource']
    data['spec']['dataSchema']['dataSource'] = str(jData).replace(jData, source)
    jData = data['spec']['dataSchema']['granularitySpec']['intervals'][0]
    data['spec']['dataSchema']['granularitySpec']['intervals'] = [str(jData).replace(jData, interval)]
    jData = data['spec']['ioConfig']['inputSpec']['paths']
    data['spec']['ioConfig']['inputSpec']['paths'] = str(jData).replace(jData, hdfs_path)

    ## Save our changes to JSON file
    jsonFile = open(index_template, "w+")
    jsonFile.write(json.dumps(data, indent=4, sort_keys=True))
    jsonFile.close()
    return index_template


def updatejsonfile(**kwargs):
    index_template = kwargs.get('templates_dict').get('index_template', None)
    source = kwargs.get('templates_dict').get('source', None)
    site = kwargs.get('templates_dict').get('site', None)
    date = kwargs.get('templates_dict').get('date', None)
    enriched_dir = kwargs.get('templates_dict').get('enriched_dir', None)

    hdfs_path = buildDates(date, enriched_dir)
    jReturn = json.loads(hdfs_path)
    interval = jReturn['interval']
    hdfs_path = jReturn['path'][0]

    jsonFile = open(index_template, "r")  # Open the JSON file for reading
    tmp_index_template = index_template.replace("_template.json", "_template_{}.json".format(site))
    data = json.load(jsonFile)  # Read the JSON into the buffer
    jsonFile.close()  # Close the JSON file

    ## Working with buffered content

    jData = data['spec']['dataSchema']['dataSource']
    data['spec']['dataSchema']['dataSource'] = str(jData).replace(jData, source)
    jData = data['spec']['dataSchema']['granularitySpec']['intervals'][0]
    data['spec']['dataSchema']['granularitySpec']['intervals'] = [str(jData).replace(jData, interval)]
    jData = data['spec']['ioConfig']['inputSpec']['paths']
    data['spec']['ioConfig']['inputSpec']['paths'] = str(jData).replace(jData, hdfs_path)

    ## Save our changes to JSON file

    jsonFile = open(tmp_index_template, "w+")
    jsonFile.write(json.dumps(data, indent=4, sort_keys=True))
    jsonFile.close()
    return jsonFile


def updatejsonfileDelta(**kwargs):
    # ti = kwargs['ti']
    # task_instance = kwargs['templates_dict']
    index_template = kwargs.get('index_template', None)
    jsonFile = open(index_template, "r")
    source = kwargs.get('source', None)
    site = kwargs.get('site', None)
    delta = kwargs.get('delta', None)

    jsonFile = open(index_template, "r")  # Open the JSON file for reading
    tmp_index_template = index_template.replace("_template.json", "_template_{}.json".format(site))
    data = json.load(jsonFile)  # Read the JSON into the buffer
    path = []
    intervals = []
    interval = ""
    # print("Delta -->{}".format(delta)

    for deltaDate, deltaPath in delta.iteritems():
        # print('{} ---> {}'.format(deltaDate,deltaPath)
        path.append(deltaPath['path'])
        date = deltaPath['interval']
        print('{} ---- {}'.format(path, str(date).replace("/","_")))
        segments = list_segments(source,  str(date).replace("/","_"))
        print(segments)
        interval = segments[0]['interval']
        intervals.append(segments[0]['interval'])
        jData = data['spec']['dataSchema']['dataSource']
        data['spec']['dataSchema']['dataSource'] = str(jData).replace(jData, source)
        jData = data['spec']['dataSchema']['granularitySpec']['intervals'][0]

        jData = data['spec']['ioConfig']['inputSpec']['children'][0]['paths']
        data['spec']['ioConfig']['inputSpec']['children'][1]['ingestionSpec']['dataSource'] = source
        # data['spec']['ioConfig']['inputSpec']['children'][1]['ingestionSpec']['segments'] = segments


        # d = datetime.strptime(deltaDate, "%m-%d-%Y")
        # print(d.date()
        # # print(deltaDate.replace("-", "")
        # hdfs_path = buildDates(str(d.date()).replace("-", ""), deltaPath)
        # print(hdfs_path
    data['spec']['ioConfig']['inputSpec']['children'][1]['ingestionSpec']['intervals'] = intervals
    # str(jData).replace(jData, intervals)]
    data['spec']['ioConfig']['inputSpec']['children'][0]['paths'] = str(jData).replace(jData, ','.join(path))
    # data['spec']['ioConfig']['inputSpec']['children'][0]['paths'] = ','.join(path)
    jData = data['spec']['dataSchema']['granularitySpec']['intervals'][0]
    data['spec']['dataSchema']['granularitySpec']['intervals'] = [str(jData).replace(jData, interval)]
    # data['spec']['dataSchema']['granularitySpec']['intervals'] = [interval]
    hook = DruidHook(
        druid_ingest_conn_id='druid_ingest_default',
        max_ingestion_time=None
    )
    index_spec_str = json.dumps(
        data,
        sort_keys=True,
        indent=4,
        separators=(',', ': ')
    )
    log.info("Sumitting %s", index_spec_str)
    # hook.submit_indexing_job(index_spec_str)
    print(index_spec_str)
    jsonFile = open(tmp_index_template, "w+")
    jsonFile.write(json.dumps(data, indent=4, sort_keys=True))
    jsonFile.close()
    # exit(0)
    # interval = kwargs.get('interval', None)
    # date = kwargs.get('date', None)
    # print(date.replace("-", "")
    # enriched_dir = kwargs.get('enriched_dir', None)
    # hdfs_path = buildDates(date.replace("-", ""), enriched_dir)
    # jReturn = json.loads(hdfs_path)
    #
    # # interval = jReturn['interval']
    # hdfs_path = jReturn['path'][0]
    # # hdfs_path = index_template
    # jReturn = json.load(jsonFile)
    # # interval = jReturn['interval']
    #
    # # hdfs_path = jReturn['path'][0]
    # segments = list_segments(source, date)
    # interval = segments[0]['interval']
    # print(segments[0]['interval']
    # jsonFile = open(index_template, "r")  # Open the JSON file for reading
    # tmp_index_template = index_template.replace("_template.json", "_template_{}.json".format(site))
    # data = json.load(jsonFile)  # Read the JSON into the buffer
    # jsonFile.close()  # Close the JSON file
    #
    # ## Working with buffered content
    #
    # jData = data['spec']['dataSchema']['dataSource']
    # data['spec']['dataSchema']['dataSource'] = str(jData).replace(jData, source)
    # jData = data['spec']['dataSchema']['granularitySpec']['intervals'][0]
    # data['spec']['dataSchema']['granularitySpec']['intervals'] = [str(jData).replace(jData, interval)]
    # jData = data['spec']['ioConfig']['inputSpec']['children'][0]['paths']
    # data['spec']['ioConfig']['inputSpec']['children'][0]['paths'] = str(jData).replace(jData, hdfs_path)
    # data['spec']['ioConfig']['inputSpec']['children'][1]['ingestionSpec']['dataSource'] = source
    # data['spec']['ioConfig']['inputSpec']['children'][1]['ingestionSpec']['segments'] = segments
    # data['spec']['ioConfig']['inputSpec']['children'][1]['ingestionSpec']['intervals'] = [
    #     str(jData).replace(jData, interval)]
    # ## Save our changes to JSON file
    #
    # jsonFile = open(tmp_index_template, "w+")
    # jsonFile.write(json.dumps(data, indent=4, sort_keys=True))
    # jsonFile.close()
    return jsonFile


def getIndexJsonFileInterval(dirPath):
    # read files name from given dirPath
    p = subprocess.Popen("hdfs dfs -ls " + dirPath + " | awk 'NF > 7 {print $8}'", shell=True, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)

    # list object to hold all datetimes
    list = []

    # iterate over files name and collect into list
    for line in p.stdout.readlines():
        try:
            list.append(datetime.strptime(line.strip()[-10:], "%m-%d-%Y"))
        except:
            print('bad input: ' + line)

    # if list is not empty return mix and max
    if list:
        minDate = min(list)
        maxDate = max(list)
        return minDate.strftime("%Y-%m-%d") + "/" + (maxDate + timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        return datetime.now().strftime("%Y-%m-%d") + "/" + (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")


def getIndexFiles(dirPath):
    # read files name from given dirPath
    print(('Path -->' + dirPath))
    p = subprocess.Popen("hdfs dfs -ls " + dirPath + " | awk 'NF > 7 {print $8}'", shell=True, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    ignoreList = ['_SUCCESS', 'null', 'No such file or directory']
    # list object to hold all datetimes
    list = []

    # iterate over files name and collect into list
    for line in p.stdout.readlines():
        try:
            if not line.__contains__('No such file or directory'):
                list.append(line.rstrip())
            else:
                print(('bad path found'))
        except:
            print(('bad input: ' + line))

    pathList = [str for str in list if not any(i in str for i in ignoreList)]

    return pathList


def buildDates(datestr, enrichedDir):
    d = datetime.strptime(datestr, "%Y%m%d")
    enrichedDir = enrichedDir.split("enriched")[0]+"enriched/"
    dlist = []
    pathList = []
    intervalPathList = []
    print(druidLookbackDays)
    for dec in range(druidLookbackDays + 2):
        fdat = d - timedelta(dec)
        day = "0{}".format(fdat.day) if len(str(fdat.day)) == 1 else fdat.day
        month = "0{}".format(fdat.month) if len(str(fdat.month)) == 1 else fdat.month
        dlist.append("{}-{}-{}".format(fdat.year, month, day))
        print('Found path for druid ->' + enrichedDir + "{}/{}/{}".format(fdat.year, month, day))
        fileIndexLen = getIndexFiles(enrichedDir + "{}/{}/{}".format(fdat.year, month, day))
        if len(fileIndexLen) > 0:
            pathList = pathList + fileIndexLen

    if dlist:
        minDate = d - timedelta(druidLookbackDays)
        maxDate = d
    print("pathList ={}".format(pathList))
    for path in pathList:
        try:
            cdate = datetime.strptime(path.strip()[-10:], "%m-%d-%Y")
            if cdate >= minDate and cdate <= maxDate:
                intervalPathList.append(path + '/*')

        except:
            print('bad input: ' + path)


    retJson = {'interval': minDate.strftime("%Y-%m-%d") + "/" + (maxDate + timedelta(1)).strftime("%Y-%m-%d"), 'path': [ ','.join(intervalPathList)]}
    print(retJson)
    return json.dumps(retJson)

def copyFilesToDeltaDir(task_id, **kwargs):

    task = 'init_task_{0}'.format(task_id)
    ti = kwargs['ti']
    xcom_keys = "index_template,source,date,log_enrich_path,enriched_dir_delta"
    xcom_values = pullXcom(ti, task,xcom_keys)  #pull data from xcom object
    datestr = xcom_values['date']
    enrichedDir = xcom_values['log_enrich_path']
    deltaDir = xcom_values['enriched_dir_delta']

    dlist =[]
    pathList=[]
    intervalPathList=[]

    d = datetime.strptime(datestr,"%Y%m%d")
    day = "0{}".format(d.day) if len(str(d.day)) == 1 else d.day
    month = "0{}".format(d.month) if len(str(d.month)) == 1 else d.month
    dlist.append("{}-{}-{}".format(d.year, month, day))

    print('deltadir:'+ deltaDir)


    fileIndexLen= getIndexFiles(enrichedDir)
    if len(fileIndexLen) >0:
        pathList = pathList + fileIndexLen

    itr = 0;
    if dlist:
        minDate = d - timedelta(druidLookbackDays)

    for path in pathList:

        try:
            cdate=  datetime.strptime(path.strip()[-10:], "%m-%d-%Y")
            if cdate < minDate :
                if itr == 0 :
                    x = subprocess.Popen("hdfs dfs -rm -r "+deltaDir, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    y = subprocess.Popen("hdfs dfs -mkdir -p "+deltaDir, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    itr = 1
                intervalPathList.append(path+'/*')
                y=subprocess.Popen("hdfs dfs -mv "+path +" "+ deltaDir, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                print(path)
        except:
            print('bad input: '+path)


    retJson = { 'path': [ ','.join(intervalPathList)]}


def copyToDeltaDir(**kwargs):

    task = 'boot_task'
    ti = kwargs['ti']
    xcom_keys = "index_template,source,date,log_enrich_path,enriched_dir_delta"
    xcom_values = pullXcom(ti, task,xcom_keys)  #pull data from xcom object
    datestr = xcom_values['date']
    enrichedDir = xcom_values['log_enrich_path']
    deltaDir = xcom_values['enriched_dir_delta']

    dlist =[]
    pathList=[]
    intervalPathList=[]

    d = datetime.strptime(datestr,"%Y%m%d")
    day = "0{}".format(d.day) if len(str(d.day)) == 1 else d.day
    month = "0{}".format(d.month) if len(str(d.month)) == 1 else d.month
    dlist.append("{}-{}-{}".format(d.year, month, day))

    print('deltadir:'+ deltaDir)
    x = subprocess.Popen("hdfs dfs -mkdir -p "+deltaDir, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    fileIndexLen= getIndexFiles(enrichedDir)
    if len(fileIndexLen) >0:
        pathList = pathList + fileIndexLen

    if dlist:
        minDate = d - timedelta(druidLookbackDays)
    for path in pathList:

        try:
            cdate=  datetime.strptime(path.strip()[-10:], "%m-%d-%Y")
            if cdate < minDate :
                intervalPathList.append(path+'/*')
                y=subprocess.Popen("hdfs dfs -mv "+path +" "+ deltaDir, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                print(path)
        except:
            print('bad input: '+path)


    retJson = { 'path': [ ','.join(intervalPathList)]}


def request_status(request):
    if request.status_code != 200:
        print("Exiting...received HTTP status code " + str(request.status_code) + " with the following response:")
        print(request.text)


def date_to_interval(date, separator="_", endpointsOnly=False):
    """ Take a date YYYY-MM-DD, calculate the next day, and create an interval of the following form with these dates as endpoints:
        2018-08-02_2018-08-03
        Zero-prefix all digits.
    """
    print("datetointerval")
    p = re.compile('^(\d{4})-(\d{2})-(\d{2})$')
    z = re.compile('^0(\d)$')
    nz = re.compile('^\d$')
    m = p.match(date)
    startyear = m.group(1)
    startmonth = m.group(2)
    startday = m.group(3)

    if z.match(startmonth):  # get rid of zero prefix for datetime
        startmonth = z.match(startmonth).group(1)
    if z.match(startday):  # get rid of zero prefix for datetime
        startday = z.match(startday).group(1)

    startdate = datetime(int(startyear), int(startmonth), int(startday))
    tmpenddate = startdate + timedelta(days=1)
    endyear = str(tmpenddate.year)
    endmonth = str(tmpenddate.month)
    endday = str(tmpenddate.day)

    if nz.match(endmonth):
        endmonth = "0" + endmonth
    if nz.match(endday):
        endday = "0" + endday

    enddate = endyear + "-" + endmonth + "-" + endday

    if endpointsOnly:
        return [date, enddate]

    return date + separator + enddate


def list_segments(datasource, date):
    print("list_segments")
    # /druid/coordinator/v1/datasources/src/intervals/2018-08-02_2018-08-03
    segmentList = []
    if date:
        # print("date {}".format( date))
        print('URL ' + SOPHIA_CONFIG[
            'COORDINATOR_DATASOURCE_URL'] + "/" + datasource + "/intervals/" + date)
        r = requests.get(
            # SOPHIA_CONFIG['COORDINATOR_DATASOURCE_URL'] + "/" + datasource + "/intervals/" + date_to_interval(
            #     date) + "?full")
            SOPHIA_CONFIG['COORDINATOR_DATASOURCE_URL'] + "/" + datasource + "/intervals/" +
            date + "?full")
        request_status(r)
        jsonData = r.json()
        print(jsonData)
        # print(json.dumps(jsonData, indent=4, sort_keys=True)
        # for segment in json:
        #     print(segment)
        # print(json
        for i in jsonData:
            for j in jsonData[i]:
                segmentList.append(jsonData[i][j]['metadata'])
        # print(segmentList[0]['interval']
        # print(json.dumps(segmentList,indent=4, sort_keys=True)
        return segmentList
    else:
        print(SOPHIA_CONFIG['COORDINATOR_METADATA_URL'] + "/" + datasource + "/segments?full")
        r = requests.get(SOPHIA_CONFIG['COORDINATOR_METADATA_URL'] + "/" + datasource + "/segments?full")
        #    r = requests.get(ConfObj.COORDINATOR_METADATA_URL + "/" + datasource + "/segments")
        request_status(r)
        jsonData = r.json()
        for segment in jsonData:
            print(segment)


# def getDeltaDates(source,hdfs_path):
#     (ret, out, err) = run_cmd(['hdfs', 'dfs', '-stat', '%n', hdfs_path + '/*'], False)
#     deltaDict = {}
#     if ret != 0:
#         print("No dates avaliable !!!"
#         return deltaDict
#     else:
#         if out:
#             print("Found Dates for processing !!!"
#             sDates = str(out).split("\n")
#             for sDate in sDates:
#                 if str(sDate).strip().__contains__("_SUCCESS"):
#                     continue
#                 if str(sDate).strip() != "":
#                     # date.append(sDate[-10:])
#                     # path.append(hdfs_path + "/" + sDate)
#                     d = datetime.strptime(sDate[-10:], "%m-%d-%Y")
#
#                     minDate = d - timedelta(0)
#                     maxDate = d
#                     segments = list_segments(str(source).lower(),  minDate.strftime("%Y-%m-%d") + "_" + (maxDate + timedelta(1)).strftime("%Y-%m-%d"))
#                     if segments:
#                         interval = segments[0]['interval'].encode("utf-8")
#                         deltaDict[sDate[-10:]] = {
#                             'interval': interval,
#                             'path': hdfs_path + "/" + sDate + "/*"}
#             return deltaDict

def getDeltaDates(source,hdfs_path):
    (ret, out, err) = run_cmd(['hdfs', 'dfs', '-stat', '%n', hdfs_path + '/*'], False)
    deltaDict = {}
    path = []
    interval = []
    if ret != 0:
        print("No dates avaliable !!!")
        return deltaDict
    else:
        if out:
            print("Found Dates for processing !!!")
            sDates = str(out).split("\n")
            for sDate in sDates:
                if str(sDate).strip().__contains__("_SUCCESS") or str(sDate).strip().__contains__("null"):
                    continue
                if str(sDate).strip() != "":
                    print(sDate)
                    # date.append(sDate[-10:])
                    # path.append(hdfs_path + "/" + sDate)
                    d = datetime.strptime(sDate[-10:], "%m-%d-%Y")

                    minDate = d - timedelta(0)
                    maxDate = d
                    segments = list_segments(str(source).lower(),  minDate.strftime("%Y-%m-%d") + "_" + (maxDate + timedelta(1)).strftime("%Y-%m-%d"))
                    if segments:
                        interval.append(segments[0]['interval'].encode("utf-8"))
                        path.append(hdfs_path  + "/" + sDate + "/*")
                        deltaDict = {
                            'interval': interval,
                            'path': path,
                            'segments': segments
                        }
            return deltaDict


def updatejsonfileDeltas(**kwargs):
    # ti = kwargs['ti']
    # task_instance = kwargs['templates_dict']
    index_template = kwargs.get('index_template', None)
    jsonFile = open(index_template, "r")
    source = kwargs.get('source', None)
    site = kwargs.get('site', None)
    # print('This is dict {}'.format(kwargs.get('delta', None))
    delta = json.loads(kwargs.get('delta', None))
    jsonFile = open(index_template, "r")  # Open the JSON file for reading
    tmp_index_template = index_template.replace("_template.json"
                                                "", "_template_{}.json".format(site))
    data = json.load(jsonFile)  # Read the JSON into the buffer
    path = []
    intervals = []
    interval = ""
    # print("Delta -->{}".format(delta)
    # print(data['spec']['ioConfig']['inputSpec']['children'][0]['ingestionSpec']['intervals']
    # print(data['spec']['ioConfig']['inputSpec']['children'][1]['paths']

    # print(index_spec_str
    jData  = data
    paths = []
    ingestionSpec = []
    # print(data['spec']['ioConfig']['inputSpec']['children']
    # for i, (deltaDate, deltaPath) in enumerate(delta.iteritems()):
    #     data['spec']['ioConfig']['inputSpec']['children'].append(i)
    # for i, (deltaDate, deltaPath) in enumerate(delta.iteritems()):
    #     # print('{} {} ---> {} {} '.format(i, deltaDate,deltaPath['interval'],deltaPath['paths'])
    #     # print('---------------------'
    #     x = {
    #         'dataSource': deltaDate,
    #         'intervals': deltaPath['interval']
    #     }
    #     ingestionSpec.append(x)
    #     # data['spec']['ioConfig']['inputSpec']['children'][0]['ingestionSpec'] = [{
    #     #     'dataSource': deltaDate,
    #     #     'intervals': deltaPath['interval']
    #     # }]
    #     paths.append(deltaPath['paths'])
    #     # data['spec']['ioConfig']['inputSpec']['children'][0]['ingestionSpec'] = {
    #     #     'dataSource': deltaDate,
    #     #     'intervals': deltaPath['interval']
    #     # }
    #     # data['spec']['ioConfig']['inputSpec']['children'][1]['paths'] = deltaPath['paths']
    #     # data['spec']['ioConfig']['inputSpec']['children'].append({
    #     #     'dataSource': deltaDate,
    #     #     'intervals': deltaPath['interval']
    #     # })


    finalPath = []
    for path in paths:
        # print("--{}--".format(path)
        for p in path:
            # print(p
            finalPath.append(p)
    for i, (deltaSource, deltaPath) in enumerate(delta.iteritems()):

        print("template file {} {} ".format(deltaSource,str(index_template).replace("src",deltaSource).lower()))
        jsonFile = open(str(index_template).replace("src",deltaSource).lower(), "r")
        data = json.load(jsonFile)

        x = {
            'dataSource': str(deltaSource).lower(),
            'intervals': deltaPath['interval'],
            # 'segments': deltaPath['segments']
        }
        y = {
            'type': 'static',
            'paths': ','.join(deltaPath['paths'])
        }
        z = {
            "type" : "dataSource",
            "ingestionSpec": x
        }

        data['spec']['dataSchema']['granularitySpec']['intervals'] = deltaPath['interval']
        data['spec']['ioConfig']['inputSpec']['children'][1] = y
        data['spec']['ioConfig']['inputSpec']['children'][0] = z
        # data['spec']['ioConfig']['inputSpec']['children'][0]['ingestionSpec']['segments'] = deltaPath['segments']
        index_spec_str = json.dumps(
            data,
            sort_keys=True,
            indent=4,
            separators=(',', ': ')
        )
        hook = DruidHook(
            druid_ingest_conn_id='druid_ingest_default',
            max_ingestion_time=None
        )
        log.info("Sumitting %s", index_spec_str)
        # if str(deltaSource).lower() == "src":
        #     hook.submit_indexing_job(index_spec_str)

    jsonFile = open("delta.json", "w+")
    jsonFile.write(json.dumps(data, indent=4, sort_keys=True))
    jsonFile.close()
