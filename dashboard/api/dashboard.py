''' dashboard.py
    NeuronBridge precompute dashboard
'''

from operator import attrgetter
import inspect
import json
import re
import sys
import boto3
from boto3.dynamodb.conditions import Key
from flask import Flask, make_response, render_template, request
from flask_cors import CORS
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught

# *****************************************************************************
# * Application                                                               *
# *****************************************************************************

__version__ = '1.0.0'
app = Flask(__name__, template_folder='templates')
app.config.from_pyfile("config.cfg")
CORS(app)
# Database
DB = {}
# Navigation
NAV = {"Labels": None,
       "Tags" : {"View tags": "tags",
                 "View publishedURL tags": "url_tags",
                },
       "EM bodies": None,
       "Search": None,
      }

# *****************************************************************************
# * Flask                                                                     *
# *****************************************************************************

@app.before_request
def before_request():
    ''' Set transaction start time and increment counters.
        If needed, initilize global variables.
    '''
    if "neuronbridge" not in DB or not DB['neuronbridge']['conn']:
        try:
            dbconfig = JRC.get_config("databases")
        except Exception as err:
            terminate_program(err)
        for source in ("jacs", "neuronbridge"):
            dbo = attrgetter(f"{source}.prod.read")(dbconfig)
            print(f"Connecting to {dbo.name} prod on {dbo.host} as {dbo.user}")
            try:
                DB[source] = JRC.connect_database(dbo)
            except Exception as err:
                database_error(err)
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        DB['DYNAMOCLIENT'] = boto3.client('dynamodb', region_name='us-east-1')
    except Exception as err:
        terminate_program(err)
    for tname in (app.config['PTABLE'], 'publishing-doi'):
        fullname = f"{app.config['DDBASE']}-{tname}"
        DB[tname] = dynamodb.Table(fullname)
    DB['DYNAMO'] = dynamodb

# ******************************************************************************
# * Utility functions                                                          *
# ******************************************************************************

def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message
        Returns:
          None
    '''
    if msg:
        print(msg)
    sys.exit(-1 if msg else 0)


def colortext(text, color):
    ''' Return an HTML span with color text
        Keyword arguments:
          text: text to color
          color: color
        Returns:
          HTML span
    '''
    return f"<span style='color:{color}'>{text}</span>"


def generate_navbar(active):
    ''' Generate the web navigation bar
        Keyword arguments:
          active: name of active nav
        Returns:
          Navigation bar
    '''
    nav = '''
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark">
      <div class="collapse navbar-collapse" id="navbarSupportedContent">
        <ul class="navbar-nav mr-auto">
    '''
    for heading, subhead in NAV.items():
        basic = '<li class="nav-item active">' if heading == active else '<li class="nav-item">'
        drop = '<li class="nav-item dropdown active">' if heading == active \
               else '<li class="nav-item dropdown">'
        menuhead = '<a class="nav-link dropdown-toggle" href="#" id="navbarDropdown" ' \
                   + 'role="button" data-toggle="dropdown" aria-haspopup="true" ' \
                   + f"aria-expanded=\"false\">{heading}</a><div class=\"dropdown-menu\" "\
                   + 'aria-labelledby="navbarDropdown">'
        if subhead:
            nav += drop + menuhead
            for itm, val in subhead.items():
                link = f"/{val}" if val else ('/' + itm.replace(" ", "_")).lower()
                nav += f"<a class='dropdown-item' href='{link}'>{itm}</a>"
            nav += '</div></li>'
        else:
            nav += basic
            link = ('/' + heading.replace(" ", "_")).lower()
            nav += f"<a class='nav-link' href='{link}'>{heading}</a></li>"
    nav += '</ul></div></nav>'
    return nav


def generate_report_head(header, tclass='standard'):
    ''' Return items for report table and files
        Keyword arguments:
          header: list of column headers
        Returns:
          html: HTML for table head
          ftemplate: file row template
          wtemplate: web table row template
    '''
    html = '<table id="items" class="tablesorter ' + tclass + '"><thead><tr><th>' \
           + "</th><th>".join(header) + '</th></tr></thead><tbody>'
    ftemplate = "\t".join(["%s"]*len(header)) + "\n"
    wtemplate = '<tr><td>' + '</td><td>'.join(["%s"]*len(header)) + '</td></tr>'
    return html, ftemplate, wtemplate


def render_error(title, msg):
    ''' Render a general error
        Keyword arguments:
          title: title
          msg: message
        Returns:
          Error template
    '''
    return render_template('error.html', urlroot=request.url_root,
                           title=title, message=msg)

# ******************************************************************************
# * Database functions                                                         *
# ******************************************************************************

def database_error(err):
    ''' Render a database error
        Keyword arguments:
          err: exception
        Returns:
          Error template
    '''
    temp = "{2}: An exception of type {0} occurred. Arguments:\n{1!r}"
    mess = temp.format(type(err).__name__, err.args, inspect.stack()[0][3])
    return render_template('error.html', urlroot=request.url_root,
                           title='Database error', message=mess)


def distinct_count(dsl, dcol, collection='neuronMetadata', columns=None, fields=None):
    ''' Return a count of distinct values for a column
        Keyword arguments:
          dsl: data set label (for filtering)
          dcol: column to get count for
          collection: MongoDB collection
          columns: column names (for filtering)
          fields: column values (for filtering)
        Returns:
          Count
    '''
    if not columns:
        columns = []
        fields = []
    if collection == 'neuronMetadata':
        payload = {'datasetLabels': dsl}
    else:
        payload = {'alpsRelease': dsl}
    for key, val in zip(columns, fields):
        payload[key] = val
    coll = DB["neuronbridge"][collection]
    return len(coll.distinct(dcol, payload))


def generate_count_table(dsl, total):
    ''' Return a table with item couts for various steps along the process
        Keyword arguments:
          dsl: data set label (for filtering)
          total: total count dict
        Returns:
          HTML table
    '''
    headings = ['Published samples', 'Published lines', 'neuronMetadata samples',
                'neuronMetadata lines', 'publishedURL samples', 'publishedURL lines']
    html, _, wtemplate = generate_report_head(headings, 'standardc')
    row = get_lmrelease(dsl)
    tsn = colortext(total['samples'], 'red') if f"{row['samples']:,}" != total['samples'] \
          else colortext(total['samples'], 'lime')
    tln = colortext(total['lines'], 'red') if f"{row['lines']:,}" != total['lines'] \
          else colortext(total['lines'], 'lime')
    tsp = colortext(total['samplesp'], 'red') if total['samples'] != total['samplesp'] \
          else colortext(total['samplesp'], 'lime')
    tlp = colortext(total['linesp'], 'red') if total['lines'] != total['linesp'] \
          else colortext(total['linesp'], 'lime')
    field = [f"{row['samples']:,}", f"{row['lines']:,}", tsn, tln, tsp, tlp]
    html += wtemplate % tuple(field)
    html += "</tbody></table>"
    return html


def get_label_report(collection='neuronMetadata', dsl=None):
    ''' Return a report on a data set label (or all of them)
        Keyword arguments:
          collection: MongoDB collection
          dsl: data set label (for filtering)
        Returns:
          Aggregate rows
    '''
    if dsl:
        payload = [{"$match": {"datasetLabels": dsl}},
                   {"$project": {"alignmentSpace": 1, "anatomicalArea": 1, "objective": 1}},
                   {"$group" : {"_id": {"alignmentSpace": "$alignmentSpace",
                                        "anatomicalArea": "$anatomicalArea",
                                        "objective": "$objective"}, "count": {"$sum": 1}}},
                   {"$sort" :{"_id.alignmentSpace": 1, "_id.anatomicalArea": 1, "_id.objective": 1}}
]
    else:
        payload = [{"$unwind": "$datasetLabels"},
                   {"$project": {"_id": 0, "libraryName": 1, "datasetLabels": 1}},
                   {"$group": {"_id": {"lib": "$libraryName",
                                       "label": "$datasetLabels"}, "count":{"$sum": 1}}},
                   {"$sort": {"_id.lib": 1, "_id.label": 1}}]
    coll = DB["neuronbridge"][collection]
    return coll.aggregate(payload)


def get_lmrelease(dsl):
    ''' Return the row from lmRelease for an ALPS release
        Keyword arguments:
          dsl: data set label (for filtering)
        Returns:
          Single row from lmRelease
    '''
    payload = {'release': dsl}
    coll = DB["neuronbridge"]['lmRelease']
    return coll.find_one(payload)


def get_tag_report(collection='neuronMetadata', row='$tags'):
    ''' Return a report on tags for data all libraries
        Keyword arguments:
          collection: MongoDB collection
          row: row to query
        Returns:
          Aggregate rows
    '''
    payload = [{"$unwind": row},
               {"$project": {"_id": 0, "libraryName": 1, "alignmentSpace": 1,
                             row.replace("$", ""): 1}},
               {"$group": {"_id": {"lib": "$libraryName", "tag": row,
                                   "template": "$alignmentSpace"}, "count":{"$sum": 1}}},
               {"$sort": {"_id.lib": 1, "_id.template": 1, "_id.tag": 1}}]
    coll = DB["neuronbridge"][collection]
    return coll.aggregate(payload)


# *****************************************************************************
# * Tags                                                                      *
# *****************************************************************************

@app.route('/tags')
@app.route('/url_tags')
def show_tags():
    ''' Endpoint to display tag report
        Keyword arguments:
          None
        Returns:
          HTML page
    '''
    title = "<h2 class='hhmigreen3'>" \
            + f"{'publishedURL Tags' if 'url_tags' in request.path else 'Tags'}</h2>"
    html, _, wtemplate = generate_report_head(['Library', 'Template', 'Tag', 'MIPs'])
    if 'url_tags' in request.path:
        results = get_tag_report("publishedURL")
    else:
        results = get_tag_report()
    for row in results:
        if not re.search(r"^\d\.", row['_id']['tag']) and row['_id']['tag'] != 'unreleased':
            continue
        field = [row['_id'][k] for k in ['lib', 'template', 'tag']]
        field.append(f"{row['count']:,}")
        html += wtemplate % tuple(field)
    html += "</tbody></table>"
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         navbar=generate_navbar('Tags'),
                                         title=title, content=html))


# *****************************************************************************
# * Labels                                                                    *
# *****************************************************************************

@app.route('/')
@app.route('/labels')
def show_labels():
    ''' Endpoint to display data set labels report
        Keyword arguments:
          None
        Returns:
          HTML page
    '''
    title = "<h2 class='hhmigreen3'>Data set labels</h2>"
    html, _, wtemplate = generate_report_head(['Library', 'Label', 'MIPs'])
    results = get_label_report()
    for row in results:
        field = [row['_id'][k] for k in ['lib', 'label']]
        field[1] = f"<a href='/label/{field[1]}'>{field[1]}</a>"
        field.append(f"{row['count']:,}")
        html += wtemplate % tuple(field)
    html += "</tbody></table>"
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         navbar=generate_navbar('Labels'),
                                         title=title, content=html))


@app.route('/label/<string:dsl>', methods=['GET'])
def show_label(dsl=""):
    ''' Endpoint to report for a specific data set label
        Keyword arguments:
          dsl: data set label
        Returns:
          HTML page
    '''
    title = f"<h2 class='hhmigreen3'>Data set {dsl}</h2>"
    headings = ['Alignment space', 'MIPs', 'Samples', 'Lines'] if ':v' in dsl else \
               ['Alignment space', 'Anatomical area', 'Objective', 'MIPs', 'Samples', 'Lines']
    html, _, wtemplate = generate_report_head(headings)
    results = get_label_report('neuronMetadata', dsl)
    columns = ['alignmentSpace'] if ':v' in dsl else ['alignmentSpace', 'anatomicalArea',
                                                      'objective']
    total = {'samples': f"{distinct_count(dsl, 'sourceRefId'):,}",
             'samplesp': f"{distinct_count(dsl, 'sampleRef', 'publishedURL'):,}",
             'lines': f"{distinct_count(dsl, 'publishedName'):,}",
             'linesp': f"{distinct_count(dsl, 'publishedName', 'publishedURL'):,}",
             'mips': 0}
    for row in results:
        field = [row['_id'][k] for k in columns]
        samples = distinct_count(dsl, 'sourceRefId', columns=columns, fields=field)
        lines = distinct_count(dsl, 'publishedName', columns=columns, fields=field)
        total['mips'] += row['count']
        field.append(f"{row['count']:,}")
        field.append(f"{samples:,}")
        field.append(f"{lines:,}")
        html += wtemplate % tuple(field)
    if ':v' in dsl:
        html += wtemplate % ('TOTALS', f"{total['mips']:,}", total['samples'], total['lines'])
    else:
        html += wtemplate % ('', '', 'TOTALS', f"{total['mips']:,}", total['samples'],
                             total['lines'])
    html += "</tbody></table>"
    # Process count table
    if ':v' not in dsl:
        html += generate_count_table(dsl, total)
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         navbar=generate_navbar('Labels'),
                                         title=title, content=html))

# *****************************************************************************
# * EM bodies                                                                 *
# *****************************************************************************

@app.route('/em_bodies')
def show_embodies():
    ''' Endpoint to display data set labels report
        Keyword arguments:
          None
        Returns:
          HTML page
    '''
    title = "<h2 class='hhmigreen3'>EM bodies</h2>"
    html, _, wtemplate = generate_report_head(['Name', 'Version', 'Anatomical area', 'Bodies'])
    coll = DB["jacs"]['emDataSet']
    results = coll.find({})
    dset = {}
    for row in results:
        if row['version']:
            dset[f"{row['name']}:v{row['version']}"] = row['anatomicalArea']
        else:
            dset[row['name']] = row['anatomicalArea']
    payload = [{"$project": {"dataSetIdentifier": 1}},
               {"$group" : {"_id": {"dataSetIdentifier": "$dataSetIdentifier"},
                            "count": {"$sum": 1}}},
               {"$sort" :{"_id.dataSetIdentifier": 1}}]
    coll = DB["jacs"]['emBody']
    results = coll.aggregate(payload)
    for row in results:
        dsi = row['_id']['dataSetIdentifier']
        nfield = dsi.split(":")
        field = [nfield[0]]
        field.append(nfield[1] if len(nfield) > 1 else "")
        field.append(dset[dsi] if dsi in dset else '')
        field.append(f"{row['count']:,}")
        html += wtemplate % tuple(field)
    html += "</tbody></table>"
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         navbar=generate_navbar('EM bodies'),
                                         title=title, content=html))

# *****************************************************************************
# * Publishing name                                                           *
# *****************************************************************************

@app.route('/search', methods=['GET'])
def show_search():
    return make_response(render_template('search.html', urlroot=request.url_root,
                                         navbar=generate_navbar('Search')))
    

@app.route('/pname/<string:key>', methods=['GET'])
def show_pname(key=""):
    ''' Find a publishing name (or body ID or neuron type)
        Keyword arguments:
          dsl: data set label
        Returns:
          HTML page
    '''
    title = f"<h2 class='hhmigreen3'>{key}</h2>"
    html = ""
    fqn = key
    changed = False
    for tbl in ('neuronMetadata', 'publishedURL'):
        coll = DB["neuronbridge"][tbl]
        results = coll.find({"publishedName": key if tbl == 'neuronMetadata' else fqn})
        recs = []
        for row in results:
            if key.isdigit() and not changed:
                match = re.search(r'flyem_([A-Za-z_]+)(.+)', row['libraryName'])
                if match:
                    fqn = ':'.join([match.group(1),
                                    match.group(2).replace('_', '.')]).replace('_:', ':v')
                    fqn = ':'.join([fqn, key])
                    changed = True
            recs.append(json.dumps(row, indent=4, default=str))
        if recs:
            html += f"<h4>{tbl}</h4>"
            jstr = ",\n"
            html += f"<pre>{jstr.join(recs)}</pre>"
    tbl = app.config['PTABLE']
    try:
        response = DB[tbl].query(KeyConditionExpression= \
                                 Key('itemType').eq('searchString') \
                                     & Key('searchKey').eq(key.lower()))
    except Exception as err:
        return database_error(err)
    if 'Count' in response and response['Count'] and 'Items' in response and response['Items'][0]:
        html += f"<h4>{app.config['DDBASE']}-{tbl}</h4>"
        html += f"<pre>{json.dumps(response['Items'][0], indent=4)}</pre>"
    tbl = 'publishing-doi'
    try:
        response = DB[tbl].query(KeyConditionExpression=Key('name').eq(fqn))
    except Exception as err:
        return database_error(err)
    if 'Count' in response and response['Count'] and 'Items' in response \
       and response['Items'][0]:
        html += f"<h4>{app.config['DDBASE']}-{tbl}</h4>"
        html += f"<pre>{json.dumps(response['Items'][0], indent=4)}</pre>"
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         navbar=generate_navbar('EM bodies'),
                                         title=title, content=html))

# *****************************************************************************

if __name__ == '__main__':
    app.run(debug=app.config['DEBUG'])
