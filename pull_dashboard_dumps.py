import urllib
import urllib2
import cookielib
import cjson
import sys

#http_base = "http://inst1.dev.dashboard.invitemedia.com"
#credentials = {'username': 'superuser', 'password': 'imim42'}

http_base = "http://ev3.dash.invitemedia.com:1234"
credentials = {'username': 'internal_api_user', 'password': 'PMVCR24m'}

rest_paths = {
    'partner':          '/partners/?response_type=json',
    'client':           '/clients/?response_type=json',
    'pixel':            '/pixels/?response_type=json',
    'piggyback_pixel':  '/piggyback_pixels/?response_type=json',
    'creative':         '/creatives/?response_type=json',
    'budget':           '/budgets/?response_type=json',
    'learning_budget':  '/learning_budgets/?response_type=json',
    'insertion_order':  '/insertion_orders/?response_type=json',
    'campaign':         '/campaigns/?response_type=json',
    'line_item':        '/line_items/?target_options=True&response_type=json',
    'inventory_source': '/inventory_sources/?response_type=json',
    'inventory_group':  '/inventory_groups/?response_type=json',
    'inventory_unit':   '/inventory_units/?response_type=json',
    'inventory_size':   '/inventory_sizes/?response_type=json',
    'publisher_line_item': '/publisher_lineitems/?target_options=True&response_type=json',
    'universal_site':   '/universal_sites/?response_type=json',
}

cjar = cookielib.CookieJar()

def request(path, data = None):
    path = '%s%s' % (http_base, path)
    print path
    req = urllib2.Request(path, data = data)
    
    h = urllib2.OpenerDirector()
    h.add_handler(urllib2.HTTPHandler()) #debuglevel = 1))
    h.add_handler(urllib2.HTTPCookieProcessor(cjar))
    return h.open(req).read()

request('/login/')
request('/login/', data = urllib.urlencode(credentials))

for (ent_type, path) in rest_paths.iteritems():
    ents = request(path)
    ents = cjson.decode(ents)
    open('%s.dump' % ent_type, 'w').write( '\n'.join(cjson.encode(i) for i in ents))

