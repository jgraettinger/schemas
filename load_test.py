
import cookielib
import urllib
import urllib2
import cjson

http_base = "http://inst1.dev.dashboard.invitemedia.com"
credentials = {'username': 'superuser', 'password': 'imim42'}

#http_base = "http://ev3.dash.invitemedia.com:82"
#credentials = {'username': 'internal_api_user', 'password': 'PMVCR24m'}

cjar = cookielib.CookieJar()

def request(path, data = None):
    path = '%s%s' % (http_base, path)
    req = urllib2.Request(path, data = data)
    
    h = urllib2.OpenerDirector()
    h.add_handler(urllib2.HTTPHandler(debuglevel = 1))
    h.add_handler(urllib2.HTTPCookieProcessor(cjar))
    return h.open(req)

request('/login/')
request('/login/', data = urllib.urlencode(credentials))

ent_types = [
    'partners/?response_type=json',
    'clients/?response_type=json',
    'creatives/?response_type=json',
    'budgets/?response_type=json',
    'insertion_orders/?response_type=json',
    'learning_budgets/?response_type=json',
    'campaigns/?response_type=json',
    'pixels/?response_type=json',
    'piggyback_pixels/?response_type=json',
    'inventory_sources/?response_type=json',
    'inventory_groups/?response_type=json',
    'inventory_units/?response_type=json',
    'inventory_sizes/?response_type=json',
    'client_goals/?response_type=json',
    'line_items/?target_options=True&response_type=json',
    'publisher_lineitems/?target_options=True&response_type=json',
    'universal_sites/?response_type=json',
]

for ent_type in ent_types:
    open('%s.dump' % ent_type.split('/')[0], 'w').write( request('/%s' % ent_type).read())

