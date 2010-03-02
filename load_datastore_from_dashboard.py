import urllib
import urllib2
import cookielib
import cjson

from schemas import datastore

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

class DatastoreManager(object):
    
    def __init__(self):
        
        self.db = datastore.datastore()
        
        self.partners = self.db.aquire_table_partner()
        self.clients = self.db.aquire_table_client()
        self.pixels = self.db.aquire_table_pixel()
        self.piggyback_pixels = self.db.aquire_table_piggyback_pixel()
        self.creatives = self.db.aquire_table_creative()
        self.budgets = self.db.aquire_table_budget()
        self.learning_budgets = self.db.aquire_table_learning_budget()
        self.insertion_orders = self.db.aquire_table_insertion_order()
        self.target_option_names = self.db.aquire_table_target_option_name()
        self.target_option_values = self.db.aquire_table_target_option_value()
        self.frequency_caps = self.db.aquire_table_frequency_cap()
        self.campaigns = self.db.aquire_table_campaign()
        self.campaign_target_options = self.db.aquire_table_campaign_target_option()
        self.client_goals = self.db.aquire_table_client_goal()
        self.client_goal_pixel = self.db.aquire_table_client_goal_pixel()
        self.payment_items = self.db.aquire_table_payment_item()
        self.payment_item_pixel = self.db.aquire_table_payment_item_pixel()
        self.bidding_strategies = self.db.aquire_table_bidding_strategy()
        self.line_items = self.db.aquire_table_line_item()
        self.inventory_sources = self.db.aquire_table_inventory_source()
        self.inventory_groups = self.db.aquire_table_inventory_group()
        self.inventory_units = self.db.aquire_table_inventory_unit()
        self.inventory_sizes = self.db.aquire_table_inventory_size()
        self.inventory_size_code = self.db.aquire_table_inventory_size_code()
        self.publisher_line_items = self.db.aquire_table_publisher_line_item()
        self.publisher_line_item_default_creatives = self.db.aquire_table_publisher_line_item_default_creative()
        self.publisher_line_item_target_options = self.db.aquire_table_publisher_line_item_target_option()
        self.universal_sites = self.db.aquire_table_universal_site()
        self.universal_site_lookups = self.db.aquire_table_universal_site_lookup()
        
        self.__next_topt_name_id = 1
        self.__next_topt_value_id = 1
        self.__rest_attempts = set()
        
        # generate _check & load methods for core entity types
        for ent_name, rest_path in rest_paths.iteritems():
            self._bind_check_and_load(ent_name, rest_path)
        return
    
    def _bind_check_and_load(self, ent_name, rest_path):
        store = eval('self.store_%s' % ent_name)
        
        def load(id):
            assert False # HACK
            assert id not in self.__rest_attempts, 'Already attempted'
            self.__rest_attempts.add(id)
            content = request('%s&id=%d' % (rest_path, id))
            return store( cjson.decode(content)[0])
        
        self.__dict__['load_%s' % ent_name] = load
        
        tab = self.__dict__['%ss' % ent_name]
        
        def check(o):
            if o == None: return None
            
            # o is a model dictionary?
            if isinstance(o, dict):
                if tab.get_id(o['pk']):
                    return o['pk']
                else:
                    return store(o)
            
            if tab.get_id(o):
                return o
            else:
                return load(o)
        
        self.__dict__['_check_%s' % ent_name] = check
        return
    
    @staticmethod
    def _freplace(tbl, kls, d):
        """Filters d to fields present in class kls, & replaces by id into table"""
        d = dict((i,j) for (i,j) in d.iteritems() if i in kls.__dict__)
        
        tbl.replace_id(d['id'], kls( **d))
        return d['id']
    
    def _store_target_options(self, arg_bag, target_options, kls, tbl):
        
        for topt_name, topt_values in target_options.iteritems():
            
            # Intern topt name string
            tn = self.target_option_names.get_name(topt_name)
            if not tn:
                tn = datastore.target_option_name(
                    id = self.__next_topt_name_id, name = topt_name)
                self.target_option_names.insert( tn)
                self.__next_topt_name_id += 1
            
            arg_bag['name_id'] = tn.id
            
            if topt_values[1] == True:
                arg_bag['exclude'] = False
                self._store_target_option_vals(arg_bag, topt_values[0], kls, tbl)
            elif topt_values[1] == False:
                arg_bag['exclude'] = True
                self._store_target_option_vals(arg_bag, topt_values[0], kls, tbl)
            else:
                arg_bag['exclude'] = False
                self._store_target_option_vals(arg_bag, [
                    i for i,j in zip(*topt_values) if j], kls, tbl)
                arg_bag['exclude'] = True
                self._store_target_option_vals(arg_bag, [
                    i for i,j in zip(*topt_values) if not j], kls, tbl)
        
        return
    
    def _store_target_option_vals(self, arg_bag, topt_values, kls, tbl):
        
        for topt_val in topt_values:
            
            # Intern topt value string
            tv = self.target_option_values.get_value(topt_val)
            if not tv:
                tv = datastore.target_option_value(
                    id = self.__next_topt_value_id, value = topt_val)
                self.target_option_values.insert( tv)
                self.__next_topt_value_id += 1
            
            arg_bag['value_id'] = tv.id
            tbl.insert( kls( **arg_bag))
        
        return
    
    def store_partner(self, inst):
        inst = inst['fields']
        # Flatten exchange permissions
        inst.update( inst['exchange_permissions'].iteritems())
        return self._freplace( self.partners, datastore.partner, inst)
    
    def store_client(self, inst):
        inst = inst['fields']
        inst['partner_id'] = self._check_partner( inst['partner'])
        return self._freplace( self.clients, datastore.client, inst)
    
    def store_pixel(self, inst):
        inst = inst['fields']
        inst['partner_id'] = self._check_partner( inst['partner'])
        inst['client_id']  = self._check_client( inst['client'])
        id = self._freplace(self.pixels, datastore.pixel, inst)
        return
    
    def store_piggyback_pixel(self, inst):
        inst['fields']['id'] = inst['pk']
        inst = inst['fields']
        inst['pixel_id'] = self._check_pixel( inst['pixel'])
        return self._freplace(self.piggyback_pixels, datastore.piggyback_pixel, inst)
    
    def store_creative(self, inst):
        inst = inst['fields']
        # creative embeddeds a stripped-down client instance
        inst['client_id'] = self._check_client( inst['client']['pk'])
        return self._freplace(self.creatives, datastore.creative, inst)
    
    def store_budget(self, inst):
        inst = inst['fields']
        return self._freplace(self.budgets, datastore.budget, inst)
    
    def store_learning_budget(self, inst):
        inst = inst['fields']
        return self._freplace(self.learning_budgets, datastore.learning_budget, inst)
    
    def store_insertion_order(self, inst):
        inst = inst['fields']
        
        inst['client_id'] = self._check_client(inst['client'])
        inst['budget_id'] = self._check_budget(inst['budget'])
        return self._freplace(self.insertion_orders, datastore.insertion_order, inst)
    
    def store_campaign(self, inst):
        inst = inst['fields']
        inst['insertion_order_id'] = self._check_insertion_order(inst['insertion_order'])
        inst['learning_budget_id'] = self._check_learning_budget(inst['learning_budget'])
        inst['budget_id'] = self._check_budget(inst['budget'])
        
        id = self._freplace(self.campaigns, datastore.campaign, inst)
        
        # These come down w/ the campaign each time
        self.frequency_caps.delete_campaign_id( id)
        self.store_frequency_cap(inst['frequency_cap'])
        
        self.client_goals.delete_campaign_id( id)
        for cgoal in inst['client_goals']:
            self.store_client_goal(cgoal)
        
        self.payment_items.delete_campaign_id( id)
        for pitem in inst['payment_items']:
            self.store_payment_item(pitem)
        
        return 
    
    def store_frequency_cap(self, inst):
        inst = inst['fields']
        inst['campaign_id'] = self._check_campaign(inst['campaign'])
        return self._freplace(self.frequency_caps, datastore.frequency_cap, inst)
    
    def store_client_goal(self, inst):
        inst = inst['fields']
        inst['campaign_id'] = self._check_campaign(inst['campaign'])
        id = self._freplace(self.client_goals, datastore.client_goal, inst)
        
        # Update client goal => pixel mapping
        self.client_goal_pixel.delete_client_goal_id( id)
        if inst['pixel']:
            self._check_pixel( inst['pixel'])
            self.client_goal_pixel.insert( datastore.client_goal_pixel(
                client_goal_id = id, pixel_id = inst['pixel']))
        return id
    
    def store_payment_item(self, inst):
        inst = inst['fields']
        inst['campaign_id'] = self._check_campaign(inst['campaign'])
        id = self._freplace(self.payment_items, datastore.payment_item, inst)
        
        # Update payment item => pixel mapping
        self.payment_item_pixel.delete_payment_item_id( id)
        if inst['pixel']:
            self._check_pixel( inst['pixel'])
            self.payment_item_pixel.insert( datastore.payment_item_pixel(
                payment_item_id = id, pixel_id = inst['pixel']))
        return id
    
    def store_bidding_strategy(self, inst):
        inst = inst['fields']
        inst['campaign_id'] = self._check_campaign(inst['campaign'])
        return self._freplace(self.bidding_strategies, datastore.bidding_strategy, inst)
    
    def store_line_item(self, inst):
        inst = inst['fields']
        
        inst['campaign_id'] = self._check_campaign(inst['campaign'])
        inst['creative_id'] = self._check_creative(inst['creative'])
        
        id = self._freplace(self.line_items, datastore.line_item, inst)
        
        for bid_strat in inst['bidding_strategies']:
            bid_strat_id = self.store_bidding_strategy(bid_strat)
        
        # Campaign target options come down with each line item
        self.campaign_target_options.delete_campaign_id( inst['campaign_id'])
        arg_bag = {'campaign_id': inst['campaign_id']}
        self._store_target_options(
            arg_bag, inst['target_options'],
            datastore.campaign_target_option,
            self.campaign_target_options
        )
        return id
    
    def store_inventory_source(self, inst):
        inst = inst['fields']
        inst['partner_id'] = self._check_partner(inst['partner'])
        inst['piggyback_pixel_id'] = self._check_piggyback_pixel(inst['piggyback_pixel'])
        return self._freplace(self.inventory_sources, datastore.inventory_source, inst)
    
    def store_inventory_group(self, inst):
        inst = inst['fields']
        inst['inventory_source_id'] = self._check_inventory_source(inst['inventory_source'])
        return self._freplace(self.inventory_groups, datastore.inventory_group, inst)
    
    def store_inventory_unit(self, inst):
        inst = inst['fields']
        inst['inventory_group_id'] = self._check_inventory_group(inst['inventory_group'])
        return self._freplace(self.inventory_units, datastore.inventory_unit, inst)
    
    def store_inventory_size(self, inst):
        inst = inst['fields']
        inst['partner_id'] = self._check_partner(inst['partner'])
        inst['inventory_unit_id'] = self._check_inventory_unit(inst['inventory_unit'])
        inst['inventory_source_id'] = self._check_inventory_source(inst['inventory_source'])
        
        id = self._freplace(self.inventory_sizes, datastore.inventory_size, inst)
        
        # update code lookup
        self.inventory_size_code.delete_inventory_size_id( id)
        ucode = inst.get('unit_integration_code')
        gcode = inst.get('group_integration_code')
        if ucode or gcode:
            self.inventory_size_code.insert( datastore.inventory_size_code(
                inventory_size_id = id,
                is_group = False if ucode else True,
                integration_code = ucode or gcode,
                partner_id = inst['partner_id'],
                width  = inst['width'],
                height = inst['height'],
            ))
        return id
    
    def store_publisher_line_item(self, inst):
        inst = inst['fields']
        inst['partner_id'] = self._check_partner(inst['partner'])
        inst['inventory_source_id'] = self._check_inventory_source(inst['inventory_source'])
        
        id = self._freplace(self.publisher_line_items, datastore.publisher_line_item, inst)
        
        # Default creatives
        self.publisher_line_item_default_creatives.delete_publisher_line_item_id( id)
        for size, creative_id in inst['default_creatives'].iteritems():
            self._check_creative( creative_id)
            width, height = [int(i) for i in size.split('x')]
            
            self.publisher_line_item_default_creatives.insert(
                datastore.publisher_line_item_default_creative(
                    publisher_line_item_id = id,
                    width = width, height = height, creative_id = creative_id))
        
        # Target options
        self.publisher_line_item_target_options.delete_publisher_line_item_id( id)
        arg_bag = {'publisher_line_item_id': id}
        self._store_target_options(
            arg_bag, inst['target_options'],
            datastore.publisher_line_item_target_option,
            self.publisher_line_item_target_options
        )
        return
    
    def store_publisher_line_item_default_creative(self, inst):
        inst = inst['fields']
        inst['creative_id'] = self._check_creative(inst['creative'])
        inst['publisher_line_item_id'] = \
            self._check_publisher_line_item(inst['publisher_line_item'])
        return self._freplace(self.publisher_line_item_default_creatives,
            datastore.publisher_line_item_default_creative, inst)
    
    def store_universal_site(self, inst):
        inst = inst['fields']
        
        id = self._freplace(self.universal_sites, datastore.universal_site, inst)
        
        # Update lookups
        self.universal_site_lookups.delete_universal_site_id( id)
        for lookup in inst['lookups']:
            self.store_universal_site_lookup(lookup)
        return id
    
    def store_universal_site_lookup(self, inst):
        inst = inst['fields']
        
        inst['universal_site_id'] = self._check_universal_site(inst['universal_site'])
        return self._freplace(self.universal_site_lookups, datastore.universal_site_lookup, inst)

dstore = DatastoreManager()

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

ent_types = [
    ('../dumps/partners.dump', dstore.store_partner),
    ('../dumps/clients.dump', dstore.store_client),
    ('../dumps/pixels.dump', dstore.store_pixel),
    ('../dumps/piggyback_pixels.dump', dstore.store_piggyback_pixel),
    ('../dumps/creatives.dump', dstore.store_creative),
    ('../dumps/budgets.dump', dstore.store_budget),
    ('../dumps/learning_budgets.dump', dstore.store_learning_budget),
    ('../dumps/insertion_orders.dump', dstore.store_insertion_order),
    ('../dumps/campaigns.dump', dstore.store_campaign),
    ('../dumps/line_items.dump', dstore.store_line_item),
    ('../dumps/inventory_sources.dump', dstore.store_inventory_source),
    ('../dumps/inventory_groups.dump', dstore.store_inventory_group),
    ('../dumps/inventory_units.dump', dstore.store_inventory_unit),
    ('../dumps/inventory_sizes.dump', dstore.store_inventory_size),
    ('../dumps/publisher_line_items.dump', dstore.store_publisher_line_item),
    ('../dumps/universal_sites.dump', dstore.store_universal_site),
]

for (path, loader) in ent_types:
    for inst in open(path):
        try:
            loader( cjson.decode(inst))
        except Exception, e:
            if e.message:
                print e.message

