import urllib
import urllib2
import cookielib
import cjson

from schemas import datastore

http_base = "http://inst1.dev.dashboard.invitemedia.com"
credentials = {'username': 'superuser', 'password': 'imim42'}

#http_base = "http://ev3.dash.invitemedia.com:82"
#credentials = {'username': 'internal_api_user', 'password': 'PMVCR24m'}

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
        return
    
    @staticmethod
    def _move(d, old_name, new_name):
        d[new_name] = d[old_name]
        del d[old_name]
    
    @staticmethod
    def _freplace(tbl, kls, d):
        """Filters d to fields present in class kls, & replaces by id into table"""
        d = dict((i,j) for (i,j) in d.iteritems() if i in kls.__dict__)
        
        tbl.replace_id(d['id'], kls( **d))
        return d['id']
    
    def _load_target_options(self, arg_bag, target_options, kls, tbl):
        
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
                self._load_target_option_vals(arg_bag, topt_values[0], kls, tbl)
            elif topt_values[1] == False:
                arg_bag['exclude'] = True
                self._load_target_option_vals(arg_bag, topt_values[0], kls, tbl)
            else:
                arg_bag['exclude'] = False
                self._load_target_option_vals(arg_bag, topt_values[0], kls, tbl)
                arg_bag['exclude'] = True
                self._load_target_option_vals(arg_bag, topt_values[1], kls, tbl)
        
        return
    
    def _load_target_option_vals(self, arg_bag, topt_values, kls, tbl):
        
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
    
    def load_partner(self, inst):
        inst = inst['fields']
        # Flatten exchange permissions
        inst.update( inst['exchange_permissions'].iteritems())
        return self._freplace( self.partners, datastore.partner, inst)
    
    def load_client(self, inst):
        inst = inst['fields']
        self._move(inst, 'partner', 'partner_id')
        return self._freplace( self.clients, datastore.client, inst)
    
    def load_pixel(self, inst):
        inst = inst['fields']
        self._move(inst, 'partner', 'partner_id')
        self._move(inst, 'client', 'client_id')
        id = self._freplace(self.pixels, datastore.pixel, inst)
        return
    
    def load_piggyback_pixel(self, inst):
        inst['fields']['id'] = inst['pk']
        inst = inst['fields']
        self._move(inst, 'pixel', 'pixel_id')
        return self._freplace(self.piggyback_pixels, datastore.piggyback_pixel, inst)
    
    def load_creative(self, inst):
        inst = inst['fields']
        inst['client_id'] = inst['client']['pk']
        return self._freplace(self.creatives, datastore.creative, inst)
    
    def load_budget(self, inst):
        inst = inst['fields']
        return self._freplace(self.budgets, datastore.budget, inst)
    
    def load_learning_budget(self, inst):
        inst = inst['fields']
        return self._freplace(self.learning_budgets, datastore.learning_budget, inst)
    
    def load_insertion_order(self, inst):
        inst = inst['fields']
        
        if isinstance(inst['client'], dict):
            inst['client_id'] = self.load_client(inst['client'])
        else:
            self._move(inst, 'client', 'client_id')
        
        if isinstance(inst['budget'], dict):
            inst['budget_id'] = self.load_budget(inst['budget'])
        else:
            self._move(inst, 'budget', 'budget_id')
        
        return self._freplace(self.insertion_orders, datastore.insertion_order, inst)
    
    def load_campaign(self, inst):
        inst = inst['fields']
        inst['insertion_order_id'] = self.load_insertion_order(inst['insertion_order'])
        inst['learning_budget_id'] = self.load_learning_budget(inst['learning_budget'])
        inst['budget_id'] = self.load_budget(inst['budget'])
        
        id = self._freplace(self.campaigns, datastore.campaign, inst)
        
        # These come down w/ the campaign each time
        self.frequency_caps.delete_campaign_id( id)
        self.load_frequency_cap(inst['frequency_cap'])
        
        self.client_goals.delete_campaign_id( id)
        for cgoal in inst['client_goals']:
            self.load_client_goal(cgoal)
        
        self.payment_items.delete_campaign_id( id)
        for pitem in inst['payment_items']:
            self.load_payment_item(pitem)
        
        return 
    
    def load_frequency_cap(self, inst):
        inst = inst['fields']
        self._move(inst, 'campaign', 'campaign_id')
        return self._freplace(self.frequency_caps, datastore.frequency_cap, inst)
    
    def load_client_goal(self, inst):
        inst = inst['fields']
        self._move(inst, 'campaign', 'campaign_id')
        id = self._freplace(self.client_goals, datastore.client_goal, inst)
        
        # Update client goal => pixel mapping
        self.client_goal_pixel.delete_client_goal_id( id)
        if inst['pixel']:
            self.client_goal_pixel.insert( datastore.client_goal_pixel(
                client_goal_id = id, pixel_id = inst['pixel']))
        return id
    
    def load_payment_item(self, inst):
        inst = inst['fields']
        self._move(inst, 'campaign', 'campaign_id')
        id = self._freplace(self.payment_items, datastore.payment_item, inst)
        
        # Update payment item => pixel mapping
        self.payment_item_pixel.delete_payment_item_id( id)
        if inst['pixel']:
            self.payment_item_pixel.insert( datastore.payment_item_pixel(
                payment_item_id = id, pixel_id = inst['pixel']))
        return id
    
    def load_bidding_strategy(self, inst):
        inst = inst['fields']
        self._move(inst, 'campaign', 'campaign_id')
        return self._freplace(self.bidding_strategies, datastore.bidding_strategy, inst)
    
    def load_line_item(self, inst):
        inst = inst['fields']
        
        self._move(inst, 'campaign', 'campaign_id')
        self._move(inst, 'creative', 'creative_id')
        self._move(inst, 'insertion_order', 'insertion_order_id')
        
        # DIRTY HACK -- Dashboard DB currently violates creative/campaign
        #  line item uniqueness. We have to account for it here.
        temp_li = self.line_items.get_campaign_and_creative((inst['campaign_id'], inst['creative_id']))
        if temp_li and temp_li.id != inst['id']:
            print "Skipping %r" % inst
            return
        # End Hack
        
        id = self._freplace(self.line_items, datastore.line_item, inst)
        
        for bid_strat in inst['bidding_strategies']:
            bid_strat_id = self.load_bidding_strategy(bid_strat)
        
        # Campaign target options come down with each line item
        self.campaign_target_options.delete_campaign_id( inst['campaign_id'])
        arg_bag = {'campaign_id': inst['campaign_id']}
        self._load_target_options(
            arg_bag, inst['target_options'],
            datastore.campaign_target_option,
            self.campaign_target_options
        )
        return id
    
    def load_inventory_source(self, inst):
        inst = inst['fields']
        self._move(inst, 'partner', 'partner_id')
        self._move(inst, 'piggyback_pixel', 'piggyback_pixel_id')
        return self._freplace(self.inventory_sources, datastore.inventory_source, inst)
    
    def load_inventory_group(self, inst):
        inst = inst['fields']
        self._move(inst, 'inventory_source', 'inventory_source_id')
        return self._freplace(self.inventory_groups, datastore.inventory_group, inst)
    
    def load_inventory_unit(self, inst):
        inst = inst['fields']
        self._move(inst, 'inventory_group', 'inventory_group_id')
        return self._freplace(self.inventory_units, datastore.inventory_unit, inst)
    
    def load_inventory_size(self, inst):
        inst = inst['fields']
        self._move(inst, 'partner', 'partner_id')
        self._move(inst, 'inventory_unit', 'inventory_unit_id')
        self._move(inst, 'inventory_source', 'inventory_source_id')
        
        id = self._freplace(self.inventory_sizes, datastore.inventory_size, inst)
        
        # update code lookup
        self.inventory_size_code.delete_inventory_size_id( id)
        ucode = inst.get('unit_integration_code')
        gcode = inst.get('group_integration_code')
        if ucode or gcode:
            self.inventory_size_code.insert( datastore.inventory_size_code(
                inventory_size_id = id,
                is_group = True if gcode else False,
                integration_code = gcode or ucode,
                partner_id = inst['partner_id'],
                width = inst['width'],
                height = inst['height'],
            ))
        return id
    
    def load_publisher_line_item(self, inst):
        inst = inst['fields']
        self._move(inst, 'partner', 'partner_id')
        self._move(inst, 'inventory_source', 'inventory_source_id')
        
        id = self._freplace(self.publisher_line_items, datastore.publisher_line_item, inst)
        
        for dc_inst in inst['default_creatives'].values():
            self.load_publisher_line_item_default_creative(dc_inst)
        
        # Target options
        self.publisher_line_item_target_options.delete_publisher_line_item_id( id)
        arg_bag = {'publisher_line_item_id': id}
        self._load_target_options(
            arg_bag, inst['target_options'],
            datastore.publisher_line_item_target_option,
            self.publisher_line_item_target_options
        )
        return
    
    def load_publisher_line_item_default_creative(self, inst):
        inst = inst['fields']
        self.move(inst, 'creative', 'creative_id')
        self.move(inst, 'publisher_line_item', 'publisher_line_item_id')
        return self._freplace(self.publisher_line_item_default_creatives,
            datastore.publisher_line_item_default_creative, inst)
    
    def load_universal_site(self, inst):
        inst = inst['fields']
        
        id = self._freplace(self.universal_sites, datastore.universal_site, inst)
        
        # Update lookups
        self.universal_site_lookups.delete_universal_site_id( id)
        for lookup in inst['lookups']:
            self.load_universal_site_lookup(lookup)
        return id
    
    def load_universal_site_lookup(self, inst):
        inst = inst['fields']
        self._move(inst, 'universal_site', 'universal_site_id')
        return self._freplace(self.universal_site_lookups, datastore.universal_site_lookup, inst)

dstore = DatastoreManager()

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
    ('/partners/?response_type=json', dstore.load_partner),
    ('/clients/?response_type=json', dstore.load_client),
    ('/pixels/?response_type=json', dstore.load_pixel),
    ('/piggyback_pixels/?response_type=json', dstore.load_piggyback_pixel),
    ('/creatives/?response_type=json', dstore.load_creative),
    ('/budgets/?response_type=json', dstore.load_budget),
    ('/learning_budgets/?response_type=json', dstore.load_learning_budget),
    ('/insertion_orders/?response_type=json', dstore.load_insertion_order),
    ('/campaigns/?response_type=json', dstore.load_campaign),
    ('/line_items/?target_options=True&response_type=json', dstore.load_line_item),
    ('/inventory_sources/?response_type=json', dstore.load_inventory_source),
    ('/inventory_groups/?response_type=json', dstore.load_inventory_group),
    ('/inventory_units/?response_type=json', dstore.load_inventory_unit),
    ('/inventory_sizes/?response_type=json', dstore.load_inventory_size),
    ('/publisher_lineitems/?target_options=True&response_type=json', dstore.load_publisher_line_item),
    ('/universal_sites/?response_type=json', dstore.load_universal_site),
]

for (path, loader) in ent_types:
    for inst in cjson.decode( request(path).read()):
        loader( inst)

