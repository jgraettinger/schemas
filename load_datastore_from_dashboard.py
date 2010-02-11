

import cjson
import uuid
from schemas import datastore

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
        self.frequency_caps = self.db.aquire_table_frequency_cap()
        self.campaigns = self.db.aquire_table_campaign()
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
        # creative's client is incomplete
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
        
        id = self._freplace(self.line_items, datastore.line_item, inst)
        
        for bid_strat in inst['bidding_strategies']:
            bid_strat_id = self.load_bidding_strategy(bid_strat)
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
        self.move(inst, 'partner', 'partner_id')
        self.move(inst, 'insertion_order', 'insertion_order_id')
        self.move(inst, 'inventory_source', 'inventory_source_id')
        return self._freplace(self.publisher_line_items, datastore.publisher_line_item, inst)


dstore = DatastoreManager()

for o in cjson.decode(open('../dumps/partners.dump').read()):
    dstore.load_partner(o)

for o in cjson.decode(open('../dumps/clients.dump').read()):
    dstore.load_client(o)

for o in cjson.decode(open('../dumps/pixels.dump').read()):
    dstore.load_pixel(o)

for o in cjson.decode(open('../dumps/piggyback_pixels.dump').read()):
    dstore.load_piggyback_pixel(o)

for o in cjson.decode(open('../dumps/creatives.dump').read()):
    dstore.load_creative(o)

for o in cjson.decode(open('../dumps/budgets.dump').read()):
    dstore.load_budget(o)

for o in cjson.decode(open('../dumps/insertion_orders.dump').read()):
    dstore.load_insertion_order(o)

for o in cjson.decode(open('../dumps/campaigns.dump').read()):
    dstore.load_campaign(o)

for o in cjson.decode(open('../dumps/line_items.dump').read()):
    if o['pk'] in (52,): continue
    dstore.load_line_item(o)

for o in cjson.decode(open('../dumps/inventory_sources.dump').read()):
    dstore.load_inventory_source(o)

for o in cjson.decode(open('../dumps/inventory_groups.dump').read()):
    dstore.load_inventory_group(o)

for o in cjson.decode(open('../dumps/inventory_units.dump').read()):
    dstore.load_inventory_unit(o)

for o in cjson.decode(open('../dumps/inventory_sizes.dump').read()):
    dstore.load_inventory_size(o)

