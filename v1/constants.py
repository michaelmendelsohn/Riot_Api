import json

def get_items(lol_watcher):
    latest = lol_watcher.data_dragon.versions_for_region('na1')['n']['champion']
    static_item_list = lol_watcher.data_dragon.items(latest, False)
    return {item:static_item_list['data'][item]['name'] for item in static_item_list['data'].keys()}
   
def get_spells(lol_watcher):
    latest = lol_watcher.data_dragon.versions_for_region('na1')['n']['champion']
    static_spell_list = lol_watcher.data_dragon.summoner_spells(latest, False)
    return {static_spell_list['data'][spell]['key']:static_spell_list['data'][spell]['name'] for spell  in static_spell_list['data'].keys()}

def get_queues():
    with open('queues.json') as user_file:
        queue_json = json.load(user_file)
    return { str(queue['queueId']) : queue['description'] for queue in queue_json['queues_list']}
    