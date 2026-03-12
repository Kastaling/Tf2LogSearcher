import json
import os
import sys
char = sys.argv[1]
logslist = os.listdir(r'/srv/http/tf2/Tf2LogSearcher/logs/')
logslist = [log.replace('.json','') for log in logslist]
logslist = [int(i) for i in logslist]
logslist.sort(reverse=True)
logslist = logslist[:20000]
char = sys.argv[1]
primaryusage = []
secondaryusage = []
meleeusage = []
primaries = ["tf_projectile_rocket","quake_rl","minigun","iron_bomber","shotgun_primary","scattergun","degreaser","sniperrifle","tf_projectile_arrow","compound_bow","revolver","airstrike","rocketlauncher_directhit","blackbox","flamethrower","liberty_launcher","cow_mangler","dumpster_device"]
secondaries = ["shotgun_soldier","detonator","panic_attack"]
melees = ["unique_pickaxe","disciplinary_action","market_gardener","paintrain"]
for log in logslist:
    with open(r'/srv/http/tf2/Tf2LogSearcher/logs//' + str(log) + '.json', 'r') as f:
        try:
            logtext = json.load(f)
        except:
            continue
        players = logtext["players"]
        for player in players:
            class_stats = players[player]["class_stats"]
            for stats in class_stats:
                damage = stats["dmg"]
                if stats["type"] == char:
                    weapons = stats["weapon"]
                    for weapon in weapons:
                        if any(weapon == primary for primary in primaries):
                            if not damage == 0:
                                primaryusage.append(weapons[weapon]["dmg"] / damage)
                                print(f"Adding {(weapons[weapon]['dmg'] / damage) * 100}% primary usage.")
                        elif any(weapon == secondary for secondary in secondaries):
                            if not damage == 0:
                                secondaryusage.append(weapons[weapon]["dmg"] / damage)
                                print(f"Adding {(weapons[weapon]['dmg'] / damage) * 100}% secondary usage.")
                        if any(weapon == melee for melee in melees):
                            if not damage == 0:
                                meleeusage.append(weapons[weapon]["dmg"] / damage)
                                print(f"Adding {(weapons[weapon]['dmg'] / damage) * 100}% primary usage.")
                                
                                
print(len(primaryusage))
print(f"The primary usage rate for {char} over {len(logslist)} logs is {(sum(primaryusage)/ len(primaryusage)) * 100}%")
print(len(secondaryusage))
print(f"The secondary usage rate for {char} over {len(logslist)} logs is {(sum(secondaryusage)/ len(secondaryusage)) * 100}%")
print(len(meleeusage))
print(f"The melee usage rate for {char} over {len(logslist)} logs is {(sum(meleeusage)/ len(meleeusage)) * 100}%")
                            