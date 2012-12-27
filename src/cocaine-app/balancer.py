# encoding: utf-8

from cocaine.context import Log, manifest
import msgpack

import traceback
import sys

import elliptics
from balancelogicadapter import add_raw_node, SymmGroup, config, get_group
import balancelogic
import inventory

logging = Log()

logging.info("balancer.py")

stats = {}
groups = {}
symm_groups = {}
symm_groups_all = {}
bad_groups = {}
empty_groups = []

symmetric_groups_key = "metabalancer\0symmetric_groups"

def get_groups(n):
    global groups

    if not groups:
        aggregate(n)

    return groups

def get_symmetric_groups(n):
    global symm_groups

    if "symmetric_groups" in manifest() and manifest()["symmetric_groups"]:
        if not symm_groups:
            collect(n)
        return list(set(symm_groups.values()))
    else:
        return None

def get_bad_groups(n):
    global bad_groups

    if "symmetric_groups" in manifest() and manifest()["symmetric_groups"]:
        if not bad_groups:
            collect(n)
        logging.info("bad_groups: " + str(bad_groups))
        return bad_groups
    else:
        return None

def get_empty_groups(n):
    global empty_groups

    if "symmetric_groups" in manifest() and manifest()["symmetric_groups"]:
        if not empty_groups:
            collect(n)
        return empty_groups
    else:
        return None

def get_symmetric_groups_raw(n):
    global groups, empty_groups
    lsymm_groups = {}
    lempty_groups = []

    if not groups:
        aggregate(n)

    s = elliptics.Session(n)
    for group in groups.values():
        try:
            s.add_groups([group])
            lsymm_groups[group] = msgpack.unpackb(s.read_data(symmetric_groups_key))
            logging.info("lsymm_groups[%d] = %s" % (group, str(lsymm_groups[group])))
            get_group(int(group)).setBad(False)
        except:
            logging.error("Failed to read symmetric_groups from group %d" % group)
            lempty_groups.append(group)
            get_group(int(group)).setBad(True)

    empty_groups = list(set(lempty_groups))
    return lsymm_groups

def get_bad_groups_raw(s, lsymm_groups):
    to_erase = []
    for group in lsymm_groups:
        try:
            erase = False
            symms = lsymm_groups[group]

            if not symms.count(group):
                erase = True

            logging.info("erase = %s" % erase)
            for g in symms:
                if lsymm_groups[g] != symms:
                    erase = True
                    logging.info("erase = %s, lsymm_groups[%d] %s != %s " % (erase, g, str(lsymm_groups[symms]), str(symms)))
                    break

            if erase:
                to_erase.extend(symms)
                to_erase.append(group)
        except:
            to_erase.append(group)
    to_erase = list(set(to_erase))
    return to_erase

def collect(n):
    global groups, symm_groups, symm_groups_all, bad_groups
    lsymm_groups = {}

    try:
        lsymm_groups = get_symmetric_groups_raw(n)
        logging.info("symm_groups: " + str(lsymm_groups))

        # check for consistency
        to_erase = []
        to_erase = get_bad_groups_raw(n, lsymm_groups)

        lbad_groups = {}
        for g in to_erase:
            lbad_groups[g] = lsymm_groups[g]
            del lsymm_groups[g]

        bad_groups = lbad_groups

        logging.info("lsymm_groups after check: %s" % str(lsymm_groups))
        symm_groups = lsymm_groups
        logging.info("symm_groups: %s" % str(symm_groups))

        try:
            max_group = int(n.meta_session.read("mastermind:max_group"))
        except:
            max_group = 0
        curr_max_group = max(groups.values())
        if curr_max_group > max_group:
            n.meta_session.write("mastermind:max_group", str(curr_max_group))
        

    except Exception as e:
        logging.error("Error: " + str(e) + "\n" + traceback.format_exc())
        return {'error': str(e)}

def calc_rating(node):
    node['rating'] = node['free_space_rel'] * 1000 + (node['la'] + 0.1) * 100

def parse(raw_node):
    ret = dict()

    ret['group_id'] = raw_node["group_id"]
    ret['addr'] = raw_node['addr']

    bsize = raw_node['counters']['DNET_CNTR_BSIZE'][0]
    avail = raw_node['counters']['DNET_CNTR_BAVAIL'][0]
    total = raw_node['counters']['DNET_CNTR_BLOCKS'][0]

    ret['free_space_rel'] = float(avail) / total;
    ret['free_space_abs'] = float(avail) / 1024 / 1024 / 1024 * bsize

    ret['la'] = float(raw_node['counters']['DNET_CNTR_LA15'][0]) / 100

    return ret

def aggregate(n):
    global stats, groups
    logging.info("Start aggregate test in balancer.py")
    try:
        s = elliptics.Session(n)
        raw_stats = s.stat_log()
 
        lstats = {}
        lgroups = {}
 
        for raw_node in raw_stats:
            node = parse(raw_node)
            add_raw_node(raw_node)
            calc_rating(node)
            lstats[node['addr']] = node
            lgroups[str(node['group_id'])] = node['group_id'] 

        #logging.info(groups)
        #db["stats"].save({'_id': 'stats', 'content': stats})
        #db["stats"].save({'_id': 'groups', 'content': groups})#, upsert = True)
        stats = lstats
        groups = lgroups
    except Exception as e:
        logging.error("Error: " + str(e) + "\n" + traceback.format_exc())
        return {'error': str(e)}

def get_group_weights(n):
    if not symm_groups:
        collect(n)
    size_to_sgs = {}
    all_symm_groups = []
    for tuple_symm_group in set(symm_groups.values()):
        symm_group = SymmGroup(tuple_symm_group)
        sized_symm_groups = size_to_sgs.setdefault(len(tuple_symm_group), [])
        sized_symm_groups.append(symm_group)
        all_symm_groups.append(symm_group)
        logging.info(str(symm_group))
    
    result = {}
    
    for size in size_to_sgs:
        (group_weights, info) = balancelogic.rawBalance(all_symm_groups, config(size))
        result[size] = [item for item in group_weights.items()]
        logging.info("Cluster info: " + str(info))
    
    logging.info(str(result))
    return result

def balance(n, request):
    global stats, groups, symm_groups
    try:
        logging.info("----------------------------------------")
        logging.info("New request" + str(len(request)))
        logging.info(request)

        if not stats or not groups:
            aggregate(n)
        #stats = db["stats"].find_one({'_id': 'stats'})['content']
        #groups = db["stats"].find_one({'_id': 'groups'})['content']
        logging.info(stats)
        logging.info(groups)

        s = elliptics.Session(n)
        object_id = elliptics.Id(list(request[2]), 0, 0)
        target_groups = []

        if "symmetric_groups" in manifest() and manifest()["symmetric_groups"]:
            if not symm_groups:
                collect(n)

            for gr_list in list(set(symm_groups.values())):

                logging.info("gr_list: %s %d" % (str(gr_list), request[0]))
                if len(gr_list) != request[0]:
                    continue

                grl = {'rating': 0, 'groups': gr_list}

                for group in gr_list:
                    object_id.group_id = int(group)
                    addr = s.lookup_addr(object_id)
                    logging.info(addr)
                    grl["rating"] += stats[addr]['rating']

                logging.info("grl: %s" % str(grl))
                target_groups.append(grl)

            logging.info("target_groups: %s" % str(target_groups))
            if target_groups:
                sorted_groups = sorted(target_groups, key=lambda gr: gr['rating'], reverse=True)[0]
                logging.info(sorted_groups)
                result = (sorted_groups['groups'], request[1])
            else:
                result = ([], request[1])

        else:
            for group_id in groups:
                object_id.group_id = int(group_id)
                logging.info("object_id: " + str(object_id.__class__))
                addr = s.lookup_addr(object_id)
                logging.info(addr)
                target_groups.append(stats[addr])

            sorted_groups = sorted(target_groups, key=lambda gr: gr['rating'], reverse=True)[:int(request[0])]
            logging.info(sorted_groups)
            result = ([g['group_id'] for g in sorted_groups], request[1])

        logging.info("result: %s" % str(result))
        return result
    except Exception as e:
        logging.error("Balancer error: " + str(e) + "\n" + traceback.format_exc())
        return {'Balancer error': str(e)}

def repair_groups(n, request):
    global stats, groups, symm_groups, bad_groups
    try:
        logging.info("----------------------------------------")
        logging.info("New repair groups request" + str(request))
        logging.info(request)

        if not bad_groups:
            collect(n)

        group = int(request)
        couple = list(bad_groups[group])

        logging.info("couple: " + str(couple))
        # It should be some checks for couples crossing

        packed = msgpack.packb(couple)
        logging.info("packed couple: " + str(packed))

        s = elliptics.Session(n)
        for g in couple:
            s.add_groups([g])
            s.write_data(symmetric_groups_key, packed)

        collect(n)

        return {"message": "Success fully repaired couple", 'couple': couple}

    except Exception as e:
        logging.error("Balancer error: " + str(e) + "\n" + traceback.format_exc())
        return {'Balancer error': str(e)}

def get_group_info(n, request):
    global stats, groups, bad_groups
    try:
        if not stats or not groups:
            aggregate(n)
            collect(n)

        group = int(request)

        res = {}

        res['nodes'] = [val for key, val in stats.iteritems() if val['group_id'] == group]

        logging.info("bad_groups: " + str(bad_groups))
        logging.info("symm_groups: " + str(symm_groups))
        if group in bad_groups:
            res['status'] = 'bad'
            res['couples'] = bad_groups[group]

        if group in symm_groups:
            res['status'] = 'coupled'
            res['couples'] = symm_groups[group]
        
        return res
        
    except Exception as e:
        logging.error("Balancer error: " + str(e) + "\n" + traceback.format_exc())
        return {'Balancer error': str(e)}

def couple_groups(n, request):
    global stats, groups, symm_groups, bad_groups
    try:
        logging.info("----------------------------------------")
        logging.info("New couple groups request" + str(request))
        logging.info(request)

        collect(n)

        size = int(request[0])
        rgroups = [int(g) for g in request[1]]
        groups_to_couple = []
        empty_groups = get_empty_groups(n)

        used_dcs = set()

        for g in rgroups:
            if not g in empty_groups:
                raise Exception("Group " + str(g) + " doesn't listed as empty")

            info = get_group_info(n, g)

            host = info['nodes'][0]['addr'].split(':')[0]
            dc = inventory.get_dc_by_host(host)

            if dc in used_dcs:
                raise Exception('Group ' + str(g) + ' is in same DC ' + dc + ' as one of previous groups')

            used_dcs.add(dc)
            groups_to_couple.append(g)

        while len(groups_to_couple) < size:
            g = empty_groups.pop()
            logging.info("g: " + str(g))
            info = get_group_info(n, g)

            host = info['nodes'][0]['addr'].split(':')[0]
            dc = inventory.get_dc_by_host(host)

            if dc in used_dcs:
                if not empty_groups:
                    raise Exception('Group ' + str(g) + ' is in same DC ' + dc + ' as one of previous groups, and there is no any more unused groups')
                continue

            used_dcs.add(dc)
            groups_to_couple.append(g)

        if len(groups_to_couple) == size:
            packed = msgpack.packb(groups_to_couple)
            s = elliptics.Session(n)
            for g in groups_to_couple:
                s.add_groups([g])
                s.write_data(symmetric_groups_key, packed)

        collect(n)

        return groups_to_couple
    except Exception as e:
        logging.error("Balancer error: " + str(e) + "\n" + traceback.format_exc())
        return {'Balancer error': str(e)}

def break_couple(n, request):
    global symm_groups, bad_groups
    try:

        logging.info("----------------------------------------")
        logging.info("New break couple request" + str(request))
        logging.info(request)

        collect(n)

        groups = [int(g) for g in request[0]]
        groups.sort()
        confirm = request[1]

        logging.info("groups: " + str(groups) + " confirmation: " + confirm)

        # Check if all groups are in symm_groups
        in_symm = True

        for g in groups:
            if g not in symm_groups:
                in_symm = False
                break

            sg = list(symm_groups[g])
            sg.sort()
            logging.info("cheching group " + str(g) + ", sg = " + str(sg))
            if sg != groups:
                in_symm = False
                break

        logging.info("in_symm: " + str(in_symm))
        if in_symm:
            correct_confirm = "Yes, I want to break good couple " + ':'.join([str(g) for g in request[0]])
            if confirm != correct_confirm:
                raise Exception('Incorrect confirmation string')
        else:
            in_bad = True
            # Check if all groups are in bad_groups
            logging.info("bad_groups: " + str(bad_groups))
            for g in groups:
                if not g in bad_groups:
                    in_bad = False
                    break
            logging.info("in_bad: " + str(in_bad))
            if not in_bad:
                raise Exception("Some group are not in bad groups")

            correct_confirm = "Yes, I want to break bad couple " + ':'.join([str(g) for g in request[0]])
            if confirm != correct_confirm:
                raise Exception('Incorrect confirmation string')

        s = elliptics.Session(n)
        s.add_groups(groups)
        s.remove(symmetric_groups_key)

        collect(n)

        return True

    except Exception as e:
        logging.error("Balancer error: " + str(e) + "\n" + traceback.format_exc())
        return {'Balancer error': str(e)}


def get_get_next_group_number(n, request):
    try:
        groups_count = int(request)
        if groups_count < 0 or groups_count > 100:
            raise Exception('Incorrect groups count')

        try:
            max_group = int(n.meta_session.read("mastermind:max_group"))
        except:
            max_group = 0

        new_max_group = max_group + groups_count
        n.meta_session.write("mastermind:max_group", str(new_max_group))

        return range(max_group+1, max_group+1 + groups_count)

    except Exception as e:
        logging.error("Mastermind error: " + str(e) + "\n" + traceback.format_exc())
        return {'Mastermind error': str(e)}
