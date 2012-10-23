# encoding: utf-8

from cocaine.context import Log, manifest
import msgpack

import traceback
import sys
import xml.dom.minidom
import urllib2
import socket

import elliptics

logging = Log()

logging.info("balancer.py")

stats = {}
groups = {}
symm_groups = {}
bad_groups = []
empty_groups = []

def get_dc_by_host(addr):
    host = socket.gethostbyaddr(addr)[0]
    hostxml = urllib2.urlopen("http://c.yandex-team.ru/api/hosts/" + host)
    hostinfo = xml.dom.minidom.parse(hostxml)
    return hostinfo.getElementsByTagName('data')[0].getElementsByTagName('item')[0].getElementsByTagName('root_datacenter')[0].firstChild.data

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
        return symm_groups
    else:
        return None

def get_bad_groups(n):
    global bad_groups

    if "symmetric_groups" in manifest() and manifest()["symmetric_groups"]:
        if not bad_groups:
            collect(n)
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

    if not groups:
        aggregate(n)

    for group in groups.values():
        try:
            n.add_groups([group])
            lsymm_groups[group] = msgpack.unpackb(n.read_data("metabalancer\0symmetric_groups", 0, 0, 0, 0, 0))
            logging.info("lsymm_groups[%d] = %s" % (group, str(lsymm_groups[group])))
        except:
            logging.error("Failed to read symmetric_groups from group %d" % group)
            empty_groups.append(group)

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
    global groups, symm_groups, bad_groups
    lsymm_groups = {}

    try:
        s = elliptics.Session(n)
        lsymm_groups = get_symmetric_groups_raw(n)
        logging.info(lsymm_groups)

        # check for consistency
        to_erase = []
        to_erase = get_bad_groups_raw(s, lsymm_groups)
        bad_groups = to_erase

        for g in to_erase:
            del lsymm_groups[g]

        logging.info("lsymm_groups after check: %s" % str(lsymm_groups))
        symm_groups = list(set(lsymm_groups.values()))
        logging.info("symm_groups: %s" % str(symm_groups))

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

            for gr_list in symm_groups:

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

