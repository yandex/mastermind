# encoding: utf-8

from cocaine.worker import Worker
from cocaine.logging import Logger
#from cocaine.service.services import Service
import msgpack

import traceback
import sys
import copy

import elliptics
import balancelogicadapter as bla
import balancelogic
import storage

logging = Logger()

logging.info("balancer.py")

symmetric_groups_key = "metabalancer\0symmetric_groups"

def get_groups(n):
    return bla.all_group_ids()

def get_symmetric_groups(n):
    result = [couple.as_tuple() for couple in storage.couples if couple.status == storage.Status.OK]
    logging.debug("good_symm_groups: " + str(result))
    return result

def get_bad_groups(n):
    result = [couple.as_tuple() for couple in storage.couples if couple.status != storage.Status.OK]
    logging.debug("bad_symm_groups: " + str(result))
    return result

def get_empty_groups(n):
    logging.info("len(storage.groups) = %d" % (len(storage.groups.elements)))
    logging.info("groups: %s" % str([(group.group_id, group.couple) for group in storage.groups if group.couple is None]))
    result = [group.group_id for group in storage.groups if group.couple is None]
    logging.debug("uncoupled groups: " + str(result))
    return result

def get_group_weights(n):
    try:
        sizes = set()
        all_symm_group_objects = []
        for couple in storage.couples:
            if couple.status != storage.Status.OK:
                continue

            symm_group = bla.SymmGroup(couple)
            sizes.add(len(couple))
            all_symm_group_objects.append(symm_group)
            logging.debug(str(symm_group))

        result = {}

        for size in sizes:
            (group_weights, info) = balancelogic.rawBalance(all_symm_group_objects, bla.getConfig(), bla.GroupSizeEquals(size))
            result[size] = [item for item in group_weights.items()]
            logging.info("Cluster info: " + str(info))

        logging.info(str(result))
        return result
    except Exception as e:
        logging.error("Balancelogic error: " + str(e) + "\n" + traceback.format_exc())
        return {'Balancelogic error': str(e)}

def balance(n, request):
    try:
        logging.info("----------------------------------------")
        logging.info("New request" + str(len(request)))
        logging.info(request)

        weighted_groups = get_group_weights(n)

        target_groups = []

        if manifest.get("symmetric_groups", False):
            lsymm_groups = weighted_groups[request[0]]

            for (gr_list, weight) in lsymm_groups:
                logging.info("gr_list: %s %d" % (str(gr_list), request[0]))
                grl = {'rating': weight, 'groups': gr_list}
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
            for group_id in bla.all_group_ids():
                target_groups.append(bla.get_group(int(group_id)))
            sorted_groups = sorted(target_groups, key=lambda gr: gr.freeSpaceInKb(), reverse=True)[:int(request[0])]
            logging.info(sorted_groups)
            result = ([g.groupId() for g in sorted_groups], request[1])

        logging.info("result: %s" % str(result))
        return result
    except Exception as e:
        logging.error("Balancer error: " + str(e) + "\n" + traceback.format_exc())
        return {'Balancer error': str(e)}

def make_symm_group(n, couple):
    couple = tuple(couple)
    logging.info("writing couple info: " + str(couple))
    packed = msgpack.packb(couple)
    logging.info("packed couple: \"%s\"" % str(packed).encode("hex"))
    s = elliptics.Session(n)
    good = []
    bad = ()
    for g in couple:
        try:
            s.add_groups([g])
            s.write_data(symmetric_groups_key, packed)
            good.append(g)
        except Exception as e:
            logging.error("Failed to write symm group info, group %d: %s\n%s"
                          % (g, str(e), traceback.format_exc()))
            bad = (g, e)
            break
    storage.couples.add([storage.groups[g] for g in couple])
    return (good, bad)

def repair_groups(n, request):
    try:
        logging.info("----------------------------------------")
        logging.info("New repair groups request: " + str(request))
        logging.info(request)

        group_id = int(request)

        good_symm_groups = [couple for couple in storage.couples if couple.status == storage.Status.OK]
        bad_symm_groups = [couple for couple in storage.couples if couple.status != storage.Status.OK]

        if good_symm_groups:
            logging.error("Balancer error: cannot repair, group %d is in couple %s" % (group_id, str(good_symm_groups[0])))
            return {"Balancer error" : "cannot repair, group %d is in couple %s" % (group_id, str(good_symm_groups[0]))}

        if not bad_symm_groups:
            logging.error("Balancer error: cannot repair, group %d is not a member of any couple" % group_id)
            return {"Balancer error" : "cannot repair, group %d is not a member of any couple" % group_id}

        if len(bad_symm_groups) > 1:
            logging.error("Balancer error: cannot repair, group %d is a member of several couples: %s" % (group_id, str(bad_symm_groups)))
            return {"Balancer error" : "cannot repair, group %d is a member of several couples: %s" % (group_id, str(bad_symm_groups))}

        couple = list(bad_symm_groups)[0]
        (good, bad) = make_symm_group(n, couple)
        if bad:
            raise bad[1]

        return {"message": "Successfully repaired couple", 'couple': couple}

    except Exception as e:
        logging.error("Balancer error: " + str(e) + "\n" + traceback.format_exc())
        return {'Balancer error': str(e)}

def get_group_info(n, request):
    try:
        group = int(request)
        logging.info('get_group_info: request: %s, %s' % (str(request), repr(storage.groups[group])))

        if not group in storage.groups:
            return {'Balancer error': 'Group %d is not found' % (group)}

        return storage.groups[group].info()

    except Exception as e:
        logging.error("Balancer error: " + str(e) + "\n" + traceback.format_exc())
        return {'Balancer error': str(e)}

def couple_groups(n, request):
    try:
        logging.info("----------------------------------------")
        logging.info("New couple groups request: " + str(request))
        logging.info(request)
        uncoupled_groups = get_empty_groups(n)
        dc_by_group_id = {}
        group_by_dc = {}
        for group_id in uncoupled_groups:
            group = storage.groups[group_id]
            dc = group.nodes[0].host.get_dc()
            dc_by_group_id[group_id] = dc
            groups_in_dc = group_by_dc.setdefault(dc, [])
            groups_in_dc.append(group_id)
        logging.info("dc by group: %s" % str(dc_by_group_id))
        logging.info("group_by_dc: %s" % str(group_by_dc))
        size = int(request[0])
        mandatory_groups = [int(g) for g in request[1]]
        # check mandatory set
        for group_id in mandatory_groups:
            if group_id not in uncoupled_groups:
                raise Exception("group %d is coupled" % group_id)
            dc = dc_by_group_id[group_id]
            if dc not in group_by_dc:
                raise Exception("groups must be in different dcs")
            del group_by_dc[dc]

        groups_to_couple = copy.copy(mandatory_groups)

        # need better algorithm for this. For a while - only 1 try and random selection
        n_groups_to_add = size - len(groups_to_couple)
        if n_groups_to_add > len(group_by_dc):
            raise Exception("Not enough dcs")
        if n_groups_to_add < 0:
            raise Exception("Too many mandatory groups")

        some_dcs = group_by_dc.keys()[:n_groups_to_add]

        for dc in some_dcs:
            groups_to_couple.append(group_by_dc[dc].pop())

        (good, bad) = make_symm_group(n, groups_to_couple)
        if bad:
            raise bad[1]

        return groups_to_couple
    except Exception as e:
        logging.error("Balancer error: " + str(e) + "\n" + traceback.format_exc())
        return {'Balancer error': str(e)}

def kill_symm_group(n, groups):
    logging.info("Killing symm groups: %s" % str(groups))
    s = elliptics.Session(n)
    s.add_groups(groups)
    s.remove(symmetric_groups_key)

def break_couple(n, request):
    try:
        logging.info("----------------------------------------")
        logging.info("New break couple request: " + str(request))
        logging.info(request)

        couple_str = ':'.join(sorted([int(g) for g in request[0]]))
        if not couple_str in storage.couples:
            raise KeyError('Couple %s was not found' % (couple_str))

        couple = storage.couples[couple_str]
        confirm = request[1]

        logging.info("groups: %s; confirmation: \"%s\"" % (couple_str, confirm))

        correct_confirm = "Yes, I want to break "
        if couple.status == storage.Status.OK:
            correct_confirm += "good"
        else:
            correct_confirm += "bad"
            
        correct_confirm += " couple " + couple_str

        if confirm != correct_confirm:
            raise Exception('Incorrect confirmation string')

        kill_symm_group(n, [group.group_id for group in couple])
        couple.destroy()

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
            max_group = int(n.meta_session.read_data("mastermind:max_group"))
        except:
            max_group = 0

        new_max_group = max_group + groups_count
        n.meta_session.write_data("mastermind:max_group", str(new_max_group))

        return range(max_group+1, max_group+1 + groups_count)

    except Exception as e:
        logging.error("Mastermind error: " + str(e) + "\n" + traceback.format_exc())
        return {'Mastermind error': str(e)}
