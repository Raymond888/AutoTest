#!/usr/bin/python
# -*- coding: utf-8 -*-


import random

from ETLAuto.objects.base import BaseItem


class TestCollector(object):
    def test_collector(self):
        usergroup = BaseItem(item='usergroup')
        usergroup_uri = usergroup.url

        # create usergroup
        usergroup.url = usergroup_uri + '/add'
        json_data_create = {'name': usergroup.item_name,
                            'description': usergroup.random_id[:20],
                            }
        res_data = usergroup.create_or_post_item(json_data_create)
        assert res_data.get('error_code') == 0

        # list usergroup
        usergroup.url = usergroup_uri + '?page=1&size=10000&name='
        usergroup_id = usergroup.get_item_id(property='user_group')

        collectors = BaseItem(item='collectors')
        collectors_uri = collectors.url

        # list collectors
        collectors.url = collectors_uri + '/{usergroup_id}?groupId={usergroup_id}&page=1&size=10000&name='\
            .format(usergroup_id=usergroup_id)
        collectors_list = collectors.get_item_list(property='collectors')

        if not collectors_list:
            raise ValueError('collectors empty')

        # add collectors
        collectors_to_add = collectors_list if len(collectors_list) == 1 \
            else random.choices(collectors_list, k=random.choice(range(1, len(collectors_list))))
        ids_to_add = [c.get('id') for c in collectors_to_add]
        collectors.url = collectors_uri + '/privileges/add/{usergroup_id}'.format(usergroup_id=usergroup_id)

        privilege_list = ['查看', '编辑']

        json_data_create = {'groupId': usergroup_id,
                            'collector_ids': ids_to_add,
                            'privilege': random.choice(privilege_list),
                            }
        res_data = collectors.create_or_post_item(json_data_create)
        assert res_data.get('error_code') == 0

        # list usergroup collectors
        collectors.url = collectors_uri.strip('/collectors') \
                         + '/{usergroup_id}/collectors/privileges?groupId={usergroup_id}&page=1&size=10000&name='\
                             .format(usergroup_id=usergroup_id)
        usergroup_collectors = collectors.get_item_list(property='privileges')

        # edit collector or collectors
        collectors_to_edit = usergroup_collectors if len(usergroup_collectors) == 1\
            else random.choices(usergroup_collectors, k=random.choice(range(1, len(usergroup_collectors))))
        ids_to_edit = [c.get('collector_id') for c in collectors_to_edit]
        collectors.url = collectors_uri + '/privileges/edit/{usergroup_id}'.format(usergroup_id=usergroup_id)
        json_data_post = {'groupId': usergroup_id,
                          'collector_ids': ids_to_edit,
                          'privilege': random.choice(privilege_list),  # allow keep original privilege
                          }
        collectors.create_or_post_item(json_data_post)

        # delete collector or collectors
        collectors_to_delete = usergroup_collectors if len(usergroup_collectors) == 1\
            else random.choices(usergroup_collectors, k=random.choice(range(1, len(usergroup_collectors))))
        ids_to_delete = [c.get('collector_id') for c in collectors_to_delete]
        collectors.url = collectors_uri + '/privileges/delete/{usergroup_id}'.format(usergroup_id=usergroup_id)
        json_data_post = {'groupId': usergroup_id,
                          'collector_ids': ids_to_delete,
                          }
        collectors.create_or_post_item(json_data_post)





