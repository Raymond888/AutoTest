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

        datasource = BaseItem(item='datasource')
        datasource_uri = datasource.url

        # list datasource
        datasource.url = datasource_uri + '/{usergroup_id}?groupId={usergroup_id}&page=1&size=10000&name='\
            .format(usergroup_id=usergroup_id)
        datasource_list = datasource.get_item_list(property='datasources')

        if not datasource_list:
            raise ValueError('collectors empty')

        # add datasources
        datasources_to_add = datasource_list if len(datasource_list) == 1 \
            else random.choices(datasource_list, k=random.choice(range(1, len(datasource_list))))
        ids_to_add = [c.get('id') for c in datasources_to_add]
        datasource.url = datasource_uri + '/privileges/add/{usergroup_id}'.format(usergroup_id=usergroup_id)

        privilege_list = ['查看', '编辑']

        json_data_create = {'groupId': usergroup_id,
                            'datasource_ids': ids_to_add,
                            'privilege': random.choice(privilege_list),
                            }
        res_data = datasource.create_or_post_item(json_data_create)
        assert res_data.get('error_code') == 0

        # list usergroup datasources
        datasource.url = datasource_uri.strip('/datasource') \
                         + '/{usergroup_id}/datasource/privileges?groupId={usergroup_id}&page=1&size=10000&name='\
                             .format(usergroup_id=usergroup_id)
        usergroup_datasources = datasource.get_item_list(property='privileges')

        # edit datasource or datasources
        datasources_to_edit = usergroup_datasources if len(usergroup_datasources) == 1\
            else random.choices(usergroup_datasources, k=random.choice(range(1, len(usergroup_datasources))))
        ids_to_edit = [d.get('datasource_id') for d in datasources_to_edit]
        datasource.url = datasource_uri + '/privileges/edit/{usergroup_id}'.format(usergroup_id=usergroup_id)
        json_data_post = {'groupId': usergroup_id,
                          'datasource_ids': ids_to_edit,
                          'privilege': random.choice(privilege_list),  # allow keep original privilege
                          }
        datasource.create_or_post_item(json_data_post)

        # delete datasource or datasources
        datasources_to_delete = usergroup_datasources if len(usergroup_datasources) == 1\
            else random.choices(usergroup_datasources, k=random.choice(range(1, len(usergroup_datasources))))
        ids_to_delete = [d.get('datasource_id') for d in datasources_to_delete]
        datasource.url = datasource_uri + '/privileges/delete/{usergroup_id}'.format(usergroup_id=usergroup_id)
        json_data_post = {'groupId': usergroup_id,
                          'datasource_ids': ids_to_delete,
                          }
        datasource.create_or_post_item(json_data_post)





