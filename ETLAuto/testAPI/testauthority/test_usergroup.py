#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.objects.base import BaseItem


class TestUserGroup(object):
    def test_usergroup(self):
        item = BaseItem(item='usergroup')
        uri = item.url

        # create
        item.url = uri + '/add'
        json_data_create = {'name': item.item_name,
                            'description': item.random_id[:20],
                            }
        res_data = item.create_or_post_item(json_data_create)
        assert res_data.get('error_code') == 0

        # list
        item.url = uri + '?page=1&size=10000&name='
        item_id = item.get_item_id(property='user_group')

        # delete
        item.url = uri + '/delete'
        json_data_delete = {'ids': [item_id]}
        res_data = item.delete_item(json_data_delete)
        assert res_data.get('error_code') == 0
