#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.objects.base import BaseItem


class TestTag(object):
    def test_tag(self):
        item = BaseItem(item='tags')
        uri = item.url

        # create
        json_data_create = {'tag_name': item.item_name,
                            'note': item.random_id[:20],
                            }
        res_data = item.create_or_post_item(json_data_create)
        assert res_data.get('error_code') == 0

        # list
        item.url = uri + '?page=1&size=10000&search='
        item_id = item.get_item_id(property='tags')

        # delete
        item.url = uri + '/delete'
        json_data_delete = {'ids': [item_id]}
        res_data = item.delete_item(json_data_delete)
        assert res_data.get('error_code') == 0
