#!/usr/bin/python
# -*- coding: utf-8 -*-


import random

from ETLAuto.objects.base import BaseItem


class TestMember(object):
    def test_member(self):
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

        members = BaseItem(item='users')
        members_uri = members.url

        # list users
        members.url = members_uri + '/{usergroup_id}?groupId={usergroup_id}&page=1&size=30&name='\
            .format(usergroup_id=usergroup_id)
        members_list = members.get_item_list(property='users')

        # add members
        members_to_add = members_list if len(members_list) == 1 \
            else random.choices(members_list, k=random.choice(range(1, len(members_list))))
        members.url = members_uri.strip('/users') + '/member/add/{usergroup_id}'.format(usergroup_id=usergroup_id)
        json_data_create = {'groupId': usergroup_id,
                            'user': members_to_add,
                            }
        res_data = members.create_or_post_item(json_data_create)
        assert res_data.get('error_code') == 0

        # list usergroup members
        members.url = members_uri.strip('/users') + '/member?usergroupid={usergroup_id}&page=1&size=10000&username='\
            .format(usergroup_id=usergroup_id)
        usergroup_members = members.get_item_list(property='members')

        # delete member or members
        members_to_delete = usergroup_members if len(usergroup_members) == 1 \
            else random.choices(usergroup_members, k=random.choice(range(1, len(usergroup_members))))
        ids_to_delete = [m.get('id') for m in members_to_delete]
        members.url = members_uri.strip('/users') + '/member/delete/{usergroup_id}'.format(usergroup_id=usergroup_id)
        json_data_post = {'groupId': usergroup_id,
                          'ids': ids_to_delete,
                          }
        members.create_or_post_item(json_data_post)




