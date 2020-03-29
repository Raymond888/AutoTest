#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.settings import XML_SETTINGS, JSON_SETTINGS, FOLDER_SETTINGS
from ETLAuto.objects.base import BaseDownload


class TestRunnerManager(object):
    def setup(self):
        pass

    def teardown(self):
        pass

    def download_template(self):
        res = BaseDownload().download_template()
        assert len(res.content) > 0

    def start_runner(self, runner_obj):
        runner = runner_obj()
        runner_data = runner.get_runner_data()

        assert 'agents' in runner_data and 'runners' in runner_data, runner_data
        agents = runner_data.get('agents')
        runners = runner_data.get('runners')

        # time.sleep(5)
        assert len(agents) > 0 and len(runners) > 0, \
            'agents number: {}, runners number: {}'.format(len(agents), len(runners))

        #  verify runner state
        agent_ip = agents[0].get('ip', '')
        runner_state = runners[0].get('state', '')
        status = runners[0].get('status', {})

        runner_class_name = runner.__class__.__name__

        insert_succeed = status.get('readInsertSucceedRecords', 0)
        insert_total = status.get('readInsertTotalRecords', 0)
        update_succeed = status.get('readUpdateSucceedRecords', 0)
        update_total = status.get('readUpdateTotalRecords', 0)
        delete_succeed = status.get('readDeleteSucceedRecords', 0)
        delete_total = status.get('readDeleteTotalRecords', 0)
        read_total = status.get('readTotalRecords', 0)
        write_succeed = status.get('writeSucceedRecords', 0)
        write_received = status.get('writeReceivedRecords', 0)

        assert runner_state != 'FAILED', 'execute state:{}'.format(runner_state)

        # Kafka runner state is always running, also increment runner
        if not runner_class_name.startswith('Kafka') and not runner_class_name.startswith('Increase'):
            assert runner_state == 'FINISHED', 'execute state:{}'.format(runner_state)

        if runner_class_name.startswith('Increase'):
            assert insert_succeed == insert_total, \
                'insert_succeed:{}, insert_total:{}'.format(insert_succeed, insert_total)
            assert update_succeed == update_total, \
                'update_succeed:{}, update_total:{}'.format(update_succeed, update_total)
            assert delete_succeed == delete_total, \
                'delete_succeed:{}, delete_total:{}'.format(delete_succeed, delete_total)
        elif runner_class_name.startswith('Folder'):
            assert agent_ip.strip() == FOLDER_SETTINGS['READER']['host'].strip(), agent_ip.strip()
            assert read_total == write_succeed == write_received == 0, \
                'read_total:{}, write_succeed:{}, total:{}'.format(read_total, write_succeed, write_received)
        else:
            assert write_succeed not in [None, 0, '0'], 'total: {}'.format(write_received)

            if runner_class_name in ['Oracle2HiveRunnerTransformerFilter']:
                assert write_succeed == write_received
            else:
                assert read_total == write_succeed == write_received, \
                    'read_total:{}, write_succeed:{}, total:{}'.format(read_total, write_succeed, write_received)

            if runner_class_name.startswith('Xml'):
                assert agent_ip.strip() == XML_SETTINGS['READER']['host'].strip()

            if runner_class_name.startswith('Json'):
                assert agent_ip.strip() == JSON_SETTINGS['READER']['host'].strip()

        # TODO: ETL results verification
