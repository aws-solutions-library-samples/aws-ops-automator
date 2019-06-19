######################################################################################################################
#  Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           #
#                                                                                                                    #
#  Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance        #
#  with the License. A copy of the License is located at                                                             #
#                                                                                                                    #
#      http://aws.amazon.com/asl/                                                                                    #
#                                                                                                                    #
#  or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES #
#  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    #
#  and limitations under the License.                                                                                #
######################################################################################################################
import inspect
import unittest
from types import FunctionType

import actions.dynamodb_set_capacity_action as dbb
import services
import services.dynamodb_service
from testing.console_logger import ConsoleLogger
from testing.dynamodb import DynamoDB
from testing.stack import Stack
from tests.action_tests import region, get_resource_stack, get_task_runner, template_path, tasklist_tagname

TESTED_ACTION = "DynamodbSetCapacity"
TEST_RESOURCES_TEMPLATE = "test_resources.template"

KEEP_AND_USE_EXISTING_ACTION_STACK = False
KEEP_AND_USE_EXISTING_RESOURCES_STACK = False

INDEX_NAME = "Index1"


class TestAction(unittest.TestCase):
    dynamodb = None
    logger = None
    resource_stack = None
    table_name = None
    task_runner = None

    def __init__(self, method_name):
        unittest.TestCase.__init__(self, method_name)
        self.account = services.get_aws_account()

    @classmethod
    def get_methods(cls):
        return [x for x, y in cls.__dict__.items() if type(y) == FunctionType and x.startswith("test_")]

    @classmethod
    def setUpClass(cls):

        cls.logger = ConsoleLogger()

        cls.resource_stack = get_resource_stack(TESTED_ACTION,
                                                create_resource_stack_func=cls.create_resource_stack,
                                                use_existing=KEEP_AND_USE_EXISTING_RESOURCES_STACK,
                                                region_name=region())
        assert (cls.resource_stack is not None)

        cls.dynamodb = DynamoDB(region=region())

        cls.table_name = cls.resource_stack.stack_outputs["TableName"]
        cls.task_runner = get_task_runner(TESTED_ACTION, KEEP_AND_USE_EXISTING_ACTION_STACK)

        cls.logger.debug("Waiting for continuous backups to become available for table {}", cls.table_name)
        assert (cls.dynamodb.wait_until_table_backups_available(cls.table_name))

    @classmethod
    def create_resource_stack(cls, resource_stack_name):
        try:
            cls.logger.test("Creating test resources stack {}", resource_stack_name)
            resource_stack = Stack(resource_stack_name, region=region())
            resource_stack.create_stack(template_file=template_path(__file__, TEST_RESOURCES_TEMPLATE), iam_capability=False,
                                        params={
                                            "IndexName": INDEX_NAME,
                                            "TaskListTagName": tasklist_tagname(TESTED_ACTION),
                                            "TaskListTagValue": "/".join(cls.get_methods())
                                        })
            return resource_stack
        except Exception as ex:
            cls.logger.test("Error creating stack {}, {}", resource_stack_name, ex)
            return None

    def test_set_capacity_table_and_index(self):

        test_method = inspect.stack()[0][3]

        table_read_units, table_write_units, index_read_units, index_write_units = self.get_provisioned_throughput()

        self.logger.test("Running task")
        self.task_runner.run(task_name=test_method,
                             parameters={
                                 dbb.PARAM_TABLE_NAME: self.table_name,
                                 dbb.PARAM_TABLE_READ_UNITS: table_read_units * 2,
                                 dbb.PARAM_TABLE_WRITE_UNITS: table_write_units * 2,
                                 dbb.PARAM_GSI_NAME.format(1): INDEX_NAME,
                                 dbb.PARAM_GSI_READ_UNITS.format(1): index_read_units * 2,
                                 dbb.PARAM_GSI_WRITE_UNITS.format(1): index_write_units * 2
                             },
                             complete_check_polling_interval=30)

        self.assertTrue(self.task_runner.success(expected_executed_tasks=1), "Task executed successfully")
        self.logger.test("[X] Task completed")

        table_read_units_new, table_write_units_new, index_read_units_new, index_write_units_new = self.get_provisioned_throughput()

        self.assertEquals(table_read_units * 2, table_read_units_new, "Table read units set")
        self.assertEquals(table_write_units * 2, table_write_units_new, "Table write units set")
        self.logger.test("[X] Table throughput capacity set")

        self.assertEquals(index_read_units * 2, index_read_units_new, "Index read units set")
        self.assertEquals(index_write_units * 2, index_write_units_new, "Index write units set")
        self.logger.test("[X] Index throughput capacity set")

    def test_set_capacity_table(self):

        test_method = inspect.stack()[0][3]

        table_read_units, table_write_units, index_read_units, index_write_units = self.get_provisioned_throughput()

        self.logger.test("Running task")
        self.task_runner.run(task_name=test_method,
                             parameters={
                                 dbb.PARAM_TABLE_NAME: self.table_name,
                                 dbb.PARAM_TABLE_READ_UNITS: table_read_units * 2,
                                 dbb.PARAM_TABLE_WRITE_UNITS: table_write_units * 2
                             },
                             complete_check_polling_interval=30)

        self.assertTrue(self.task_runner.success(expected_executed_tasks=1), "Task executed successfully")
        self.logger.test("[X] Task completed")

        table_read_units_new, table_write_units_new, index_read_units_new, index_write_units_new = self.get_provisioned_throughput()

        self.assertEquals(table_read_units * 2, table_read_units_new, "Table read units set")
        self.assertEquals(table_write_units * 2, table_write_units_new, "Table write units set")
        self.logger.test("[X] Table throughput capacity set")

        self.assertEquals(index_read_units, index_read_units_new, "Index read units retained")
        self.assertEquals(index_write_units, index_write_units_new, "Index write units retained")
        self.logger.test("[X] Index throughput capacity retained")

    def test_set_capacity_index(self):

        test_method = inspect.stack()[0][3]

        table_read_units, table_write_units, index_read_units, index_write_units = self.get_provisioned_throughput()

        self.logger.test("Running task")
        self.task_runner.run(task_name=test_method,
                             parameters={
                                 dbb.PARAM_TABLE_NAME: self.table_name,
                                 dbb.PARAM_TABLE_READ_UNITS: table_read_units,
                                 dbb.PARAM_TABLE_WRITE_UNITS: table_write_units,
                                 dbb.PARAM_GSI_NAME.format(1): INDEX_NAME,
                                 dbb.PARAM_GSI_READ_UNITS.format(1): index_read_units * 2,
                                 dbb.PARAM_GSI_WRITE_UNITS.format(1): index_write_units * 2
                             },
                             complete_check_polling_interval=30)
        self.assertTrue(self.task_runner.success(expected_executed_tasks=1), "Task executed successfully")
        self.logger.test("[X] Task completed")

        table_read_units_new, table_write_units_new, index_read_units_new, index_write_units_new = self.get_provisioned_throughput()

        self.assertEquals(table_read_units, table_read_units_new, "Table read units retained")
        self.assertEquals(table_write_units, table_write_units_new, "Table write units retained")
        self.logger.test("[X] Table throughput capacity retained")

        self.assertEquals(index_read_units * 2, index_read_units_new, "Index read units set")
        self.assertEquals(index_write_units * 2, index_write_units_new, "Index write units set")
        self.logger.test("[X] Index throughput capacity set")

    def test_set_capacity_already_at_capacity(self):

        test_method = inspect.stack()[0][3]

        table_read_units, table_write_units, index_read_units, index_write_units = self.get_provisioned_throughput()

        self.logger.test("Running task")
        self.task_runner.run(task_name=test_method,
                             parameters={
                                 dbb.PARAM_TABLE_NAME: self.table_name,
                                 dbb.PARAM_TABLE_READ_UNITS: table_read_units,
                                 dbb.PARAM_TABLE_WRITE_UNITS: table_write_units,
                                 dbb.PARAM_GSI_NAME.format(1): INDEX_NAME,
                                 dbb.PARAM_GSI_READ_UNITS.format(1): index_read_units,
                                 dbb.PARAM_GSI_WRITE_UNITS.format(1): index_write_units
                             },
                             complete_check_polling_interval=15)
        self.assertTrue(self.task_runner.success(expected_executed_tasks=1), "Task executed successfully")
        self.logger.test("[X] Task completed")

        table_read_units_new, table_write_units_new, index_read_units_new, index_write_units_new = self.get_provisioned_throughput()

        self.assertEquals(table_read_units, table_read_units_new, "Table read units retained")
        self.assertEquals(table_write_units, table_write_units_new, "Table write units retained")
        self.logger.test("[X] Table throughput capacity retained")

        self.assertEquals(index_read_units, index_read_units_new, "Index read units set")
        self.assertEquals(index_write_units, index_write_units_new, "Index write units set")
        self.logger.test("[X] Index throughput capacity retained")

    def get_provisioned_throughput(self):
        table = self.dynamodb.get_table(self.table_name)
        table_read_units = table["ProvisionedThroughput"]["ReadCapacityUnits"]
        table_write_units = table["ProvisionedThroughput"]["WriteCapacityUnits"]
        index_read_units = table["GlobalSecondaryIndexes"][0]["ProvisionedThroughput"]["ReadCapacityUnits"]
        index_write_units = table["GlobalSecondaryIndexes"][0]["ProvisionedThroughput"]["WriteCapacityUnits"]
        return table_read_units, table_write_units, index_read_units, index_write_units

    @classmethod
    def tearDownClass(cls):
        cls.dynamodb.delete_table_backups(cls.table_name)
        if cls.resource_stack is not None and not KEEP_AND_USE_EXISTING_RESOURCES_STACK:
            cls.resource_stack.delete_stack()

        if cls.task_runner is not None:
            cls.task_runner.cleanup(KEEP_AND_USE_EXISTING_ACTION_STACK)

    def setUp(self):
        pass

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
