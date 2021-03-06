'''
@author Luke Campbell
@file ion/services/dm/transformation/test_transform_service.py
@description Unit Test for Transform Management Service
'''

from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.core.exception import NotFound, BadRequest
from pyon.ion.transform import TransformDataProcess
from pyon.util.containers import DotDict
from pyon.public import IonObject, RT, PRED
from pyon.util.unit_test import PyonTestCase
from mock import Mock
from nose.plugins.attrib import attr
from pyon.util.int_test import IonIntegrationTestCase
from ion.services.dm.transformation.transform_management_service import TransformManagementService
from ion.processes.data.transforms.transform_example import TransformExample
from interface.objects import ProcessDefinition, StreamQuery
from pyon.public import   StreamSubscriberRegistrar, StreamPublisherRegistrar
from Queue import Empty
import os
import time
import gevent

import unittest


class TestTransform(TransformDataProcess):
    pass

@attr('UNIT',group='dm')
class TransformManagementServiceTest(PyonTestCase):
    """Unit test for TransformManagementService

    """
    def setUp(self):
        mock_clients = self._create_service_mock('transform_management')
        self.transform_service = TransformManagementService()
        self.transform_service.clients = mock_clients
        self.transform_service.clients.pubsub_management = DotDict()
        self.transform_service.clients.pubsub_management['XP'] = 'science.data'
        self.transform_service.clients.pubsub_management['create_stream'] = Mock()
        self.transform_service.clients.pubsub_management['create_subscription'] = Mock()
        self.transform_service.clients.pubsub_management['register_producer'] = Mock()
        self.transform_service.clients.pubsub_management['activate_subscription'] = Mock()
        self.transform_service.clients.pubsub_management['read_subscription'] = Mock()
        self.transform_service.container = DotDict()
        self.transform_service.container['spawn_process'] = Mock()
        self.transform_service.container['id'] = 'mock_container_id'
        self.transform_service.container['proc_manager'] = DotDict()
        self.transform_service.container.proc_manager['terminate_process'] = Mock()
        self.transform_service.container.proc_manager['procs'] = {}
        # CRUD Shortcuts
        self.mock_rr_create = self.transform_service.clients.resource_registry.create
        self.mock_rr_read = self.transform_service.clients.resource_registry.read
        self.mock_rr_update = self.transform_service.clients.resource_registry.update
        self.mock_rr_delete = self.transform_service.clients.resource_registry.delete
        self.mock_rr_find = self.transform_service.clients.resource_registry.find_objects
        self.mock_rr_find_res = self.transform_service.clients.resource_registry.find_resources
        self.mock_rr_assoc = self.transform_service.clients.resource_registry.find_associations
        self.mock_rr_create_assoc = self.transform_service.clients.resource_registry.create_association
        self.mock_rr_del_assoc = self.transform_service.clients.resource_registry.delete_association

        self.mock_pd_create = self.transform_service.clients.process_dispatcher.create_process_definition
        self.mock_pd_read = self.transform_service.clients.process_dispatcher.read_process_definition
        self.mock_pd_update = self.transform_service.clients.process_dispatcher.update_process_definition
        self.mock_pd_delete = self.transform_service.clients.process_dispatcher.delete_process_definition
        self.mock_pd_schedule = self.transform_service.clients.process_dispatcher.schedule_process
        self.mock_pd_cancel = self.transform_service.clients.process_dispatcher.cancel_process

        self.mock_ps_create_stream = self.transform_service.clients.pubsub_management.create_stream
        self.mock_ps_create_sub = self.transform_service.clients.pubsub_management.create_subscription
        self.mock_ps_register = self.transform_service.clients.pubsub_management.register_producer
        self.mock_ps_activate = self.transform_service.clients.pubsub_management.activate_subscription
        self.mock_ps_read_sub = self.transform_service.clients.pubsub_management.read_subscription

        self.mock_cc_spawn = self.transform_service.container.spawn_process
        self.mock_cc_terminate = self.transform_service.container.proc_manager.terminate_process
        self.mock_cc_procs = self.transform_service.container.proc_manager.procs

    # test a create transform with identical names here

    def test_create_transform_full_config(self):
        # mocks
        proc_def = DotDict()
        proc_def['executable'] = {'module':'my_module', 'class':'class'}
        self.mock_pd_read.return_value = proc_def
        self.mock_rr_find_res.return_value = ([],[])
        self.mock_pd_schedule.return_value = '123' # PID
        self.mock_rr_create.return_value = ('transform_id','garbage')
        self.mock_ps_read_sub.return_value = DotDict({'exchange_name':'input_stream_id'})



        # execution
        configuration = {'proc_args':{'arg1':'value'}}
        ret = self.transform_service.create_transform(name='test_transform',
            process_definition_id='mock_procdef_id',
            in_subscription_id='mock_subscription_id',
            out_streams={'output':'mock_output_stream_id'},
            configuration=configuration)

        # assertions
        # look up on procdef
        self.mock_ps_read_sub.assert_called_with(subscription_id='mock_subscription_id')

        # (1) sub, (1) stream, (1) procdef
        out_config = {
            'process':{
                'name':'test_transform',
                'type':'stream_process',
                'listen_name':'input_stream_id',
                'publish_streams':{'output':'mock_output_stream_id'},
                'transform_id': 'transform_id'
            },
            'proc_args':{
                'arg1':'value'
            }
        }
        self.assertEquals(self.mock_rr_create_assoc.call_count,3)
        self.mock_pd_schedule.assert_called_with(
            'mock_procdef_id',
            None,
            out_config,
            ""
        )
        self.assertTrue(self.mock_rr_create.called)

    def test_create_transform_no_config(self):
        # mocks
        proc_def = DotDict()
        proc_def['executable'] = {'module':'my_module', 'class':'class'}
        self.mock_pd_read.return_value = proc_def
        self.mock_rr_find_res.return_value = ([],[])

        self.mock_cc_spawn.return_value = '123' #PID
        self.mock_pd_schedule.return_value = '123' # PID
        self.mock_rr_create.return_value = ('transform_id','garbage')
        self.mock_ps_read_sub.return_value = DotDict({'exchange_name':'input_stream_id'})


        # execution
        configuration = {}
        ret = self.transform_service.create_transform(name='test_transform',
            process_definition_id='mock_procdef_id',
            in_subscription_id='mock_subscription_id',
            out_streams={'output':'mock_output_stream_id'},
            configuration=configuration)

        # assertions
        # look up on procdef
        self.mock_ps_read_sub.assert_called_with(subscription_id='mock_subscription_id')

        # (1) sub, (1) stream, (1) procdef
        out_config = {
            'process':{
                'name':'test_transform',
                'type':'stream_process',
                'listen_name':'input_stream_id',
                'publish_streams':{'output':'mock_output_stream_id'},
                'transform_id': 'transform_id'
            }
        }
        self.assertEquals(self.mock_rr_create_assoc.call_count,3)
        self.mock_pd_schedule.assert_called_with(
            'mock_procdef_id',
            None,
            out_config,
            ""
        )

        self.assertTrue(self.mock_rr_create.called)

    def test_create_transform_no_stream(self):
        # mocks
        proc_def = DotDict()
        proc_def['executable'] = {'module':'my_module', 'class':'class'}
        self.mock_pd_read.return_value = proc_def
        self.mock_rr_find_res.return_value = ([],[])
        self.mock_pd_schedule.return_value = '123' # PID
        self.mock_rr_create.return_value = ('transform_id','garbage')
        self.mock_ps_read_sub.return_value = DotDict({'exchange_name':'input_stream_id'})


        # execution
        configuration = {'proc_args':{'arg1':'value'}}
        ret = self.transform_service.create_transform(name='test_transform',
            process_definition_id='mock_procdef_id',
            in_subscription_id='mock_subscription_id',
            configuration=configuration)

        # assertions
        # look up on procdef
        self.mock_ps_read_sub.assert_called_with(subscription_id='mock_subscription_id')

        # (1) sub, (1) stream, (1) procdef
        out_config = {
            'process':{
                'name':'test_transform',
                'type':'stream_process',
                'listen_name':'input_stream_id',
                'transform_id': 'transform_id'
            },
            'proc_args':{
                'arg1':'value'
            }
        }
        self.assertEquals(self.mock_rr_create_assoc.call_count,2)
        self.mock_pd_schedule.assert_called_with(
            'mock_procdef_id',
            None,
            out_config,
            ""
        )

        self.assertTrue(self.mock_rr_create.called)




    def test_update_transform(self):
        with self.assertRaises(NotImplementedError):
            self.transform_service.update_transform()


    def test_read_transform(self):
        # mocks
        self.mock_rr_read.return_value = 'mock_object'

        # execution
        ret = self.transform_service.read_transform('transform_id')

        # assertions
        self.mock_rr_read.assert_called_with('transform_id','')
        self.assertEquals(ret,'mock_object')


    def test_delete_transform(self):
        # mocks
        self.transform_service.read_transform = Mock()
        self.transform_service.read_transform.return_value = DotDict({'process_id':'pid'})
        find_list = ['process_definition','subscription_id','stream_id']
        def finds(*args, **kwargs):
            return ([find_list.pop(0)],'junk')
        self.mock_rr_find.side_effect = finds
        association_list = ['one','two','three']
        def associations(*args,**kwargs):
            return [association_list.pop(0)]
        self.mock_rr_assoc.side_effect = associations


        # execution
        ret = self.transform_service.delete_transform('mock_transform_id')

        # assertions
        self.transform_service.read_transform.assert_called_with(transform_id='mock_transform_id')
        self.mock_pd_cancel.assert_called_with('pid')
        self.assertEquals(self.mock_rr_find.call_count,3)
        self.assertEquals(self.mock_rr_del_assoc.call_count,3)
        self.assertEquals(self.mock_rr_delete.call_count,1)



    def test_activate_transform(self):
        # mocks
        self.mock_rr_find.return_value = [['one','two','three'],'garbage']

        # execution
        ret = self.transform_service.activate_transform('transform_id')

        # assertions
        self.mock_rr_find.assert_called_with('transform_id',PRED.hasSubscription,RT.Subscription,True)
        self.assertEquals(self.mock_ps_activate.call_count,3)

        # ---

    def test_activate_transform_nonexist(self):
        # mocks
        self.mock_rr_find.return_value = ([],'')

        # execution
        with self.assertRaises(NotFound):
            ret = self.transform_service.activate_transform('transform_id')



    def test_schedule_transform(self):
        # not implemented
        with self.assertRaises(NotImplementedError):
            self.transform_service.schedule_transform()

    def test_execute_transform(self):
        # Mocks
        procdef = ProcessDefinition(
            executable={
                'module':'ion.processes.data.transforms.transform_example',
                'class':'ReverseTransform'}
        )
        self.mock_pd_read.return_value = procdef

        retval = self.transform_service.execute_transform('1234',[1,2,3,4])

        self.assertEquals(retval,[4,3,2,1])


@attr('INT', group='dm')
class TransformManagementServiceIntTest(IonIntegrationTestCase):

    def setUp(self):
        # set up the container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')

        self.pubsub_cli = PubsubManagementServiceClient(node=self.container.node)
        self.tms_cli = TransformManagementServiceClient(node=self.container.node)
        self.rr_cli = ResourceRegistryServiceClient(node=self.container.node)
        self.procd_cli = ProcessDispatcherServiceClient(node=self.container.node)

        self.input_stream_id = self.pubsub_cli.create_stream(name='input_stream',original=True)

        self.input_subscription_id = self.pubsub_cli.create_subscription(query=StreamQuery(stream_ids=[self.input_stream_id]),exchange_name='transform_input',name='input_subscription')

        self.output_stream_id = self.pubsub_cli.create_stream(name='output_stream',original=True)

        self.process_definition = ProcessDefinition(name='basic_transform_definition')
        self.process_definition.executable = {'module': 'ion.processes.data.transforms.transform_example',
                                              'class':'TransformExample'}
        self.process_definition_id = self.procd_cli.create_process_definition(process_definition=self.process_definition)

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    def test_create_transform(self):
        configuration = {'program_args':{'arg1':'value'}}

        transform_id = self.tms_cli.create_transform(
              name='test_transform',
              in_subscription_id=self.input_subscription_id,
              out_streams={'output':self.output_stream_id},
              process_definition_id=self.process_definition_id)

        # test transform creation in rr
        transform = self.rr_cli.read(transform_id)
        self.assertEquals(transform.name,'test_transform')


        # test associations
        predicates = [PRED.hasSubscription, PRED.hasOutStream, PRED.hasProcessDefinition]
        assocs = []
        for p in predicates:
            assocs += self.rr_cli.find_associations(transform_id,p,id_only=True)
        self.assertEquals(len(assocs),3)

        # test process creation
        transform = self.tms_cli.read_transform(transform_id)
        pid = transform.process_id
        proc = self.container.proc_manager.procs.get(pid)
        self.assertIsInstance(proc,TransformExample)

        # clean up
        self.tms_cli.delete_transform(transform_id)

    def test_create_transform_no_procdef(self):
        with self.assertRaises(NotFound):
            self.tms_cli.create_transform(name='test',in_subscription_id=self.input_subscription_id)

    def test_create_transform_bad_procdef(self):
        with self.assertRaises(NotFound):
            self.tms_cli.create_transform(name='test',
                in_subscription_id=self.input_subscription_id,
                process_definition_id='bad')
    
    def test_create_transform_no_config(self):
        transform_id = self.tms_cli.create_transform(
            name='test_transform',
            in_subscription_id=self.input_subscription_id,
            out_streams={'output':self.output_stream_id},
            process_definition_id=self.process_definition_id,
        )
        self.tms_cli.delete_transform(transform_id)

    def test_create_transform_name_failure(self):
        transform_id = self.tms_cli.create_transform(
            name='test_transform',
            in_subscription_id=self.input_subscription_id,
            out_streams={'output':self.output_stream_id},
            process_definition_id=self.process_definition_id,
        )
        with self.assertRaises(BadRequest):
            transform_id = self.tms_cli.create_transform(
                name='test_transform',
                in_subscription_id=self.input_subscription_id,
                out_streams={'output':self.output_stream_id},
                process_definition_id=self.process_definition_id,
            )
        self.tms_cli.delete_transform(transform_id)

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    def test_create_no_output(self):
        transform_id = self.tms_cli.create_transform(
            name='test_transform',
            in_subscription_id=self.input_subscription_id,
            process_definition_id=self.process_definition_id,
        )

        predicates = [PRED.hasSubscription, PRED.hasOutStream, PRED.hasProcessDefinition]
        assocs = []
        for p in predicates:
            assocs += self.rr_cli.find_associations(transform_id,p,id_only=True)
        self.assertEquals(len(assocs),2)

        # test process creation
        transform = self.tms_cli.read_transform(transform_id)
        pid = transform.process_id
        proc = self.container.proc_manager.procs.get(pid)
        self.assertIsInstance(proc,TransformExample)

        self.tms_cli.delete_transform(transform_id)
    def test_read_transform_exists(self):
        trans_obj = IonObject(RT.Transform,name='trans_obj')
        trans_id, _ = self.rr_cli.create(trans_obj)

        res = self.tms_cli.read_transform(trans_id)
        actual = self.rr_cli.read(trans_id)

        self.assertEquals(res._id,actual._id)


    def test_read_transform_nonexist(self):
        with self.assertRaises(NotFound) as e:
            self.tms_cli.read_transform('123')

    def test_activate_transform(self):

        transform_id = self.tms_cli.create_transform(
            name='test_transform',
            in_subscription_id=self.input_subscription_id,
            out_streams={'output':self.output_stream_id},
            process_definition_id=self.process_definition_id
        )

        self.tms_cli.activate_transform(transform_id)

        # pubsub check if activated?
        self.tms_cli.delete_transform(transform_id)

    def test_activate_transform_nonexist(self):
        with self.assertRaises(NotFound):
            self.tms_cli.activate_transform('1234')

    def test_delete_transform(self):

        transform_id = self.tms_cli.create_transform(
            name='test_transform',
            in_subscription_id=self.input_subscription_id,
            process_definition_id=self.process_definition_id
        )
        self.tms_cli.delete_transform(transform_id)

        # assertions
        with self.assertRaises(NotFound):
            self.rr_cli.read(transform_id)


    def test_delete_transform_nonexist(self):
        with self.assertRaises(NotFound):
            self.tms_cli.delete_transform('123')

    def test_execute_transform(self):
        # set up
        process_definition = ProcessDefinition(name='procdef_execute')
        process_definition.executable['module'] = 'ion.processes.data.transforms.transform_example'
        process_definition.executable['class'] = 'ReverseTransform'
        data = [1,2,3]

        process_definition_id, _ = self.rr_cli.create(process_definition)

        retval = self.tms_cli.execute_transform(process_definition_id,data)

        self.assertEquals(retval,[3,2,1])


    def test_integrated_transform(self):
        '''
        This example script runs a chained three way transform:
            B
        A <
            C
        Where A is the even_odd transform (generates a stream of even and odd numbers from input)
        and B and C are the basic transforms that receive even and odd input
        '''
        cc = self.container
        assertions = self.assertTrue

        pubsub_cli = PubsubManagementServiceClient(node=cc.node)
        rr_cli = ResourceRegistryServiceClient(node=cc.node)
        tms_cli = TransformManagementServiceClient(node=cc.node)
        #-------------------------------
        # Process Definition
        #-------------------------------
        # Create the process definition for the basic transform
        process_definition = IonObject(RT.ProcessDefinition, name='basic_transform_definition')
        process_definition.executable = {
            'module': 'ion.processes.data.transforms.transform_example',
            'class':'TransformExample'
        }
        basic_transform_definition_id, _ = rr_cli.create(process_definition)

        # Create The process definition for the TransformEvenOdd
        process_definition = IonObject(RT.ProcessDefinition, name='evenodd_transform_definition')
        process_definition.executable = {
            'module': 'ion.processes.data.transforms.transform_example',
            'class':'TransformEvenOdd'
        }
        evenodd_transform_definition_id, _ = rr_cli.create(process_definition)

        #-------------------------------
        # Streams
        #-------------------------------
        streams = [pubsub_cli.create_stream() for i in xrange(5)]

        #-------------------------------
        # Subscriptions
        #-------------------------------

        query = StreamQuery(stream_ids=[streams[0]])
        input_subscription_id = pubsub_cli.create_subscription(query=query, exchange_name='input_queue')

        query = StreamQuery(stream_ids = [streams[1]]) # even output
        even_subscription_id = pubsub_cli.create_subscription(query=query, exchange_name='even_queue')

        query = StreamQuery(stream_ids = [streams[2]]) # odd output
        odd_subscription_id = pubsub_cli.create_subscription(query=query, exchange_name='odd_queue')


        #-------------------------------
        # Launch the EvenOdd Transform
        #-------------------------------

        evenodd_id = tms_cli.create_transform(name='even_odd',
            in_subscription_id=input_subscription_id,
            out_streams={'even':streams[1], 'odd':streams[2]},
            process_definition_id=evenodd_transform_definition_id,
            configuration={})
        tms_cli.activate_transform(evenodd_id)


        #-------------------------------
        # Launch the Even Processing Transform
        #-------------------------------

        even_transform_id = tms_cli.create_transform(name='even_transform',
            in_subscription_id = even_subscription_id,
            out_streams={'even_plus1':streams[3]},
            process_definition_id=basic_transform_definition_id,
            configuration={})
        tms_cli.activate_transform(even_transform_id)

        #-------------------------------
        # Launch the Odd Processing Transform
        #-------------------------------

        odd_transform_id = tms_cli.create_transform(name='odd_transform',
            in_subscription_id = odd_subscription_id,
            out_streams={'odd_plus1':streams[4]},
            process_definition_id=basic_transform_definition_id,
            configuration={})
        tms_cli.activate_transform(odd_transform_id)

        #-------------------------------
        # Set up final subscribers
        #-------------------------------

        evenplus1_subscription_id = pubsub_cli.create_subscription(
            query=StreamQuery([streams[3]]),
            exchange_name='evenplus1_queue',
            name='EvenPlus1Subscription',
            description='EvenPlus1 SubscriptionDescription'
        )
        oddplus1_subscription_id = pubsub_cli.create_subscription(
            query=StreamQuery([streams[4]]),
            exchange_name='oddplus1_queue',
            name='OddPlus1Subscription',
            description='OddPlus1 SubscriptionDescription'
        )

        total_msg_count = 2

        msgs = gevent.queue.Queue()


        def even1_message_received(message, headers):
            input = int(message.get('num'))
            assertions( (input % 2) ) # Assert it is odd (transform adds 1)
            msgs.put(True)


        def odd1_message_received(message, headers):
            input = int(message.get('num'))
            assertions(not (input % 2)) # Assert it is even
            msgs.put(True)

        subscriber_registrar = StreamSubscriberRegistrar(process=cc, node=cc.node)
        even_subscriber = subscriber_registrar.create_subscriber(exchange_name='evenplus1_queue', callback=even1_message_received)
        odd_subscriber = subscriber_registrar.create_subscriber(exchange_name='oddplus1_queue', callback=odd1_message_received)

        # Start subscribers
        even_subscriber.start()
        odd_subscriber.start()

        # Activate subscriptions
        pubsub_cli.activate_subscription(evenplus1_subscription_id)
        pubsub_cli.activate_subscription(oddplus1_subscription_id)

        #-------------------------------
        # Set up fake stream producer
        #-------------------------------

        pid = cc.spawn_process(name='dummy_process_for_test',
            module='pyon.ion.process',
            cls='SimpleProcess',
            config={})
        dummy_process = cc.proc_manager.procs[pid]

        # Normally the user does not see or create the publisher, this is part of the containers business.
        # For the test we need to set it up explicitly
        publisher_registrar = StreamPublisherRegistrar(process=dummy_process, node=cc.node)
        stream_publisher = publisher_registrar.create_publisher(stream_id=streams[0])

        #-------------------------------
        # Start test
        #-------------------------------

        # Publish a stream
        for i in xrange(total_msg_count):
            stream_publisher.publish({'num':str(i)})

        time.sleep(0.5)

        for i in xrange(total_msg_count * 2):
            try:
                msgs.get()
            except Empty:
                assertions(False, "Failed to process all messages correctly.")

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST',False),'CEI incompatible')
    def test_transform_restart(self):
        tms_cli = self.tms_cli
        rr_cli = self.rr_cli
        pubsub_cli = self.pubsub_cli
        cc = self.container
        assertions = self.assertTrue

        proc_def = ProcessDefinition(executable=
            {
            'module':'ion.services.dm.transformation.test.test_transform_service',
            'class':'TestTransform'
        })
        proc_def_id, _ = rr_cli.create(proc_def)

        stream_id = pubsub_cli.create_stream()
        subscription_id = pubsub_cli.create_subscription(query=StreamQuery(stream_ids=[stream_id]),exchange_name='test_transform')

        transform_id = tms_cli.create_transform(
            name='test_transform',
            in_subscription_id=subscription_id,
            process_definition_id=proc_def_id
        )
        tms_cli.activate_transform(transform_id)

        transform = rr_cli.read(transform_id)
        pid = transform.process_id

        assertions(cc.proc_manager.procs.has_key(pid), 'Transform process was never started.')
        cc.proc_manager.terminate_process(pid)

        assertions(not cc.proc_manager.procs.has_key(pid),'Transform process was never killed')

        handle = cc.proc_manager.procs_by_name['transform_management']
        handle._restart_transform(transform_id)

        transform = rr_cli.read(transform_id)
        pid = transform.process_id

        assertions(cc.proc_manager.procs.has_key(pid), 'Transform process was never restarted.')
        

