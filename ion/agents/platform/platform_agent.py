#!/usr/bin/env python

"""
@package ion.agents.platform.platform_agent
@file    ion/agents/platform/platform_agent.py
@author  Carlos Rueda
@brief   Supporting types for platform agents.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
from pyon.ion.stream import StreamPublisher
from pyon.ion.stream import StandaloneStreamPublisher
from pyon.agent.agent import ResourceAgent
from pyon.agent.agent import ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent
from interface.objects import AgentCommand
from pyon.agent.agent import ResourceAgentClient
from pyon.util.context import LocalContextMixin

# Pyon exceptions.
from pyon.core.exception import BadRequest

from ion.agents.instrument.common import BaseEnum

from ion.agents.platform.exceptions import PlatformException
from ion.agents.platform.platform_driver import AttributeValueDriverEvent
from ion.agents.platform.exceptions import CannotInstantiateDriverException

from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.services.dm.utility.granule.granule import build_granule
import numpy
from ion.agents.platform.test.adhoc import adhoc_get_parameter_dictionary

from ion.agents.instrument.instrument_fsm import InstrumentFSM

from ion.agents.platform.platform_agent_launcher import LauncherFactory


# NOTE: the bigger the platform network size starting from the platform
# associated with a PlatformAgent instance, the more the time that should be
# given for commands to sub-platforms to complete. The following TIMEOUT value
# intends to be big enough for all typical cases.
TIMEOUT = 90


PA_MOD = 'ion.agents.platform.platform_agent'
PA_CLS = 'PlatformAgent'


# TODO clean up log-and-throw anti-idiom in several places, which is used
# because the exception alone does not show up in the logs!

class PlatformAgentState(ResourceAgentState):
    """
    Platform agent state enum.
    """
    pass


class PlatformAgentEvent(ResourceAgentEvent):
    PING_AGENT            = 'PLATFORM_AGENT_PING_AGENT'
    GET_SUBPLATFORM_IDS   = 'PLATFORM_AGENT_GET_SUBPLATFORM_IDS'


class PlatformAgentCapability(BaseEnum):
    INITIALIZE                = PlatformAgentEvent.INITIALIZE
    RESET                     = PlatformAgentEvent.RESET
    GO_ACTIVE                 = PlatformAgentEvent.GO_ACTIVE
    GO_INACTIVE               = PlatformAgentEvent.GO_INACTIVE
    RUN                       = PlatformAgentEvent.RUN
    GET_RESOURCE_CAPABILITIES = PlatformAgentEvent.GET_RESOURCE_CAPABILITIES
    PING_RESOURCE             = PlatformAgentEvent.PING_RESOURCE
    GET_RESOURCE              = PlatformAgentEvent.GET_RESOURCE

    PING_AGENT                = 'PLATFORM_AGENT_PING_AGENT'
    GET_SUBPLATFORM_IDS       = 'PLATFORM_AGENT_GET_SUBPLATFORM_IDS'



# TODO Use appropriate process in ResourceAgentClient instance construction below.
# for now, just replicating typical mechanism in test cases.
class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


class PlatformAgent(ResourceAgent):
    """
    Platform resource agent.
    """

    # Override to publish specific types of events
    COMMAND_EVENT_TYPE = "DeviceCommandEvent" #TODO how this works?

    # Override to set specific origin type
    ORIGIN_TYPE = "PlatformDevice"  #TODO how this works?

    def __init__(self, standalone=None):
        log.info("PlatformAgent constructor called")
        ResourceAgent.__init__(self)
        self._standalone = standalone
        self._plat_config = None
        self._platform_id = None
        self._topology = None
        self._plat_driver = None

        # Platform ID of my parent, if any. This is mainly used for diagnostic
        # purposes
        self._parent_platform_id = None

        # Dictionary of data stream IDs for data publishing. Constructed
        # by stream_config agent config member during process on_init.
        self._data_streams = {}

        self._param_dicts = {}

        # Dictionary of data stream publishers. Constructed by
        # stream_config agent config member during process on_init.
        self._data_publishers = {}

        # {subplatform_id: (ResourceAgentClient, PID), ...}
        self._pa_clients = {}  # Never None

        self._launcher = LauncherFactory.createLauncher(standalone=standalone)
        log.debug("launcher created: %s", str(type(self._launcher)))

        #
        # TODO the following defined here as in InstrumentAgent,
        # but these will likely be part of the platform (or associated
        # instruments) metadata
        self._lat = 0
        self._lon = 0
        self._height = 0


        # standalone stuff
        self.container = None
        if self._standalone:
            self.resource_id = self._standalone['platform_id']
            self.container = self._standalone.get('container', None)
            self._on_init()

        log.info("PlatformAgent constructor complete.")

    def _reset(self):
        """
        Resets this platform agent (terminates sub-platforms processes,
        clears self._pa_clients, destroys driver).

        NOTE that this method is to be called *after* sending the RESET command
        to my sub-platforms (if any).
        """
        log.debug("%r: resetting", self._platform_id)

        # terminate sub-platform agent processes:
        if len(self._pa_clients):
            log.debug("%r: terminating sub-platform agent processes (%d)",
                self._platform_id, len(self._pa_clients))
            for subplatform_id in self._pa_clients:
                _, pid = self._pa_clients[subplatform_id]
                try:
                    self._launcher.cancel_process(pid)
                except Exception as e:
                    log.warn("%r: exception in cancel_process for subplatform_id=%r, pid=%r: %s",
                             self._platform_id, subplatform_id, pid, str(e)) #, exc_Info=True)

        self._pa_clients.clear()

        self._plat_config = None
        self._platform_id = None
        if self._plat_driver:
            self._plat_driver.destroy()
            self._plat_driver = None

    def _pre_initialize(self):
        """
        Does verification of self._plat_config.

        @raises PlatformException if the verification fails for some reason.
        """
        log.debug("%r: plat_config=%s ",
            self._platform_id, str(self._plat_config))

        if not self._plat_config:
            msg = "plat_config not provided"
            log.error(msg)
            raise PlatformException(msg)

        for k in ['platform_id', 'driver_config', 'container_name']:
            if not k in self._plat_config:
                msg = "'%s' key not given in plat_config=%s" % (k, self._plat_config)
                log.error(msg)
                raise PlatformException(msg)

        self._platform_id = self._plat_config['platform_id']
        driver_config = self._plat_config['driver_config']
        for k in ['dvr_mod', 'dvr_cls']:
            if not k in driver_config:
                msg = "%r: '%s' key not given in driver_config=%s" % (
                    self._platform_id, k, driver_config)
                log.error(msg)
                raise PlatformException(msg)

        self._container_name = self._plat_config['container_name']

        if 'platform_topology' in self._plat_config:
            self._topology = self._plat_config['platform_topology']

        ppid = self._plat_config.get('parent_platform_id', None)
        if ppid:
            self._parent_platform_id = ppid
            log.debug("_parent_platform_id set to: %s", self._parent_platform_id)

    def _create_publisher(self, stream_id=None, stream_route=None):
        if self._standalone:
            publisher = StandaloneStreamPublisher(stream_id, stream_route)
        else:
            publisher = StreamPublisher(process=self, stream_id=stream_id, stream_route=stream_route)

        return publisher

    def _construct_data_publishers(self):
        """
        Construct the stream publishers from the stream_config agent
        config variable.
        @retval None
        """

        stream_info = self.CFG.stream_config
        log.debug("%r: stream_info = %s",
            self._platform_id, stream_info)

        for (stream_name, stream_config) in stream_info.iteritems():

            stream_route = stream_config['stream_route']

            log.debug("%r: stream_name=%r, stream_route=%r",
                self._platform_id, stream_name, stream_route)

            stream_id = stream_config['stream_id']
            self._data_streams[stream_name] = stream_id
            self._param_dicts[stream_name] = adhoc_get_parameter_dictionary(stream_name)
            publisher = self._create_publisher(stream_id=stream_id, stream_route=stream_route)
            self._data_publishers[stream_name] = publisher
            log.debug("%r: created publisher for stream_name=%r",
                  self._platform_id, stream_name)

    def _create_driver(self):
        """
        Creates the platform driver object for this platform agent.

        NOTE: the driver object is created directly (not via a spawned process)
        """
        driver_config = self._plat_config['driver_config']
        driver_module = driver_config['dvr_mod']
        driver_class = driver_config['dvr_cls']

        assert self._platform_id is not None, "must know platform_id to create driver"

        log.debug('%r: creating driver: %s',
            self._platform_id,  driver_config)

        try:
            module = __import__(driver_module, fromlist=[driver_class])
            classobj = getattr(module, driver_class)
            driver = classobj(self._platform_id, driver_config, self._parent_platform_id)

        except Exception as e:
            msg = '%r: could not import/construct driver: module=%s, class=%s' % (
                self._platform_id, driver_module, driver_class)
            log.error("%s; reason=%s", msg, str(e))  #, exc_Info=True)
            raise CannotInstantiateDriverException(msg=msg, reason=e)

        self._plat_driver = driver
        self._plat_driver.set_event_listener(self.evt_recv)

        if self._topology:
            self._plat_driver.set_topology(self._topology)

        log.debug("%r: driver created: %s",
            self._platform_id, str(driver))

    def _assert_driver(self):
        assert self._plat_driver is not None, "_create_driver must have been called first"

    def _do_initialize(self):
        """
        Does the main initialize sequence, which includes activation of the
        driver and launch of the sub-platforms
        """
        self._pre_initialize()
        self._construct_data_publishers()
        self._create_driver()
        self._plat_driver.go_active()

    def _do_go_active(self):
        """
        Does nothing at the moment.
        """
        pass

    def _go_inactive(self):
        """
        Does nothing at the moment.
        """
        pass

    def _run(self):
        """
        """
        self._start_resource_monitoring()

    def _start_resource_monitoring(self):
        """
        Calls self._plat_driver.start_resource_monitoring()
        """
        self._assert_driver()
        self._plat_driver.start_resource_monitoring()

    def _stop_resource_monitoring(self):
        """
        Calls self._plat_driver.stop_resource_monitoring()
        """
        self._assert_driver()
        self._plat_driver.stop_resource_monitoring()

    def evt_recv(self, driver_event):
        """
        Callback to receive asynchronous driver events.
        @param driver_event The driver event received.
        """
        log.debug('%r: in state=%s: received driver_event=%s',
            self._platform_id, self.get_agent_state(), str(driver_event))

        if not isinstance(driver_event, AttributeValueDriverEvent):
            log.warn('%r: driver_event not handled: %s',
                self._platform_id, str(type(driver_event)))
            return

        stream_name = driver_event._attr_id
        if not stream_name in self._data_streams:
            log.warn('%r: got attribute value event for unconfigured stream %r',
                     self._platform_id, stream_name)
            return

        publisher = self._data_publishers.get(stream_name, None)
        if not publisher:
            log.warn('%r: no publisher configured for stream %r',
                     self._platform_id, stream_name)
            return

        param_dict = self._param_dicts.get(stream_name, None)
        if not param_dict:
            log.warn('%r: No ParameterDictionary given for stream %r',
                     self._platform_id, stream_name)
            return

        rdt = RecordDictionaryTool(param_dictionary=param_dict)

        rdt['value'] =  numpy.array([driver_event._value])
        rdt['lat'] =    numpy.array([self._lat])
        rdt['lon'] =    numpy.array([self._lon])
        rdt['height'] = numpy.array([self._height])

        g = build_granule(data_producer_id=self.resource_id,
            param_dictionary=param_dict, record_dictionary=rdt)

        stream_id = self._data_streams[stream_name]
        publisher.publish(g, stream_id=stream_id)
        log.debug('%r: published data granule on stream %r, rdt=%r',
            self._platform_id, stream_name, str(rdt))

    ##########################################################################
    # TBD
    ##########################################################################

    def add_instrument(self, instrument_config):
        # TODO addition of instruments TBD in general
        pass

    def add_instruments(self):
        # TODO this is just a sketch; not all operations will necessarily happen
        # in this same call.
        # query resource registry to find all instruments
#        for instr in my_instruments:
#            launch_instrument_agent(...)
#            launch_port_agent(...)
#            activate_instrument(...)
        pass


    ##############################################################
    # supporting routines dealing with sub-platforms
    ##############################################################

    def _launch_platform_agent(self, subplatform_id):
        """
        Launches a sub-platform agent, creates ResourceAgentClient, and pings
        and initializes the sub-platform agent.

        @param subplatform_id Platform ID
        """
        agent_config = {
            'agent':            {'resource_id': subplatform_id},
            'stream_config':    self.CFG.stream_config,
            'test_mode':        True
        }

        log.debug("%r: launching sub-platform agent %s",
            self._platform_id, subplatform_id)
        pid = self._launcher.launch(subplatform_id, agent_config)

        if self._standalone:
            pa_client = pid
        else:
            pa_client = self._create_resource_agent_client(subplatform_id)

        self._pa_clients[subplatform_id] = (pa_client, pid)

        self._ping_subplatform(subplatform_id)
        self._initialize_subplatform(subplatform_id)

    def _create_resource_agent_client(self, subplatform_id):
        """
        Creates and returns a ResourceAgentClient instance.

        @param subplatform_id Platform ID
        """
        log.debug("%r: _create_resource_agent_client: subplatform_id=%s",
            self._platform_id, subplatform_id)

        pa_client = ResourceAgentClient(subplatform_id, process=FakeProcess())

        log.debug("%r: got platform agent client %s",
            self._platform_id, str(pa_client))

        state = pa_client.get_agent_state()
        assert PlatformAgentState.UNINITIALIZED == state

        log.debug("%r: ResourceAgentClient CREATED: subplatform_id=%s",
            self._platform_id, subplatform_id)

        return pa_client

    def _ping_subplatform(self, subplatform_id):
        log.debug("%r: _ping_subplatform -> %r",
            self._platform_id, subplatform_id)

        pa_client, _ = self._pa_clients[subplatform_id]

        cmd = AgentCommand(command=PlatformAgentEvent.PING_AGENT)
        retval = pa_client.execute_agent(cmd, timeout=TIMEOUT)
        log.debug("%r: _ping_subplatform %r  retval = %s",
            self._platform_id, subplatform_id, str(retval))

        if "PONG" != retval.result:
            msg = "unexpected ping response from sub-platform agent: %s " % retval.result
            log.error(msg)
            raise PlatformException(msg)

    def _initialize_subplatform(self, subplatform_id):
        log.debug("%r: _initialize_subplatform -> %r",
            self._platform_id, subplatform_id)

        pa_client, _ = self._pa_clients[subplatform_id]

        # now, initialize the sub-platform agent so the agent network gets
        # built and initialized recursively:
        platform_config = {
            'platform_id': subplatform_id,
            'platform_topology' : self._topology,
            'parent_platform_id' : self._platform_id,
            'driver_config': self._plat_config['driver_config'],
            'container_name': self._container_name,
        }

        kwargs = dict(plat_config=platform_config)
        cmd = AgentCommand(command=PlatformAgentEvent.INITIALIZE, kwargs=kwargs)
        retval = pa_client.execute_agent(cmd, timeout=TIMEOUT)
        log.debug("%r: _initialize_subplatform %r  retval = %s",
            self._platform_id, subplatform_id, str(retval))

    def _subplatforms_launch(self):
        """
        Launches all my sub-platforms storing the corresponding
        ResourceAgentClient objects in _pa_clients.
        """
        self._pa_clients.clear()
        subplatform_ids = self._plat_driver.get_subplatform_ids()
        if len(subplatform_ids):
            log.debug("%r: launching subplatforms %s",
                self._platform_id, str(subplatform_ids))
            for subplatform_id in subplatform_ids:
                self._launch_platform_agent(subplatform_id)

    def _subplatforms_execute_agent(self, command=None, create_command=None,
                                    expected_state=None):
        """
        Supporting routine for the ones below.

        @param create_command invoked as create_command(subplatform_id) for
               each sub-platform to create the command to be executed.
        @param expected_state
        """
        subplatform_ids = self._plat_driver.get_subplatform_ids()
        assert subplatform_ids == self._pa_clients.keys()

        if not len(subplatform_ids):
            # I'm a leaf.
            return

        if command:
            log.debug("%r: executing command %r on my sub-platforms: %s",
                        self._platform_id, command, str(subplatform_ids))
        else:
            log.debug("%r: executing command on my sub-platforms: %s",
                        self._platform_id, str(subplatform_ids))

        #
        # TODO what to do if a sub-platform fails in some way?
        #
        for subplatform_id in self._pa_clients:
            pa_client, _ = self._pa_clients[subplatform_id]
            cmd = AgentCommand(command=command) if command else create_command(subplatform_id)

            # execute command:
            try:
                retval = pa_client.execute_agent(cmd, timeout=TIMEOUT)
            except Exception as e:
                exc = "%s: %s" % (e.__class__.__name__, str(e))
                log.error("%r: exception executing command %r in subplatform %r: %s",
                            self._platform_id, command, subplatform_id, exc) #, exc_Info=True)
                continue

            # verify state:
            try:
                state = pa_client.get_agent_state()
                if expected_state and expected_state != state:
                    log.error("%r: expected subplatform state %r but got %r",
                                self._platform_id, expected_state, state)
            except Exception as e:
                exc = "%s: %s" % (e.__class__.__name__, str(e))
                log.error("%r: exception while calling get_agent_state to subplatform %r: %s",
                            self._platform_id, subplatform_id, exc) #, exc_Info=True)

    def _subplatforms_reset(self):
        self._subplatforms_execute_agent(command=PlatformAgentEvent.RESET,
                                         expected_state=PlatformAgentState.UNINITIALIZED)

    def _subplatforms_go_active(self):
        self._subplatforms_execute_agent(command=PlatformAgentEvent.GO_ACTIVE,
                                         expected_state=PlatformAgentState.IDLE)

    def _subplatforms_go_inactive(self):
        self._subplatforms_execute_agent(command=PlatformAgentEvent.GO_INACTIVE,
                                         expected_state=PlatformAgentState.INACTIVE)

    def _subplatforms_run(self):
        self._subplatforms_execute_agent(command=PlatformAgentEvent.RUN,
                                         expected_state=PlatformAgentState.COMMAND)

    ##############################################################
    # major operations
    ##############################################################

    def _ping_agent(self, *args, **kwargs):
        result = "PONG"
        return result

    def _initialize(self, *args, **kwargs):
        self._plat_config = kwargs.get('plat_config', None)
        self._do_initialize()

        # done with the initialization for this particular agent; and now
        # we have information to launch the sub-platform agents:
        self._subplatforms_launch()

        result = None
        return result

    def _go_active(self):
        # first myself, then sub-platforms
        self._do_go_active()
        self._subplatforms_go_active()
        result = None
        return result

    def _ping_resource(self, *args, **kwargs):
        result = self._plat_driver.ping()
        return result

    ##############################################################
    # UNINITIALIZED event handlers.
    ##############################################################

    def _handler_uninitialized_initialize(self, *args, **kwargs):
        """
        """
        log.debug("%r/%s args=%s kwargs=%s",
            self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = self._initialize(*args, **kwargs)
        next_state = PlatformAgentState.INACTIVE

        return (next_state, result)

    ##############################################################
    # INACTIVE event handlers.
    ##############################################################

    def _handler_inactive_reset(self, *args, **kwargs):
        """
        """
        log.debug("%r/%s args=%s kwargs=%s",
            self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = None
        next_state = PlatformAgentState.UNINITIALIZED

        # first sub-platforms, then myself
        self._subplatforms_reset()
        self._reset()

        return (next_state, result)

    def _handler_inactive_go_active(self, *args, **kwargs):
        """
        """
        log.debug("%r/%s args=%s kwargs=%s",
            self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        next_state = PlatformAgentState.IDLE

        result = self._go_active()

        return (next_state, result)

    ##############################################################
    # IDLE event handlers.
    ##############################################################

    def _handler_idle_reset(self, *args, **kwargs):
        """
        """
        log.debug("%r/%s args=%s kwargs=%s",
            self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = None
        next_state = PlatformAgentState.UNINITIALIZED

        # first sub-platforms, then myself
        self._subplatforms_reset()
        self._reset()

        return (next_state, result)

    def _handler_idle_go_inactive(self, *args, **kwargs):
        """
        """
        log.debug("%r/%s args=%s kwargs=%s",
            self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = None
        next_state = PlatformAgentState.INACTIVE

        # first sub-platforms, then myself
        self._subplatforms_go_inactive()
        self._go_inactive()

        return (next_state, result)

    def _handler_idle_run(self, *args, **kwargs):
        """
        """
        log.debug("%r/%s args=%s kwargs=%s",
            self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = None
        next_state = PlatformAgentState.COMMAND

        # first myself, then sub-platforms
        self._run()
        self._subplatforms_run()

        return (next_state, result)


    ##############################################################
    # COMMAND event handlers.
    ##############################################################

    def _handler_command_reset(self, *args, **kwargs):
        """
        """
        log.debug("%r/%s args=%s kwargs=%s",
            self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = None
        next_state = PlatformAgentState.UNINITIALIZED

        # first sub-platforms, then myself
        self._subplatforms_reset()
        self._reset()

        return (next_state, result)

    def _handler_command_get_subplatform_ids(self, *args, **kwargs):
        """
        Gets the IDs of my direct subplatforms.
        """
        log.debug("%r/%s args=%s kwargs=%s",
            self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = self._plat_driver.get_subplatform_ids()

        next_state = self.get_agent_state()

        return (next_state, result)


    ##############################################################
    # Capabilities interface and event handlers.
    ##############################################################

    def _handler_get_resource_capabilities(self, *args, **kwargs):
        """
        """
        log.debug("%r/%s args=%s kwargs=%s",
            self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        # TODO

        result = None
        next_state = None

#        result = self._dvr_client.cmd_dvr('get_resource_capabilities', *args, **kwargs)
        res_cmds = []
        res_params = []
        result = [res_cmds, res_params]
        return (next_state, result)

    def _filter_capabilities(self, events):

        events_out = [x for x in events if PlatformAgentCapability.has(x)]
        return events_out

    ##############################################################
    # Resource interface and common resource event handlers.
    ##############################################################

    def _handler_get_resource(self, *args, **kwargs):
        """
        """
        log.debug("%r/%s args=%s kwargs=%s",
            self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        attr_names = kwargs.get('attr_names', None)
        if attr_names is None:
            raise BadRequest('get_resource missing attr_names argument.')

        from_time = kwargs.get('from_time', None)
        if from_time is None:
            raise BadRequest('get_resource missing from_time argument.')

        try:
            result = self._plat_driver.get_attribute_values(attr_names, from_time)

            next_state = self.get_agent_state()

        except Exception as ex:
            log.error("error in get_attribute_values %s", str(ex)) #, exc_Info=True)
            raise

        return (next_state, result)

    def _handler_ping_agent(self, *args, **kwargs):
        """
        Pings the agent.
        """
        log.debug("%r/%s args=%s kwargs=%s",
            self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = self._ping_agent(*args, **kwargs)

        next_state = self.get_agent_state()

        return (next_state, result)

    def _handler_ping_resource(self, *args, **kwargs):
        """
        Pings the driver.
        """
        log.debug("%r/%s args=%s kwargs=%s",
            self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = self._ping_resource(*args, **kwargs)

        next_state = self.get_agent_state()

        return (next_state, result)

    ##############################################################
    # FSM setup.
    ##############################################################

    def _construct_fsm(self):
        """
        """
        log.debug("constructing fsm")

        # Instrument agent state machine.
        self._fsm = InstrumentFSM(PlatformAgentState, PlatformAgentEvent,
                                  PlatformAgentEvent.ENTER, PlatformAgentEvent.EXIT)

        for state in PlatformAgentState.list():
            self._fsm.add_handler(state, PlatformAgentEvent.ENTER, self._common_state_enter)
            self._fsm.add_handler(state, PlatformAgentEvent.EXIT, self._common_state_exit)

        # UNINITIALIZED state event handlers.
        self._fsm.add_handler(PlatformAgentState.UNINITIALIZED, PlatformAgentEvent.INITIALIZE, self._handler_uninitialized_initialize)
        self._fsm.add_handler(ResourceAgentState.UNINITIALIZED, PlatformAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.UNINITIALIZED, PlatformAgentEvent.PING_AGENT, self._handler_ping_agent)

        # INACTIVE state event handlers.
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.RESET, self._handler_inactive_reset)
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.GET_SUBPLATFORM_IDS, self._handler_command_get_subplatform_ids)
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.GO_ACTIVE, self._handler_inactive_go_active)
        self._fsm.add_handler(ResourceAgentState.INACTIVE, PlatformAgentEvent.PING_AGENT, self._handler_ping_agent)
        self._fsm.add_handler(ResourceAgentState.INACTIVE, PlatformAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(ResourceAgentState.INACTIVE, PlatformAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)

        # IDLE state event handlers.
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.RESET, self._handler_idle_reset)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.GO_INACTIVE, self._handler_idle_go_inactive)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.RUN, self._handler_idle_run)
        self._fsm.add_handler(ResourceAgentState.IDLE, PlatformAgentEvent.PING_AGENT, self._handler_ping_agent)
        self._fsm.add_handler(ResourceAgentState.IDLE, PlatformAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(ResourceAgentState.IDLE, PlatformAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)

        # COMMAND state event handlers.
        self._fsm.add_handler(PlatformAgentState.COMMAND, PlatformAgentEvent.GO_INACTIVE, self._handler_idle_go_inactive)
        self._fsm.add_handler(PlatformAgentState.COMMAND, PlatformAgentEvent.RESET, self._handler_command_reset)
        self._fsm.add_handler(PlatformAgentState.COMMAND, PlatformAgentEvent.GET_SUBPLATFORM_IDS, self._handler_command_get_subplatform_ids)
        self._fsm.add_handler(ResourceAgentState.COMMAND, PlatformAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.COMMAND, PlatformAgentEvent.PING_AGENT, self._handler_ping_agent)
        self._fsm.add_handler(ResourceAgentState.COMMAND, PlatformAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(ResourceAgentState.COMMAND, PlatformAgentEvent.GET_RESOURCE, self._handler_get_resource)
#        ...

