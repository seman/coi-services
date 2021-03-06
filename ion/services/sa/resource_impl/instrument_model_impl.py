#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.instrument_model_impl
@author   Ian Katz
"""



#from pyon.core.exception import BadRequest, NotFound
from pyon.public import RT, LCS, PRED, LCE

from ion.services.sa.resource_impl.resource_simple_impl import ResourceSimpleImpl
from ion.services.sa.resource_impl.instrument_device_impl import InstrumentDeviceImpl
from ion.services.sa.resource_impl.instrument_agent_impl import InstrumentAgentImpl

class InstrumentModelImpl(ResourceSimpleImpl):
    """
    @brief resource management for InstrumentModel resources
    """

    def on_simpl_init(self):
        self.instrument_agent = InstrumentAgentImpl(self.clients)
        self.instrument_device = InstrumentDeviceImpl(self.clients)

        self.add_lce_precondition(LCE.RETIRE, self.lcs_precondition_retired)


    def _primary_object_name(self):
        return RT.InstrumentModel

    def _primary_object_label(self):
        return "instrument_model"

    def lcs_precondition_retired(self, instrument_model_id):
        """
        can't retire if any devices or agents are using this model
        """
        if 0 < self.instrument_agent.find_having_model(instrument_model_id):
            return "Can't retire an instrument_model still associated to instrument agent(s)"
        
        if 0 < self.instrument_device.find_having_model(instrument_model_id):
            return "Can't retire an instrument_model still associated to instrument_device(s)"

        return ""
       
    def link_stream_definition(self, instrument_model_id='', stream_definition_id=''):
        return self._link_resources(instrument_model_id, PRED.hasStreamDefinition, stream_definition_id)

    def unlink_stream_definition(self, instrument_model_id='', stream_definition_id=''):
        return self._unlink_resources(instrument_model_id, PRED.hasStreamDefinition, stream_definition_id)

    def find_having_stream_definition(self, stream_definition_id):
        return self._find_having(PRED.hasStreamDefinition, stream_definition_id)

    def find_stemming_stream_definition(self, instrument_model_id):
        return self._find_stemming(instrument_model_id, PRED.hasStreamDefinition, RT.StreamDefinition)

