#!/usr/bin/env python

"""
@package ion.agents.data.test.test_external_dataset_agent_netcdf
@file ion/agents/data/test/test_external_dataset_agent_netcdf
@author Christopher Mueller
@brief 
"""

# Import pyon first for monkey patching.
from pyon.public import log
from pyon.ion.resource import PRED, RT
from pyon.ion.granule.taxonomy import TaxyTool
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.objects import ExternalDatasetAgent, ExternalDatasetAgentInstance, ExternalDataProvider, DataProduct, DataSourceModel, ContactInformation, UpdateDescription, DatasetDescription, ExternalDataset, Institution, DataSource

from ion.agents.data.test.test_external_dataset_agent import ExternalDatasetAgentTestBase, IonIntegrationTestCase

from nose.plugins.attrib import attr

@attr('INT', group='eoi')
class TestExternalDatasetAgent_Netcdf(ExternalDatasetAgentTestBase, IonIntegrationTestCase):
    DVR_CONFIG = {
        'dvr_mod' : 'ion.agents.data.handlers.netcdf_data_handler',
        'dvr_cls' : 'NetcdfDataHandler',
        }

    HIST_CONSTRAINTS_1 = {
        'count':15,
        'temporal_slice':'(slice(4,10))',
        }

    HIST_CONSTRAINTS_2 = {
        'count':10,
        'temporal_slice':'(slice(0,16,2))',
        }


    def _setup_resources(self):
        # TODO: some or all of this (or some variation) should move to DAMS'

        # Build the test resources for the dataset
        dams_cli = DataAcquisitionManagementServiceClient()
        dpms_cli = DataProductManagementServiceClient()
        rr_cli = ResourceRegistryServiceClient()

        eda = ExternalDatasetAgent()
        eda_id = dams_cli.create_external_dataset_agent(eda)

        eda_inst = ExternalDatasetAgentInstance()
        eda_inst_id = dams_cli.create_external_dataset_agent_instance(eda_inst, external_dataset_agent_id=eda_id)

        # Create and register the necessary resources/objects

        # Create DataProvider
        dprov = ExternalDataProvider(institution=Institution(), contact=ContactInformation())
        dprov.contact.name = 'Christopher Mueller'
        dprov.contact.email = 'cmueller@asascience.com'

        # Create DataSource
        dsrc = DataSource(protocol_type='DAP', institution=Institution(), contact=ContactInformation())
        dsrc.connection_params['base_data_url'] = ''
        dsrc.contact.name='Tim Giguere'
        dsrc.contact.email = 'tgiguere@asascience.com'

        # Create ExternalDataset
        ds_name = 'usgs_test_dataset'
        dset = ExternalDataset(name=ds_name, dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())

        # The usgs.nc test dataset is a download of the R1 dataset found here:
        # http://thredds-test.oceanobservatories.org/thredds/dodsC/ooiciData/E66B1A74-A684-454A-9ADE-8388C2C634E5.ncml
        dset.dataset_description.parameters['dataset_path'] = 'test_data/usgs.nc'
        dset.dataset_description.parameters['temporal_dimension'] = 'time'
        dset.dataset_description.parameters['zonal_dimension'] = 'lon'
        dset.dataset_description.parameters['meridional_dimension'] = 'lat'
        dset.dataset_description.parameters['vertical_dimension'] = 'z'
        dset.dataset_description.parameters['variables'] = [
            'water_temperature',
            'streamflow',
            'water_temperature_bottom',
            'water_temperature_middle',
            'specific_conductance',
            'data_qualifier',
            ]

        # Create DataSourceModel
        dsrc_model = DataSourceModel(name='dap_model')
        dsrc_model.model = 'DAP'
        dsrc_model.data_handler_module = 'N/A'
        dsrc_model.data_handler_class = 'N/A'

        ## Run everything through DAMS
        ds_id = dams_cli.create_external_dataset(external_dataset=dset)
        ext_dprov_id = dams_cli.create_external_data_provider(external_data_provider=dprov)
        ext_dsrc_id = dams_cli.create_data_source(data_source=dsrc)
        ext_dsrc_model_id = dams_cli.create_data_source_model(dsrc_model)

        # Register the ExternalDataset
        dproducer_id = dams_cli.register_external_data_set(external_dataset_id=ds_id)

        # Or using each method
        dams_cli.assign_data_source_to_external_data_provider(data_source_id=ext_dsrc_id, external_data_provider_id=ext_dprov_id)
        dams_cli.assign_data_source_to_data_model(data_source_id=ext_dsrc_id, data_source_model_id=ext_dsrc_model_id)
        dams_cli.assign_external_dataset_to_data_source(external_dataset_id=ds_id, data_source_id=ext_dsrc_id)
        dams_cli.assign_external_dataset_to_agent_instance(external_dataset_id=ds_id, agent_instance_id=eda_inst_id)
        #        dams_cli.assign_external_data_agent_to_agent_instance(external_data_agent_id=self.eda_id, agent_instance_id=self.eda_inst_id)

        # Generate the data product and associate it to the ExternalDataset
        dprod = DataProduct(name='usgs_parsed_product', description='parsed usgs product')
        dproduct_id = dpms_cli.create_data_product(data_product=dprod)

        dams_cli.assign_data_product(input_resource_id=ds_id, data_product_id=dproduct_id, create_stream=True)

        stream_id, assn = rr_cli.find_objects(subject=dproduct_id, predicate=PRED.hasStream, object_type=RT.Stream, id_only=True)
        stream_id = stream_id[0]

        log.info('Created resources: {0}'.format({'ExternalDataset':ds_id, 'ExternalDataProvider':ext_dprov_id, 'DataSource':ext_dsrc_id, 'DataSourceModel':ext_dsrc_model_id, 'DataProducer':dproducer_id, 'DataProduct':dproduct_id, 'Stream':stream_id}))

        #CBM: Use CF standard_names

        ttool = TaxyTool()
        ttool.add_taxonomy_set('time','time')
        ttool.add_taxonomy_set('lon','longitude')
        ttool.add_taxonomy_set('lat','latitude')
        ttool.add_taxonomy_set('z','water depth')
        ttool.add_taxonomy_set('water_temperature', 'average water temperature')
        ttool.add_taxonomy_set('water_temperature_bottom','water temperature at bottom of water column')
        ttool.add_taxonomy_set('water_temperature_middle', 'water temperature at middle of water column')
        ttool.add_taxonomy_set('streamflow', 'flow velocity of stream')
        ttool.add_taxonomy_set('specific_conductance', 'specific conductance of water')
        ttool.add_taxonomy_set('data_qualifier','data qualifier flag')

        ttool.add_taxonomy_set('coords','This group contains coordinate parameters')
        ttool.add_taxonomy_set('data','This group contains data parameters')

        # Create the logger for receiving publications
        self.create_stream_and_logger(name='usgs',stream_id=stream_id)

        self.EDA_RESOURCE_ID = ds_id
        self.EDA_NAME = ds_name
        self.DVR_CONFIG['dh_cfg'] = {
            'TESTING':True,
            'stream_id':stream_id,
            'external_dataset_res':dset,
            'taxonomy':ttool._t,
            'data_producer_id':dproducer_id,#CBM: Should this be put in the main body of the config - with mod & cls?
            'max_records':4,
        }



