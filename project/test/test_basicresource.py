# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from spdb.project import BossResourceBasic


class TestBasicResource(unittest.TestCase):

    def get_image_dict(self):
        """Method to generate an initial set of parameters to use to instantiate a basic resource8
        Returns:
            dict - a dictionary of data to initialize a basic resource8

        """
        data = {}
        data['boss_key'] = ['col1&exp1&ch1&0']
        data['lookup_key'] = ['1&1&1&0']
        data['collection'] = {}
        data['collection']['name'] = "col1"
        data['collection']['description'] = "Test collection 1"

        data['coord_frame'] = {}
        data['coord_frame']['name'] = "coord_frame_1"
        data['coord_frame']['description'] = "Test coordinate frame"
        data['coord_frame']['x_start'] = 0
        data['coord_frame']['x_stop'] = 2000
        data['coord_frame']['y_start'] = 0
        data['coord_frame']['y_stop'] = 5000
        data['coord_frame']['z_start'] = 0
        data['coord_frame']['z_stop'] = 200
        data['coord_frame']['x_voxel_size'] = 4
        data['coord_frame']['y_voxel_size'] = 4
        data['coord_frame']['z_voxel_size'] = 35
        data['coord_frame']['voxel_unit'] = "nanometers"
        data['coord_frame']['time_step'] = 0
        data['coord_frame']['time_step_unit'] = "na"

        data['experiment'] = {}
        data['experiment']['name'] = "exp1"
        data['experiment']['description'] = "Test experiment 1"
        data['experiment']['num_hierarchy_levels'] = 7
        data['experiment']['hierarchy_method'] = 'slice'
        data['experiment']['max_time_sample'] = 0

        data['channel_layer'] = {}
        data['channel_layer']['name'] = "ch1"
        data['channel_layer']['description'] = "Test channel 1"
        data['channel_layer']['is_channel'] = True
        data['channel_layer']['datatype'] = 'uint8'

        return data

    def test_basic_resource_col(self):
        """Test basic get collection interface

        Returns:
            None

        """
        setup_data = self.get_image_dict()
        resource = BossResourceBasic(setup_data)

        col = resource.get_collection()

        assert col.name == setup_data['collection']['name']
        assert col.description == setup_data['collection']['description']

    def test_basic_resource_coord_frame(self):
        """Test basic get coordinate frame interface

        Returns:
            None

        """
        setup_data = self.get_image_dict()
        resource = BossResourceBasic(setup_data)

        coord = resource.get_coord_frame()

        assert coord.name == setup_data['coord_frame']['name']
        assert coord.description == setup_data['coord_frame']['description']
        assert coord.x_start == setup_data['coord_frame']['x_start']
        assert coord.x_stop == setup_data['coord_frame']['x_stop']
        assert coord.y_start == setup_data['coord_frame']['y_start']
        assert coord.y_stop == setup_data['coord_frame']['y_stop']
        assert coord.z_start == setup_data['coord_frame']['z_start']
        assert coord.z_stop == setup_data['coord_frame']['z_stop']
        assert coord.x_voxel_size == setup_data['coord_frame']['x_voxel_size']
        assert coord.y_voxel_size == setup_data['coord_frame']['y_voxel_size']
        assert coord.z_voxel_size == setup_data['coord_frame']['z_voxel_size']
        assert coord.voxel_unit == setup_data['coord_frame']['voxel_unit']
        assert coord.time_step == setup_data['coord_frame']['time_step']
        assert coord.time_step_unit == setup_data['coord_frame']['time_step_unit']

    def test_basic_resource_experiment(self):
        """Test basic get experiment interface

        Returns:
            None

        """
        setup_data = self.get_image_dict()
        resource = BossResourceBasic(setup_data)

        exp = resource.get_experiment()

        assert exp.name == setup_data['experiment']['name']
        assert exp.description == setup_data['experiment']['description']
        assert exp.num_hierarchy_levels == setup_data['experiment']['num_hierarchy_levels']
        assert exp.hierarchy_method == setup_data['experiment']['hierarchy_method']
        assert exp.max_time_sample == setup_data['experiment']['max_time_sample']

    def test_basic_resource_channel_no_time(self):
        """Test basic get channel interface

        Returns:
            None

        """
        setup_data = self.get_image_dict()
        resource = BossResourceBasic(setup_data)

        assert resource.is_channel() == True

        assert not resource.get_layer()

        channel = resource.get_channel()
        assert channel.name == setup_data['channel_layer']['name']
        assert channel.description == setup_data['channel_layer']['description']
        assert channel.datatype == setup_data['channel_layer']['datatype']

    def test_basic_resource_layer_no_time(self):
        """Test basic get layer interface

        Returns:
            None

        """
        setup_data = self.get_image_dict()
        setup_data['channel_layer']['name'] = "layer1"
        setup_data['channel_layer']['description'] = "Test layer 1"
        setup_data['channel_layer']['is_channel'] = False
        setup_data['channel_layer']['layer_map'] = ['ch1']
        setup_data['channel_layer']['base_resolution'] = 2
        resource = BossResourceBasic(setup_data)

        assert resource.is_channel() == False

        assert not resource.get_channel()

        channel = resource.get_layer()
        assert channel.name == setup_data['channel_layer']['name']
        assert channel.description == setup_data['channel_layer']['description']
        assert channel.datatype == setup_data['channel_layer']['datatype']
        assert channel.parent_channels == setup_data['channel_layer']['layer_map']
        assert channel.base_resolution == setup_data['channel_layer']['base_resolution']

    def test_basic_resource_get_boss_key(self):
        """Test basic get boss key interface

        Returns:
            None

        """
        setup_data = self.get_image_dict()
        resource = BossResourceBasic(setup_data)

        assert resource.get_boss_key() == setup_data['boss_key']

    def test_basic_resource_get_lookup_key(self):
        """Test basic get lookup key interface

        Returns:
            None

        """
        setup_data = self.get_image_dict()
        resource = BossResourceBasic(setup_data)

        assert resource.get_lookup_key() == setup_data['lookup_key']

    def test_basic_resource_get_data_type(self):
        """Test basic get datatype interface

        Returns:
            None

        """
        setup_data = self.get_image_dict()
        resource = BossResourceBasic(setup_data)

        assert resource.get_data_type() == setup_data['channel_layer']['datatype']

