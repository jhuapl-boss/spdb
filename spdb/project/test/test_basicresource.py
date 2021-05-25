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
import numpy as np
import json

from spdb.project import BossResourceBasic
from spdb.project.test.resource_setup import get_image_dict, get_anno_dict


class TestBasicResource(unittest.TestCase):

    def test_basic_resource_col(self):
        """Test basic get collection interface

        Returns:
            None

        """
        setup_data = get_image_dict()
        resource = BossResourceBasic(setup_data)

        col = resource.get_collection()

        assert col.name == setup_data['collection']['name']
        assert col.description == setup_data['collection']['description']

    def test_basic_resource_coord_frame(self):
        """Test basic get coordinate frame interface

        Returns:
            None

        """
        setup_data = get_image_dict()
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

    def test_basic_resource_experiment(self):
        """Test basic get experiment interface

        Returns:
            None

        """
        setup_data = get_image_dict()
        resource = BossResourceBasic(setup_data)

        exp = resource.get_experiment()

        assert exp.name == setup_data['experiment']['name']
        assert exp.description == setup_data['experiment']['description']
        assert exp.num_hierarchy_levels == setup_data['experiment']['num_hierarchy_levels']
        assert exp.hierarchy_method == setup_data['experiment']['hierarchy_method']
        assert exp.num_time_samples == setup_data['experiment']['num_time_samples']
        assert exp.time_step == setup_data['experiment']['time_step']
        assert exp.time_step_unit == setup_data['experiment']['time_step_unit']

    def test_basic_resource_channel_no_time(self):
        """Test basic get channel interface

        Returns:
            None

        """
        setup_data = get_image_dict()
        resource = BossResourceBasic(setup_data)

        channel = resource.get_channel()
        assert channel.is_image() is True
        assert channel.name == setup_data['channel']['name']
        assert channel.description == setup_data['channel']['description']
        assert channel.datatype == setup_data['channel']['datatype']
        assert channel.base_resolution == setup_data['channel']['base_resolution']
        assert channel.sources == setup_data['channel']['sources']
        assert channel.related == setup_data['channel']['related']
        assert channel.default_time_sample == setup_data['channel']['default_time_sample']
        assert channel.downsample_status == setup_data['channel']['downsample_status']

    def test_basic_resource_channel_cloudvolume_uint8(self):
        """Test basic get channel interface

        Returns:
            None

        """
        setup_data = get_image_dict(storage_type='cloudvol')
        resource = BossResourceBasic(setup_data)

        channel = resource.get_channel()
        assert channel.is_image() is True
        assert channel.is_cloudvolume() is True
        assert channel.name == setup_data['channel']['name']
        assert channel.description == setup_data['channel']['description']
        assert channel.datatype == setup_data['channel']['datatype']
        assert channel.base_resolution == setup_data['channel']['base_resolution']
        assert channel.sources == setup_data['channel']['sources']
        assert channel.related == setup_data['channel']['related']
        assert channel.default_time_sample == setup_data['channel']['default_time_sample']
        assert channel.downsample_status == setup_data['channel']['downsample_status']
        assert channel.storage_type == setup_data['channel']['storage_type']
        assert channel.bucket == setup_data['channel']['bucket']
        assert channel.cv_path == setup_data['channel']['cv_path']

    def test_basic_resource_channel_cloudvolume_uint16(self):
        """Test basic get channel interface

        Returns:
            None

        """
        setup_data = get_image_dict(datatype="uint16", storage_type='cloudvol')
        resource = BossResourceBasic(setup_data)

        channel = resource.get_channel()
        assert channel.is_image() is True
        assert channel.is_cloudvolume() is True
        assert channel.name == setup_data['channel']['name']
        assert channel.description == setup_data['channel']['description']
        assert channel.datatype == setup_data['channel']['datatype']
        assert channel.base_resolution == setup_data['channel']['base_resolution']
        assert channel.sources == setup_data['channel']['sources']
        assert channel.related == setup_data['channel']['related']
        assert channel.default_time_sample == setup_data['channel']['default_time_sample']
        assert channel.downsample_status == setup_data['channel']['downsample_status']
        assert channel.storage_type == setup_data['channel']['storage_type']
        assert channel.bucket == setup_data['channel']['bucket']
        assert channel.cv_path == setup_data['channel']['cv_path']

    def test_is_downsampled(self):
        """Test is downsampled method

        Returns:
            None

        """
        setup_data = get_image_dict()
        resource = BossResourceBasic(setup_data)

        self.assertFalse(resource.is_downsampled())

        setup_data['channel']['downsample_status'] = "DOWNSAMPLED"
        resource = BossResourceBasic(setup_data)

        self.assertTrue(resource.is_downsampled())

    def test_basic_resource_get_boss_key(self):
        """Test basic get boss key interface

        Returns:
            None

        """
        setup_data = get_image_dict()
        resource = BossResourceBasic(setup_data)

        assert resource.get_boss_key() == setup_data['boss_key']

    def test_basic_resource_get_lookup_key(self):
        """Test basic get lookup key interface

        Returns:
            None

        """
        setup_data = get_image_dict()
        resource = BossResourceBasic(setup_data)

        assert resource.get_lookup_key() == setup_data['lookup_key']

    def test_basic_resource_get_data_type(self):
        """Test basic get datatype interface

        Returns:
            None

        """
        setup_data = get_image_dict()
        resource = BossResourceBasic(setup_data)

        assert resource.get_data_type() == setup_data['channel']['datatype']

    def test_basic_resource_get_bit_depth(self):
        """Test basic get bit depth interface

        Returns:
            None

        """
        setup_data = get_image_dict()
        resource = BossResourceBasic(setup_data)

        assert resource.get_bit_depth() == 8

    def test_basic_resource_numpy_data_type(self):
        """Test basic get bit depth interface

        Returns:
            None

        """
        setup_data = get_image_dict()
        resource = BossResourceBasic(setup_data)

        assert resource.get_numpy_data_type() == np.uint8

    def test_basic_resource_to_dict(self):
        """Test basic to dict serialization method

        Returns:
            None

        """
        setup_data = get_image_dict()
        resource = BossResourceBasic(setup_data)

        data = resource.to_dict()

        assert data['channel'] == setup_data['channel']

        assert data['collection'] == setup_data['collection']

        assert data['experiment'] == setup_data['experiment']

        assert data['lookup_key'] == '4&3&2'
        assert data['boss_key'] == 'col1&exp1&ch1'

    def test_basic_resource_from_dict(self):
        """Test basic to dict deserialization method

        Returns:
            None

        """
        setup_data = get_image_dict()
        resource1 = BossResourceBasic(setup_data)

        resource2 = BossResourceBasic()
        resource2.from_dict(resource1.to_dict())

        # Check Collection
        col = resource2.get_collection()
        assert col.name == setup_data['collection']['name']
        assert col.description == setup_data['collection']['description']

        # Check coord frame
        coord = resource2.get_coord_frame()
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


        # Check exp
        exp = resource2.get_experiment()
        assert exp.name == setup_data['experiment']['name']
        assert exp.description == setup_data['experiment']['description']
        assert exp.num_hierarchy_levels == setup_data['experiment']['num_hierarchy_levels']
        assert exp.hierarchy_method == setup_data['experiment']['hierarchy_method']
        assert exp.num_time_samples == setup_data['experiment']['num_time_samples']
        assert exp.time_step == setup_data['experiment']['time_step']
        assert exp.time_step_unit == setup_data['experiment']['time_step_unit']

        # Check channel
        channel = resource2.get_channel()
        assert channel.is_image() is True
        assert channel.name == setup_data['channel']['name']
        assert channel.description == setup_data['channel']['description']
        assert channel.datatype == setup_data['channel']['datatype']
        assert channel.base_resolution == setup_data['channel']['base_resolution']
        assert channel.sources == setup_data['channel']['sources']
        assert channel.related == setup_data['channel']['related']
        assert channel.default_time_sample == setup_data['channel']['default_time_sample']

        # check keys
        assert resource2.get_lookup_key() == setup_data['lookup_key']
        assert resource2.get_boss_key() == setup_data['boss_key']

    def test_basic_resource_to_json(self):
        """Test basic to json serialization method

        Returns:
            None

        """
        setup_data = get_image_dict()
        resource = BossResourceBasic(setup_data)

        data = resource.to_json()

        data = json.loads(data)

        assert data['channel'] == setup_data['channel']
        assert data['collection'] == setup_data['collection']
        assert data['experiment'] == setup_data['experiment']
        assert data['coord_frame'] == setup_data['coord_frame']

        assert data['lookup_key'] == '4&3&2'
        assert data['boss_key'] == 'col1&exp1&ch1'

    def test_basic_resource_from_json(self):
        """Test basic to json deserialization method

        Returns:
            None

        """
        setup_data = get_image_dict()
        resource1 = BossResourceBasic(setup_data)

        resource2 = BossResourceBasic()
        resource2.from_json(resource1.to_json())

        # Check Collection
        col = resource2.get_collection()
        assert col.name == setup_data['collection']['name']
        assert col.description == setup_data['collection']['description']

        # Check coord frame
        coord = resource2.get_coord_frame()
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

        # Check exp
        exp = resource2.get_experiment()
        assert exp.name == setup_data['experiment']['name']
        assert exp.description == setup_data['experiment']['description']
        assert exp.num_hierarchy_levels == setup_data['experiment']['num_hierarchy_levels']
        assert exp.hierarchy_method == setup_data['experiment']['hierarchy_method']
        assert exp.num_time_samples == setup_data['experiment']['num_time_samples']
        assert exp.time_step == setup_data['experiment']['time_step']
        assert exp.time_step_unit == setup_data['experiment']['time_step_unit']

        # Check channel
        channel = resource2.get_channel()
        assert channel.is_image() is True
        assert channel.name == setup_data['channel']['name']
        assert channel.description == setup_data['channel']['description']
        assert channel.datatype == setup_data['channel']['datatype']
        assert channel.base_resolution == setup_data['channel']['base_resolution']
        assert channel.sources == setup_data['channel']['sources']
        assert channel.related == setup_data['channel']['related']
        assert channel.default_time_sample == setup_data['channel']['default_time_sample']

        # check keys
        assert resource2.get_lookup_key() == setup_data['lookup_key']
        assert resource2.get_boss_key() == setup_data['boss_key']

    def test_basic_resource_channel_with_source(self):
        """Test basic get channel interface

        Returns:
            None

        """
        setup_data = get_image_dict()
        setup_data['channel']['sources'] = ["src_ch_1"]
        resource = BossResourceBasic(setup_data)

        channel = resource.get_channel()
        assert channel.is_image() is True
        assert channel.name == setup_data['channel']['name']
        assert channel.description == setup_data['channel']['description']
        assert channel.datatype == setup_data['channel']['datatype']
        assert channel.base_resolution == setup_data['channel']['base_resolution']
        assert channel.sources == setup_data['channel']['sources']
        assert channel.related == setup_data['channel']['related']
        assert channel.default_time_sample == setup_data['channel']['default_time_sample']

    def test_basic_resource_channel_with_source1(self):
        """Test basic get channel interface

        Returns:
            None

        """
        setup_data = get_image_dict()
        setup_data['channel']['sources'] = ["src_ch_1", "src_ch_2"]
        resource = BossResourceBasic(setup_data)

        channel = resource.get_channel()
        assert channel.is_image() is True
        assert channel.name == setup_data['channel']['name']
        assert channel.description == setup_data['channel']['description']
        assert channel.datatype == setup_data['channel']['datatype']
        assert channel.base_resolution == setup_data['channel']['base_resolution']
        assert channel.sources == setup_data['channel']['sources']
        assert channel.related == setup_data['channel']['related']
        assert channel.default_time_sample == setup_data['channel']['default_time_sample']

    def test_basic_resource_channel_with_related(self):
        """Test basic get channel interface

        Returns:
            None

        """
        setup_data = get_image_dict()
        setup_data['channel']['related'] = ["ch_2", "ch_3"]
        resource = BossResourceBasic(setup_data)

        channel = resource.get_channel()
        assert channel.is_image() is True
        assert channel.name == setup_data['channel']['name']
        assert channel.description == setup_data['channel']['description']
        assert channel.datatype == setup_data['channel']['datatype']
        assert channel.base_resolution == setup_data['channel']['base_resolution']
        assert channel.sources == setup_data['channel']['sources']
        assert channel.related == setup_data['channel']['related']
        assert channel.default_time_sample == setup_data['channel']['default_time_sample']

    def test_basic_resource_annotation_no_time(self):
        """Test basic get layer interface

        Returns:
            None

        """
        setup_data = get_anno_dict()
        resource = BossResourceBasic(setup_data)

        channel = resource.get_channel()
        assert channel.is_image() is False
        assert channel.name == setup_data['channel']['name']
        assert channel.description == setup_data['channel']['description']
        assert channel.datatype == setup_data['channel']['datatype']
        assert channel.base_resolution == setup_data['channel']['base_resolution']
        assert channel.sources == setup_data['channel']['sources']
        assert channel.related == setup_data['channel']['related']
        assert channel.default_time_sample == setup_data['channel']['default_time_sample']

    def test_basic_resource_from_json_annotation(self):
        """Test basic to json deserialization method

        Returns:
            None

        """
        setup_data = get_anno_dict()
        resource1 = BossResourceBasic(setup_data)

        resource2 = BossResourceBasic()
        resource2.from_json(resource1.to_json())

        # Check Collection
        col = resource2.get_collection()
        assert col.name == setup_data['collection']['name']
        assert col.description == setup_data['collection']['description']

        # Check coord frame
        coord = resource2.get_coord_frame()
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

        # Check exp
        exp = resource2.get_experiment()
        assert exp.name == setup_data['experiment']['name']
        assert exp.description == setup_data['experiment']['description']
        assert exp.num_hierarchy_levels == setup_data['experiment']['num_hierarchy_levels']
        assert exp.hierarchy_method == setup_data['experiment']['hierarchy_method']
        assert exp.num_time_samples == setup_data['experiment']['num_time_samples']
        assert exp.time_step == setup_data['experiment']['time_step']
        assert exp.time_step_unit == setup_data['experiment']['time_step_unit']

        # Check channel
        channel = resource2.get_channel()
        assert channel.is_image() is False
        assert channel.name == setup_data['channel']['name']
        assert channel.description == setup_data['channel']['description']
        assert channel.datatype == setup_data['channel']['datatype']
        assert channel.base_resolution == setup_data['channel']['base_resolution']
        assert channel.sources == setup_data['channel']['sources']
        assert channel.related == setup_data['channel']['related']
        assert channel.default_time_sample == setup_data['channel']['default_time_sample']

        # check keys
        assert resource2.get_lookup_key() == setup_data['lookup_key']
        assert resource2.get_boss_key() == setup_data['boss_key']

    def test_basic_resource_get_iso_level_anisotropic(self):
        """Test get iso level anisotropic

        Returns:
            None

        """
        setup_data = get_anno_dict()
        resource = BossResourceBasic(setup_data)

        self.assertEqual(resource.get_isotropic_level(), 3)

    def test_basic_resource_get_iso_level_isotropic(self):
        """Test get iso level isotropic

        Returns:
            None

        """
        setup_data = get_anno_dict()
        setup_data["experiment"]['hierarchy_method'] = "isotropic"
        resource = BossResourceBasic(setup_data)

        self.assertEqual(resource.get_isotropic_level(), 0)

    def test_basic_resource_get_downsampled_voxel_dims_anisotropic(self):
        """Test downsample voxel dims anisotropic

        Returns:
            None

        """
        setup_data = get_anno_dict()
        resource = BossResourceBasic(setup_data)

        voxel_dims = resource.get_downsampled_voxel_dims()
        self.assertEqual(len(voxel_dims), setup_data["experiment"]['num_hierarchy_levels'])
        self.assertEqual(voxel_dims[0], [4, 4, 35])
        self.assertEqual(voxel_dims[4], [64, 64, 35])

    def test_basic_resource_get_downsampled_voxel_dims_anisotropic_iso(self):
        """Test downsample voxel dims anisotropic with iso flag

        Returns:
            None

        """
        setup_data = get_anno_dict()
        resource = BossResourceBasic(setup_data)

        voxel_dims = resource.get_downsampled_voxel_dims(iso=True)
        self.assertEqual(len(voxel_dims), setup_data["experiment"]['num_hierarchy_levels'])
        self.assertEqual(voxel_dims[0], [4, 4, 35])
        self.assertEqual(voxel_dims[1], [8, 8, 35])
        self.assertEqual(voxel_dims[2], [16, 16, 35])
        self.assertEqual(voxel_dims[3], [32, 32, 35])
        self.assertEqual(voxel_dims[4], [64, 64, 70])
        self.assertEqual(voxel_dims[5], [128, 128, 140])

    def test_basic_resource_get_downsampled_voxel_dims_isotropic(self):
        """Test downsample voxel dims isotropic

        Returns:
            None

        """
        setup_data = get_anno_dict()
        setup_data['coord_frame']['x_voxel_size'] = 6
        setup_data['coord_frame']['y_voxel_size'] = 6
        setup_data['coord_frame']['z_voxel_size'] = 6
        setup_data['experiment']['hierarchy_method'] = "isotropic"
        resource = BossResourceBasic(setup_data)

        voxel_dims = resource.get_downsampled_voxel_dims()
        self.assertEqual(len(voxel_dims), setup_data["experiment"]['num_hierarchy_levels'])
        self.assertEqual(voxel_dims[0], [6, 6, 6])
        self.assertEqual(voxel_dims[3], [48, 48, 48])

    def test_basic_resource_get_downsampled_extent_dims_anisotropic(self):
        """Test downsample extent anisotropic

        Returns:
            None

        """
        setup_data = get_anno_dict()
        resource = BossResourceBasic(setup_data)

        extent_dims = resource.get_downsampled_extent_dims()
        self.assertEqual(len(extent_dims), setup_data["experiment"]['num_hierarchy_levels'])
        self.assertEqual(extent_dims[0], [2000, 5000, 200])
        self.assertEqual(extent_dims[4], [125, 313, 200])


    def test_basic_resource_get_downsampled_extent_dims_anisotropic_iso(self):
        """Test downsample extent anisotropic with isotropic flag

        Returns:
            None

        """
        setup_data = get_anno_dict()
        resource = BossResourceBasic(setup_data)

        extent_dims = resource.get_downsampled_extent_dims(iso=True)
        self.assertEqual(len(extent_dims), setup_data["experiment"]['num_hierarchy_levels'])
        self.assertEqual(extent_dims[0], [2000, 5000, 200])
        self.assertEqual(extent_dims[3], [250, 625, 200])
        self.assertEqual(extent_dims[4], [125, 313, 100])

    def test_basic_resource_get_downsampled_extent_dims_isotropic(self):
        """Test downsample extent isotropic

        Returns:
            None

        """
        setup_data = get_anno_dict()
        setup_data['coord_frame']['x_voxel_size'] = 6
        setup_data['coord_frame']['y_voxel_size'] = 6
        setup_data['coord_frame']['z_voxel_size'] = 6
        setup_data['experiment']['hierarchy_method'] = "isotropic"
        resource = BossResourceBasic(setup_data)

        extent_dims = resource.get_downsampled_extent_dims()
        self.assertEqual(len(extent_dims), setup_data["experiment"]['num_hierarchy_levels'])
        self.assertEqual(extent_dims[0], [2000, 5000, 200])
        self.assertEqual(extent_dims[1], [1000, 2500, 100])
        self.assertEqual(extent_dims[3], [250, 625, 25])




