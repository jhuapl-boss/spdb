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

from abc import ABCMeta, abstractmethod
import numpy as np
import json


class Collection:
    """
    Class to store collection attributes

    Args:
      name (str): Unique string to identify the collection
      description (str): A short description of the collection and what it contains

    Attributes:
      name (str): Unique string to identify the collection
      description (str): A short description of the collection and what it contains
    """
    def __init__(self, name, description):
        self.name = name
        self.description = description


class Experiment:
    """
    Class to store experiment attributes

    Args:
      name (str): Unique string to identify the experiment
      description (str): A short description of the experiment and what it contains
      num_hierarchy_levels (int): Number of levels in the zoom resolution hierarchy
      hierarchy_method (str): The style of down-sampling used to build the resolution hierarchy.
      Valid values are 'near_iso', 'iso', and 'slice'.
      max_time_step (int): The maximum supported time sample

    Attributes:
      name (str): Unique string to identify the collection
      description (str): A short description of the collection and what it contains
      num_hierarchy_levels (int): Number of levels in the zoom resolution hierarchy
      hierarchy_method (str): The style of down-sampling used to build the resolution hierarchy.
      Valid values are 'near_iso', 'iso', and 'slice'.
      max_time_step (int): The maximum supported time sample
    """
    def __init__(self, name, description, num_hierarchy_levels, hierarchy_method, max_time_sample):
        self.name = name
        self.description = description
        self.num_hierarchy_levels = num_hierarchy_levels
        self.hierarchy_method = hierarchy_method
        self.max_time_sample = max_time_sample


class CoordinateFrame:
    """
    Class to store coordinate frame attributes

    Args:
      name (str): Unique string to identify the coordinate frame
      description (str): A short description of the coordinate frame and what it contains
      x_start (int): The starting X value for the coordinate frame
      x_stop (int): The ending X value for the coordinate frame, exclusive (python convention)
      y_start (int): The starting Y value for the coordinate frame
      y_stop (int): The ending Y value for the coordinate frame, exclusive (python convention)
      z_start (int): The starting Z value for the coordinate frame
      z_stop (int): The ending Z value for the coordinate frame, exclusive (python convention)
      x_voxel_size (int): The physical size of a voxel in the x-dimension
      y_voxel_size (int): The physical size of a voxel in the y-dimension
      z_voxel_size (int): The physical size of a voxel in the z-dimension
      voxel_unit (str): The unit to use for x/y/z voxel size. Valid values are "nanometer", "micrometer", "millimeter",
        "centimeter"
      time_step (int): The increment in time between two time samples (assumes constant rate)
      time_step_unit (str): The unit to use for the time step. Valid values are "nanosecond", "microsecond",
        "millisecond", "second"

    Attributes:
      name (str): Unique string to identify the coordinate frame
      description (str): A short description of the coordinate frame and what it contains
      x_start (int): The starting X value for the coordinate frame
      x_stop (int): The ending X value for the coordinate frame, exclusive (python convention)
      y_start (int): The starting Y value for the coordinate frame
      y_stop (int): The ending Y value for the coordinate frame, exclusive (python convention)
      z_start (int): The starting Z value for the coordinate frame
      z_stop (int): The ending Z value for the coordinate frame, exclusive (python convention)
      x_voxel_size (int): The physical size of a voxel in the x-dimension
      y_voxel_size (int): The physical size of a voxel in the y-dimension
      z_voxel_size (int): The physical size of a voxel in the z-dimension
      voxel_unit (str): The unit to use for x/y/z voxel size. Valid values are "nanometer", "micrometer", "millimeter",
        "centimeter"
      time_step (int): The increment in time between two time samples (assumes constant rate)
      time_step_unit (str): The unit to use for the time step. Valid values are "nanosecond", "microsecond",
        "millisecond", "second"
    """
    def __init__(self, name, description, x_start, x_stop, y_start, y_stop, z_start, z_stop,
                 x_voxel_size, y_voxel_size, z_voxel_size, voxel_unit, time_step, time_step_unit):

        self.name = name
        self.description = description
        self.x_start = x_start
        self.x_stop = x_stop
        self.y_start = y_start
        self.y_stop = y_stop
        self.z_start = z_start
        self.z_stop = z_stop
        self.x_voxel_size = x_voxel_size
        self.y_voxel_size = y_voxel_size
        self.z_voxel_size = z_voxel_size
        self.voxel_unit = voxel_unit
        self.time_step = time_step
        self.time_step_unit = time_step_unit


class Channel:
    """
    Class to store channel properties

    Args:
      name (str): Unique string to identify the channel
      description (str): A short description of the channel and what it contains
      datatype (int): The bitdepth of the channel.  Valid choices are uint8, uint16, uint32, and uint64

    Attributes:
      name (str): Unique string to identify the channel
      description (str): A short description of the channel and what it contains
      datatype (int): The bitdepth of the channel.  Valid choices are uint8, uint16, uint32, and uint64
    """
    def __init__(self, name, description, datatype):
        self.name = name
        self.description = description
        self.datatype = datatype


class Layer:
    """
    Class to store layer properties

    Args:
      name (str): Unique string to identify the layer
      description (str): A short description of the layer and what it contains
      datatype (int): The bitdepth of the channel.  Valid choices are uint8, uint16, uint32, and uint64
      base_resolution (int): The resolution level of primary annotation/data (mainly used in layers).
      parent_channels (list): The names of the parent channel(s) to which the Layer is linked

    Attributes:
      name (str): Unique string to identify the layer
      description (str): A short description of the layer and what it contains
      datatype (int): The bitdepth of the channel.  Valid choices are uint8, uint16, uint32, and uint64
      base_resolution (int): The resolution level of primary annotation and indicates where dynamic resampling will occur
      parent_channels (list): The names of the parent channel(s) to which the Layer is linked
    """
    def __init__(self, name, description, datatype, base_resolution, parent_channels):
        self.name = name
        self.description = description
        self.datatype = datatype
        self.base_resolution = base_resolution
        self.parent_channels = parent_channels


class BossResource(metaclass=ABCMeta):
    """
    Parent class to represent a Boss data model resource.

    Attributes:
      _collection (spdb.project.resource.Collection): A Collection instance for the resource
      _coord_frame (spdb.project.resource.CoordinateFrame): A coordinate frame instance for the resource
      _experiment (spdb.project.resource.Experiment): A experiment instance for the resource
      _channel (spdb.project.resource.Channel): A channel instance for the resource (if a channel)
      _layer (spdb.project.resource.Layer): A layer instance for the resource (if a layer)
      _boss_key (str): The unique, plain text key identifying the resource - used to query for the lookup key
      _lookup_key (str): The unique key identifying the resource that enables renaming resources and physically used to
      ID data in databases
    """
    def __init__(self):
        self._collection = None
        self._coord_frame = None
        self._experiment = None
        self._channel = None
        self._layer = None
        self._boss_key = None
        self._lookup_key = None

    def to_json(self):
        """
        Method to serialize a resource to a JSON object
        Returns:
            (str): a JSON encoded string
        """
        # Serialize and return
        return json.dumps(self.to_dict())

    def to_dict(self):
        """
        Method to convert a resource to a dictionary
        Returns:
            (dict): a dict of all the parameters
        """
        # Populate everything
        self.populate_collection()
        self.populate_coord_frame()
        self.populate_experiment()
        self.populate_channel_or_layer()
        self.populate_boss_key()
        self.populate_lookup_key()

        # Collect Data
        data = {"collection": self._collection.__dict__,
                "coord_frame": self._coord_frame.__dict__,
                "experiment": self._experiment.__dict__,
                "boss_key": self._boss_key,
                "lookup_key": self._lookup_key,
                }

        if self._channel:
            data['channel_layer'] = self._channel.__dict__
            data['channel_layer']['is_channel'] = True
        else:
            data['channel_layer'] = self._layer.__dict__
            data['channel_layer']['is_channel'] = False

        # Serialize and return
        return data

    # Methods to populate class properties
    @abstractmethod
    def populate_collection(self):
        """
        Method to create a Collection instance and set self._collection.  Should be overridden.
        """
        return NotImplemented

    @abstractmethod
    def populate_coord_frame(self):
        """
        Method to create a CoordinateFrame instance and set self._coord_frame.  Should be overridden.
        """
        return NotImplemented

    @abstractmethod
    def populate_experiment(self):
        """
        Method to create a Experiment instance and set self._experiment.  Should be overridden.
        """
        return NotImplemented

    @abstractmethod
    def populate_channel_or_layer(self):
        """
        Method to create a Channel or Layer instance and set self._channel or self._layer.  Should be overridden.
        """
        return NotImplemented


    @abstractmethod
    def populate_boss_key(self):
        """
        Method to set self._boss_key.  Should be overridden.
        """
        return NotImplemented

    @abstractmethod
    def populate_lookup_key(self):
        """
        Method to set self._lookup_key.  Should be overridden.
        """
        return NotImplemented

    # GETTERS
    def get_collection(self):
        """Method to get the current Collection instance.  Lazily populated.

        :returns A Collection instance for the given resource
        :rtype spdb.project.Collection
        """
        if not self._collection:
            self.populate_collection()
        return self._collection

    def get_experiment(self):
        """Method to get the current Experiment instance.  Lazily populated.

        :returns A Experiment instance for the given resource
        :rtype spdb.project.resource.Experiment
        """
        if not self._experiment:
            self.populate_experiment()
        return self._experiment

    def get_coord_frame(self):
        """Method to get the current Coordinate Frame instance.  Lazily populated.

        :returns A Coordinate Frame instance for the given resource
        :rtype spdb.project.resource.CoordinateFrame
        """
        if not self._coord_frame:
            self.populate_coord_frame()
        return self._coord_frame

    def get_channel(self):
        """Method to get the current Channel instance.  Lazily populated. None if current resource is a layer

        :returns A Channel instance for the given resource
        :rtype spdb.project.Channel
        """
        if not self._channel and not self._layer:
            self.populate_channel_or_layer()
        return self._channel

    def get_layer(self):
        """Method to get the current Layer instance.  Lazily populated. None if current resource is a channel

        :returns A Layer instance for the given resource
        :rtype spdb.project.Layer
        """
        if not self._channel and not self._layer:
            self.populate_channel_or_layer()
        return self._layer

    def is_channel(self):
        """Method to check if the resource is a channel or a layer

        :returns True if resource is a channel. False if a layer
        :rtype bool
        """
        if not self._channel and not self._layer:
            self.populate_channel_or_layer()
        return self._layer is None

    def get_boss_key(self):
        """Method to get the current boss key.  Lazily populated.

        :returns The boss key
        :rtype str
        """
        if not self._boss_key:
            self.populate_boss_key()
        return self._boss_key

    def get_lookup_key(self):
        """Method to get the current lookup keys.  Lazily populated.

        :returns The lookup key
        :rtype list[str]
        """
        if not self._lookup_key:
            self.populate_lookup_key()
        return self._lookup_key

    # TODO: Look into putting kv-engine in django model and support different engines?
    def get_kv_engine(self):
        """Method to get the key-value engine for the current resources

        :note Currently only a single kv engine/cache. Always returns redis

        :returns The kv engine
        :rtype str
        """
        return "redis"

    # TODO: Need to implement propagation in data model to support propagation status tracking
    def is_propagated(self):
        """Check if a layer/channel has been propagated, building out the res hierarchy

        Returns:
            bool: True if the resource has been propagated, False if not.

        """
        return False

    def get_data_type(self):
        """Method to get data type.  Lazily populated. None if current resource is not a channel or layer

        :returns A string identifying the data type for the channel or layer
        :rtype str
        """
        if not self._channel and not self._layer:
            self.populate_channel_or_layer()

        data_type = None
        if self.is_channel():
            if self._channel:
                # You have a channel
                data_type = self._channel.datatype
        else:
            if self._layer:
                # You have a layer
                data_type = self._layer.datatype

        return data_type

    def get_bit_depth(self):
        """Method to get the bit depth of the channel or layer

        :returns An integer indicating the bit depth
        :rtype int
        """
        data_type = self.get_data_type()
        if data_type == "uint8":
            bit_depth = 8
        elif data_type == "uint16":
            bit_depth = 16
        elif data_type == "uint64":
            bit_depth = 64
        else:
            return ValueError("Unsupported datatype")

        return bit_depth

    def get_numpy_data_type(self):
        """Method to get data type as a numpy data type instance

        """
        data_type = self.get_data_type()
        if data_type == "uint8":
            bit_depth = np.uint8
        elif data_type == "uint16":
            bit_depth = np.uint16
        elif data_type == "uint64":
            bit_depth = np.uint64
        else:
            return ValueError("Unsupported data type")

        return bit_depth

    # Methods to delete the entry from the data model tables
    @abstractmethod
    def delete_collection_model(self):
        """Method to delete a collection from the data model. Should not be called directly; Use delete_collection.

        Returns:
            None
        """
        return NotImplemented

    @abstractmethod
    def delete_experiment_model(self):
        """Method to delete a experiment from the data model. Should not be called directly; Use delete_experiment.

        Returns:
            None
        """
        return NotImplemented

    @abstractmethod
    def delete_coord_frame_model(self):
        """Method to delete a coord_frame from the data model. Should not be called directly; Use delete_coord_frame.

        Returns:
            None
        """
        return NotImplemented

    @abstractmethod
    def delete_channel_layer_model(self):
        """Method to delete a channel_layer from the data model. Should not be called directly; Use delete_channel_layer.

        Returns:
            None
        """
        return NotImplemented

    # Methods to delete Boss data model resources
    # TODO: Add S3 support on deletes.
    # TODO: Add delete support
    def delete_collection(self):
        """Delete the Collection"""
        return NotImplemented

    def delete_experiment(self):
        """Delete the experiment"""
        return NotImplemented

    def delete_coordinate_frame(self):
        """Delete the coordinate frame"""
        return NotImplemented

    def delete_channel(self):
        """Delete a channel"""
        return NotImplemented

    def delete_layer(self):
        """Delete a channel"""
        return NotImplemented

    def delete_time_sample(self, time_sample=None):
        """Delete the time sample"""
        return NotImplemented

