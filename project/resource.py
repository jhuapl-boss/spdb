from abc import ABCMeta, abstractmethod


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

    Attributes:
      name (str): Unique string to identify the collection
      description (str): A short description of the collection and what it contains
      num_hierarchy_levels (int): Number of levels in the zoom resolution hierarchy
      hierarchy_method (str): The style of down-sampling used to build the resolution hierarchy.
      Valid values are 'near_iso', 'iso', and 'slice'.
    """
    def __init__(self, name, description, num_hierarchy_levels, hierarchy_method):
        self.name = name,
        self.description = description
        self.num_hierarchy_levels = num_hierarchy_levels
        self.hierarchy_method = hierarchy_method


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
      max_time_step (int): The maximum supported time sample

    Attributes:
      name (str): Unique string to identify the channel
      description (str): A short description of the channel and what it contains
      datatype (int): The bitdepth of the channel.  Valid choices are uint8, uint16, uint32, and uint64
      max_time_step (int): The maximum supported time sample
    """
    def __init__(self, name, description, datatype, max_time_step):
        self.name = name,
        self.description = description
        self.datatype = datatype
        self.max_time_step = max_time_step


class Layer:
    """
    Class to store layer properties

    Args:
      name (str): Unique string to identify the layer
      description (str): A short description of the layer and what it contains
      datatype (int): The bitdepth of the channel.  Valid choices are uint8, uint16, uint32, and uint64
      max_time_step (int): The maximum supported time sample
      parent_channels (list): The names of the parent channel(s) to which the Layer is linked

    Attributes:
      name (str): Unique string to identify the layer
      description (str): A short description of the layer and what it contains
      datatype (int): The bitdepth of the channel.  Valid choices are uint8, uint16, uint32, and uint64
      max_time_step (int): The maximum supported time sample
      parent_channels (list): The names of the parent channel(s) to which the Layer is linked
    """
    def __init__(self, name, description, datatype, max_time_step, parent_channels):
        self.name = name,
        self.description = description
        self.datatype = datatype
        self.max_time_step = max_time_step
        self.parent_channels = parent_channels


class BossResource(metaclass=ABCMeta):
    """
    Parent class to represent a Boss data model resource.

    Attributes:
      __collection (spdb.project.resource.Collection): A Collection instance for the resource
      __coord_frame (spdb.project.resource.CoordinateFrame): A coordinate frame instance for the resource
      __experiment (spdb.project.resource.Experiment): A experiment instance for the resource
      __channel (spdb.project.resource.Channel): A channel instance for the resource (if a channel)
      __layer (spdb.project.resource.Layer): A layer instance for the resource (if a layer)
      __time_samples (list): The time sample index for the resource
      __boss_key (str): The unique, plain text key identifying the resource - used to query for the lookup key
      __lookup_key (str): The unique key identifying the resource that enables renaming resources and physically used to
      ID data in databases
    """
    def __init__(self):
        self.__collection = None
        self.__coord_frame = None
        self.__experiment = None
        self.__channel = None
        self.__layer = None
        self.__time_samples = []
        self.__boss_key = None
        self.__lookup_key = None

    # Methods to populate class properties
    @abstractmethod
    def populate_collection(self):
        """
        Method to create a Collection instance and set self.__collection.  Should be overridden.
        """
        pass

    @abstractmethod
    def populate_coord_frame(self):
        """
        Method to create a CoordinateFrame instance and set self.__coord_frame.  Should be overridden.
        """
        pass

    @abstractmethod
    def populate_experiment(self):
        """
        Method to create a Experiment instance and set self.__experiment.  Should be overridden.
        """
        pass

    @abstractmethod
    def populate_channel_or_layer(self):
        """
        Method to create a Channel or Layer instance and set self.__channel or self.__layer.  Should be overridden.
        """
        pass

    @abstractmethod
    def populate_time_samples(self):
        """
        Method to set self.__time_samples.  Should be overridden.
        """
        pass

    @abstractmethod
    def populate_boss_key(self):
        """
        Method to set self.__boss_key.  Should be overridden.
        """
        pass

    @abstractmethod
    def populate_lookup_key(self):
        """
        Method to set self.__lookup_key.  Should be overridden.
        """
        pass

    # GETTERS
    def get_collection(self):
        """Method to get the current Collection instance.  Lazily populated.

        :returns A Collection instance for the given resource
        :rtype spdb.project.resource.Collection
        """
        if not self.__collection:
            self.populate_collection()
        return self.__collection

    def get_experiment(self):
        """Method to get the current Experiment instance.  Lazily populated.

        :returns A Experiment instance for the given resource
        :rtype spdb.project.resource.Experiment
        """
        if not self.__experiment:
            self.populate_experiment()
        return self.__experiment

    def get_coord_frame(self):
        """Method to get the current Coordinate Frame instance.  Lazily populated.

        :returns A Coordinate Frame instance for the given resource
        :rtype spdb.project.resource.CoordinateFrame
        """
        if not self.__coord_frame:
            self.populate_coord_frame()
        return self.__coord_frame

    def get_channel(self):
        """Method to get the current Channel instance.  Lazily populated. None if current resource is a layer

        :returns A Channel instance for the given resource
        :rtype spdb.project.resource.Channel
        """
        if not self.__channel and not self.__layer:
            self.populate_channel_or_layer()
        return self.__channel

    def get_layer(self):
        """Method to get the current Layer instance.  Lazily populated. None if current resource is a channel

        :returns A Layer instance for the given resource
        :rtype spdb.project.resource.Layer
        """
        if not self.__channel and not self.__layer:
            self.populate_channel_or_layer()
        return self.__layer

    def is_channel(self):
        """Method to check if the resource is a channel or a layer

        :returns True if resource is a channel. False if a layer
        :rtype bool
        """
        if not self.__channel and not self.__layer:
            self.populate_channel_or_layer()
        return (self.__layer == None)

    def get_time_samples(self):
        """Method to get the current time sample index or indicies.  Lazily populated.

        :returns The index or indicies for the current resource.
        :rtype list[int]
        """
        if not self.__time_samples:
            self.populate_time_samples()
        return self.__time_samples

    def get_boss_key(self):
        """Method to get the current boss key.  Lazily populated.

        :returns The boss key
        :rtype str
        """
        if not self.__boss_key:
            self.populate_boss_key()
        return self.__boss_key

    def get_lookup_key(self):
        """Method to get the current lookup key.  Lazily populated.

        :returns The lookup key
        :rtype str
        """
        if not self.__lookup_key:
            self.populate_lookup_key()
        return self.__lookup_key

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
        if not self.__channel and not self.__layer:
            self.populate_channel_or_layer()

        data_type = None
        if self.is_channel():
            if self.__channel:
                # You have a channel
                data_type = self.__channel.datatype
        else:
            if self.__layer():
                # You have a layer
                data_type = self.__layer.datatype

        return data_type

    # Methods to delete the entry from the data model tables
    @abstractmethod
    def __delete_collection_model(self):
        pass

    @abstractmethod
    def __delete_experiment_model(self):
        pass

    @abstractmethod
    def __delete_coord_frame_model(self):
        pass

    @abstractmethod
    def __delete_channel_layer_model(self):
        pass

    # Methods to delete Boss data model resources
    # TODO: Add S3 support on deletes.
    # TODO: Add delete support
    def delete_collection(self):
        """Delete the Collection"""
        pass

    def delete_experiment(self):
        """Delete the experiment"""
        pass

    def delete_coordinate_frame(self):
        """Delete the coordinate frame"""
        pass

    def delete_channel(self):
        """Delete a channel"""
        pass

    def delete_layer(self):
        """Delete a channel"""
        pass

    def delete_time_sample(self, time_sample=None):
        """Delete the time sample"""
        pass

