from .resource import BossResource, Collection, CoordinateFrame, Experiment, Channel, Layer


class BossResourceDjango(BossResource):
    """
    Resource class for when using SPDB within a Django app.  It takes a BossRequest as an input to the constructor,
    and from this is able to populate all values as needed.

     Args:
      boss_request (BossRequest): BossRequest instance that has already validated a request

    Attributes:
      boss_request (BossRequest): BossRequest instance that has already validated a request
    """
    def __init__(self, boss_request):
        super().__init__()

        self.boss_request = boss_request

    # Methods to populate class properties
    def populate_collection(self):
        """
        Method to create a Collection instance and set self._collection.
        """
        self._collection = Collection(self.boss_request.collection.name,
                                      self.boss_request.collection.description)

    def populate_coord_frame(self):
        """
        Method to create a CoordinateFrame instance and set self._coord_frame.
        """
        self._coord_frame = CoordinateFrame(self.boss_request.coord_frame.name,
                                            self.boss_request.coord_frame.description,
                                            self.boss_request.coord_frame.x_start,
                                            self.boss_request.coord_frame.x_stop,
                                            self.boss_request.coord_frame.y_start,
                                            self.boss_request.coord_frame.y_stop,
                                            self.boss_request.coord_frame.z_start,
                                            self.boss_request.coord_frame.z_stop,
                                            self.boss_request.coord_frame.x_voxel_size,
                                            self.boss_request.coord_frame.y_voxel_size,
                                            self.boss_request.coord_frame.z_voxel_size,
                                            self.boss_request.coord_frame.voxel_unit,
                                            self.boss_request.coord_frame.time_step,
                                            self.boss_request.coord_frame.time_step_unit)

    def populate_experiment(self):
        """
        Method to create a Experiment instance and set self._experiment.
        """
        self._experiment = Experiment(self.boss_request.experiment.name,
                                      self.boss_request.experiment.description,
                                      self.boss_request.experiment.num_hierarchy_levels,
                                      self.boss_request.experiment.hierarchy_method)

    def populate_channel_or_layer(self):
        """
        Method to create a Channel or Layer instance and set self._channel or self._layer.
        """
        if self.boss_request.channel_layer.is_channel:
            # You have a channel request
            self._channel = Channel(self.boss_request.channel_layer.name,
                                    self.boss_request.channel_layer.description,
                                    self.boss_request.channel_layer.datatype,
                                    self.boss_request.channel_layer.base_resolution,
                                    self.boss_request.channel_layer.max_time_step)
        else:
            # You have a layer request
            self._layer = Layer(self.boss_request.channel_layer.name,
                                self.boss_request.channel_layer.description,
                                self.boss_request.channel_layer.datatype,
                                self.boss_request.channel_layer.base_resolution,
                                self.boss_request.channel_layer.max_time_step,
                                self.boss_request.channel_layer.layer_map)

    def populate_time_samples(self):
        """
        Method to set self._time_samples.
        """
        # TODO: add time sample support
        self._time_samples = [0]

    def populate_boss_key(self):
        """
        Method to set self._boss_key.
        """
        self._boss_key = self.boss_request.get_bosskey

    def populate_lookup_key(self):
        """
        Method to set self._lookup_key.  Should be overridden.
        """
        # TODO: add look up key
        pass

    # Methods to delete the entry from the data model tables
    def delete_collection_model(self):
        pass

    def delete_experiment_model(self):
        pass

    def delete_coord_frame_model(self):
        pass

    def delete_channel_layer_model(self):
        pass

