def get_image_dict():
    """Method to generate an initial set of parameters to use to instantiate a basic resource for an IMAGE dataset
    Returns:
        dict - a dictionary of data to initialize a basic resource

    """
    data = {}
    data['boss_key'] = 'col1&exp1&ch1'
    data['lookup_key'] = '4&3&2'
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

    data['channel'] = {}
    data['channel']['name'] = "ch1"
    data['channel']['description'] = "Test channel 1"
    data['channel']['type'] = "image"
    data['channel']['datatype'] = 'uint8'
    data['channel']['base_resolution'] = 0
    data['channel']['sources'] = []
    data['channel']['related'] = []
    data['channel']['default_time_step'] = 0

    return data


def get_anno_dict():
    """Method to generate an initial set of parameters to use to instantiate a basic resource for an ANNOTATION dataset
    Returns:
        dict - a dictionary of data to initialize a basic resource

    """
    data = get_image_dict()
    data['channel']['name'] = "anno1"
    data['channel']['description'] = "Test annotation channel 1"
    data['channel']['type'] = "annotation"
    data['channel']['datatype'] = 'uint64'
    data['channel']['base_resolution'] = 1
    data['channel']['sources'] = ["ch1"]
    data['channel']['default_time_step'] = 0
    data['boss_key'] = 'col1&exp1&ch2'
    data['lookup_key'] = '4&3&6'

    return data