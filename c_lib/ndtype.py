# Copyright 2014 NeuroData (http://neurodata.io)
# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import numpy as np

# Cuboid Size
# TODO: Look into moving this into a data model table and set based on voxel size
# non-isotropic slices should be 128,128,16. As you downsample move to 64,64,64 past isotropic
CUBOIDSIZE = [[512, 512, 16],
              [512, 512, 16],
              [512, 512, 16],
              [64, 64, 64],
              [64, 64, 64],
              [64, 64, 64],
              [64, 64, 64],
              [64, 64, 64],
              [64, 64, 64],
              [64, 64, 64]]

# SuperCube Size
SUPERCUBESIZE = [10, 10, 10]

# ND Data Types, Mapping, Groups
UINT8 = 'uint8'
UINT16 = 'uint16'
UINT32 = 'uint32'
UINT64 = 'uint64'
FLOAT32 = 'float32'

DTYPE_uint8 = [UINT8]
DTYPE_uint16 = [UINT16]
DTYPE_uint32 = [UINT32]
DTYPE_uint64 = [UINT64]
DTYPE_float32 = [FLOAT32]

ND_dtypetonp = {UINT8: np.uint8, UINT16: np.uint16, UINT32: np.uint32, UINT64: np.uint64, FLOAT32: np.float32}

# Propagated Values
# TODO: Propagation not supported yet
PROPAGATED = 2
UNDER_PROPAGATION = 1
NOT_PROPAGATED = 0


