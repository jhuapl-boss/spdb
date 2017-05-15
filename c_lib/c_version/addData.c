/*
* Copyright 2014 NeuroData (http://neurodata.io)
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/


/*
 * Add Data Functions
 * Naive implementation
 */

#include<stdint.h>
#include<ndlib.h>

// Determine the annotation value at the next level of the hierarchy from a 2x2

uint32_t getAnnValue ( uint32_t value00, uint32_t value01, uint32_t value10, uint32_t value11 )
{
  uint32_t value = value00;

  if ( value == 0 )
    value = value01;

  if ( value10 != 0 )
    if ( value == 0 )
      value = value10;
    else if ( value10 == value00 || value10 == value01 )
      value = value10;

  if ( value11 != 0 )
    if ( value == 0 )
      value = value10;
    else if ( value11 == value00 || value11 == value01 || value11 == value10 )
      value = value11;

  return value;
}

uint64_t getAnnValue64 ( uint64_t value00, uint64_t value01, uint64_t value10, uint64_t value11 )
{
  uint64_t value = value00;

  if ( value == 0 )
    value = value01;

  if ( value10 != 0 )
    if ( value == 0 )
      value = value10;
    else if ( value10 == value00 || value10 == value01 )
      value = value10;

  if ( value11 != 0 )
    if ( value == 0 )
      value = value10;
    else if ( value11 == value00 || value11 == value01 || value11 == value10 )
      value = value11;

  return value;
}


// Add the contribution of the input data to the next level at the given offset in the output cube

void addDataZSlice ( uint32_t * cube, uint32_t * output, int * offset, int * dims )
{
  int i,j,k;

  int zdim = dims[0];
  int ydim = dims[1];
  int xdim = dims[2];

  for ( i=0; i<zdim; i++ )
    for ( j=0; j<(ydim/2); j++ )
      for ( k=0; k<(xdim/2); k++ )
      {
        int index1 = (i*ydim*xdim)+(j*2*xdim)+(k*2);
        int index2 = (i*ydim*xdim)+(j*2*xdim)+(k*2+1);
        int index3 = (i*ydim*xdim)+((j*2+1)*xdim)+(k*2);
        int index4 = (i*ydim*xdim)+((j*2+1)*xdim)+(k*2+1);
        int output_index = ( (i+offset[2]) *ydim*xdim*2*2 ) + ( (j+offset[1]) *xdim*2 ) + (k+offset[0]);
        output[output_index] = getAnnValue ( cube[index1], cube[index2], cube[index3], cube[index4] );
      }
}


// Add the contribution of the input data to the next level at the given offset in the output cube

void addDataIsotropic ( uint32_t * cube, uint32_t * output, int * offset, int * dims )
{
  int i,j,k;

  int zdim = dims[0];
  int ydim = dims[1];
  int xdim = dims[2];

  uint32_t value;

  for ( i=0; i<zdim/2; i++ )
    for ( j=0; j<(ydim/2); j++ )
      for ( k=0; k<(xdim/2); k++ )
      {
        int index1 = (i*ydim*xdim)+(j*2*xdim)+(k*2);
        int index2 = (i*ydim*xdim)+(j*2*xdim)+(k*2+1);
        int index3 = (i*ydim*xdim)+((j*2+1)*xdim)+(k*2);
        int index4 = (i*ydim*xdim)+((j*2+1)*xdim)+(k*2+1);
        value = getAnnValue ( cube[index1], cube[index2], cube[index3], cube[index4] );

        if ( value == 0 )
        {
          index1 = ((i*2+1)*ydim*xdim)+(j*2*xdim)+(k*2);
          index2 = ((i*2+1)*ydim*xdim)+(j*2*xdim)+(k*2+1);
          index3 = ((i*2+1)*ydim*xdim)+((j*2+1)*xdim)+(k*2);
          index4 = ((i*2+1)*ydim*xdim)+((j*2+1)*xdim)+(k*2+1);
          value = getAnnValue ( cube[index1], cube[index2], cube[index3], cube[index4] );
        }
        int output_index = ( (i+offset[2]) *ydim*xdim*2*2 ) + ( (j+offset[1]) *xdim*2 ) + (k+offset[0]);
        output[output_index] = getAnnValue ( cube[index1], cube[index2], cube[index3], cube[index4] );
      }
}

/*
 * Downsample Annotations from a volume of shape (cubes * dims) into an output cube
 * of shape (dims).
 *
 * Note: Currently only supports downsampling by a factor of 1x2x2 or 2x2x2 (ZYX)
 *
 * Args:
 *      volume (NumPy array) : Size is (cubes * dims)
 *      output (NumPy array) : Size is (dims)
 *      cubes ([z,y,x]) : Number of cubes of size dims in volume
 *      dims ([z,y,x]) : Dimensions of a single cube in volume / of the output buffer
 */
void addAnnotationData(uint64_t * volume, uint64_t * output, int * cubes, int * dims)
{
    int x,y,z;
    uint64_t annotation;

    int dim_z = dims[0];
    int dim_y = dims[1];
    int dim_x = dims[2];
    int cube_z = cubes[0];
    int cube_y = cubes[1];
    int cube_x = cubes[2];

    // DP NOTE: could be sizeof(uint64_t)
    int dsize = 8; // size of an individual element in volume / output

    /* Offset calculations (DP NOTE: may assume C ordered arrays)
     * z,y,x is the target index within the output array
     * z,y,x * cubes is the corner index within the volume array of a
     *               cubes size area to downsample into a single result
     *               normally downsampling a 1x2x2 or 2x2x2 into 1x1x1
     *
     * To calculate the offset of the first byte of data in a numpy array
     * idx * array.strides or
     * (idx_z, idx_y, idx_x) * (dim_x * dim_y * dsize, dim_x * dsize, dsize)
     */
    #define OFFSET(val_x, val_y, val_z) (((val_z) * cube_z * dim_x * dim_y * dsize) + \
                                         ((val_y) * cube_y * dim_x * dsize) + \
                                         ((val_x) * cube_x * dsize))

    for(z=0; z<dim_z; z++)
        for(y=0; y<dim_y; y++)
            for(x=0; x<dim_x; x++)
            {
                // index1 === zyx * cubes
                uint32_t index1 = OFFSET(z, y, x);
                uint32_t index2 = OFFSET(z, y, x + 1);
                uint32_t index3 = OFFSET(z, y + 1, x);
                uint32_t index4 = OFFSET(z, y + 1, x + 1);
                annotation = getAnnValue64 ( volume[index1], volume[index2], volume[index3], volume[index4] );

                if(annotation == 0 && cube_z == 2)
                {
                    index1 = OFFSET(z + 1, y, x);
                    index2 = OFFSET(z + 1, y, x + 1);
                    index3 = OFFSET(z + 1, y + 1, x);
                    index4 = OFFSET(z + 1, y + 1, x + 1);
                    annotation = getAnnValue64 ( volume[index1], volume[index2], volume[index3], volume[index4] );
                }

                // output_index === zyx
                int output_index = (z * dim_y * dim_x * dsize) + (y * dim_x * dsize) + (x * dsize);
                output[output_index] = annotation;
            }
}
