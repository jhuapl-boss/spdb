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
 * Merge Cube Function 
 * Naive implementation 
 */

#include<stdint.h>
#include<ndlib.h>

void isotropicBuild32( uint32_t * data1, uint32_t * data2, uint32_t * newdata, int * dims )
{
		int i,j,index;

    int ydim = dims[0];
    int xdim = dims[1];

    for ( j=0; j<ydim; j++ )
      for ( i=0; i<xdim; i++ )
      {
        index = (j*xdim) + (i);
        if ( data2 [index] == 0 )
          newdata[index] = data1[index];
        else if ( data1[index] == 0 )
          newdata[index] = data2[index];
        else
          newdata[index] = ( data1[index] + data2[index] ) / 2;
      }
}


void isotropicBuild16( uint16_t * data1, uint16_t * data2, uint16_t * newdata, int * dims )
{
		int i,j,index;

    int ydim = dims[0];
    int xdim = dims[1];

    for ( j=0; j<ydim; j++ )
      for ( i=0; i<xdim; i++ )
      {
        index = (j*xdim) + (i);
        if ( data2 [index] == 0 )
          newdata[index] = data1[index];
        else if ( data1[index] == 0 )
          newdata[index] = data2[index];
        else
          newdata[index] = ( data1[index] + data2[index] )/2;
      }
}


void isotropicBuild8( uint8_t * data1, uint8_t * data2, uint8_t * newdata, int * dims )
{
		int i,j,index;

    int ydim = dims[0];
    int xdim = dims[1];

    for ( j=0; j<ydim; j++ )
      for ( i=0; i<xdim; i++ )
      {
        index = (j*xdim) + (i);
        if ( data2 [index] == 0 )
          newdata[index] = data1[index];
        else if ( data1[index] == 0 )
          newdata[index] = data2[index];
        else
          newdata[index] = ( data1[index] + data2[index] ) / 2;
      }
}

void isotropicBuildF32( float * data1, float * data2, float * newdata, int * dims )
{
		int i,j,index;

    int ydim = dims[0];
    int xdim = dims[1];

    for ( j=0; j<ydim; j++ )
      for ( i=0; i<xdim; i++ )
      {
        index = (j*xdim) + (i);
        if ( data2 [index] == 0.0 )
          newdata[index] = data1[index];
        else if ( data1[index] == 0.0 )
          newdata[index] = data2[index];
        else
          newdata[index] = ( data1[index] + data2[index] ) / 2;
      }
}
