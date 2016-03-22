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
 * Recolor Slice Function 
 * Naive implementation 
 */

#include<stdint.h>
#include<omp.h>
#include<ndlib.h>

void recolorCubeOMP ( uint32_t * cutout, int xdim, int ydim, uint32_t * imagemap, uint32_t * rgbColor)
{
		int i,j;
#pragma omp parallel num_threads( omp_get_max_threads() )
    {
#pragma omp for private(i,j) schedule(dynamic)
      for ( i=0; i<xdim; i++)
        for ( j=0; j<ydim; j++)
          if ( cutout [(i*ydim)+j] != 0 )
            imagemap [(i*ydim)+j] = rgbColor[ cutout [(i*ydim)+j] % 217 ];
    }
}
