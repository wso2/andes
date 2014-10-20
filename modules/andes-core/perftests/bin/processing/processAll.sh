#!/bin/bash 
#
# 
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# 
#


usage()
{
 echo "processResults.sh <search dir>"
 exit 1
}

root=`pwd`
echo $root
graphFile=graph.data

if [ $# != 1 ] ; then
 usage
fi

mkdir -p results

for file in `find $1 -name $graphFile` ; do

  dir=`dirname $file`

  echo Processing : $dir
  pushd $dir &> /dev/null

  $root/process.sh $graphFile

  echo Copying Images
  cp work/*png $root/results/

  echo Copying Stats
  cp work/*.statistics.txt $root/results/
  

  popd &> /dev/null
  echo Done
done