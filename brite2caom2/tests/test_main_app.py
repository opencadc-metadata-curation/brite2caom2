# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2019.                            (c) 2019.
#  Government of Canada                 Gouvernement du Canada
#  National Research Council            Conseil national de recherches
#  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#  All rights reserved                  Tous droits réservés
#
#  NRC disclaims any warranties,        Le CNRC dénie toute garantie
#  expressed, implied, or               énoncée, implicite ou légale,
#  statutory, of any kind with          de quelque nature que ce
#  respect to the software,             soit, concernant le logiciel,
#  including without limitation         y compris sans restriction
#  any warranty of merchantability      toute garantie de valeur
#  or fitness for a particular          marchande ou de pertinence
#  purpose. NRC shall not be            pour un usage particulier.
#  liable in any event for any          Le CNRC ne pourra en aucun cas
#  damages, whether direct or           être tenu responsable de tout
#  indirect, special or general,        dommage, direct ou indirect,
#  consequential or incidental,         particulier ou général,
#  arising from the use of the          accessoire ou fortuit, résultant
#  software.  Neither the name          de l'utilisation du logiciel. Ni
#  of the National Research             le nom du Conseil National de
#  Council of Canada nor the            Recherches du Canada ni les noms
#  names of its contributors may        de ses  participants ne peuvent
#  be used to endorse or promote        être utilisés pour approuver ou
#  products derived from this           promouvoir les produits dérivés
#  software without specific prior      de ce logiciel sans autorisation
#  written permission.                  préalable et particulière
#                                       par écrit.
#
#  This file is part of the             Ce fichier fait partie du projet
#  OpenCADC project.                    OpenCADC.
#
#  OpenCADC is free software:           OpenCADC est un logiciel libre ;
#  you can redistribute it and/or       vous pouvez le redistribuer ou le
#  modify it under the terms of         modifier suivant les termes de
#  the GNU Affero General Public        la “GNU Affero General Public
#  License as published by the          License” telle que publiée
#  Free Software Foundation,            par la Free Software Foundation
#  either version 3 of the              : soit la version 3 de cette
#  License, or (at your option)         licence, soit (à votre gré)
#  any later version.                   toute version ultérieure.
#
#  OpenCADC is distributed in the       OpenCADC est distribué
#  hope that it will be useful,         dans l’espoir qu’il vous
#  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
#  without even the implied             GARANTIE : sans même la garantie
#  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
#  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
#  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#  $Revision: 4 $
#
# ***********************************************************************
#
import brite2caom2.storage_name
from brite2caom2 import fits2caom2_augmentation, main_app
from caom2.diff import get_differences
from caom2pipe.caom_composable import get_all_artifact_keys
from caom2pipe import manage_composable as mc
from brite2caom2 import reader
from datetime import datetime

import glob
import os

from mock import patch


THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = os.path.join(THIS_DIR, 'data')
PLUGIN = os.path.join(os.path.dirname(THIS_DIR), 'main_app.py')


def pytest_generate_tests(metafunc):
    obs_id_list = [
        ii for ii in glob.glob(f'{TEST_DATA_DIR}/*/*') if ('xml' not in ii and '.md5' not in ii and '.lst' not in ii)
    ]
    metafunc.parametrize('test_name', obs_id_list)


@patch('caom2pipe.client_composable.ClientCollection')
def test_main_app(clients_mock, test_name):
    mc.StorageName.collection = main_app.COLLECTION
    original_scheme = mc.StorageName.scheme
    mc.StorageName.scheme = 'cadc'
    try:
        storage_name = brite2caom2.storage_name.BriteName(entry=test_name)
        metadata_reader = reader.BriteFileMetadataReader()
        metadata_reader.set(storage_name)
        kwargs = {
            'storage_name': storage_name,
            'metadata_reader': metadata_reader,
            'clients': clients_mock,
        }
        expected_fqn = f'{TEST_DATA_DIR}/{storage_name.obs_id}.expected.xml'
        expected = mc.read_obs_from_file(expected_fqn)
        in_fqn = expected_fqn.replace('.expected', '.in')
        ext = test_name.split('.')[-1]
        actual_fqn = expected_fqn.replace('.expected', f'.{ext}.actual')
        observation = None
        if os.path.exists(actual_fqn):
            os.unlink(actual_fqn)
        if os.path.exists(in_fqn):
            observation = mc.read_obs_from_file(in_fqn)
        observation = fits2caom2_augmentation.visit(observation, **kwargs)
        assert observation is not None, f'expect an observation {test_name}'
        artifact_uris = get_all_artifact_keys(observation)
        if len(artifact_uris) == 5:
            # only check expected observation structure once all the artifacts have been added
            if os.path.exists(in_fqn):
                # make sure future test runs start with a non-existent observation
                os.unlink(in_fqn)
            try:
                _set_release_date_values(observation)
                compare_result = get_differences(expected, observation)
            except Exception as e:
                mc.write_obs_to_file(observation, actual_fqn)
                raise e
            if compare_result is not None:
                mc.write_obs_to_file(observation, actual_fqn)
                compare_text = '\n'.join([r for r in compare_result])
                msg = f'Differences found in observation {expected.observation_id}\n{compare_text}'
                raise AssertionError(msg)
        else:
            mc.write_obs_to_file(observation, in_fqn)
        # assert False  # cause I want to see logging messages
    finally:
        mc.StorageName.collection = None
        mc.StorageName.scheme = original_scheme


def _set_release_date_values(observation):
    # the release date is "the time at which the file is received at CADC", which is random, and therfore hard to
    # test with, so over-ride with a known value before doing the comparison to the expected value
    release_date = datetime.strptime('2022-10-26T20:28:35.155000', '%Y-%m-%dT%H:%M:%S.%f')
    observation.meta_release = release_date
    for plane in observation.planes.values():
        plane.meta_release = release_date
        plane.data_release = release_date
