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

from mock import patch

from blank2caom2 import fits2caom2_augmentation, main_app
from caom2.diff import get_differences
from caom2pipe import astro_composable as ac
from caom2pipe import manage_composable as mc
from caom2pipe import reader_composable as rdc

import glob
import os

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = os.path.join(THIS_DIR, 'data')
PLUGIN = os.path.join(os.path.dirname(THIS_DIR), 'main_app.py')


def pytest_generate_tests(metafunc):
    obs_id_list = glob.glob(f'{TEST_DATA_DIR}/*.fits.header')
    metafunc.parametrize('test_name', obs_id_list)


@patch('caom2utils.data_util.get_local_headers_from_fits')
def test_main_app(header_mock, test_name):
    header_mock.side_effect = ac.make_headers_from_file
    storage_name = main_app.BlankName(entry=test_name)
    metadata_reader = rdc.FileMetadataReader()
    metadata_reader.set(storage_name)
    file_type = 'application/fits'
    metadata_reader.file_info[storage_name.file_uri].file_type = file_type
    kwargs = {
        'storage_name': storage_name,
        'metadata_reader': metadata_reader,
    }
    expected_fqn = f'{TEST_DATA_DIR}/{test_name}.expected.xml'
    expected = mc.read_obs_from_file(expected_fqn)
    in_fqn = expected_fqn.replace('.expected', '.in')
    actual_fqn = expected_fqn.replace('expected', 'actual')
    observation = None
    if os.path.exists(in_fqn):
        observation = mc.read_obs_from_file(in_fqn)
    observation = fits2caom2_augmentation.visit(observation, **kwargs)
    try:
        compare_result = get_differences(expected, observation)
    except Exception as e:
        mc.write_obs_to_file(observation, actual_fqn)
        raise e
    if compare_result is not None:
        mc.write_obs_to_file(observation, actual_fqn)
        compare_text = '\n'.join([r for r in compare_result])
        msg = (
            f'Differences found in observation {expected.observation_id}\n'
            f'{compare_text}'
        )
        raise AssertionError(msg)
    # assert False  # cause I want to see logging messages
