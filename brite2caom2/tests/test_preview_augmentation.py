# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2022.                            (c) 2022.
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
#  : 4 $
#
# ***********************************************************************
#

import glob
from os.path import basename

from caom2pipe.caom_composable import get_all_artifact_keys
from caom2pipe import manage_composable as mc

import brite2caom2.storage_name
from brite2caom2 import preview_augmentation, main_app, reader
from mock import Mock, patch
import test_main_app


def pytest_generate_tests(metafunc):
    obs_id_list = [ii for ii in glob.glob(f'{test_main_app.TEST_DATA_DIR}/*') if '.expected.xml' in ii]
    metafunc.parametrize('test_name', obs_id_list)


@patch('caom2pipe.client_composable.ClientCollection')
def test_preview_visit(clients_mock, test_name):
    original_collection = mc.StorageName.collection
    try:
        obs_id = basename(test_name).replace('.expected.xml', '')
        dir_name = obs_id.split('_')[0]
        rlog_fqn = f'{test_main_app.TEST_DATA_DIR}/{dir_name}/{obs_id}.rlogdb'
        orig_fqn = f'{test_main_app.TEST_DATA_DIR}/{dir_name}/{obs_id}.orig'
        for test_fqn in [orig_fqn, rlog_fqn]:
            mc.StorageName.collection = main_app.COLLECTION
            metadata_reader = reader.BriteFileMetadataReader()
            test_obs = mc.read_obs_from_file(test_name)
            # pre-condition
            uris = get_all_artifact_keys(test_obs)
            assert len(uris) == 5, f'precondition failure {test_name}'
            test_storage_name = brite2caom2.storage_name.BriteName(test_fqn)
            metadata_reader.set(test_storage_name)
            kwargs = {
                'working_directory': test_main_app.TEST_DATA_DIR,
                'cadc_client': None,
                'metadata_reader': metadata_reader,
                'observable': Mock(),
                'storage_name': test_storage_name,
            }
            test_obs = preview_augmentation.visit(test_obs, **kwargs)
            assert test_obs is not None, f'visit broken for {test_name}'
            uris = get_all_artifact_keys(test_obs)
            assert len(uris) == 7, f'no preview artifacts added {test_name}'
            # there's a thumbnail and a preview
            preview_found = False
            thumbnail_found = False
            for uri in uris:
                if uri.endswith('_prev.jpg'):
                    preview_found = True
                if uri.endswith('_prev_256.jpg'):
                    thumbnail_found = True
            assert preview_found, f'expect a preview artifact {test_fqn}'
            assert thumbnail_found, f'expect a thumbnail artifact {test_fqn}'
    finally:
        mc.StorageName.collection = original_collection
