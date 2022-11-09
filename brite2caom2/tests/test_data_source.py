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


from caom2pipe.manage_composable import Config, TaskType
from brite2caom2.data_source import BriteLocalFilesDataSource

import pytest
from mock import call, Mock, patch


EXTENSIONS = ['.avedb', '.freq0db', '.lst', '.md5', '.ndatdb', '.orig', '.rlogdb']


@pytest.fixture
def test_config():
    config = Config()
    config.logging_level = 'DEBUG'
    config.task_types = [TaskType.STORE]
    config.store_modified_files_only = True
    config.cleanup_files_when_storing = True
    config.cleanup_failure_destination = '/test_files/failure'
    config.cleanup_success_destionation = '/test_files/success'
    config.retry_failures = False
    config.data_source_extensions = EXTENSIONS
    config.data_sources = ['/test_files']
    return config


@patch('brite2caom2.data_source.BriteLocalFilesDataSource._move_action')
@patch('caom2pipe.client_composable.ClientCollection')
def test_data_source_nominal_todo_cleanup(clients_mock, move_mock, test_config):
    # all the files have yet to be stored to CADC
    clients_mock.return_value.data_client.info.return_value = None
    test_reader = Mock()
    test_subject = BriteLocalFilesDataSource(
        test_config, clients_mock.return_value.data_client, test_reader, recursive=True
    )
    test_subject._capture_success = Mock()
    test_subject._capture_failure = Mock()

    # first test - five files that make up a single Observation
    nominal_dir_listing = _create_dir_listing(EXTENSIONS)
    with patch('os.scandir') as scandir_mock:
        scandir_mock.return_value.__enter__.return_value = nominal_dir_listing
        test_subject.get_work()
        test_subject.group_work_by_obs()
        test_subject.remove_unarchived()
        test_result = test_subject._work
        assert len(test_result) == 5, 'nominal execution fail, wrong length'
        assert test_subject._capture_success.called, 'success should be called'
        assert test_subject._capture_success.call_count == 2, 'success call count'
        assert not test_subject._capture_failure.called, 'failure should not be called'
        assert move_mock.called, 'cleanup should be called for non-archived files'


@patch('brite2caom2.data_source.BriteLocalFilesDataSource._move_action')
@patch('caom2pipe.client_composable.ClientCollection')
def test_data_source_missing_file_todo_cleanup(clients_mock, move_mock, test_config):
    # all the files have yet to be stored to CADC
    clients_mock.return_value.data_client.info.return_value = None

    test_reader = Mock()
    test_subject = BriteLocalFilesDataSource(
        test_config, clients_mock.return_value.data_client, test_reader, recursive=True
    )
    test_subject._capture_success = Mock()
    test_subject._capture_failure = Mock()

    # second test - missing a file, no correct observations
    # post-conditions - did the clean up happen?
    missing_file_listing = _create_dir_listing(EXTENSIONS[:-1])
    with patch('os.scandir') as scandir_mock:
        scandir_mock.return_value.__enter__.return_value = missing_file_listing
        test_subject.get_work()
        test_subject.group_work_by_obs()
        test_subject.remove_unarchived()
        test_result = test_subject._work
        assert len(test_result) == 0, 'missing file execution fail, wrong length'
        assert move_mock.called, 'cleanup should be called'
        assert move_mock.call_count == 6, 'wrong number of move calls'
        move_mock.assert_has_calls(
            [
                call('/test_files/A.avedb', '/test_files/failure'),
                call('/test_files/A.freq0db', '/test_files/failure'),
                call('/test_files/A.lst', '/test_files/failure'),
                call('/test_files/A.md5', '/test_files/failure'),
                call('/test_files/A.ndatdb', '/test_files/failure'),
                call('/test_files/A.orig', '/test_files/failure'),
            ],
        ), 'wrong move calls'
        assert len(test_subject._work) == 0, 'wrong number of files left over after clean up'
        assert not test_subject._capture_success.called, 'success should not be called'
        assert test_subject._capture_failure.called, 'failure should be called'
        assert test_subject._capture_failure.call_count == 6, 'failure call count'


@patch('brite2caom2.data_source.BriteLocalFilesDataSource._move_action')
@patch('caom2pipe.client_composable.ClientCollection')
def test_data_source_mixed_bag_todo_cleanup(clients_mock, move_mock, test_config):
    # all the files have yet to be stored to CADC
    clients_mock.return_value.data_client.info.return_value = None
    test_config.cleanup_files_when_storing = False

    test_reader = Mock()
    test_subject = BriteLocalFilesDataSource(
        test_config, clients_mock.return_value.data_client, test_reader, recursive=True
    )
    test_subject._capture_success = Mock()
    test_subject._capture_failure = Mock()

    # third test - one missing a file, one correct observation
    # post-conditions - did the clean up happen?
    missing_file_listing = _create_dir_listing(EXTENSIONS[:-1])
    correct_file_listing = _create_dir_listing(EXTENSIONS, 'B')
    test_listing = missing_file_listing + correct_file_listing
    with patch('os.scandir') as scandir_mock:
        scandir_mock.return_value.__enter__.return_value = test_listing
        test_subject.get_work()
        test_subject.group_work_by_obs()
        test_subject.remove_unarchived()
        test_result = test_subject._work
        assert len(test_result) == 5, 'mixed bag execution fail, wrong length'
        assert not move_mock.called, 'cleanup should not be called'
        assert test_subject._capture_success.called, 'success should be called'
        assert test_subject._capture_success.call_count == 2, 'success call count'
        assert test_subject._capture_failure.called, 'failure should be called'
        assert test_subject._capture_failure.call_count == 6, 'failure call count'


def _create_dir_listing(extensions, prefix='A'):
    stat_return_value = type('', (), {})
    stat_return_value.st_mtime = 1579740835.7357888

    result = []
    for ii in extensions:
        dir_entry = type('', (), {})
        dir_entry.name = f'{prefix}{ii}'
        dir_entry.path = f'/test_files/{dir_entry.name}'
        dir_entry.stat = Mock(return_value=stat_return_value)
        dir_entry.is_dir = Mock(return_value=False)
        result.append(dir_entry)
    return result
