# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2020.                            (c) 2020.
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

import glob
import os
import test_main_app

from collections import defaultdict
from mock import patch
from shutil import copy

from caom2utils import data_util
from caom2pipe import manage_composable as mc
from brite2caom2 import composable
import test_data_source


TEST_ROOT_DIR = f'{test_main_app.TEST_DATA_DIR}/HD37202'


def test_run_by_state():
    pass


@patch('brite2caom2.data_source.BriteLocalFilesDataSource._move_action')
@patch('brite2caom2.composable.ClientCollection')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_run_local_clean_up(access_mock, client_mock, move_mock, test_config, tmp_path):
    access_mock.return_value = 'https://localhost'
    test_config.change_working_directory(tmp_path.as_posix())

    # dict to track how many times info has been called for a particular URI
    info_uri_calls = defaultdict(int)

    def _info_mock(uri):
        info_uri_calls[uri] += 1
        if info_uri_calls[uri] > 1:
            fqn = f'{TEST_ROOT_DIR}/{os.path.basename(uri)}'
            return data_util.get_local_file_info(fqn)
        else:
            return None

    client_mock.return_value.data_client.info.side_effect = _info_mock

    test_files = glob.glob(f'{TEST_ROOT_DIR}/*')
    orig_cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        test_config.logging_level = 'INFO'
        test_config.task_types = [mc.TaskType.STORE, mc.TaskType.INGEST]
        test_config.store_modified_files_only = True
        test_config.cleanup_files_when_storing = True
        test_config.cleanup_failure_destination = f'{tmp_path.as_posix()}/failure'
        test_config.cleanup_success_destination = f'{tmp_path.as_posix()}/success'
        test_config.retry_failures = False
        test_config.log_to_file = True
        test_config.use_local_files = True
        test_config.data_source_extensions = test_data_source.EXTENSIONS
        test_config.data_sources = [TEST_ROOT_DIR]
        test_config.features.supports_latest_client = True
        test_config.features.supports_decompression = True
        test_config.proxy_file_name = 'test_proxy.pem'
        test_config.write_to_file(test_config)

        def _meta_read_mock(ignore_collection, obs_id):
            result = None
            fqn = f'{tmp_path.as_posix()}/logs/{obs_id}.xml'
            if os.path.exists(fqn):
                result = mc.read_obs_from_file(fqn)
            return result

        client_mock.return_value.metadata_client.read.side_effect = _meta_read_mock

        for d in [test_config.cleanup_failure_destination, test_config.cleanup_success_destination]:
            os.mkdir(d)

        with open(test_config.proxy_fqn, 'w') as f:
            f.write('test content')

        def _move_mock(source, destination):
            # assumes all the move actions for this test are to success
            assert source in test_files, f'unexpected source {source}'
            assert destination == f'{tmp_path.as_posix()}/success', f'unexpected destination {destination}'

        move_mock.side_effect = _move_mock

        test_result = composable._run()
        assert test_result == 0, 'expect success'
        # 42 files making up 6 observations, 5 files per observation should be stored
        # two calls per file, one to make sure it's not already stored, and another call after storing to ensure
        # the file was stored correctly
        #
        # 6 * 5 * 2 = 60 calls
        assert (
            client_mock.return_value.data_client.info.call_count == 60
        ), f'wrong info count {client_mock.return_value.data_client.info.call_count}'
        assert client_mock.return_value.data_client.put.called, 'put should be called'
        # 54 = 30 science files + 24 preview files
        # 24 preview files => 2 planes per observation, 2 files per plane => 4 files/observation
        #    6 observations * 4 files = 24 preview files
        assert client_mock.return_value.data_client.put.call_count == 54, 'put call count'
        # 30 = once per archived file
        assert client_mock.return_value.metadata_client.read.call_count == 30, 'meta read call count'
        assert client_mock.return_value.metadata_client.create.call_count == 6, 'meta create call count'
        assert client_mock.return_value.metadata_client.update.call_count == 24, 'meta update call count'
        prefix = os.path.basename(tmp_path.as_posix())
        report_fqn = f'{tmp_path.as_posix()}/logs/{prefix}_report.txt'
        _check_report_file(report_fqn, 42)
    finally:
        os.chdir(orig_cwd)


def test_run_scrape(test_config, tmp_path):
    test_config.change_working_directory(tmp_path.as_posix())
    test_config.logging_level = 'INFO'
    test_config.use_local_files = True
    test_config.data_sources = [f'{test_main_app.TEST_DATA_DIR}/HD36486']
    test_config.data_source_extensions = test_data_source.EXTENSIONS
    test_config.task_types = [mc.TaskType.SCRAPE]
    orig_cwd = os.getcwd()
    try:
        os.chdir(tmp_path.as_posix())
        mc.Config.write_to_file(test_config)
        test_result = composable._run()
        assert test_result == 0, 'wrong return value'
        previews = glob.glob(f'{tmp_path.as_posix()}/*/*.jpg')
        assert len(previews) == 36, 'preview generation failed'
        # 63 = 7 files * 9 observations
        _check_report_file(test_config.report_fqn, 63)
    finally:
        os.chdir(orig_cwd)


@patch('brite2caom2.composable.ClientCollection')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_run_reingest_retry(access_mock, client_mock, test_config, tmp_path):
    access_mock.return_value = 'https://localhost'
    test_config.change_working_directory(tmp_path.as_posix())

    def _info_mock(uri):
        fqn = f'{TEST_ROOT_DIR}/{os.path.basename(uri)}'
        return data_util.get_local_file_info(fqn)

    client_mock.return_value.data_client.info.side_effect = _info_mock

    test_files = glob.glob(f'{TEST_ROOT_DIR}/*')

    orig_cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        test_config.logging_level = 'INFO'
        test_config.task_types = [mc.TaskType.MODIFY]
        test_config.store_modified_files_only = False
        test_config.cleanup_files_when_storing = False
        test_config.retry_failures = True
        test_config.retry_count = 1
        test_config.proxy_file_name = 'test_proxy.pem'
        test_config.write_to_file(test_config)

        with open(test_config.work_fqn, 'w') as f:
            for temp in test_files:
                if '.lst' in temp or '.md5' in temp:
                    continue
                f_name = os.path.basename(temp)
                f.write(f'{f_name}\n')

        def _cadcget_mock(uri, dest):
            f_name = os.path.basename(uri)
            fqn = f'{TEST_ROOT_DIR}/{f_name}'
            with open(fqn, 'rb') as f_in:
                dest.writelines(f_in.readlines())

        # this mock is for the BriteStorageClientMetadataReader
        client_mock.return_value.data_client.cadcget.side_effect = _cadcget_mock

        def _data_get_mock(working_directory, uri):
            f_name = os.path.basename(uri)
            fqn = f'{TEST_ROOT_DIR}/{f_name}'
            copy(fqn, working_directory)

        # this mock is for the ClientComposable read
        client_mock.return_value.data_client.get.side_effect = _data_get_mock

        def _meta_read_mock(ignore_collection, obs_id):
            fqn = f'{test_main_app.TEST_DATA_DIR}/{obs_id}.expected.xml'
            return mc.read_obs_from_file(fqn)

        client_mock.return_value.metadata_client.read.side_effect = _meta_read_mock

        with open(test_config.proxy_fqn, 'w') as f:
            f.write('test content')

        test_result = composable._run()
        assert test_result == 0, 'expect success'
        assert (
            client_mock.return_value.data_client.info.call_count == 30
        ), f'wrong info count {client_mock.return_value.data_client.info.call_count}'
        assert client_mock.return_value.data_client.put.called, 'put called for previews'
        assert client_mock.return_value.data_client.put.call_count == 24, 'wrong put call count'
        assert client_mock.return_value.data_client.cadcget.called, 'cadcget should be called'
        # 18 = 6 observations * 3 files (.avedb, .orig, .ndatdb) / observation needing cadcget retrieval
        assert client_mock.return_value.data_client.cadcget.call_count == 18, 'cadcget call count'
        # 30 = once per archived file
        assert client_mock.return_value.metadata_client.read.call_count == 30, 'meta read call count'
        assert client_mock.return_value.metadata_client.update.call_count == 30, 'meta update call count'
        _check_report_file(test_config.report_fqn, 30)
    finally:
        os.chdir(orig_cwd)


# do this if there's a need to re-generate the sc2repo content with big changes
# def test_run_roundtrip():
#     test_config = mc.Config()
#     test_config.collection = main_app.COLLECTION
#     test_config.logging_level = 'DEBUG'
#     test_config.use_local_files = True
#     test_config.data_sources = [f'{test_main_app.TEST_DATA_DIR}/HD36486', f'{test_main_app.TEST_DATA_DIR}/HD37202']
#     # test_config.data_sources = [f'{test_main_app.TEST_DATA_DIR}/HD37202']
#     test_config.data_source_extensions = test_data_source.EXTENSIONS
#     test_config.task_types = [mc.TaskType.SCRAPE]
#     temp_dir = f'{test_main_app.THIS_DIR}/round_trip'
#     os.chdir(temp_dir)
#     test_config.log_file_directory = f'{temp_dir}/logs'
#     mc.Config.write_to_file(test_config)
#     test_result = composable._run()
#     assert test_result == 0, 'wrong return value'
#     previews = glob.glob(f'{temp_dir}/*/*.jpg')
#     assert len(previews) == 60, 'preview generation failed'
#     report_fqn = f'{temp_dir}/logs/app_report.txt'
#     _check_report_file(report_fqn)


def _check_report_file(fqn, expected_count):
    assert os.path.exists(fqn), 'expect report file'
    input_count = None
    success_count = None
    with open(fqn) as f:
        for line in f:
            if "Number of Inputs" in line:
                input_count = line.split(':')[-1].strip()
            if 'Number of Successes' in line:
                success_count = line.split(':')[-1].strip()
    assert input_count is not None, 'expect an input count value'
    assert success_count is not None, 'expect an success count value'
    assert input_count == success_count, f'expect {input_count} to equal {success_count}'
    assert int(input_count) == expected_count, 'wrong input count'
