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

"""
Implements the default entry point functions for the workflow 
application.

'run' executes based on either provided lists of work, or files on disk.
'run_incremental' executes incrementally, usually based on time-boxed
intervals.
"""

import logging
import sys
import traceback

from caom2pipe.client_composable import ClientCollection
from caom2pipe.manage_composable import Config, StorageName
from caom2pipe.name_builder_composable import EntryBuilder
from caom2pipe.reader_composable import StorageClientReader
from caom2pipe.run_composable import common_runner_init, run_by_todo, TodoRunner
from caom2pipe.transfer_composable import modify_transfer_factory, store_transfer_factory

from brite2caom2 import data_source, main_app, reader, storage_name
from brite2caom2 import fits2caom2_augmentation, preview_augmentation


BRITE_BOOKMARK = 'brite_timestmap'
META_VISITORS = [fits2caom2_augmentation, preview_augmentation]
DATA_VISITORS = []


"""
How to handle the metadata_reader:
- because the data are not in FITS formats, there will always be a reader
"""


class BriteTodoRunner(TodoRunner):
    """Specialize the handling of the complete record count, because there are sentinel files that must be present
    before storage/ingestion can take place, although the sentinel files themselves are not archived.
    """

    def __init__(self, config, organizer, builder, source, metadata_reader, application):
        super().__init__(config, organizer, builder, source, metadata_reader, application)

    def _build_todo_list(self):
        """
        This is where the initial record count is set for the summary report, and because some BRITE-Constellation
        files are tracked for failure/success, but are not processed, that requires different handling.
        """
        self._logger.debug(f'Begin _build_todo_list with {self._data_source.__class__.__name__}.')
        # the initial directory listing
        self._data_source.get_work()
        # check to make sure all seven files per Observation are present - files may be moved to failure here, without
        # being tracked in the 'complete record count'
        self._data_source.group_work_by_obs()
        self._todo_list = self._data_source._work
        self._organizer.complete_record_count = len(self._todo_list)
        self._logger.info(f'Processing {self._organizer.complete_record_count} records.')
        # remove the sentinel files from the list of work to be done, and put those files in the success location
        self._data_source.remove_unarchived()
        self._logger.debug('End _build_todo_list.')


def _common_init(config):
    builder = EntryBuilder(storage_name.BriteName)
    StorageName.collection = config.collection
    clients = ClientCollection(config)
    if config.use_local_files:
        metadata_reader = reader.BriteFileMetadataReader()
    else:
        metadata_reader = reader.BriteStorageClientMetadataReader(clients.data_client)
    source = data_source.BriteLocalFilesDataSource(
        config, clients.data_client, metadata_reader, config.recurse_data_sources
    )
    logging.getLogger('matplotlib').setLevel(logging.ERROR)
    return builder, clients, metadata_reader, source


def _run():
    """
    Uses a todo file to identify the work to be done.

    :return 0 if successful, -1 if there's any sort of failure. Return status
        is used by airflow for task instance management and reporting.
    """
    config = Config()
    config.get_executors()
    builder, clients, metadata_reader, source = _common_init(config)
    if config.use_local_files:
        modify_transfer = modify_transfer_factory(config, clients)
        store_transfer = store_transfer_factory(config, clients)
        (
            config,
            clients,
            name_builder,
            source,
            modify_transfer,
            metadata_reader,
            store_transfer,
            organizer,
        ) = common_runner_init(
            config,
            clients,
            builder,
            source,
            modify_transfer,
            metadata_reader,
            False,
            store_transfer,
            META_VISITORS,
            DATA_VISITORS,
            None,
        )

        runner = BriteTodoRunner(
            config, organizer, name_builder, source, metadata_reader, main_app.APPLICATION
        )
        result = runner.run()
        result |= runner.run_retry()
        runner.report()
    else:
        result = run_by_todo(
            config=config,
            name_builder=builder,
            meta_visitors=[],
            data_visitors=META_VISITORS,  # because there's no fhead for text files
            clients=clients,
            metadata_reader=metadata_reader,
        )
    return result


def run():
    """Wraps _run in exception handling, with sys.exit calls."""
    try:
        result = _run()
        sys.exit(result)
    except Exception as e:
        logging.error(e)
        tb = traceback.format_exc()
        logging.debug(tb)
        sys.exit(-1)
