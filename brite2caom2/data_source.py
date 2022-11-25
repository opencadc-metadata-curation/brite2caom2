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

from collections import Counter, deque
from datetime import datetime
from os.path import basename

from caom2pipe.data_source_composable import DataSource, LocalFilesDataSource

from brite2caom2.storage_name import BriteName


class BriteLocalFilesDataSource(LocalFilesDataSource):
    """
    DB 26-10-22
    only store/ingest files where all the files that make up an Observation are present on disk

    BRITE-Constellation creates seven files per Observation. All those files must be present for an Observation to be
    created.  Two of those files are not archived: *.lst and *.md5.

    This class does the work of making sure this file => Observation organization is in place, as part of finding the
    local work to be done.
    """

    def __init__(self, config, cadc_client, metadata_reader, recursive):
        super().__init__(config, cadc_client, metadata_reader, recursive)
        self._correct_listing_length = 0

    def _verify_file(self, fqn):
        """As of now, there are no checks of file content in addition to what is already done by the MetadataReader
        specialization, which is why the function is returning the default value of 'the file is good to store'."""
        return True

    @property
    def correct_listing_length(self):
        return self._correct_listing_length

    def group_work_by_obs(self):
        """
        Check that all the files for an Observation are present, otherwise, reject the files for ingestion.
        """
        # need to clean up the file that are not part of a valid observation
        temp = [
            k for k, v in Counter(
                [BriteLocalFilesDataSource._get_obs_id_from_fqn(ii) for ii in self._work]
            ).items() if v != len(self._extensions)
        ]
        if len(temp) > 0:
            clean_up_files = [
                ii for ii in self._work if BriteLocalFilesDataSource._get_obs_id_from_fqn(ii) in temp
            ]
            for fqn in clean_up_files:
                # don't use self.clean_up, because it invokes a CADC storage info call, and that check doesn't
                # matter for this failure case
                self._logger.warning(
                    f'Fail {fqn} because not all the file types are present for observation '
                    f'{BriteLocalFilesDataSource._get_obs_id_from_fqn(fqn)}.'
                )
                if self._cleanup_when_storing:
                    self._move_action(fqn, self._cleanup_failure_directory)
                temp_storage_name = BriteName(entry=fqn)
                self._reporter.capture_failure(temp_storage_name, BaseException('manifest errors'), 'manifest errors')
                self._work.remove(fqn)

    def clean_up(self, entry, execution_result, current_count):
        if BriteName.archived(entry):
            super().clean_up(entry, execution_result, current_count)
        else:
            # avoid the check for the presence of the file in CADC storage prior to picking a clean up destination.
            if self._cleanup_when_storing:
                self._move_action(entry, self._cleanup_success_directory)
                self._reporter.capture_success(
                    BriteLocalFilesDataSource._get_obs_id_from_fqn(entry),
                    basename(entry),
                    datetime.utcnow().timestamp(),
                )
            self._logger.debug('End clean_up.')

    def default_filter(self, entry):
        if BriteName.archived(entry.path):
            work_with_file = super().default_filter(entry)
        else:
            # avoid the check for the presence of the file in CADC storage, since it will never be in CADC storage.
            work_with_file = True
        return work_with_file

    def remove_unarchived(self):
        # remove the files that are not archived from the list of work, and put them in the success destination
        remove_these = []
        for entry in self._work:
            if not BriteName.archived(entry):
                if self._cleanup_when_storing:
                    self._move_action(entry, self._cleanup_success_directory)
                remove_these.append(entry)
                self._reporter.capture_success(
                    BriteLocalFilesDataSource._get_obs_id_from_fqn(entry),
                    basename(entry),
                    datetime.utcnow().timestamp(),
                )
        for entry in remove_these:
            self._work.remove(entry)

    @staticmethod
    def _get_obs_id_from_fqn(fqn):
        return BriteName.remove_extensions(basename(fqn))
