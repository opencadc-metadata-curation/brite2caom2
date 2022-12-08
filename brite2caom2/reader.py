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

from astropy.io import fits
from caom2pipe.manage_composable import CadcException
from caom2pipe import reader_composable as rdc
from collections import defaultdict
from io import BytesIO
from brite2caom2.storage_name import BriteName


__all__ = ['BriteFileMetadataReader', 'BriteStorageClientMetadataReader']


class BriteMetaDataReader(rdc.MetadataReader):
    """
    DB 01-02-2021
    BRITE files are all ascii csv files.

    The _retrieve_file_info method makes sense as it. The _retrieve_header
    method does not make sense from a name point of view, but there is still
    an access for metadata and data retrieval.

    A single Observation relies on the existence of five files with different extensions. Ensure those five files
    exist before ingestion, since the metadata from one file applies to all files, and successful preview generation
    relies on the content of two other files.
    """
    comment_char = '#'

    @property
    def metadata(self):
        return self._metadata

    @property
    def time_series(self):
        return self._time_series

    def _read_file(self, brite_fh, uri):
        """
        Read the metadata and data from csv files.

        :param brite_fh: file handle
        :param uri:
        """
        if uri.endswith('.orig'):
            self._read_orig_file(brite_fh, uri)
        elif uri.endswith('.ndatdb'):
            self._read_bjd_file(brite_fh, uri, ['BJD', 'BRITEMAG', 'SIGMA_BRITEMAG'])
        elif uri.endswith('.avedb'):
            self._read_bjd_file(brite_fh, uri, ['ave_BJD', 'ave_BRITEMAG', 'ave_SIGMA_BRITEMAG'])

    def _read_bjd_file(self, brite_fh, uri, keys):
        """
        :param brite_fh: file handle
        :param uri: CADC uri for the file
        :param keys: key names for accessing the timeseries
        """
        default_found = False
        data = defaultdict(list)
        # Read average magnitudes/orbit from the 'avedb' file.
        for line in brite_fh:
            if len(line) == 0:
                continue
            if line[0] == BriteMetaDataReader.comment_char:
                # skip header lines
                # DB 26-10-22
                # Note:  for preview generation, the x-axis assumes the 2456000.0 value in this header line doesn’t
                # change:
                # (1) BJD(TDB) - 2456000.0    : Barycentric Julian Date.
                #
                # Add a check to ensure the default doesn't change.
                if '2456000.0' in line:
                    default_found = True
                continue
            else:
                # Read time series data into dictionary arrays
                datapoint = line.split()
                data[keys[0]].append(float(datapoint[0]))
                data[keys[1]].append(float(datapoint[1]))
                data[keys[2]].append(float(datapoint[3]))

        if not default_found:
            raise CadcException(f'Wrong default x-axis value found for {uri}. Stopping.')

        self._metadata[uri] = {}
        self._time_series[uri] = data
        self._headers[uri] = [fits.Header()]

    def _read_orig_file(self, brite_fh, uri):
        """
        This file contains most of the metadata for a CAOM2 record.
        :param brite_fh: file handle
        :param uri: CADC uri for the file
        """
        data = defaultdict(list)
        metadata = {}
        for line in brite_fh:
            if len(line) == 0:
                continue
            # First read header content of data file
            if line[0] == BriteMetaDataReader.comment_char:
                ll = line.split('=', 1)
                if '----------' in ll[0]:
                    continue
                keyword = ll[0].replace(f'{BriteMetaDataReader.comment_char} ', '').strip()
                [value, comment] = ll[1].split('/', 1)
                value = value.strip()
                metadata[keyword] = value
                # Initialize time series data arrays
                if 'column' in keyword:
                    data[value] = []
            else:
                datapoint = line.split()
                for x in range(0, len(datapoint)):
                    key = 'column' + str(x + 1)
                    data[metadata[key]].append(float(datapoint[x]))

        self._metadata[uri] = metadata
        self._time_series[uri] = data
        self._headers[uri] = [fits.Header()]

    def set(self, storage_name):
        self.set_file_info(storage_name)
        self.set_time_series(storage_name)

    def reset(self):
        super().reset()
        self._time_series = {}
        self._metadata = {}

    def __str__(self):
        ts_keys = '\n'.join(ii for ii in self._time_series)
        return f'\nKeys:\n{ts_keys}\n'

    def set_file_info(self, storage_name):
        """Retrieves FileInfo information to memory."""
        self._logger.debug(f'Begin set_file_info for {storage_name.file_name}')
        for index, entry in enumerate(storage_name.destination_uris):
            if entry not in self._file_info and BriteName.is_archived(entry):
                self._logger.debug(f'Retrieve FileInfo for {entry}')
                self._retrieve_file_info(entry, storage_name.source_names[index])
        self._logger.debug('End set_file_info')


class BriteFileMetadataReader(BriteMetaDataReader, rdc.FileMetadataReader):

    def __init__(self):
        super().__init__()
        self._time_series = {}
        self._metadata = {}

    def set_time_series(self, storage_name):
        for index, entry in enumerate(storage_name.destination_uris):
            if entry not in self._time_series.keys():
                if storage_name.has_data:
                    self._logger.debug(f'Retrieve content for {entry}')
                    with open(storage_name.source_names[index]) as f:
                        self._read_file(f, entry)
                else:
                    self._logger.info(f'No Content for {entry}')


class BriteStorageClientMetadataReader(BriteMetaDataReader, rdc.StorageClientReader):

    def __init__(self, client):
        super().__init__(client)
        self._time_series = {}
        self._metadata = {}

    def set_time_series(self, storage_name):
        for index, entry in enumerate(storage_name.destination_uris):
            if entry not in self._time_series.keys():
                if storage_name.has_data:
                    self._logger.debug(f'Retrieve content for {entry}')
                    buffer = BytesIO()
                    self._client.cadcget(storage_name.file_uri, buffer)
                    self._read_file(buffer.getvalue().decode().split('\n'), storage_name.file_uri)
                    # I think this does a memory clean-up
                    buffer.close()
                else:
                    self._logger.info(f'No Content for {entry}')
