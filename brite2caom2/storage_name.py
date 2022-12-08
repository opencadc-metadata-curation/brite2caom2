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

from os.path import basename, dirname, exists, splitext
from caom2pipe.manage_composable import StorageName


__all__ = ['get_entry', 'BriteName']


class BriteName(StorageName):
    """
    For the decorrelated planes, need to have the content of multiple files to generate the previews, so implement
    this class to collect the artifacts by plane product ID. This decision is captured in how _source_names and
    _destination_uris are set.
    """

    def __init__(self, entry):
        super().__init__(
            file_name=basename(entry),
            source_names=[entry],
        )

    @property
    def average_uri(self):
        return self._get_uri(f'{self._obs_id}.avedb', StorageName.scheme)

    @property
    def decorrelated_uri(self):
        return self._get_uri(f'{self._obs_id}.ndatdb', StorageName.scheme)

    @property
    def has_decorrelated_metadata(self):
        return self._file_name.endswith('.ndatdb')

    @property
    def has_undecorrelated_metadata(self):
        return self._file_name.endswith('.orig')

    @property
    def has_data(self):
        return (
            self._file_name.endswith('.orig')
            or self._file_name.endswith('.ndatdb')
            or self._file_name.endswith('.avedb')
        )

    @property
    def is_last_to_ingest(self):
        return self._file_name.endswith('.rlogdb')

    @property
    def prev(self):
        result = f'{self._obs_id}_prev.jpg'
        if self.has_undecorrelated_metadata:
            result = f'{self._obs_id}_un_prev.jpg'
        return result

    @property
    def thumb(self):
        result = f'{self._obs_id}_prev_256.jpg'
        if self.has_undecorrelated_metadata:
            result = f'{self._obs_id}_un_prev_256.jpg'
        return result

    def is_valid(self):
        return True

    def use_different_file(self, original_extension, new_extension):
        """
        Common code to refer to a different file than the one currently being processed.
        :return:
        """
        new_fqn = self._source_names[0].replace(original_extension, new_extension)
        new_uri = self._get_uri(basename(new_fqn), StorageName.scheme)
        return new_fqn, new_uri

    def set_file_id(self):
        self._file_id = BriteName.remove_extensions(self._file_name)

    def set_product_id(self):
        # DB 07-12-22
        self._product_id = 'timeseries'

    @staticmethod
    def remove_extensions(f_name):
        return splitext(f_name)[0]

    @staticmethod
    def is_archived(file_name):
        # files whose purpose is other than storage/ingestion at CADC
        return not (file_name.endswith('.lst') or file_name.endswith('.md5'))


def get_entry(sn, original_extension, new_extension, clients, metadata_reader):
    """
    Common code to retrieve and reference a different file than the one currently being processed in the collection
    of 5 that make up a BRITE-Constellation Observation.

    This happens because the unit of work for any pipeline is a file, but the BRITE-Constellation plane- and
    artifact-level metadata obtain most of their metadata and data from the .orig file, with a few exceptions:
      - the TemporalWCS metadata for the decorrelated plane
      - the previews for the decorrelated plane require data from two files
    """
    fqn, uri = sn.use_different_file(original_extension, new_extension)
    if dirname(fqn) is None or dirname(fqn) == '':
        fqn = f'./{fqn}'
    # retrieve the file if it doesn't already exist - e.g. if re-ingesting files/observations
    if not exists(fqn) and clients is not None:
        clients.data_client.get(dirname(fqn), uri)
    # retrieve the file metadata if it doesn't already exist
    with open(fqn) as f:
        metadata_reader._read_file(f, uri)
    return uri, fqn
