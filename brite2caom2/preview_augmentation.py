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

"""
Plotting routines to be adapted for previews and thumbnails from  @dbohlender.
"""

import numpy as np
from matplotlib import pylab

from caom2pipe import manage_composable as mc
from brite2caom2.storage_name import get_entry


class BRITEDecorrelatedPreview(mc.PreviewVisitor):

    def __init__(self, instrument_name, **kwargs):
        super().__init__(**kwargs)
        self._instrument_name = instrument_name
        # do the things necessary to read the metadata for the .ndatdb and .avedb files
        get_entry(self._storage_name, '.rlogdb', '.ndatdb', self._clients, self._metadata_reader)
        get_entry(self._storage_name, '.rlogdb', '.avedb', self._clients, self._metadata_reader)

    def generate_plots(self, obs_id):
        mjd_decorr = np.array(self._metadata_reader.time_series[self._storage_name.decorrelated_uri]['BJD'])
        mag_decorr = np.array(self._metadata_reader.time_series[self._storage_name.decorrelated_uri]['BRITEMAG'])
        sigma_decorr = np.array(self._metadata_reader.time_series[self._storage_name.decorrelated_uri]['SIGMA_BRITEMAG'])
        mjd_ave = np.array(self._metadata_reader.time_series[self._storage_name.average_uri]['ave_BJD'])
        mag_ave = np.array(self._metadata_reader.time_series[self._storage_name.average_uri]['ave_BRITEMAG'])
        sigma_ave = np.array(self._metadata_reader.time_series[self._storage_name.average_uri]['ave_SIGMA_BRITEMAG'])

        pylab.plot(mjd_decorr, mag_decorr, 'k.', label=self._instrument_name)
        pylab.errorbar(mjd_decorr, mag_decorr, yerr=sigma_decorr, xerr=None, fmt='k.')
        pylab.plot(mjd_ave, mag_ave, 'co', label='Average/orbit')
        pylab.errorbar(mjd_ave, mag_ave, yerr=sigma_ave, xerr=None, fmt='c.')
        pylab.xlabel('Barycentric Julian Date - 2456000.0', color='k')
        pylab.ylabel('BRITE Magnitude', color='k')
        pylab.xlim(mjd_decorr.min(), mjd_decorr.max())
        # DB 07-11-22
        # flip the y-axis direction on the ndatdb/ave plots since brighter = lower magnitude value
        pylab.ylim(
            mag_decorr.max() + sigma_decorr.max(),
            mag_decorr.min() - sigma_decorr.max(),
        )
        pylab.title(obs_id, color='k', fontweight='bold')
        pylab.legend()
        pylab.savefig(self._preview_fqn, format='png')
        return self._save_figure()


def visit(observation, **kwargs):
    storage_name = kwargs.get('storage_name')
    result = observation
    if storage_name.is_last_to_ingest:
        # attempt to make sure the instrument name has been set from the .orig file
        instrument_name = observation.instrument.name if observation.instrument is not None else 'BRITE Data'
        result = BRITEDecorrelatedPreview(instrument_name, **kwargs).visit(observation)
    return result
