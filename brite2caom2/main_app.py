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

"""
This module implements the ObsBlueprint mapping, as well as the workflow 
entry point that executes the workflow.
"""

from astropy.coordinates import SkyCoord
from caom2 import DataProductType, CalibrationLevel, ProductType, ReleaseType
from caom2pipe import caom_composable as cc
from caom2pipe.manage_composable import CadcException, to_float
from caom2utils.caom2blueprint import update_artifact_meta
from datetime import datetime

from brite2caom2.storage_name import add_entry, BriteName


__all__ = ['APPLICATION', 'mapping_factory']


APPLICATION = 'brite2caom2'


class BriteMapping(cc.TelescopeMapping):
    """
    The mapping for the files with no metadata.
    """

    def __init__(self, storage_name, metadata_reader, clients):
        super().__init__(storage_name, headers=[], clients=clients)
        self._metadata_reader = metadata_reader

    def accumulate_blueprint(self, bp, application=None):
        """Configure the telescope-specific ObsBlueprint at the CAOM model
        Observation level."""
        self._logger.debug('Begin accumulate_blueprint.')
        super().accumulate_blueprint(bp, APPLICATION)

        # mapping by @dbohlender
        # DB 26-10-22
        # set release date to the time the file is received at CADC
        release_date = datetime.utcnow().isoformat()
        bp.set('Observation.metaRelease', release_date)
        bp.set('Observation.intent', 'science')
        bp.set('Observation.type', 'object')

        bp.set('Plane.dataProductType', DataProductType.TIMESERIES)
        bp.set('Plane.dataRelease', release_date)
        bp.set('Plane.metaRelease', release_date)
        bp.set('Plane.provenance.project', 'BRITE-Constellation Nano-Satellites for Astrophysics')
        bp.set('Plane.provenance.reference', 'http://brite-wiki.astro.uni.wroc.pl/bwiki/doku.php?id=start')

        bp.set('Artifact.productType', '_get_artifact_product_type()')
        bp.set('Artifact.releaseType', ReleaseType.DATA)
        self._logger.debug('Done accumulate_blueprint.')

    def _get_artifact_product_type(self, ext):
        # DB 07-11-22
        # set the rlogdb and freq0db artifacts productType to info
        result = ProductType.INFO
        if self._storage_name.has_data:
            result = ProductType.SCIENCE
        return result

    def update(self, observation, file_info):
        """
        Update the Artifact file-based metadata. Override if it's necessary
        to carry out more/different updates.

        :param observation: Observation instance
        :param file_info: FileInfo instance
        :param clients: ClientCollection instance
        :return:
        """
        self._logger.debug(f'Begin update for {observation.observation_id}')
        for plane in observation.planes.values():
            if plane.product_id != self._storage_name.product_id:
                self._logger.debug(
                    f'Product ID is {plane.product_id} but working on {self._storage_name.product_id}. Continuing.'
                )
                continue
            for artifact in plane.artifacts.values():
                if artifact.uri != self._storage_name.file_uri or not self._storage_name.archived:
                    self._logger.debug(f'{self._storage_name.file_uri} is not archived. Continuing.')
                    continue
                update_artifact_meta(artifact, file_info)
                # BRITE-Constellation file extensions are pretty unique, over-ride the common code behaviour for
                # content type
                artifact.content_type = 'text/plain'
                self._update_artifact(artifact)

        self._update_copy_metadata(observation)
        self._logger.debug('End update')
        return observation

    def _update_copy_metadata(self, observation):
        """
        Copy the plane metadata from the un-decorrelated plane to the decorrelated plane.

        Copy the artifact metadata in the decorrelated plane for the .natdb artifact to the other artifacts in the
        decorrelated plane.
        """
        de_plane_key = 'decorrelated'
        un_plane_key = 'un-decorrelated'
        if de_plane_key in observation.planes.keys() and un_plane_key in observation.planes.keys():
            self._logger.debug(f'Updating plane.provenance metadata for {self._storage_name.file_uri}')
            de_plane = observation.planes[de_plane_key]
            un_plane = observation.planes[un_plane_key]
            de_plane.provenance = un_plane.provenance
            de_plane.calibration_level = CalibrationLevel.CALIBRATED

            # account for preview and thumbnail artifacts
            if len(de_plane.artifacts.values()) >= 4:
                dat_artifact_key = self._storage_name.file_uri.split('.')[0] + '.ndatdb'
                dat_artifact = de_plane.artifacts[dat_artifact_key]
                for de_artifact in de_plane.artifacts.values():
                    if '.jpg' in de_artifact.uri or '.natdb' in de_artifact.uri:
                        continue
                    for dat_part in dat_artifact.parts.values():
                        part_copy = cc.copy_part(dat_part)
                        de_artifact.parts.add(part_copy)
                        for dat_chunk in dat_part.chunks:
                            chunk_copy = cc.copy_chunk(dat_chunk)
                            part_copy.chunks.append(chunk_copy)


class BriteUndecorrelatedMapping(BriteMapping):
    """
    The mapping for the file with all the metadata.
    """

    def __init__(self, storage_name, metadata_reader, clients):
        super().__init__(storage_name, metadata_reader, clients)
        self._md_ptr = self._metadata_reader.metadata[self._storage_name.file_uri]

    def accumulate_blueprint(self, bp, application=None):
        """Configure the telescope-specific ObsBlueprint at the CAOM model
        Observation level."""
        self._logger.debug('Begin accumulate_blueprint.')
        super().accumulate_blueprint(bp, APPLICATION)

        # mapping by @dbohlender
        object_metadata = self._md_ptr.get('StarInFo').split(',')
        target_name = object_metadata[0]
        bp.set('Observation.target.name', target_name)
        bp.set('Observation.target.standard', False)
        bp.set('Observation.telescope.name', self._md_ptr.get('SatfulID'))
        bp.set('Observation.instrument.name', self._md_ptr.get('SatfulID'))

        bp.set('Plane.calibrationLevel', CalibrationLevel.RAW_STANDARD)
        bp.set('Plane.provenance.name', self._md_ptr.get('RedMetho'))
        bp.set('Plane.provenance.producer', self._md_ptr.get('RedProID'))
        bp.set('Plane.provenance.version', self._md_ptr.get('RedVersi'))
        bp.set('Plane.provenance.runID', self._md_ptr.get('ReleaseV'))

        bp.configure_position_axes((1, 2))
        # this is here to fake out the Blueprint
        bp.set('Chunk.naxis', 4)
        # spatial WCS, assuming 5" x 5" aperture
        object_coords = SkyCoord.from_name(target_name)
        bp.set('Chunk.position.axis.axis1.ctype', 'RA---TAN')
        bp.set('Chunk.position.axis.axis1.cunit', 'deg')
        bp.set('Chunk.position.axis.axis2.ctype', 'DEC--TAN')
        bp.set('Chunk.position.axis.axis2.cunit', 'deg')

        # plate scale (arcsec/pixel)
        plate_scale = to_float(self._md_ptr.get('InstPScl'))
        # aperture for photometry
        # DB 26-10-22
        # set the aperture size to 5.0 by default
        # DB 04-11-22
        # FOV is about 5' x 5'
        aperture_size = 5.0
        if plate_scale is not None:
            temp = plate_scale * aperture_size / 3600.0
            bp.set('Chunk.position.axis.function.cd11', temp)
            bp.set('Chunk.position.axis.function.cd12', 0.0)
            bp.set('Chunk.position.axis.function.cd21', 0.0)
            bp.set('Chunk.position.axis.function.cd22', temp)

        bp.set('Chunk.position.axis.function.dimension.naxis1', 1)
        bp.set('Chunk.position.axis.function.dimension.naxis2', 1)
        bp.set('Chunk.position.axis.function.refCoord.coord1.pix', 1.0)
        bp.set('Chunk.position.axis.function.refCoord.coord1.val', object_coords.ra.degree)
        bp.set('Chunk.position.axis.function.refCoord.coord2.pix', 1.0)
        bp.set('Chunk.position.axis.function.refCoord.coord2.val', object_coords.dec.degree)

        bp.configure_energy_axis(3)
        # DB - original script - units are microns
        # CAOM2 blueprint does 'm'
        # DB 26-10-22
        # remove the 'BRITE-' from the bandpass name
        if self._md_ptr.get('SatellID')[-1] == 'b':
            bandpass_name = 'Blue'
            lower_wl = 0.39
            upper_wl = 0.46
        elif self._md_ptr.get('SatellID')[-1] == 'r':
            bandpass_name = 'Red'
            lower_wl = 0.54
            upper_wl = 0.70
        else:
            raise CadcException(f'Unknown filter value ' f'{self._md_ptr.get("SatellID")[-1]}')
        bp.set('Chunk.energy.specsys', 'TOPOCENT')
        bp.set('Chunk.energy.bandpassName', bandpass_name)
        bp.set('Chunk.energy.resolvingPower', (lower_wl + upper_wl) / (2.0 * (upper_wl - lower_wl)))
        bp.set('Chunk.energy.axis.axis.ctype', 'WAVE')
        bp.set('Chunk.energy.axis.axis.cunit', 'm')
        bp.set('Chunk.energy.axis.range.start.pix', 0.5)
        bp.set('Chunk.energy.axis.range.start.val', lower_wl * 10e-7)
        bp.set('Chunk.energy.axis.range.end.pix', 1.5)
        bp.set('Chunk.energy.axis.range.end.val', upper_wl * 10e-7)
        # this is to avoid a FITS ValueError _only_
        bp.set('Chunk.energy.axis.function.naxis', 1)

        bp.configure_time_axis(4)
        bp.set('Chunk.time.exposure', float(self._md_ptr.get('ObsExpoT')) / 1000.0)  # seconds
        bp.set('Chunk.time.axis.axis.ctype', 'TIME')
        bp.set('Chunk.time.axis.axis.cunit', 'd')
        bp.set('Chunk.time.axis.range.start.pix', 0.5)
        bp.set('Chunk.time.axis.range.start.val', '_get_time_axis_range_start_val()')
        bp.set('Chunk.time.axis.range.end.pix', 1.5)
        bp.set('Chunk.time.axis.range.end.val', '_get_time_axis_range_end_val()')
        # this is to avoid a FITS ValueError _only_
        bp.set('Chunk.time.axis.function.naxis', 1)
        self._logger.debug('Done accumulate_blueprint.')

    def _get_time_axis_range_end_val(self, ext):
        return to_float(self._metadata_reader.time_series.get(self._storage_name.file_uri).get('HJD')[-1]) - 2400000.5

    def _get_time_axis_range_start_val(self, ext):
        return to_float(self._metadata_reader.time_series.get(self._storage_name.file_uri).get('HJD')[0]) - 2400000.5

    def _update_artifact(self, artifact):
        self._logger.debug(f'Begin _update_artifact for {artifact.uri}')
        for part in artifact.parts.values():
            for chunk in part.chunks:
                # no cutouts
                chunk.naxis = None
                chunk.position_axis_1 = None
                chunk.position_axis_2 = None
                chunk.energy_axis = None
                chunk.time_axis = None
                # using range for BRITE-Constellation
                if chunk.time is not None and chunk.time.axis is not None and chunk.time.axis.function is not None:
                    chunk.time.axis.function = None
                if (
                    chunk.energy is not None
                    and chunk.energy.axis is not None
                    and chunk.energy.axis.function is not None
                ):
                    chunk.energy.axis.function = None
        self._logger.debug('End _update_artifact')


class BriteDecorrelatedMapping(BriteUndecorrelatedMapping):
    """
    The mapping for the ndatdb file which has the time metadata for the plane at calibration level 2.
    """

    def __init__(self, storage_name, metadata_reader, clients):
        super().__init__(storage_name, metadata_reader, clients)
        # do the things necessary to read the metadata for the .orig file, which is the origin of most of
        # the Observation content, except for the TemporalWCS start/stop times
        orig_uri, new_fqn = add_entry(self._storage_name, '.ndatdb', '.orig', self._clients, self._metadata_reader)
        self._logger.debug('Add .orig URI to MetadataReader.')
        self._md_ptr = self._metadata_reader.metadata[orig_uri]

    def _get_time_axis_range_end_val(self, ext):
        return to_float(
            self._metadata_reader.time_series.get(self._storage_name.file_uri).get('BJD')[-1] + 2456000.0 - 2400000.5
        )

    def _get_time_axis_range_start_val(self, ext):
        return to_float(
            self._metadata_reader.time_series.get(self._storage_name.file_uri).get('BJD')[0] + 2456000.0 - 2400000.5
        )


def mapping_factory(storage_name, metadata_reader, clients, logger):
    if BriteName.archived(storage_name.file_name):
        if storage_name.has_undecorrelated_metadata:
            result = BriteUndecorrelatedMapping(storage_name, metadata_reader, clients)
        elif storage_name.has_decorrelated_metadata:
            result = BriteDecorrelatedMapping(storage_name, metadata_reader, clients)
        else:
            result = BriteMapping(storage_name, metadata_reader, clients)
        logger.debug(f'Created an instance of {type(result)} for {storage_name.file_uri}')
    else:
        logger.debug(f'Not archiving {storage_name.file_name}.')
        result = None
    return result
