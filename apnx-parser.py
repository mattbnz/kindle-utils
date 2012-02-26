#!/usr/bin/env python
# Kindle Page Number Sidecar file parsing routines
#
# This file is released under the GPLv2 license.
#     Copyright (C) 2012 Matt Brown <matt@mattb.net.nz>
#
import json
import logging
import struct
import sys

logger = logging.getLogger().getChild('apnx-parser')


class ApnxException(Exception):
    pass

class BinaryFile(object):

    def __init__(self, filename):
        self.data_file = open(filename, 'rb').read()
        self.end = len(self.data_file)
        self.pos = 0

    def _Unpack(self, format_str):
        start = self.pos
        self.pos += struct.calcsize(format_str)
        return struct.unpack(format_str, self.data_file[start:self.pos])

    def ReadByte(self):
        return self._Unpack('B')[0]

    def ReadUShort(self):
        return self._Unpack('>H')[0]

    def ReadUInt(self):
        return self._Unpack('>I')[0]

    def ReadBytes(self, num_bytes):
        start = self.pos
        self.pos += num_bytes
        return self.data_file[start:self.pos]

class ApnxFile(BinaryFile):

    MAX_IN_MEMORY_POSITION = 2147483647

    def __init__(self, filename):
        """Load and parse the bytes in filename."""
        BinaryFile.__init__(self, filename)
        self._metadataRead = False

    def _ReadHeader(self):
        if self._metadataRead:
            return;
        self.pos = 0
        self._header_version = self.ReadUShort()
        if self._header_version != 1:
            raise ApnxException(
                    'Unsupported page numbering file format version %s',
                    self._header_version);
        self._num_editions = self.ReadUShort()
        self._edition_offset = []
        for i in xrange(0, self._num_editions):
            self._edition_offset.append(self.ReadUInt())
        metadata_len = self.ReadUInt()
        self._header_metadata = self.ReadBytes(metadata_len)
        self._edition_data_offset = [0] * self._num_editions
        self._edition_pagination_format = [0] * self._num_editions
        self._edition_page_count = [0] * self._num_editions
        self._edition_position_width = [0] * self._num_editions
        self._edition_positions = [None] * self._num_editions
        self._edition_json = [''] * self._num_editions
        self._edition_read = [False] * self._num_editions
        self._metadataRead = True;

    def _ReadEditionFormatVersion(self, edition_idx):
        self._ReadHeader();
        self._CheckEditionIndex(edition_idx)
        if self._edition_read[edition_idx]:
            return;
        self.pos = self._edition_offset[edition_idx]
        self._edition_pagination_format[edition_idx] = self.ReadUShort()

    def _ReadEdition(self, edition_idx):
        if self._edition_read[edition_idx]:
            return;
        self._ReadEditionFormatVersion(edition_idx)
        if self._edition_pagination_format[edition_idx] != 1:
            raise ApnxException('Unsupported edition pagination format %d for '
                                'edition %d' %
                                (self._edition_pagination_format[edition_idx],
                                 edition_idx))

        json_length = self.ReadUShort()
        page_count = self.ReadUShort()
        position_width = self.ReadUShort()
        if position_width > 32:
            raise ApnxException('Unsupported pagination format position width '
                                '%d for edition %d' %
                                (position_width, edition_idx))

        json_txt = ''
        if json_length:
            json_txt = self.ReadBytes(json_length)
        self._edition_data_offset[edition_idx] = self.pos
        self._edition_page_count[edition_idx] = page_count
        self._edition_position_width[edition_idx] = position_width
        self._edition_json[edition_idx] = json_txt
        self._edition_read[edition_idx] = True

    def _ReadEditionPositions(self, edition_idx):
        if self._edition_positions[edition_idx]:
            return
        self._ReadEdition(edition_idx)
        pages = self._edition_page_count[edition_idx]
        pos_width = self._edition_position_width[edition_idx] / 8
        positions = {}
        self.pos = self._edition_data_offset[edition_idx]
        for page in xrange(0, pages):
            page_pos = self._ReadPosition(pos_width)
            self._CheckPagePosition(page_pos)
            positions[page] = page_pos
        self._edition_positions[edition_idx] = positions

    def _ReadPosition(self, pos_width):
        rv = 0
        byte_pos = 8 * pos_width
        for i in xrange(0, pos_width):
            byte_pos -= 8;
            rv += (self.ReadByte() << byte_pos);
        return rv;

    def _CheckEditionIndex(self, edition_idx):
        if edition_idx >= self._num_editions or edition_idx <= -1:
            raise ApnxException(
                    'The requested page numbering edition at index %d is out '
                    'of bounds for the available count of editions %d' %
                    (edition_idx, self._num_editions))
            
    def _CheckPagePosition(self, position):
        if position > self.MAX_IN_MEMORY_POSITION:
            raise ApnxException('Unsupported pagination position %d '
                                'exceeding the maximum in-memory position %d' %
                                (position, self.MAX_IN_MEMORY_POSITION))

    def GetEditionPaginationFormat(self, edition_idx):
        self._ReadEditionFormatVersion(edition_idx)
        return self._edition_pagination_format[edition_idx]

    def GetEditionPageCount(self, edition_idx):
        self._ReadEdition(edition_idx)
        return self._edition_page_count[edition_idx]

    def GetEditionJSON(self, edition_idx):
        self._ReadEdition(edition_idx)
        return self._edition_json[edition_idx]

    def GetPagePositions(self, edition_idx):
        self._ReadEditionPositions(edition_idx)
        return self._edition_positions[edition_idx]

    @property
    def header_version(self):
        self._ReadHeader()
        return self._header_version

    @property
    def header_metadata(self):
        self._ReadHeader()
        return self._header_metadata

    @property
    def num_editions(self):
        self._ReadHeader()
        return self._num_editions

def main():
    if len(sys.argv) < 2:
        logging.fatal('You must specify a book to parse!')
        sys.exit(1)

    sidecar = ApnxFile(sys.argv[1])
    
    print '%s\n----------' % sys.argv[1]
    print 'Header Version: %d' % sidecar.header_version
    print 'Header Metadata: %s' % sidecar.header_metadata
    print 'Number of Editions: %s' % sidecar.num_editions
    for edition_idx in xrange(0, sidecar.num_editions):
        print '----- Edition %d -----' % edition_idx
        print ('Edition File format verson: %d' %
               sidecar.GetEditionPaginationFormat(edition_idx))
        print 'Page Count: %d' % sidecar.GetEditionPageCount(edition_idx)
        try:
            json_obj = json.loads(sidecar.GetEditionJSON(edition_idx))
        except ValueError:
            json_obj = {}
        print 'Page Map: %s' % json_obj.get('pageMap',
                                            '!!MISSING!! Broken metadata!')
        page_positions = sidecar.GetPagePositions(edition_idx)
        for page in xrange(0, sidecar.GetEditionPageCount(edition_idx)):
            print 'ordinal page: %d, position %d, page label: "%s"' % (
                page, page_positions[page], '')


if __name__ == '__main__':
    main()        
