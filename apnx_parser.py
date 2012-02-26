#!/usr/bin/env python
# Kindle Page Number Sidecar file parsing routines
#
# This file is released under the GPLv2 license.
#     Copyright (C) 2012 Matt Brown <matt@mattb.net.nz>
#
from bisect import bisect_right
import json
import logging
import struct
import sys

logger = logging.getLogger().getChild('apnx_parser')


class ApnxException(Exception):
    pass


class RomanPageNumber(object):
    """Roman page number representation class."""

    def __init__(self, pageno):
        self._pageno = pageno

    def __str__(self):
        # TODO(mattb): Actually implement this!
        return 'Roman(%d)' % self._pageno


class PageLabelType(object):
    """Page label type class. Basically a set of consts with some helpers."""

    DESCRIPTIONS = {
            'r': 'Roman',
            'a': 'Arabic',
            'i': 'Integer',
            'c': 'Custom',
    }

    def __init__(self, label_type):
        self._type = label_type

    @property
    def description(self):
        return self.DESCRIPTIONS.get(self._type)

    @property
    def label_type(self):
        return self._type

    @classmethod
    def FromChar(self, char):
        if char == 'r':
            return PageLabelType.ROMAN
        elif char == 'a':
            return PageLabelType.ARABIC
        elif char == 'i':
            return PageLabelType.INTEGER
        elif char == 'c':
            return PageLabelType.CUSTOM

PageLabelType.ROMAN = PageLabelType('r')
PageLabelType.ARABIC = PageLabelType('a')
PageLabelType.INTEGER = PageLabelType('i')
PageLabelType.CUSTOM = PageLabelType('c')


class PageNumberScheme(object):
    """A page number scheme describes how to label a range of pages."""

    def __init__(self, scheme_def):
        parts = scheme_def.split(',')
        self._first_ordinal_page = int(parts[0])
        self._last_ordinal_page = self._first_ordinal_page
        self._label_type = PageLabelType.FromChar(parts[1][0])
        if not self._label_type:
            raise ApnxException('Unknown page numbering scheme: %s' % parts[1])
        self._first_page_label = 0
        self._custom_labels = None
        if self._label_type == PageLabelType.CUSTOM:
            self._custom_labels = parts[2].split('|')
            self._first_page_label = 1
        elif self._label_type != PageLabelType.INTEGER:
            self._first_page_label = int(parts[2])

    def SetLastOrdinalPage(self, last_page):
        if last_page < self._first_ordinal_page:
            raise ApnxException('Last ordinal page number must be >= first '
                                'ordinal page number! first=%d, last=%d' %
                                (self._first_ordinal_page, last_page))
        self._last_ordinal_page = last_page

    def GetLabelAtOffset(self, offset):
        offset += self._first_page_label
        if self._label_type == PageLabelType.ARABIC:
            return int(offset)
        elif self._label_type == PageLabelType.ROMAN:
            return str(RomanPageNumber(offset))
        elif self._label_type == PageLabelType.CUSTOM:
            offset -= 1
            if offset < len(self._custom_labels):
                return self._custom_labels[offset]
        return ''

    @property
    def first_ordinal_page(self):
        return self._first_ordinal_page

    @property
    def last_ordinal_page(self):
        return self._last_ordinal_page

    @property
    def first_page_label(self):
        return self._first_page_label

    @property
    def last_page_label(self):
        return self._first_page_label + (
                self._last_ordinal_page - self._first_ordinal_page)

    @property
    def label_type(self):
        return self._label_type

    @property
    def label_range(self):
        return self._first_page_label, self.last_page_label

    def __cmp__(self, other):
        return cmp(self.first_ordinal_page, other.first_ordinal_page)


class PageLabelIndex(object):
    """Maps from all pages in an edition to various PageNumberSchemes.

    An ebook edition may have several different page numberings used in
    different sections (e.g. the main text vs an index). Each edition has a
    number of pages, referred to as 'ordinal' pages. Counted from 1 to N (in
    zero-based arrays). Each ordinal page number may be associated with a page
    label, which represents the corresponding page in a physical book. This
    association between ordinal page and page label is provided by a
    PageNumberScheme.
    """

    def __init__(self, page_map, total_pages):
        if not page_map or not page_map.strip():
            raise ApnxException('Page map string cannot be blank')
        if total_pages < 0:
            raise ApnxException('Total pages cannot be less than 0: %s' %
                                total_pages)

        self._page_map = page_map
        self._total_pages = total_pages
        self._label_cache = {}
        self._schemes = []
        self._first_pages = []

        last_scheme = None
        pos = 0
        while pos < len(self._page_map):
            if self._page_map[pos] != '(':
                raise ApnxException('Page number scheme must start with '
                                    '\'(\'; was given: %s' %
                                    self._page_map[pos:])
            pos += 1
            try :
                endpos = self._page_map.index(')', pos)
            except ValueError:
                raise ApnxException('Page number scheme must end with \')\'; '
                                    'was given: %s' %
                                    self._page_map[pos:])
            scheme = PageNumberScheme(self._page_map[pos:endpos])
            if last_scheme:
                last_scheme.SetLastOrdinalPage(scheme.first_ordinal_page - 1)
            self._schemes.append(scheme)
            last_scheme = scheme
            pos = endpos + 1
            if pos < len(self._page_map) and self._page_map[pos] == ',':
                pos += 1
        last_scheme.SetLastOrdinalPage(total_pages)
        self._schemes.sort()
        for scheme in self._schemes:
            self._first_pages.append(scheme.first_ordinal_page)

    def GetSchemeForPage(self, pageno):
        if pageno < 1 or pageno > self._total_pages:
            return None
        i = bisect_right(self._first_pages, pageno)
        if not i:
            return None
        return self._schemes[i-1]

    def GetLabelForPage(self, pageno):
        pageno += 1
        if pageno in self._label_cache:
            return self._label_cache[pageno]

        scheme = self.GetSchemeForPage(pageno)
        if not scheme:
            return ""

        offset = pageno - scheme.first_ordinal_page
        label = scheme.GetLabelAtOffset(offset)
        self._label_cache[pageno] = label
        return label

    def _FindLastPageOfType(self, label_type):
        last = 0
        for scheme in self._schemes:
            if scheme.label_type != label_type:
                continue
            if scheme.last_ordinal_page > last:
                last = scheme.last_ordinal_page
        return last

    @property
    def arabic_only(self):
        for scheme in self._schemes:
            if scheme.label_type != PageLabelType.ARABIC:
                return False
        return True

    @property
    def largest_page_label(self):
        last = self._FindLastPageOfType(PageLabelType.ARABIC)
        if not last:
            last = self._FindLastPageOfType(PageLabelType.ROMAN)
        if last:
            return self.GetLabelForPage(last)
        return ''

    @property
    def schemes(self):
        return self._schemes

    @property
    def total_pages(self):
        return self._total_pages


class BinaryFile(object):
    """Helper to perform basic binary object reads from a file.

    Keeps track of the current position in the file, and increments
    appropriately after each read.
    """

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
    """Parser class for Kindle .apnx files (aka Kindle Page Label Sidecars).

    Each sidecar file contains details on a number of editions of the ebook,
    and provides the necessary data to create a PageLabelIndex for each
    edition.
    """

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

    logging.basicConfig()
    if sys.argv[1] == '-d':
        logger.setLevel(logging.DEBUG)
        sys.argv.remove('-d')

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
        page_label_idx = PageLabelIndex(
                json_obj.get('pageMap', ''),
                sidecar.GetEditionPageCount(edition_idx))
        print 'Arabic Only?: %s' % page_label_idx.arabic_only
        print 'Largest Page Label: %s' % page_label_idx.largest_page_label
        print 'Num Sequences: %d' % len(page_label_idx.schemes)
        for scheme in page_label_idx.schemes:
            print 'Sequence -- start: %s, end: %s' % scheme.label_range
            print ' %s' % scheme.label_type.description
            print '  first ordinal page: %d, first page label: %s' % (
                    scheme.first_ordinal_page, scheme.first_page_label)
            print '  last ordinal page: %d, last page label: %s' % (
                    scheme.last_ordinal_page, scheme.last_page_label)
        for page in xrange(0, sidecar.GetEditionPageCount(edition_idx)):
            print 'ordinal page: %d, position %d, page label: "%s"' % (
                page+1, page_positions[page],
                page_label_idx.GetLabelForPage(page))


if __name__ == '__main__':
    main()        
