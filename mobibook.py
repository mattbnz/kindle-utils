#!/usr/bin/env python
# Kindle Mobibook metadata extraction class.
#
# Modified from mobidedrm v0.30.
#
import logging
import struct
import sys

CRYPTO_NONE = 0
CRYPTO_MOBIPOCKET = 1
CRYPTO_AMAZON = 2

COMPRESSION_NONE = 0
COMPRESSION_PALMDOC = 1
COMPRESSION_HUFFDIC = 17480

EXTH_MAP_STRINGS = { 
        100 : 'Creator',
        101 : 'Publisher',
        102 : 'Imprint',
        103 : 'Description',
        104 : 'ISBN',
        105 : 'Subject',
        106 : 'Published',
        107 : 'Review',
        108 : 'Contributor',
        109 : 'Rights',
        110 : 'SubjectCode',
        111 : 'Type',
        112 : 'Source',
        113 : 'ASIN',
        118 : 'Price',
        119 : 'Currency',
        503 : 'UpdatedTitle',
}
EXTH_RMAP_STRINGS = dict([(v.lower(), k) for k, v in EXTH_MAP_STRINGS.items()])
EXTH_MAP_VALUES = { 
        116 : 'StartOffset',
        201 : "CoverOffset",
}
EXTH_RMAP_VALUES = dict([(v.lower(), k) for k, v in EXTH_MAP_VALUES.items()])

logger = logging.getLogger().getChild('mobibook')


class MobiException(Exception):
    pass


class MobiBook(object):

    def __init__(self, infile):
        """Load and parse the bytes in infile, an opened file-like object."""
        # initial sanity check on file
        self.data_file = infile.read()
        self.header = self.data_file[0:78]
        self.magic = self.header[0x3C:0x3C+8]
        if self.magic != 'BOOKMOBI' and self.magic != 'TEXtREAd':
            raise MobiException("invalid file format")
        self.crypto_type = -1

        # build up section offset and flag info
        self.parseSections()

        # parse information from section 0
        self.record0 = self.loadSection(0)
        self.compression, = struct.unpack('>H', self.record0[0x0:0x0+2])
        self.txt_records, = struct.unpack('>H', self.record0[0x8:0x8+2])
        self.firstimg = self.txt_records + 1
        self.extra_data_flags = 0
        self.meta_array = {}
        self.mobi_length = 0
        self.mobi_version = -1

        if self.magic == 'TEXtREAd':
            return
        self.parseMobiHeader()

    def parseMobiHeader(self):
        self.mobi_length, = struct.unpack('>L',self.record0[0x14:0x18])
        self.mobi_version, = struct.unpack('>L',self.record0[0x68:0x6C])
        self.exth_off = self.mobi_length + 16  # EXTH block offset, if any.
        self.firstimg, = struct.unpack_from('>L',self.record0, 0x6C)
 
        # Extract the extra_data_flags, indicating if there is extra data
        # present at the end of each text record.
        if (self.mobi_length >= 0xE4) and (self.mobi_version >= 5):
           self.extra_data_flags, = struct.unpack('>H', self.record0[0xF2:0xF4])
        
        # Extract the DRM and Crypto information.
        self.crypto_type, = struct.unpack('>H', self.record0[0xC:0xC+2])
        self.drm_ptr, self.drm_count, self.drm_size, _ = struct.unpack('>LLLL',
                self.record0[0xA8:0xA8+16])

        # Try and parse any EXTH data that is present.
        exth_flag, = struct.unpack('>L', self.record0[0x80:0x84])
        if exth_flag & 0x40:
            if not self.processEXTH(self.storeEXTH):
                self.meta_array = {}

    def processEXTH(self, callback):
        try:
            exth = self.record0[self.exth_off:]
            if len(exth) < 4 or exth[:4] != 'EXTH':
                logger.warn('Could not find expected EXTH record!')
                return

            nitems, = struct.unpack('>I', exth[8:12])
            pos = 12
            for i in xrange(nitems):
                exth_type, size = struct.unpack('>II', exth[pos: pos + 8])
                content = exth[pos + 8: pos + size]
                callback(exth_type, pos, content)
                pos += size
        except:
            logger.error('Failed to parse EXTH record!')
            return False
        return True

    def storeEXTH(self, exth_type, pos, content):
        self.meta_array[exth_type] = content

    def parseSections(self):
        """Build a list of section description tuples for all sections."""
        self.num_sections, = struct.unpack('>H', self.header[76:78])
        self.sections = []
        for i in xrange(self.num_sections):
            offset, a1, a2, a3, a4 = struct.unpack('>LBBBB',
                    self.data_file[78+i*8:78+i*8+8])
            flags, uniqueID = a1, a2<<16|a3<<8|a4
            self.sections.append((offset, flags, uniqueID))

    def loadSection(self, section):
        """Returns raw bytes for the specified section."""
        if (section + 1 == self.num_sections):
            endoff = len(self.data_file)
        else:
            endoff = self.sections[section + 1][0]
        off = self.sections[section][0]
        return self.data_file[off:endoff]

    def __getattr__(self, name):
        if name in EXTH_RMAP_STRINGS:
            return self.meta_array.get(EXTH_RMAP_STRINGS[name], '')
        if name in EXTH_RMAP_VALUES:
            return self.returnEXTHValue(EXTH_RMAP_VALUES[name])
        logger.debug('No attribute named: %s', name)
        raise AttributeError

    @property
    def title(self):
        title = self.updatedtitle
        if not title:
            toff, tlen = struct.unpack('>II', self.record0[0x54:0x5c])
            title = self.record0[toff:toff+tlen]
        title = title.strip()
        if not title:
            title = self.header[:32].strip()
            title = title.split("\0")[0]
        return title

    def returnEXTHValue(self, exth_type):
        content = self.meta_array.get(exth_type, '')
        size = len(content)
        if size == 0:
            return ''
        elif size == 4:
            value, = struct.unpack('>I', content)
            return str(value)
        elif size == 9:
            value, = struct.unpack('B',content)
            return str(value)
        elif size == 10:
            value, = struct.unpack('>H',content)
            return str(value)
        elif size == 12:
            value, = struct.unpack('>L',content)
            return str(value)
        else:
            msg = 'EXTH value %s has unknown length %s: %s' % (
                    exth_type, size, content)
            logger.debug(msg)
            raise AttributeError(msg)


def main():
    if len(sys.argv) < 2:
        logging.fatal('You must specify a book to parse!')
        sys.exit(1)

    logging.basicConfig()
    if sys.argv[1] == '-d':
        logger.setLevel(logging.DEBUG)
        sys.argv.remove('-d')

    fp = open(sys.argv[1], 'r')
    book = MobiBook(fp)
    
    print '%s: %s' % ('File'.rjust(15), sys.argv[1])
    print '%s: %s' % ('Title'.rjust(15), book.title)
    for name in EXTH_RMAP_STRINGS:
        try:
            v = getattr(book, name)
        except AttributeError, e:
            v = 'ERROR: %s' % e
        if v:
            print '%s: %s' % (name.rjust(15), v)
    for name in EXTH_RMAP_VALUES:
        try:
            v = getattr(book, name)
        except AttributeError, e:
            v = 'ERROR: %s' % e
        if v:
            print '%s: %s' % (name.rjust(15), v)


if __name__ == '__main__':
    main()        
