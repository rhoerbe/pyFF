from .utils import xml_in_text, DirectoryListing, find_matching_files
from .samlmd import parse_saml_metadata
from .constants import config
from datetime import datetime
import os

try:
    from cStringIO import StringIO
except ImportError:  # pragma: no cover
    print(" *** install cStringIO for better performance")
    from StringIO import StringIO


class ParserException(Exception):
    def __init__(self, msg, wrapped=None, data=None):
        self._wraped = wrapped
        self._data = data
        super(self.__class__, self).__init__(msg)

    def raise_wraped(self):
        raise self._wraped


class ResourceParser(object):
    def __init__(self):
        pass

    def magic(self, content):
        return False

    def parse(self, resource, content, info):
        pass


class NoParser(ResourceParser):
    def magic(self, content):
        return True

    def parse(self, resource, content, info):
        raise ParserException("No matching parser found for %s" % resource.url)


class SAMLMetadataResourceParser(ResourceParser):
    def magic(self, content):
        return xml_in_text("EntitiesDescriptor", content) or xml_in_text("EntityDescriptor", content)

    def parse(self, resource, content, info):
        validation_errors = dict()
        info['Validation Errors'] = validation_errors
        t, expire_time_offset = parse_saml_metadata(StringIO(content),
                                                    key=resource.opts['verify'],
                                                    base_url=resource.url,
                                                    fail_on_error=resource.opts['fail_on_error'],
                                                    filter_invalid=resource.opts['filter_invalid'],
                                                    validate=resource.opts['validate'],
                                                    validation_errors=validation_errors)

        ttl = config.default_cache_duration
        resource.expired = False
        if expire_time_offset is not None:
            expire_time = datetime.now() + expire_time_offset
            ttl = expire_time_offset.total_seconds()
            info['Expiration Time'] = str(expire_time)
            info['Expiration TTL'] = str(ttl)
            resource.ttl = ttl
            if resource.ttl < 0:
                resource.expired = True

        resource.t = t
        resource.type = "application/samlmetadat+xml"


class DirectoryParser(ResourceParser):
    def magic(self, content):
        return os.path.isdir(content)

    def parse(self, resource, content, info):
        listing = find_matching_files(content, ".xml")
        for fn in listing.files:
            resource.add(resource.clone("file://"+fn))
        resource.reload()


parsers = [SAMLMetadataResourceParser(), NoParser()]