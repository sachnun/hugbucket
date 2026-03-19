"""Compatibility import for S3 XML response builders."""

from hugbucket.s3.xml_responses import (
    S3_XMLNS,
    complete_multipart_upload_xml,
    copy_object_result_xml,
    delete_result_xml,
    error_xml,
    get_bucket_location_xml,
    initiate_multipart_upload_xml,
    list_buckets_xml,
    list_objects_v2_xml,
    to_xml_bytes,
)

__all__ = [
    "S3_XMLNS",
    "list_buckets_xml",
    "list_objects_v2_xml",
    "error_xml",
    "get_bucket_location_xml",
    "delete_result_xml",
    "copy_object_result_xml",
    "initiate_multipart_upload_xml",
    "complete_multipart_upload_xml",
    "to_xml_bytes",
]
