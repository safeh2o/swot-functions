# obtained from https://gist.github.com/pansapiens/110431456e8a4ba4f2eb

#!/usr/bin/env python
# A simple script to suck up HTML, convert any images to inline Base64
# encoded format and write out the converted file.
#
# Usage: python standalone_html.py <input_file.html> <output_file.html>

import base64
import mimetypes
import os

from bs4 import BeautifulSoup


def guess_type(filepath: str) -> str:
    return mimetypes.guess_type(filepath)[0]


def file_to_base64(filepath):
    """
    Returns the content of a file as a Base64 encoded string.

    :param filepath: Path to the file.
    :type filepath: str
    :return: The file content, Base64 encoded.
    :rtype: str
    """

    with open(filepath, "rb") as f:
        encoded_str = base64.b64encode(f.read())
    return encoded_str.decode("utf-8")


def make_html_images_inline(in_filepath, out_filepath):
    """
    Takes an HTML file and writes a new version with inline Base64 encoded
    images.

    :param in_filepath: Input file path (HTML)
    :type in_filepath: str
    :param out_filepath: Output file path (HTML)
    :type out_filepath: str
    """
    basepath = os.path.split(in_filepath.rstrip(os.path.sep))[0]
    soup = BeautifulSoup(open(in_filepath, "r"), "html.parser")
    for img in soup.find_all("img"):
        img_path = os.path.join(basepath, img.attrs["src"])
        mimetype = guess_type(img_path)
        img.attrs["src"] = "data:%s;base64,%s" % (mimetype, file_to_base64(img_path))

    with open(out_filepath, "w") as of:
        of.write(str(soup))
