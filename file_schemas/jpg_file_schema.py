import os
import sys
from PIL import Image, ExifTags
from PIL.ExifTags import TAGS

#gets metadata from jpg file
def jpg_metadata(response):
    
    image = Image.open(response + '.jpg')
    """info = image._getexif()
    data = {TAGS.get(tag): value for tag, value in info.items()}
    
    #get exif tag metadata and return them if not empty
    #exif = { ExifTags.TAGS[k]: v for k, v in image._getexif().items() if k in ExifTags.TAGS }
    if data != None and len(data) != 0:
        return data"""
    
    #otherwise grab metadata manually
    metadata = {}
    metadata["filename"] = image.filename
    metadata["filetype"] = image.format
    metadata["pixelformat"] = image.mode
    metadata["imagewidth"] = image.size[0]
    metadata["imageheight"] = image.size[1]

    return metadata

def jpg_index_form():
    data = {
            "name": "File Metadata DEMO",
            "description": "Store metadata of jpg files that are in Data Ocean",
            "type":"a.standard",
            "tables": {
                "jpg_file":{
                    "contains_pii": False,
                    "properties": {
                        "filename": {
                            "type":"string"
                        },
                        "filetype": {
                            "type":"string"
                        },
                        "pixelformat": {
                            "type":"string"
                        },
                        "imagewidth": {
                            "type":"int"
                        },
                        "imageheight": {
                            "type":"int"
                        }
                    }
                }
            }
        }
        
    return data
