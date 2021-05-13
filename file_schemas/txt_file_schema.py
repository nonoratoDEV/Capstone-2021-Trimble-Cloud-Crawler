#gets txt file metadata#
def txt_metadata():
    txt_file = open("practice_text.txt", 'r')

    #grabs metadata manually through reading through file
    content = txt_file.read()
    seperated_content = content.strip('\n').replace('\n', ' ')
    metadata = {}
    metadata["filename"] = txt_file.name
    metadata["filetype"] = "TXT"
    metadata["content"] = seperated_content
    metadata["wordcount"] = len(seperated_content.split())

    txt_file.close()
    return metadata
