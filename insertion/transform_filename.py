import base64

# Função para extrair video_id do nome do arquivo
def decode_base64(filename):
    base64_str = filename.split("/")[-1].replace(".xml", "")
    try:
        return base64.b64decode(base64_str).decode("utf-8")
    except:
        return None

