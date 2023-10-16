from urllib.parse import urlparse


def extract_domain(url):
    # print("Original URL:", url)

    # Supprime les préfixes http et https, et les www
    url = url.replace("http://", "").replace("https://", "").replace("www.", "")
    # print("After removing prefixes:", url)

    # Ajoute http:// s'il n'y a pas de schéma dans l'URL
    if not url.startswith("http"):
        url = "http://" + url
    
    # Utilise urlparse pour extraire le nom de domaine
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    # print("Extracted domain:", domain)

    return domain