from _0_Dependencies import *

def extract_text_from_pdf_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  

        pdf_bytes = response.content
        pdf_file = BytesIO(pdf_bytes)
        reader = PdfReader(pdf_file)

        
        full_text = ""
        for page in reader.pages:
            full_text += page.extract_text() or ""

        if len(full_text.strip()) < 50:
            images = convert_from_bytes(pdf_bytes)
            orc_text = ""
            for image in images:
                orc_text += pytesseract.image_to_string(image, lang="vie+eng")
            full_text += "\n" + orc_text
        return full_text.strip()
    except Exception as e:
        return f"Đã xảy ra lỗi: {e}"
