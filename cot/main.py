import config
import datetime
import PyPDF2
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.pagesizes import A0, landscape
import io
from discord_webhook import DiscordWebhook
import locale

locale.setlocale(locale.LC_ALL, 'fr_FR')

if __name__ == "__main__":

    webhook = DiscordWebhook(url=config.discordWeebhookUrl)

    today = datetime.datetime.now().strftime('%d %B %Y')

    pdfmetrics.registerFont(TTFont('Poppins', 'C:/Users/alexl/Documents/GitHub/CodeReaderCOT/agri/themes/Poppins.ttf'))    
        
    existing_pdf = PyPDF2.PdfFileReader(open('base.pdf', 'rb'))
    output_pdf = PyPDF2.PdfFileWriter()
    
    def first_page_date():
        packet = io.BytesIO()
        c = canvas.Canvas(packet)
        c.setFillColorRGB(0.1796875, 0.1796875, 0.1796875)
        c.setFont("Poppins", 25)
        c.drawString(65, 50, today)
        c.save()
        return packet
    
    def page_date():
        packet = io.BytesIO()
        c = canvas.Canvas(packet)
        c.setFillColorRGB(0.1796875, 0.1796875, 0.1796875)
        c.setFont("Poppins", 17)
        c.drawString(65, 50, today)
        c.save()
        return packet
    
    
    def write_img(img_string):
        packet = io.BytesIO()
        c = canvas.Canvas(packet, pagesize=landscape(A0))
        c.setFillColorRGB(0.1796875, 0.1796875, 0.1796875)
        c.setFont("Poppins", 17)
        c.drawString(65, 50, today)
        c.drawImage(f'img/{img_string}', 60, 85)#, width=980, height=210) 300
        c.save()
        return packet      
    
    page_actions = {
        0: first_page_date,
        1: page_date,
        2: lambda: write_img('EBMFutures.png'),
        3: lambda: write_img("BlétendrePhysique.png"),
        4: lambda: write_img('netMillingWheatBleEuronext.png'),
        5: lambda: write_img("seasonaliteMillingWheatBleEuronext.png"),
        6: lambda: write_img('variationMillingWheatBleEuronext.png'),

        7: lambda: write_img("EMAFutures.png"),
        8: lambda: write_img("MaisPhysique.png"),
        9: lambda: write_img("netCornMaisEuronext.png"),
        10: lambda: write_img("seasonaliteCornMaisEuronext.png"),
        11: lambda: write_img('variationCornMaisEuronext.png'),

        12: lambda: write_img("ECOFutures.png"),
        13: lambda: write_img('ColzaPhysique.png'),
        14: lambda: write_img('netRapeseedColzaEuronext.png'),
        15: lambda: write_img('seasonaliteRapeseedColzaEuronext.png'),
        16: lambda: write_img("variationRapeseedColzaEuronext.png"),

        17: page_date,
        18: lambda: write_img('ZCFutures.png'),
        19: lambda: write_img("netCORN-CHICAGOBOARDOFTRADEcbot.png"),
        20: lambda: write_img('seasonaliteCORN-CHICAGOBOARDOFTRADEcbot.png'),
        21: lambda: write_img('variationCORN-CHICAGOBOARDOFTRADEcbot.png'),

        22: lambda: write_img('ZSFutures.png'),
        23: lambda: write_img('netSOYBEANS-CHICAGOBOARDOFTRADEcbot.png'),
        24: lambda: write_img('seasonaliteSOYBEANS-CHICAGOBOARDOFTRADEcbot.png'),
        25: lambda: write_img('variationSOYBEANS-CHICAGOBOARDOFTRADEcbot.png'),

        26: lambda: write_img('ZWFutures.png'),
        27: lambda: write_img('netWHEAT-SRW-CHICAGOBOARDOFTRADEcbot.png'),
        28: lambda: write_img('seasonaliteWHEAT-SRW-CHICAGOBOARDOFTRADEcbot.png'),
        29: lambda: write_img('variationWHEAT-SRW-CHICAGOBOARDOFTRADEcbot.png'),

        30: lambda: write_img('ZLFutures.png'),
        31: lambda: write_img('netSOYBEANOIL-CHICAGOBOARDOFTRADEcbot.png'),
        32: lambda: write_img('seasonaliteSOYBEANOIL-CHICAGOBOARDOFTRADEcbot.png'),
        33: lambda: write_img('variationSOYBEANOIL-CHICAGOBOARDOFTRADEcbot.png'),

        34: lambda: write_img('ZMFutures.png'),
        35: lambda: write_img('netSOYBEANMEAL-CHICAGOBOARDOFTRADEcbot.png'),
        36: lambda: write_img('seasonaliteSOYBEANMEAL-CHICAGOBOARDOFTRADEcbot.png'),
        37: lambda: write_img('variationSOYBEANMEAL-CHICAGOBOARDOFTRADEcbot.png'),
    }
    
    for page_num in range(existing_pdf.getNumPages()):
        
        if page_num in page_actions:
            action = page_actions[page_num]
            new_pdf = PyPDF2.PdfFileReader(action())
    
        pdf_page = existing_pdf.getPage(page_num)
        pdf_page.merge_page(new_pdf.pages[0])
        
        
        output_pdf.addPage(pdf_page)

    with open('COT_Report_Agri.pdf', 'wb') as output_file:
        output_pdf.write(output_file)
    
    with open("COT_Report_Agri.pdf", "rb") as f:
        webhook.add_file(file=f.read(), filename="COT_Report_Agri.pdf")
    response = webhook.execute()
    
    