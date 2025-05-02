import json
import requests
import time
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

@retry(
    stop=stop_after_attempt(3),  # Stop after 3 attempts
    wait=wait_fixed(60),  # Wait 60 seconds between retries
    retry=retry_if_exception_type((requests.exceptions.RequestException, json.JSONDecodeError)),  # Retry on these exceptions
    reraise=True  # Re-raise the last exception
)
def get_buildid():
    # Define the URL
    url = "https://www.imovirtual.com/pt"
    # Set headers
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "pt-PT,pt;q=0.8,en;q=0.5,en-US;q=0.3",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Connection": "keep-alive",
        "Cookie": "laquesisff=gre-12226#rer-11#rer-181#rer-182; lqstatus=1717508244|18fe31269cax21cec13d|eure-26485#eure-19720||; onap=18f62a6b44ex5249a1ff-6-18fe31269cax21cec13d-40-1717503998; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Jun+04+2024+12%3A56%3A38+GMT%2B0100+(Hora+de+ver%C3%A3o+da+Europa+Ocidental)&version=202401.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=bcae014e-1369-48e9-b7c3-04ec2adfb149&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2Cgad%3A1&geolocation=%3B&AwaitingReconsent=false; OptanonAlertBoxClosed=2024-05-10T13:17:30.376Z; eupubconsent-v2=CP-ZVZgP-ZVZgAcABBENAzE8AP_gAAAAAAYgJ9NV_H_fbX1j8Xp0aft0eY1f99j7rsQxBhfJk-4FyLvW_JwX32EzNA16pqYKmRIEu3bBIQFlHIDUDUCgaogVrTDMakWMgTNKJ6BEiFMRe2dYCF5vmQFD-QKY5tpvd3d52Te9_dv83dzyz4Vnn3Kp_-e1WJCdA5cgAAAAAAAAAAAAAAAQAAAAAAAAAQAIAAAAAAAAAAAAAAAAAAAAA_cBf78AAABgSCCAAgABcAFAAVAA4AB4AEEALwA1AB4AEQAJgAVQAzABvAD0AH4AQkAhgCJAEcAJYATQAwABhwDKAMsAbIA54B3AHfAPYA-IB9gH7AP8BAACKQEXARgAjUBIgElgJ-AoMBUAFXALmAXoAxQBogDaAG4AOJAj0CRAE7AKHAUeApEBbAC5AF3gLzAYMAw2BkYGSAMnAZmAzmBq4GsgNvAbmA3UBwQDkwHLgTcCAFwAHAAkACOAQcAjgBNAC-gJWATaApABXICwgFiALcAXkAxABiwDIQGjANTAbQA24Bug4BWAAiABwAHgAXABIAD8AI4AUAA0ACOAHIAQCAg4CEAERAI4ATQAqAB0gErAJiATKAm0BScCuQK7AWIAtQBbgC6AGCAMQAYsAyEBkwDRgGpgNeAbQA2wBt0DcwN0AceA5aBzoHPgTbHQTgAFwAUABUADgAIIAXABqADwAIgATAAqwBcAF0AMQAZgA3gB6AD9AIYAiQBLACaAFGAMAAYYAygBogDZAHPAO4A7wB7QD7AP0Af8BFAEYgI6AksBPwFBgKiAq4BYgC5wF5AXoAxQBtADcAHEAOoAfYBF8CPQJEATIAnYBQ8CjwKQAU0AqwBYsC2ALZAW6AuABcgC7QF3gLzAX0AwYBhoDHoGRgZIAycBlQDLAGZgM5AabA1cDWAG3gN1AcWA5MBy4E3AJvAThIAFgAEAAPADQAOQAjgBYgC-gJtAUmArkBYgC8gGCAM8AaMA1MBtgDbgG6AOWAc-BNshAhAAWABQAFwANQAmABVAC4AGIAN4AegBHADAAHPAO4A7wB_gEUAJSAUGAqICrgFzAMUAbQA6gCPQFNAKsAWKAtEBcAC5AGRgMnAZySgSgAIAAWABQADgAMAAeABEACYAFUALgAYoBDAESAI4AUYAwABsgDvAH5AVEBVwC5gGKAOoAiYBF8CPQJEAUeApoBYoC2AF5wMjAyQBk4DOQGsANvAm4BOEkASAAuAEcAdwBAACDgEcAKgAlYBMQCbQFJgLcAYsAywBngDdAHLATbKAIwAFAAXABIAC4AI4AWwBHADkAHcAPsAgABBwCxAF1ANeAdsA_4CYgE2gKkAV2AtwBdAC8gGCAMWAZMAzwBowDUwGvQNzA3QBywE2wJwlIHgAC4AKAAqABwAEEAMAA1AB4AEQAJgAVQAxABmAD9AIYAiQBRgDAAGUANEAbIA5wB3wD8AP0AiwBGICOgJKAUGAqICrgFzALyAYoA2gBuADqAHtAPsAiYBF8CPQJEATsAocBSACmgFWALFAWwAuABcgC7QF5gL6AYbAyMDJAGTgMsAZzA1gDWQG3gN1AcEA5MCbxaAUADUARwAwAB3AF6APsApoBVgDMwJuFgBQAywCOAI9ATEAm0BXIDRgGpgN0Acs.f_wAAAAAAAAA; laquesis=eure-19720@a#eure-26485@a; laquesissu=315@my_account|0#315@my_ads_active|1#315@my_messages_received|1#315@my_payments|1; lang=pt; dfp_user_id=0c1b8763-9934-4129-905a-524e8587d9d9; PHPSESSID=ism4mrhhpi137ghd5u9pepr2qf; mobile_default=desktop; ninja_user_status=unlogged",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }
    
    # Fetch the HTML content
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an exception for bad responses
    html_content = response.text
    
    # Parse the HTML content
    soup = BeautifulSoup(html_content, "html.parser")
    script_tag = soup.find("script", id="__NEXT_DATA__")
    
    # Check if the script tag was found
    if not script_tag:
        raise ValueError("Script tag with id '__NEXT_DATA__' not found")
    
    json_content = script_tag.string
    data = json.loads(json_content)
    build_id = data["buildId"]
    
    return build_id