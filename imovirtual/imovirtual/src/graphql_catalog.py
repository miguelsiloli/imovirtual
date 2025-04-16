import json
from time import sleep

import pandas as pd
import requests

# Define the endpoint URL
url = "https://www.imovirtual.com/api/query"

query = """
query locationTree($locationId: String!) {
  locationTree(locationId: $locationId) {
    __typename
    ... on FoundLocationTree {
      locationTreeObjects {
        id
        name
        detailedLevel
        sublocations {
          id
          detailedLevel
          name
          parentIds
          __typename
        }
        __typename
      }
      __typename
    }
  }
}
"""

# Define the variables for the query
variables = {"locationId": "braga"}

# Define the headers
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Accept": "multipart/mixed, application/graphql-response+json, application/graphql+json, application/json",
    "Accept-Language": "pt-PT,pt;q=0.8,en;q=0.5,en-US;q=0.3",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Referer": "https://www.imovirtual.com/pt/resultados/arrendar/apartamento/muitas-localizacoes?limit=36&locations=%5Bporto%2Caveiro%2Cbeja%5D&by=DEFAULT&direction=DESC&viewType=listing&page=2",
    "Content-Type": "application/json",
    "re-fp-session": "a086728d-1055-4473-84bc-890ed0af0de8",
    "sentry-trace": "cc8c6f78b99b4e959c670ca3a2b379bd-8a5b921953f6f1c5-0",
    "baggage": "sentry-environment=imovirtualpt2-prd,sentry-release=frontend-platform%40v20240603T121501-imovirtualpt2,sentry-public_key=feffe528c390ea66992a4a05131c3c68,sentry-trace_id=cc8c6f78b99b4e959c670ca3a2b379bd,sentry-transaction=%2Fpt%2Fresultados%2F%5B%5B...searchingCriteria%5D%5D,sentry-sampled=false",
    "Origin": "https://www.imovirtual.com",
    "Alt-Used": "www.imovirtual.com",
    "Connection": "keep-alive",
    "Cookie": "laquesisff=gre-12226#rer-11#rer-181#rer-182; lqstatus=1717452719|18fdfd2988ex3c7a44be|eure-19720#eure-26485||; onap=18f62a6b44ex5249a1ff-5-18fdfd2988ex3c7a44be-197-1717454052; OptanonConsent=isGpcEnabled=0&datestamp=Mon+Jun+03+2024+22%3A45%3A38+GMT%2B0100+(Hora+de+ver%C3%A3o+da+Europa+Ocidental)&version=202401.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=bcae014e-1369-48e9-b7c3-04ec2adfb149&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2Cgad%3A1&geolocation=%3B&AwaitingReconsent=false; OptanonAlertBoxClosed=2024-05-10T13:17:30.376Z; eupubconsent-v2=CP-ZVZgP-ZVZgAcABBENAzE8AP_gAAAAAAYgJ9NV_H_fbX1j8Xp0aft0eY1f99j7rsQxBhfJk-4FyLvW_JwX32EzNA16pqYKmRIEu3bBIQFlHIDUDUCgaogVrTDMakWMgTNKJ6BEiFMRe2dYCF5vmQFD-QKY5tpvd3d52Te9_dv83dzyz4Vnn3Kp_-e1WJCdA5cgAAAAAAAAAAAAAAAQAAAAAAAAAQAIAAAAAAAAAAAAAAAAAAAAA_cBf78AAABgSCCAAgABcAFAAVAA4AB4AEEALwA1AB4AEQAJgAVQAzABvAD0AH4AQkAhgCJAEcAJYATQAwABhwDKAMsAbIA54B3AHfAPYA-IB9gH7AP8BAACKQEXARgAjUBIgElgJ-AoMBUAFXALmAXoAxQBogDaAG4AOJAj0CRAE7AKHAUeApEBbAC5AF3gLzAYMAw2BkYGSAMnAZmAzmBq4GsgNvAbmA3UBwQDkwHLgTcCAFwAHAAkACOAQcAjgBNAC-gJWATaApABXICwgFiALcAXkAxABiwDIQGjANTAbQA24Bug4BWAAiABwAHgAXABIAD8AI4AUAA0ACOAHIAQCAg4CEAERAI4ATQAqAB0gErAJiATKAm0BScCuQK7AWIAtQBbgC6AGCAMQAYsAyEBkwDRgGpgNeAbQA2wBt0DcwN0AceA5aBzoHPgTbHQTgAFwAUABUADgAIIAXABqADwAIgATAAqwBcAF0AMQAZgA3gB6AD9AIYAiQBLACaAFGAMAAYYAygBogDZAHPAO4A7wB7QD7AP0Af8BFAEYgI6AksBPwFBgKiAq4BYgC5wF5AXoAxQBtADcAHEAOoAfYBF8CPQJEATIAnYBQ8CjwKQAU0AqwBYsC2ALZAW6AuABcgC7QF3gLzAX0AwYBhoDHoGRgZIAycBlQDLAGZgM5AabA1cDWAG3gN1AcWA5MBy4E3AJvAThIAFgAEAAPADQAOQAjgBYgC-gJtAUmArkBYgC8gGCAM8AaMA1MBtgDbgG6AOWAc-BNshAhAAWABQAFwANQAmABVAC4AGIAN4AegBHADAAHPAO4A7wB_gEUAJSAUGAqICrgFzAMUAbQA6gCPQFNAKsAWKAtEBcAC5AGRgMnAZySgSgAIAAWABQADgAMAAeABEACYAFUALgAYoBDAESAI4AUYAwABsgDvAH5AVEBVwC5gGKAOoAiYBF8CPQJEATsAocBSACmgFWALFAWwAuABcgC7QF5gL6AYbAyMDJAGTgMsAZzA1gDWQG3gN1AcEA5MCbxaAUADUARwAwAB3AF6APsApoBVgDMwJuFgBQAywCOAI9ATEAm0BXIDRgGpgN0Acs.f_wAAAAAAAAA; laquesis=eure-19720@a#eure-26485@a; laquesissu=315@my_account|0#315@my_ads_active|1#315@my_messages_received|1#315@my_payments|1; lang=pt; dfp_user_id=0c1b8763-9934-4129-905a-524e8587d9d9; PHPSESSID=ism4mrhhpi137ghd5u9pepr2qf; mobile_default=desktop; ninja_user_status=unlogged",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Priority": "u=1",
    "TE": "trailers",
}

# Define the endpoint URL
url = "https://www.imovirtual.com/api/query"


# Send the request
def make_request(url, query, variables, headers):
    response = requests.post(
        url,
        json={"query": query, "operationName": "locationTree", "variables": variables},
        headers=headers,
    )
    return response


def flatten_df(data):
    # Extracting the data and flattening the structure
    flattened_data = []

    for location in data["data"]["locationTree"]["locationTreeObjects"]:
        district, municipality = location["id"].split("/")
        flattened_data.append(
            {
                "district": district,
                "municipality": municipality,
                "id": location["id"],
                "name": location["name"],
                "detailedLevel": location["detailedLevel"],
                "parentIds": location.get("parentIds", []),
            }
        )
        for sublocation in location.get("sublocations", []):
            flattened_data.append(
                {
                    "district": district,
                    "municipality": municipality,
                    "id": sublocation["id"],
                    "name": sublocation["name"],
                    "detailedLevel": sublocation["detailedLevel"],
                    "parentIds": sublocation.get("parentIds", []),
                }
            )

    # Creating the DataFrame
    df = pd.DataFrame(flattened_data)
    return df


districts = [
    "aveiro",
    "beja",
    "braga",
    "braganca",
    "castelo-branco",
    "coimbra",
    "evora",
    "faro",
    "guarda",
    "leiria",
    "lisboa",
    "porto",
    "portalegre",
    "santarem",
    "setubal",
    "viana-do-castelo",
    "vila-real",
    "viseu",
]

data = []

for district in districts:
    variables = {"locationId": district}

    response = make_request(url, query, variables, headers)
    flat = flatten_df(response.json())
    flat["district"] = district
    data.append(flat)
    sleep(12)
    print(flat)

data = pd.concat(data, ignore_index=True, axis=0)

data.to_csv("imovirtual_catalog.csv", index=False)


"""

district = read csv (imovirtual_catalog.csv)[id]

var districts = list [district]
LOOP districts[i]
    url = https://www.imovirtual.com/_next/data/V6HIl7KVPvVLVnSFYx-xa/pt/resultados/arrendar/apartamento/districts[i].json
    get(url)
    data = response[data]
    num_pages = response[tracking][listing][page_count]
    LOOP num_pages[i]:
        get(url + page...)

{
  "id": "integer",
  "title": "string",
  "slug": "string",
  "estate": "string",
  "developmentId": "integer",
  "developmentTitle": "string",
  "developmentUrl": "string",
  "transaction": "string",
  "location_radius": "integer",
  "city_name": "string",
  "province_name": "string",
  "images": "array",
  "isExclusiveOffer": "boolean",
  "isPrivateOwner": "boolean",
  "isPromoted": "boolean",
  "agency": "string",
  "openDays": "string",
  "totalPrice_value": "integer",
  "totalPrice_currency": "string",
  "rentPrice": "string",
  "priceFromPerSquareMeter": "string",
  "pricePerSquareMeter_value": "integer",
  "pricePerSquareMeter_currency": "string",
  "areaInSquareMeters": "integer",
  "terrainAreaInSquareMeters": "string",
  "roomsNumber": "string",
  "hidePrice": "boolean",
  "floorNumber": "string",
  "investmentState": "string",
  "investmentUnitsAreaInSquareMeters": "string",
  "peoplePerRoom": "string",
  "dateCreated": "string",
  "dateCreatedFirst": "string",
  "investmentUnitsNumber": "string",
  "investmentUnitsRoomsNumber": "string",
  "investmentEstimatedDelivery": "string",
  "pushedUpAt": "string",
  "specialOffer": "string",
  "shortDescription": "string",
  "totalPossibleImages": "integer"
}


"""
