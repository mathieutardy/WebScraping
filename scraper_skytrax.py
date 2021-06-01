import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from tqdm import tqdm

def scrape_skytrax(csv_file="data/skytrax_scrapped.csv"):

    airlines = np.array(
    [
        "adria-airways",
        "aegean-airlines",
        "aer-lingus",
        "aerocaribbean",
        "aeroflot-russian-airlines",
        "aerolineas-argentinas",
        "aeromexico",
        "aerosur",
        "afriqiyah-airways",
        "aigle-azur",
        "air-algerie",
        "air-arabia",
        "air-astana",
        "air-austral",
        "air-bagan",
        "air-berlin",
        "air-botswana",
        "air-busan",
        "air-cairo-user",
        "air-canada",
        "air-canada-rouge",
        "air-caraibes",
        "air-china",
        "air-corsica",
        "air-dolomiti",
        "air-europa",
        "air-france",
        "air-greenland",
        "air-india",
        "air-india-express",
        "air-koryo",
        "air-labrador",
        "air-macau",
        "air-madagascar",
        "air-malawi",
        "air-malta",
        "air-mauritius",
        "air-mediterranee",
        "air-memphis",
        "air-moldova",
        "air-namibia",
        "air-new-zealand",
        "air-niugini",
        "air-north-yukons-airline",
        "air-nostrum",
        "air-serbia",
        "air-seychelles",
        "air-tahiti-nui",
        "air-transat",
        "air-vanuata",
        "air-zimbabwe",
        "airasia",
        "airasia-x",
        "airasia-zest",
        "airbaltic",
        "air-blue",
        "aircalin",
        "airnorth",
        "alaska-airlines",
        "alitalia",
        "allegiant-air",
        "american-airlines",
        "american-eagle",
        "ana-all-nippon-airways",
        "anadolujet",
        "arik-air",
        "arkefly",
        "arkia-israeli",
        "asiana-airlines",
        "asky-airlines",
        "atlantic-airways",
        "atlasjet-airlines",
        "aurigny-air",
        "austrian-airlines",
        "avianca",
        "avianca-brasil",
        "aerogal-aerolineas-galapagos",
        "avior-airlines",
        "azerbaijan-airlines",
        "azul-linhas-aereas-brasileiras",
        "ba-cityflyer",
        "bahamasair",
        "bangkok-airways",
        "beijing-capital-airlines",
        "belavia",
        "berjaya-air",
        "bh-airlines",
        "bhutan-airlines",
        "biman-bangladesh",
        "binter-canarias",
        "bluexpress",
        "blue-air",
        "blue-islands",
        "blue-panorama-airlines",
        "blue1",
        "bmi-british-midland-international",
        "bmi-regional",
        "boliviana-de-aviacin",
        "british-airways",
        "brussels-airlines",
        "buddha-air",
        "bulgaria-air",
        "buraq",
        "cambodia-angkor-airlines",
        "canadian-north",
        "canjet-airlines",
        "cape-air",
        "caribbean-airlines",
        "carpatair",
        "cathay-pacific-airways",
        "cayman-airways",
        "cebu-pacific",
        "central-mountain-air",
        "china-airlines",
        "china-eastern-airlines",
        "china-southern-airlines",
        "china-united-airlines",
        "cityjet",
        "comair",
        "condor-airlines",
        "continental-airlines",
        "conviasa",
        "copa-airlines",
        "corendon-airlines",
        "corsair",
        "croatia-airlines",
        "csa-czech-airlines",
        "cubana-airlines",
        "danish-air",
        "darwin",
        "delta-air-lines",
        "dniproavia",
        "dragonair",
        "druk-air",
        "eastarjet",
        "eastern-airways",
        "easyjet",
        "edelweiss-air",
        "egyptair",
        "el-al-israel-airlines",
        "emirates",
        "era-aviation",
        "eritrean-airlines",
        "estonian-air",
        "ethiopian-airlines",
        "etihad-airways",
        "europe-airpost",
        "eurowings",
        "eva-air",
        "far-eastern",
        "felix-airways",
        "fiji-airways",
        "finnair",
        "firefly",
        "fly540-com",
        "flybe",
        "flydubai",
        "flynas",
        "free-bird",
        "frontier-airlines",
        "garuda-indonesia",
        "georgian-airways",
        "germanwings",
        "goair",
        "gol",
        "gulf-air",
        "hainan-airlines",
        "hawaiian-airlines",
        "helvetic",
        "henan-airlines",
        "hong-kong",
        "hong-kong-airlines",
        "hop",
        "horizon-air",
        "iberia",
        "icelandair",
        "indigo-airlines",
        "insel-air",
        "interjet",
        "intersky",
        "iran-air",
        "iran-aseman",
        "island",
        "israir-airlines",
        "japan-airlines",
        "jazeera-airways",
        "jazz",
        "jeju-air",
        "jet-airways",
        "jet2-com",
        "jetairfly",
        "jetblue-airways",
        "jetstar-airways",
        "jetstar-asia",
        "jetstar-pacific",
        "jin-air",
        "juneyao-airlines",
        "kam-air",
        "karthago-airlines",
        "kenya-airways",
        "kish-air",
        "klm-royal-dutch-airlines",
        "korean-air",
        "kulula",
        "kuwait-airways",
        "la-compagnie",
        "lam-mozambique-airlines",
        "lan-airlines",
        "lan-colombia",
        "lan-peru",
        "lao-airlines",
        "liat",
        "libyan-airlines",
        "lion-air",
        "loch-lomond-seaplanes",
        "loganair",
        "lot-polish-airlines",
        "lucky-air",
        "lufthansa",
        "luxair",
        "mahan-air",
        "malaysia-airlines",
        "malindo-air",
        "malm-aviation",
        "mango",
        "martinair",
        "meridiana",
        "miat-mongolian",
        "mea-middle-east-airlines",
        "mokulele-airlines",
        "moldavian-airlines",
        "monarch-airlines",
        "montenegro-airlines",
        "myanmar-airways",
        "nepal-airlines",
        "nextjet",
        "niki",
        "nok-air",
        "nordavia",
        "norwegian",
        "nouvelair",
        "novair",
        "okay-airways",
        "olympic-air",
        "oman-air",
        "onur-air",
        "openskies",
        "orient-thai",
        "pia-pakistan-international-airlines",
        "pal-express",
        "passaredo-linhas-aereas",
        "peach-aviation",
        "pegasus-airlines",
        "peruvian-airlines",
        "petroleum-air-services",
        "philippine-airlines",
        "phuket-air",
        "porter-airlines",
        "precision",
        "qantas-airways",
        "qantaslink",
        "qatar-airways",
        "regent-airways",
        "regional-express",
        "rossiya-airlines",
        "royal-air-maroc",
        "royal-brunei-airlines",
        "royal-jordanian-airlines",
        "rwandair",
        "ryanair",
        "s7-siberia-airlines",
        "sa-express",
        "safi-airways",
        "santa-barbara",
        "sas-scandinavian-airlines",
        "sata-air-azores",
        "sata-internacional",
        "saudi-arabian-airlines",
        "scoot",
        "shaheen-air",
        "shandong-airlines",
        "shanghai-airlines",
        "shenzhen-airlines",
        "sichuan-airlines",
        "silkair",
        "singapore-airlines",
        "sky-airline",
        "sky-express-airlines",
        "skybus",
        "skymark-airlines",
        "skywest-airlines",
        "small-planet-airlines-uab",
        "smartwings",
        "sol-lineas-aereas",
        "solomon-airlines",
        "somon-air",
        "south-african-airways",
        "southwest-airlines",
        "spicejet",
        "spirit-airlines",
        "spring",
        "srilankan-airlines",
        "sriwijaya-air",
        "star-per",
        "starflyer",
        "sudan-airways",
        "sun-country-airlines",
        "sun-express",
        "sunwing-airlines",
        "surinam-airways",
        "swiss-international-air-lines",
        "syrianair",
        "taag-angola-airlines",
        "taca",
        "taca-regional",
        "tacv-cabo",
        "tajikistan-airlines",
        "tam-airlines",
        "tame",
        "tap-portugal",
        "tarom-romanian-airlines",
        "thai-airasia",
        "thai-airways",
        "thomas-cook-airlines",
        "thomas-cook-belgium-airlines-customer",
        "thomas-cook-airlines-scandinavia",
        "thomson-airways",
        "tianjin-airlines",
        "tigerair",
        "transaero-airlines",
        "transasia-airways",
        "transavia",
        "tropic-air-belize",
        "tuifly",
        "tunisair",
        "turkish-airlines",
        "turkmenistan-airlines",
        "ukraine-international-airlines",
        "united-airlines",
        "ural-airlines",
        "us-airways",
        "utair-aviation",
        "uzbekistan-airways",
        "vanilla-air",
        "vietjetair",
        "vietnam-asirlines",
        "virgin-america",
        "virgin-atlantic-airways",
        "virgin-australia",
        "vlm-airlines",
        "volaris",
        "volotea",
        "vueling-airlines",
        "westjet",
        "wideroe",
        "wizz-air",
        "wow-air",
        "xiamen-airlines",
        "xl-airways-france",
        "yakutia-airlines",
        "yangon-airways",
        "yemenia",
    ],
    dtype=object,
    )

    data = []
    for airline in tqdm(airlines):
        url = "https://www.airlinequality.com/airline-reviews/{}/".format(airline)
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
        try:
            number_page = int(
                np.ceil(
                    int(
                        soup.find("div", {"class": "pagination-total"})
                        .getText()
                        .split(" ")[-2]
                    )
                    / 100
                )
            )
        except AttributeError:
            pass
        for page_count in range(1, number_page + 1):
            url = "https://www.airlinequality.com/airline-reviews/{}/page/{}/?sortby=post_date%3ADesc&pagesize=100".format(
                airline, page_count
            )
            page = requests.get(url)
            soup = BeautifulSoup(page.content, "html.parser")
            full_review = soup.findAll("div", {"class": "body"})
            for element in full_review:
                try:
                    title = element.find("h2").getText()[1:-1]
                except AttributeError:
                    title = ""
                try:
                    review = element.find("div", class_="text_content").getText()
                    review = " ".join([str(elem) for elem in review])
                except AttributeError:
                    review = ""
                try:
                    type_of_traveller = (
                        element.find(
                            "td", class_="review-rating-header type_of_traveller "
                        )
                        .findNext()
                        .getText()
                    )
                except AttributeError:
                    type_of_traveller = ""
                try:
                    cabin = (
                        element.find("td", class_="review-rating-header cabin_flown ")
                        .findNext()
                        .getText()
                    )
                except AttributeError:
                    cabin = ""
                try:
                    route = (
                        element.find("td", class_="review-rating-header route ")
                        .findNext()
                        .getText()
                    )
                except AttributeError:
                    route = ""
                try:
                    date = (
                        element.find("td", class_="review-rating-header date_flown ")
                        .findNext()
                        .getText()
                    )
                except AttributeError:
                    date = ""
                try:
                    aircraft = (
                        element.find("td", class_="review-rating-header aircraft ")
                        .findNext()
                        .getText()
                    )
                except AttributeError:
                    aircraft = ""
                try:
                    recommendation = (
                        element.find("td", class_="review-rating-header recommended")
                        .findNext()
                        .getText()
                    )
                except AttributeError:
                    recommendation = ""

                # Extraction of Overall grades
                extracted_ratings = []
                final_ratings = []
                cat_seat_comfort = element.findAll(
                    attrs={"class": "review-rating-header seat_comfort"}
                )
                cat_cabin_staff_service = element.findAll(
                    attrs={"class": "review-rating-header cabin_staff_service"}
                )
                cat_food_and_beverages = element.findAll(
                    attrs={"class": "review-rating-header food_and_beverages"}
                )
                cat_inflight_entertainment = element.findAll(
                    attrs={"class": "review-rating-header inflight_entertainment"}
                )
                cat_ground_service = element.findAll(
                    attrs={"class": "review-rating-header ground_service"}
                )
                cat_wifi_and_connectivity = element.findAll(
                    attrs={"class": "review-rating-header wifi_and_connectivity"}
                )
                cat_value_for_money = element.findAll(
                    attrs={"class": "review-rating-header value_for_money"}
                )
                categories = [
                    len(cat_seat_comfort),
                    len(cat_cabin_staff_service),
                    len(cat_food_and_beverages),
                    len(cat_inflight_entertainment),
                    len(cat_ground_service),
                    len(cat_wifi_and_connectivity),
                    len(cat_value_for_money),
                ]
                ratings = element.findAll(attrs={"class": "review-rating-stars stars"})
                for rating in ratings:
                    extracted_rating = rating.find_all(attrs={"class": "star fill"})
                    extracted_ratings.append(len(extracted_rating))
                j = 0
                for i in range(len(categories)):
                    if categories[i] == 1:
                        final_ratings.append(extracted_ratings[j])
                        j += 1
                    else:
                        final_ratings.append("NaN")

                seat_comfort = final_ratings[0]
                cabin_staff_service = final_ratings[1]
                food_and_beverages = final_ratings[2]
                inflight_entertainment = final_ratings[3]
                ground_service = final_ratings[4]
                wifi_and_connectivity = final_ratings[5]
                value_for_money = final_ratings[6]

                data.append(
                    [
                        airline,
                        title,
                        review,
                        type_of_traveller,
                        cabin,
                        route,
                        date,
                        aircraft,
                        recommendation,
                        seat_comfort,
                        cabin_staff_service,
                        food_and_beverages,
                        inflight_entertainment,
                        ground_service,
                        wifi_and_connectivity,
                        value_for_money,
                    ]
                )

    df = pd.DataFrame(
        data,
        columns=[
            "Airline",
            "Title",
            "Review",
            "Traveller",
            "Cabin",
            "Route",
            "Date",
            "Aircraft",
            "Recommendation",
            "seat_comfort",
            "cabin_staff_service",
            "food_and_beverages",
            "inflight_entertainment",
            "ground_service",
            "wifi_and_connectivity",
            "value_for_money",
        ],
    )

    df.to_csv(csv_file, index=False)

    return df