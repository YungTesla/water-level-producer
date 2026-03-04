import os

# Kafka / Redpanda
KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "redpanda.platform.svc.cluster.local:9092"
)
KAFKA_SASL_USERNAME = os.environ.get("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.environ.get("KAFKA_SASL_PASSWORD", "")
KAFKA_SASL_MECHANISM = os.environ.get("KAFKA_SASL_MECHANISM", "PLAIN")

# Nessie / Iceberg catalog
NESSIE_URI = os.environ.get(
    "NESSIE_URI", "http://nessie.platform.svc.cluster.local:19120/iceberg"
)

# MinIO / S3
S3_ENDPOINT_URL = os.environ.get(
    "S3_ENDPOINT_URL", "http://minio.platform.svc.cluster.local:9000"
)
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minioadmin123")
ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "s3://iceberg/")

# Sink tuning
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "500"))
FLUSH_INTERVAL_S = int(os.environ.get("FLUSH_INTERVAL_S", "30"))

# Water level API base URLs
RWS_BASE_URL = "https://ddapi20-waterwebservices.rijkswaterstaat.nl"
PEGELONLINE_BASE_URL = "https://pegelonline.wsv.de/webservices/rest-api/v2"
HUBEAU_BASE_URL = "https://hubeau.eaufrance.fr/api/v2/hydrometrie"
IMGW_HYDRO_URL = "https://danepubliczne.imgw.pl/api/data/hydro"
KIWIS_BASE_URL = "https://waterinfo.vlaanderen.be/tsmpub/KiWIS/KiWIS"
HEICHWAASSER_BASE_URL = "https://heichwaasser.lu/api/v1"
WATER_POLL_INTERVAL_S = int(os.environ.get("WATER_POLL_INTERVAL_S", "600"))

# Multi-provider station register
# source: "rws" | "pegelonline" | "hubeau" | "imgw" | "kiwis" | "luxembourg"
# reference_datum: "NAP" | "PNP" | "IGN69" | "TAW" | "PNP_PL" | "LU_LOCAL"
WATER_STATIONS: dict[str, dict] = {
    # ── RWS (Nederland) — kust — referentie: NAP, eenheid: cm ──
    "rws:hoekvanholland":  {"lat": 51.978, "lon": 4.121, "name": "Hoek van Holland",    "source": "rws", "reference_datum": "NAP"},
    "rws:vlissingen":      {"lat": 51.440, "lon": 3.578, "name": "Vlissingen",           "source": "rws", "reference_datum": "NAP"},
    "rws:denhelder":       {"lat": 52.963, "lon": 4.760, "name": "Den Helder",           "source": "rws", "reference_datum": "NAP"},
    "rws:ijmuiden":        {"lat": 52.463, "lon": 4.555, "name": "IJmuiden",             "source": "rws", "reference_datum": "NAP"},
    "rws:scheveningen":    {"lat": 52.103, "lon": 4.264, "name": "Scheveningen",         "source": "rws", "reference_datum": "NAP"},
    "rws:harlingen":       {"lat": 53.175, "lon": 5.414, "name": "Harlingen",            "source": "rws", "reference_datum": "NAP"},
    "rws:delfzijl":        {"lat": 53.335, "lon": 6.929, "name": "Delfzijl",             "source": "rws", "reference_datum": "NAP"},
    "rws:rotterdam":       {"lat": 51.896, "lon": 4.486, "name": "Rotterdam",            "source": "rws", "reference_datum": "NAP"},
    "rws:terneuzen":       {"lat": 51.335, "lon": 3.829, "name": "Terneuzen",            "source": "rws", "reference_datum": "NAP"},
    "rws:europlatform":    {"lat": 52.002, "lon": 3.277, "name": "Europlatform",         "source": "rws", "reference_datum": "NAP"},
    # ── RWS (Nederland) — binnenwateren ──
    "rws:lobith":    {"lat": 51.850, "lon": 6.102, "name": "Lobith (Rijn)",       "source": "rws", "reference_datum": "NAP"},
    "rws:nijmegen":  {"lat": 51.853, "lon": 5.854, "name": "Nijmegen (Waal)",     "source": "rws", "reference_datum": "NAP"},
    "rws:tiel":      {"lat": 51.899, "lon": 5.456, "name": "Tiel (Waal)",         "source": "rws", "reference_datum": "NAP"},
    "rws:deventer":  {"lat": 52.251, "lon": 6.153, "name": "Deventer (IJssel)",   "source": "rws", "reference_datum": "NAP"},
    "rws:kampen":    {"lat": 52.552, "lon": 5.926, "name": "Kampen (IJssel)",      "source": "rws", "reference_datum": "NAP"},
    "rws:eijsden":   {"lat": 50.758, "lon": 5.682, "name": "Eijsden (Maas)",      "source": "rws", "reference_datum": "NAP"},
    "rws:venlo":     {"lat": 51.370, "lon": 6.172, "name": "Venlo (Maas)",        "source": "rws", "reference_datum": "NAP"},
    "rws:grave":     {"lat": 51.761, "lon": 5.743, "name": "Grave (Maas)",        "source": "rws", "reference_datum": "NAP"},
    # ── PEGELONLINE (Duitsland) — kust — referentie: PNP, eenheid: cm ──
    "pegel:borkum_suedstrand":   {"lat": 53.577, "lon": 6.661, "name": "Borkum Südstrand",      "source": "pegelonline", "reference_datum": "PNP", "shortname": "BORKUM SÜDSTRAND"},
    "pegel:norderney_riffgat":   {"lat": 53.696, "lon": 7.158, "name": "Norderney Riffgat",     "source": "pegelonline", "reference_datum": "PNP", "shortname": "NORDERNEY RIFFGAT"},
    "pegel:borkum_fischerbalje": {"lat": 53.557, "lon": 6.748, "name": "Borkum Fischerbalje",   "source": "pegelonline", "reference_datum": "PNP", "shortname": "BORKUM FISCHERBALJE"},
    "pegel:langeoog":            {"lat": 53.723, "lon": 7.502, "name": "Langeoog",              "source": "pegelonline", "reference_datum": "PNP", "shortname": "LANGEOOG"},
    "pegel:buesum":              {"lat": 54.122, "lon": 8.859, "name": "Büsum",                 "source": "pegelonline", "reference_datum": "PNP", "shortname": "BÜSUM"},
    "pegel:cuxhaven":            {"lat": 53.868, "lon": 8.717, "name": "Cuxhaven Steubenhöft",  "source": "pegelonline", "reference_datum": "PNP", "shortname": "CUXHAVEN STEUBENHÖFT"},
    "pegel:helgoland":           {"lat": 54.179, "lon": 7.890, "name": "Helgoland Binnenhafen", "source": "pegelonline", "reference_datum": "PNP", "shortname": "HELGOLAND BINNENHAFEN"},
    "pegel:list_sylt":           {"lat": 55.017, "lon": 8.440, "name": "List auf Sylt",         "source": "pegelonline", "reference_datum": "PNP", "shortname": "LIST AUF SYLT"},
    # ── PEGELONLINE (Duitsland) — binnenwateren ──
    "pegel:emmerich":       {"lat": 51.829, "lon": 6.246,  "name": "Emmerich (Rhein)",           "source": "pegelonline", "reference_datum": "PNP", "shortname": "EMMERICH"},
    "pegel:duisburg":       {"lat": 51.455, "lon": 6.728,  "name": "Duisburg-Ruhrort (Rhein)",   "source": "pegelonline", "reference_datum": "PNP", "shortname": "DUISBURG-RUHRORT"},
    "pegel:koeln":          {"lat": 50.937, "lon": 6.963,  "name": "Köln (Rhein)",               "source": "pegelonline", "reference_datum": "PNP", "shortname": "KÖLN"},
    "pegel:koblenz":        {"lat": 50.359, "lon": 7.605,  "name": "Koblenz (Rhein)",            "source": "pegelonline", "reference_datum": "PNP", "shortname": "KOBLENZ"},
    "pegel:mainz":          {"lat": 50.004, "lon": 8.275,  "name": "Mainz (Rhein)",              "source": "pegelonline", "reference_datum": "PNP", "shortname": "MAINZ"},
    "pegel:hamburg":        {"lat": 53.545, "lon": 9.970,  "name": "Hamburg St. Pauli (Elbe)",   "source": "pegelonline", "reference_datum": "PNP", "shortname": "HAMBURG ST. PAULI"},
    "pegel:magdeburg":      {"lat": 52.130, "lon": 11.644, "name": "Magdeburg (Elbe)",           "source": "pegelonline", "reference_datum": "PNP", "shortname": "MAGDEBURG-STROMBRÜCKE"},
    "pegel:dresden":        {"lat": 51.054, "lon": 13.739, "name": "Dresden (Elbe)",             "source": "pegelonline", "reference_datum": "PNP", "shortname": "DRESDEN"},
    "pegel:bremen":         {"lat": 53.073, "lon": 8.804,  "name": "Bremen (Weser)",             "source": "pegelonline", "reference_datum": "PNP", "shortname": "GROSSE WESERBRÜCKE"},
    "pegel:porta":          {"lat": 52.249, "lon": 8.922,  "name": "Porta (Weser)",              "source": "pegelonline", "reference_datum": "PNP", "shortname": "PORTA"},
    "pegel:frankfurt_oder": {"lat": 52.358, "lon": 14.552, "name": "Frankfurt/Oder (Oder)",      "source": "pegelonline", "reference_datum": "PNP", "shortname": "FRANKFURT1 (ODER)"},
    "pegel:schwedt":        {"lat": 53.036, "lon": 14.312, "name": "Schwedt (Oder)",             "source": "pegelonline", "reference_datum": "PNP", "shortname": "SCHWEDT-ODERBRÜCKE"},
    # ── PEGELONLINE (Duitsland) — Rijn-zuiden, Moezel, Main ──
    "pegel:wesel":         {"lat": 51.660, "lon": 6.625, "name": "Wesel (Rhein)",             "source": "pegelonline", "reference_datum": "PNP", "shortname": "WESEL"},
    "pegel:duesseldorf":   {"lat": 51.221, "lon": 6.770, "name": "Düsseldorf (Rhein)",        "source": "pegelonline", "reference_datum": "PNP", "shortname": "DÜSSELDORF"},
    "pegel:andernach":     {"lat": 50.445, "lon": 7.402, "name": "Andernach (Rhein)",         "source": "pegelonline", "reference_datum": "PNP", "shortname": "ANDERNACH"},
    "pegel:bingen":        {"lat": 49.967, "lon": 7.896, "name": "Bingen (Rhein)",            "source": "pegelonline", "reference_datum": "PNP", "shortname": "BINGEN"},
    "pegel:kaub":          {"lat": 50.087, "lon": 7.766, "name": "Kaub (Rhein)",              "source": "pegelonline", "reference_datum": "PNP", "shortname": "KAUB"},
    "pegel:worms":         {"lat": 49.634, "lon": 8.361, "name": "Worms (Rhein)",             "source": "pegelonline", "reference_datum": "PNP", "shortname": "WORMS"},
    "pegel:speyer":        {"lat": 49.318, "lon": 8.440, "name": "Speyer (Rhein)",            "source": "pegelonline", "reference_datum": "PNP", "shortname": "SPEYER"},
    "pegel:mannheim":      {"lat": 49.489, "lon": 8.478, "name": "Mannheim (Rhein)",          "source": "pegelonline", "reference_datum": "PNP", "shortname": "MANNHEIM"},
    "pegel:maxau":         {"lat": 48.995, "lon": 8.330, "name": "Maxau/Karlsruhe (Rhein)",  "source": "pegelonline", "reference_datum": "PNP", "shortname": "MAXAU"},
    "pegel:breisach":      {"lat": 48.032, "lon": 7.577, "name": "Breisach (Rhein)",          "source": "pegelonline", "reference_datum": "PNP", "shortname": "BREISACH"},
    "pegel:trier":         {"lat": 49.748, "lon": 6.636, "name": "Trier (Mosel)",             "source": "pegelonline", "reference_datum": "PNP", "shortname": "TRIER UP"},
    "pegel:cochem":        {"lat": 50.143, "lon": 7.165, "name": "Cochem (Mosel)",            "source": "pegelonline", "reference_datum": "PNP", "shortname": "COCHEM"},
    "pegel:perl":          {"lat": 49.475, "lon": 6.378, "name": "Perl (Mosel)",              "source": "pegelonline", "reference_datum": "PNP", "shortname": "PERL"},
    "pegel:frankfurt_main":{"lat": 50.107, "lon": 8.689, "name": "Frankfurt (Main)",          "source": "pegelonline", "reference_datum": "PNP", "shortname": "FRANKFURT OSTHAFEN"},
    # ── Hub'Eau (Frankrijk) — referentie: IGN69, eenheid: mm (→ /10 voor cm) ──
    "hubeau:paris":       {"lat": 48.845, "lon": 2.366, "name": "Paris Austerlitz (Seine)",   "source": "hubeau", "reference_datum": "IGN69", "station_code": "F700000103"},
    "hubeau:rouen":       {"lat": 49.443, "lon": 1.072, "name": "Rouen (Seine)",              "source": "hubeau", "reference_datum": "IGN69", "station_code": "H503011001"},
    "hubeau:strasbourg":  {"lat": 48.563, "lon": 7.805, "name": "Strasbourg (Rhin)",          "source": "hubeau", "reference_datum": "IGN69", "station_code": "A060005050"},
    "hubeau:didenheim":   {"lat": 47.718, "lon": 7.307, "name": "Didenheim (Ill/Mulhouse)",   "source": "hubeau", "reference_datum": "IGN69", "station_code": "A116003002"},
    "hubeau:metz":        {"lat": 49.122, "lon": 6.167, "name": "Metz (Moselle)",             "source": "hubeau", "reference_datum": "IGN69", "station_code": "A743061001"},
    "hubeau:bollezeele":  {"lat": 50.866, "lon": 2.349, "name": "Bollezeele (Yser/Dunkerque)","source": "hubeau", "reference_datum": "IGN69", "station_code": "E490571101"},
    "hubeau:charleville": {"lat": 49.774, "lon": 4.717, "name": "Charleville-Mézières (Meuse)", "source": "hubeau", "reference_datum": "IGN69", "station_code": "B540001001"},
    "hubeau:verdun":      {"lat": 49.162, "lon": 5.392, "name": "Verdun (Meuse)",               "source": "hubeau", "reference_datum": "IGN69", "station_code": "B301001002"},
    "hubeau:epinal":      {"lat": 48.175, "lon": 6.453, "name": "Épinal (Moselle)",             "source": "hubeau", "reference_datum": "IGN69", "station_code": "A443064001"},
    "hubeau:thionville":  {"lat": 49.364, "lon": 6.168, "name": "Thionville (Moselle)",         "source": "hubeau", "reference_datum": "IGN69", "station_code": "A793061002"},
    # ── IMGW (Polen) — referentie: lokaal PNP, eenheid: cm ──
    "imgw:szczecin":  {"lat": 53.428, "lon": 14.553, "name": "Szczecin (Odra)",   "source": "imgw", "reference_datum": "PNP_PL", "station_code": "153140050"},
    "imgw:kostrzyn":  {"lat": 52.587, "lon": 14.657, "name": "Kostrzyn (Odra)",   "source": "imgw", "reference_datum": "PNP_PL", "station_code": "152140060"},
    "imgw:slubice":   {"lat": 52.353, "lon": 14.563, "name": "Słubice (Odra)",    "source": "imgw", "reference_datum": "PNP_PL", "station_code": "152140050"},
    "imgw:gdansk":    {"lat": 54.347, "lon": 18.659, "name": "Gdańsk (Wisła)",    "source": "imgw", "reference_datum": "PNP_PL", "station_code": "154180220"},
    "imgw:tczew":     {"lat": 54.093, "lon": 18.797, "name": "Tczew (Wisła)",     "source": "imgw", "reference_datum": "PNP_PL", "station_code": "154180150"},
    "imgw:warszawa":  {"lat": 52.241, "lon": 21.035, "name": "Warszawa (Wisła)",  "source": "imgw", "reference_datum": "PNP_PL", "station_code": "152210040"},
    # ── KiWIS / waterinfo.be (België, Vlaanderen) — referentie: TAW, eenheid: m (→ *100 voor cm) ──
    "kiwis:antwerpen":  {"lat": 51.232, "lon": 4.404, "name": "Antwerpen (Schelde)",      "source": "kiwis", "reference_datum": "TAW", "ts_id": "0453986010"},
    "kiwis:gentbrugge": {"lat": 51.056, "lon": 3.726, "name": "Gentbrugge (Schelde)",     "source": "kiwis", "reference_datum": "TAW", "ts_id": "0454400010"},
    "kiwis:zeebrugge":  {"lat": 51.340, "lon": 3.185, "name": "Zeebrugge (Noordzee)",     "source": "kiwis", "reference_datum": "TAW", "ts_id": "0456191010"},
    "kiwis:nieuwpoort": {"lat": 51.131, "lon": 2.722, "name": "Nieuwpoort (Noordzee)",    "source": "kiwis", "reference_datum": "TAW", "ts_id": "0455152010"},
    # ── Heichwaasser (Luxemburg) — CC0, Moezel/Sauer/Our — eenheid: m (→ *100 voor cm) ──
    # Tijdstempels gepubliceerd in UTC+1 vaste offset (geen zomertijd-aanpassing)
    "lu:wasserbillig":  {"lat": 49.716, "lon": 6.499, "name": "Wasserbillig (Moselle/Sauer)", "source": "luxembourg", "reference_datum": "LU_LOCAL", "gauge_name": "Wasserbillig"},
    "lu:diekirch":      {"lat": 49.868, "lon": 6.160, "name": "Diekirch (Sauer)",             "source": "luxembourg", "reference_datum": "LU_LOCAL", "gauge_name": "Diekirch"},
    "lu:ettelbruck":    {"lat": 49.847, "lon": 6.103, "name": "Ettelbruck (Sauer)",           "source": "luxembourg", "reference_datum": "LU_LOCAL", "gauge_name": "Ettelbrück"},
    "lu:remich":        {"lat": 49.544, "lon": 6.363, "name": "Remich (Moselle)",             "source": "luxembourg", "reference_datum": "LU_LOCAL", "gauge_name": "Remich"},
    "lu:grevenmacher":  {"lat": 49.679, "lon": 6.442, "name": "Grevenmacher (Moselle)",      "source": "luxembourg", "reference_datum": "LU_LOCAL", "gauge_name": "Grevenmacher"},
}
