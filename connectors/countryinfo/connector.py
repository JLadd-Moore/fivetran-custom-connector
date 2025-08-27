from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from marshmallow import Schema, fields, INCLUDE

from shared.api.client import ApiClient
from shared.api.auth_strategy import NoAuth
from shared.api.endpoint import Endpoint
from shared.api.xml_utils import extractor_from_schema
from shared.api.codecs import SoapCodec


COUNTRYINFO_URL = (
    "http://www.oorsprong.org/websamples.countryinfo/CountryInfoService.wso"
)


class EmptySchema(Schema):
    class Meta:
        unknown = INCLUDE


class IdentitySchema(Schema):
    class Meta:
        unknown = INCLUDE

    def dump(self, obj, *, many=None):  # type: ignore[override]
        return obj or {}


def build_envelope(action: str, body_xml: str) -> str:
    return f"""
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    {body_xml}
  </soap:Body>
  </soap:Envelope>
""".strip()


# Declarative schemas with per-field XPath metadata
class LanguageSchema(Schema):
    ISOCode = fields.String(metadata={"xpath": "./*[local-name()='sISOCode']"})
    Name = fields.String(metadata={"xpath": "./*[local-name()='sName']"})


class CountryCodeAndNameSchema(Schema):
    ISOCode = fields.String(metadata={"xpath": "./*[local-name()='sISOCode']"})
    Name = fields.String(metadata={"xpath": "./*[local-name()='sName']"})


class CountrySchema(Schema):
    ISOCode = fields.String(metadata={"xpath": ".//*[local-name()='sISOCode']"})
    Name = fields.String(metadata={"xpath": ".//*[local-name()='sName']"})
    CapitalCity = fields.String(metadata={"xpath": ".//*[local-name()='sCapitalCity']"})
    PhoneCode = fields.String(metadata={"xpath": ".//*[local-name()='sPhoneCode']"})
    ContinentCode = fields.String(
        metadata={"xpath": ".//*[local-name()='sContinentCode']"}
    )
    CurrencyISOCode = fields.String(
        metadata={"xpath": ".//*[local-name()='sCurrencyISOCode']"}
    )
    CountryFlag = fields.String(metadata={"xpath": ".//*[local-name()='sCountryFlag']"})
    # Destination expects STRING; we will join names by comma during extraction
    Languages = fields.String(
        metadata={
            "container_xpath": ".//*[local-name()='Languages']",
            "item_xpath": "./*[local-name()='tLanguage']/*[local-name()='sName']",
            "multi": True,
            "join": ",",
        }
    )


def list_countries_envelope(_: dict) -> str:
    # ListOfCountryNamesByCode has empty input
    xml = build_envelope(
        "ListOfCountryNamesByCode",
        '<ListOfCountryNamesByCode xmlns="http://www.oorsprong.org/websamples.countryinfo" />',
    )
    return xml


def full_country_info_envelope(params: dict) -> str:
    code = params.get("sCountryISOCode", "")
    xml = build_envelope(
        "FullCountryInfo",
        f"""
<FullCountryInfo xmlns="http://www.oorsprong.org/websamples.countryinfo">
  <sCountryISOCode>{code}</sCountryISOCode>
</FullCountryInfo>
""".strip(),
    )
    return xml


def schema(configuration: dict):
    return [
        {
            "table": "countries",
            "primary_key": ["ISOCode"],
            "columns": {
                "ISOCode": "STRING",
                "Name": "STRING",
                "CapitalCity": "STRING",
                "PhoneCode": "STRING",
                "ContinentCode": "STRING",
                "CurrencyISOCode": "STRING",
                "CountryFlag": "STRING",
                "Languages": "STRING",
            },
        }
    ]


def update(configuration: dict, state: dict):
    log.info("CountryInfo: Fetching countries via SOAP")

    # Endpoint 1: List all country ISO codes and names
    list_endpoint = Endpoint(
        name="ListOfCountryNamesByCode",
        path=COUNTRYINFO_URL,
        method="POST",
        request_schema=IdentitySchema(),
        response_schema=EmptySchema(),
        codec=SoapCodec(
            action="http://www.oorsprong.org/websamples.countryinfo/CountryInfoService.wso/ListOfCountryNamesByCode",
            envelope_builder=list_countries_envelope,
        ),
        extract_items=extractor_from_schema(
            ".//*[local-name()='ListOfCountryNamesByCodeResult']/*[local-name()='tCountryCodeAndName']",
            CountryCodeAndNameSchema,
        ),
    )

    # Endpoint 2: FullCountryInfo per ISO code
    full_info_endpoint = Endpoint(
        name="FullCountryInfo",
        path=COUNTRYINFO_URL,
        method="POST",
        request_schema=IdentitySchema(),
        response_schema=EmptySchema(),
        codec=SoapCodec(
            action="http://www.oorsprong.org/websamples.countryinfo/CountryInfoService.wso/FullCountryInfo",
            envelope_builder=full_country_info_envelope,
        ),
        extract_items=extractor_from_schema(
            ".//*[local-name()='FullCountryInfoResult']",
            CountrySchema,
        ),
    )

    api = ApiClient(
        auth_strategy=NoAuth(), endpoints=[list_endpoint, full_info_endpoint]
    )

    # Iterate list as dicts, then fetch full info as dicts
    cc_schema = CountryCodeAndNameSchema()
    c_schema = CountrySchema()
    for country in api.endpoint("ListOfCountryNamesByCode").items({}):
        log.info(f"Country: {country}")
        country = cc_schema.load(country)
        iso = (country.get("ISOCode") or "").strip()
        name = country.get("Name")
        if not iso:
            continue
        for row in api.endpoint("FullCountryInfo").items({"sCountryISOCode": iso}):
            log.info(f"Row: {row}")
            row = c_schema.load(row)
            if name and not row.get("Name"):
                row["Name"] = name
            if row.get("ISOCode"):
                yield op.upsert("countries", row)


connector = Connector(update=update, schema=schema)
