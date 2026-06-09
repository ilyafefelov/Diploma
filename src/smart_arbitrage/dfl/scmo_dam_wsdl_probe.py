"""Extract SCMO SOAP WSDL source leads without creating V13 receipt rows."""

from __future__ import annotations

from datetime import UTC, datetime
import re
from typing import Any, Final
from xml.etree import ElementTree

SCMO_DAM_WSDL_PROBE_CLAIM_SCOPE: Final[str] = "scmo_dam_wsdl_probe_not_receipt"
_SIGNED_REQUEST_STATUS: Final[str] = (
    "wsdl_contract_available_download_requires_signed_or_authenticated_request"
)
_SECURITY_POLICY_LOCAL_NAMES: Final[frozenset[str]] = frozenset(
    {
        "AsymmetricBinding",
        "IncludeTimestamp",
        "OnlySignEntireHeadersAndBody",
        "SignedParts",
        "SignedSupportingTokens",
        "UsernameToken",
        "X509Token",
    }
)


def build_scmo_dam_wsdl_receipt_source_probe(
    *,
    source_url: str,
    wsdl_text: str,
    service_kind: str,
    retrieved_at: datetime | None = None,
) -> dict[str, Any]:
    """Summarize a public SCMO WSDL as source-lead evidence, not receipts."""

    observed_at = retrieved_at if retrieved_at is not None else datetime.now(UTC)
    try:
        root = ElementTree.fromstring(wsdl_text)
        parse_error = None
    except ElementTree.ParseError as exc:
        root = None
        parse_error = str(exc)

    if root is None:
        operations: list[str] = []
        message_codes: list[str] = []
        market_areas: list[str] = []
        trade_request_attributes: list[str] = []
        cdsreq_request_attributes: list[str] = []
        service_addresses: list[str] = []
        security_policy_requirements: list[str] = []
        source_probe_status = "wsdl_parse_failed"
        wsdl_available = False
    else:
        operations = sorted(
            _unique(
                str(element.attrib["name"])
                for element in root.iter()
                if _local_name(element.tag) == "operation" and "name" in element.attrib
            )
        )
        message_codes = sorted(_enumerations_for_attribute(root, "message-code"))
        market_areas = sorted(_enumerations_for_attribute(root, "market-area"))
        trade_request_attributes = _trade_request_attributes(root)
        cdsreq_request_attributes = _cdsreq_request_attributes(root)
        service_addresses = _unique(
            str(element.attrib["location"])
            for element in root.iter()
            if _local_name(element.tag) == "address" and "location" in element.attrib
        )
        security_policy_requirements = sorted(
            _unique(
                local_name
                for element in root.iter()
                if (local_name := _local_name(element.tag))
                in _SECURITY_POLICY_LOCAL_NAMES
            )
        )
        wsdl_available = bool(operations or service_addresses)
        source_probe_status = (
            _SIGNED_REQUEST_STATUS if "Download" in operations else "download_operation_missing"
        )

    lead_row = {
        "lead_id": f"scmo_{_slug(service_kind)}_wsdl_download_contract",
        "source_url": source_url,
        "source_title": f"SCMO {service_kind} SOAP Download WSDL contract",
        "lead_kind": "official_wsdl_contract",
        "metadata_scope": "service_contract",
        "has_timestamp_column": "date-time" in cdsreq_request_attributes
        or "trade-day" in trade_request_attributes,
        "has_source_publication_timestamp_column": False,
        "download_auth_required": True,
        "source_probe_status": source_probe_status,
        "security_policy_requirements": ";".join(security_policy_requirements),
        "market_execution_enabled": False,
    }

    return {
        "claim_scope": SCMO_DAM_WSDL_PROBE_CLAIM_SCOPE,
        "source_url": source_url,
        "service_kind": service_kind,
        "retrieved_at": observed_at.isoformat(),
        "wsdl_available": wsdl_available,
        "wsdl_parse_error": parse_error,
        "operations": operations,
        "message_codes": message_codes,
        "market_areas": market_areas,
        "trade_request_attributes": trade_request_attributes,
        "cdsreq_request_attributes": cdsreq_request_attributes,
        "service_addresses": service_addresses,
        "security_policy_requirements": security_policy_requirements,
        "source_probe_status": source_probe_status,
        "candidate_receipt_source_found": False,
        "lead_row": lead_row,
        "receipt_csv_generated": False,
        "validated_receipt_csv_ready": False,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
        "not_market_execution": True,
    }


def _enumerations_for_attribute(root: ElementTree.Element, attribute_name: str) -> list[str]:
    for attribute in root.iter():
        if _local_name(attribute.tag) != "attribute":
            continue
        if attribute.attrib.get("name") != attribute_name:
            continue
        return _unique(
            str(element.attrib["value"])
            for element in attribute.iter()
            if _local_name(element.tag) == "enumeration" and "value" in element.attrib
        )
    return []


def _trade_request_attributes(root: ElementTree.Element) -> list[str]:
    for element in root.iter():
        if _local_name(element.tag) != "element" or element.attrib.get("name") != "Trade":
            continue
        return _unique(
            str(attribute.attrib["name"])
            for attribute in element.iter()
            if _local_name(attribute.tag) == "attribute" and "name" in attribute.attrib
        )
    return []


def _cdsreq_request_attributes(root: ElementTree.Element) -> list[str]:
    for complex_type in root.iter():
        if (
            _local_name(complex_type.tag) == "complexType"
            and complex_type.attrib.get("name") == "CDSREQType"
        ):
            return _unique(
                str(attribute.attrib["name"])
                for attribute in complex_type
                if _local_name(attribute.tag) == "attribute" and "name" in attribute.attrib
            )
    return []


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _unique(values: Any) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _slug(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", value.casefold()).strip("_")
    return normalized or "unknown"


__all__ = [
    "SCMO_DAM_WSDL_PROBE_CLAIM_SCOPE",
    "build_scmo_dam_wsdl_receipt_source_probe",
]
