"""Probe SCMO SOAP Download responses without creating V13 receipt rows."""

from __future__ import annotations

from collections.abc import Mapping
import base64
from datetime import UTC, datetime, timedelta
import hashlib
import os
from pathlib import Path
import re
import ssl
from typing import Any, Final
from uuid import uuid4
from xml.sax.saxutils import escape, quoteattr

from lxml import etree
from signxml import SignatureReference, XMLSigner, methods
from signxml.algorithms import CanonicalizationMethod

SCMO_DAM_SOAP_DOWNLOAD_PROBE_CLAIM_SCOPE: Final[str] = (
    "scmo_dam_soap_download_probe_not_receipt"
)
SCMO_EVALUATIONS_SERVICE_NAMESPACE: Final[str] = (
    "http://sfera.sk/ws/xmtrade/isot/interfaces/evaluations/services/2009/04/01"
)
SCMO_UT_TYPES_NAMESPACE: Final[str] = (
    "http://sfera.sk/ws/xmtrade/isot/interfaces/ut/types/2009/04/01"
)
SCMO_WS_SECURITY_CREDENTIAL_PREFLIGHT_CLAIM_SCOPE: Final[str] = (
    "scmo_ws_security_credential_preflight"
)
SCMO_WS_SECURITY_SIGNATURE_NOT_IMPLEMENTED_STATUS: Final[str] = (
    "xml_signature_not_implemented"
)
SCMO_WS_SECURITY_SIGNATURE_AVAILABLE_STATUS: Final[str] = (
    "xml_signature_builder_available"
)
SCMO_SOAP12_NAMESPACE: Final[str] = "http://www.w3.org/2003/05/soap-envelope"
SCMO_WSA_NAMESPACE: Final[str] = "http://schemas.xmlsoap.org/ws/2004/08/addressing"
SCMO_WSA_ANONYMOUS_ADDRESS: Final[str] = (
    "http://schemas.xmlsoap.org/ws/2004/08/addressing/role/anonymous"
)
SCMO_DEFAULT_EVALUATIONS_SERVICE_URL: Final[str] = (
    "http://scmo.oree.com.ua/Interfaces/Evaluations/Service.svc"
)
SCMO_DEFAULT_EVALUATIONS_DOWNLOAD_ACTION: Final[str] = (
    "http://sfera.sk/ws/xmtrade/isot/interfaces/evaluations/services/"
    "2009/04/01/EvaluationsContract/Download"
)
SCMO_WSSE_NAMESPACE: Final[str] = (
    "http://docs.oasis-open.org/wss/2004/01/"
    "oasis-200401-wss-wssecurity-secext-1.0.xsd"
)
SCMO_WSU_NAMESPACE: Final[str] = (
    "http://docs.oasis-open.org/wss/2004/01/"
    "oasis-200401-wss-wssecurity-utility-1.0.xsd"
)
SCMO_XMLDSIG_NAMESPACE: Final[str] = "http://www.w3.org/2000/09/xmldsig#"
SCMO_WSSE_PASSWORD_DIGEST_TYPE: Final[str] = (
    "http://docs.oasis-open.org/wss/2004/01/"
    "oasis-200401-wss-username-token-profile-1.0#PasswordDigest"
)
SCMO_WSSE_BASE64_ENCODING_TYPE: Final[str] = (
    "http://docs.oasis-open.org/wss/2004/01/"
    "oasis-200401-wss-soap-message-security-1.0#Base64Binary"
)
SCMO_WSSE_X509_V3_VALUE_TYPE: Final[str] = (
    "http://docs.oasis-open.org/wss/2004/01/"
    "oasis-200401-wss-x509-token-profile-1.0#X509v3"
)


def build_scmo_dam_download_soap_request(
    *,
    trade_day: str,
    message_code: str,
    market_area: str,
    period_from: int = 1,
    period_to: int = 24,
    request_datetime: str | datetime | None = None,
    version: str = "1",
    username: str | None = None,
    password: str | None = None,
    username_token_created: str | datetime | None = None,
    username_token_nonce: bytes | None = None,
) -> str:
    """Build the SCMO Download request used only for source probing."""

    observed_at = _request_datetime_value(request_datetime)
    security_header = _username_token_security_header(
        username=username,
        password=password,
        created=_request_datetime_value(username_token_created),
        nonce=username_token_nonce,
    )
    return (
        '<?xml version="1.0" encoding="utf-8"?>\n'
        '<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope">\n'
        f"{security_header}"
        "  <s:Body>\n"
        f"    <DownloadRequest xmlns={quoteattr(SCMO_EVALUATIONS_SERVICE_NAMESPACE)}>\n"
        f"      <CDSREQ xmlns={quoteattr(SCMO_UT_TYPES_NAMESPACE)} "
        f"message-code={quoteattr(message_code)} "
        f"date-time={quoteattr(observed_at)}>\n"
        "        <Trade "
        f"trade-day={quoteattr(trade_day)} "
        f'period-from="{int(period_from)}" '
        f'period-to="{int(period_to)}" '
        f"version={quoteattr(version)} "
        f"market-area={quoteattr(market_area)} />\n"
        "      </CDSREQ>\n"
        "    </DownloadRequest>\n"
        "  </s:Body>\n"
        "</s:Envelope>\n"
    )


def build_scmo_signed_ws_security_dam_download_soap_request(
    *,
    trade_day: str,
    message_code: str,
    market_area: str,
    client_cert_path: str | Path | None = None,
    client_key_path: str | Path | None = None,
    client_key_password: str | None = None,
    client_p12_path: str | Path | None = None,
    client_p12_password: str | None = None,
    period_from: int = 1,
    period_to: int = 24,
    request_datetime: str | datetime | None = None,
    version: str = "1",
    username: str,
    password: str,
    username_token_created: str | datetime | None = None,
    username_token_nonce: bytes | None = None,
    id_prefix: str | None = None,
    service_url: str = SCMO_DEFAULT_EVALUATIONS_SERVICE_URL,
    soap_action: str = SCMO_DEFAULT_EVALUATIONS_DOWNLOAD_ACTION,
    message_id: str | None = None,
) -> str:
    """Build a WS-Security SignedParts SCMO request for gated source probing."""

    if not username:
        raise ValueError("username is required for signed WS-Security requests.")
    if password is None:
        raise ValueError("password is required for signed WS-Security requests.")

    observed_at = _request_datetime_value(request_datetime)
    token_created = _request_datetime_value(username_token_created)
    nonce_bytes = username_token_nonce if username_token_nonce is not None else os.urandom(16)
    prefix = id_prefix or f"scmo-{uuid4().hex}"
    timestamp_id = f"{prefix}-timestamp"
    username_token_id = f"{prefix}-username-token"
    body_id = f"{prefix}-body"
    binary_security_token_id = f"{prefix}-x509-token"
    wsa_action_id = f"{prefix}-wsa-action"
    wsa_to_id = f"{prefix}-wsa-to"
    wsa_message_id = f"{prefix}-wsa-message-id"
    wsa_reply_to_id = f"{prefix}-wsa-reply-to"

    cert_bytes, key_bytes, key_passphrase = _load_ws_security_signing_material(
        client_cert_path=client_cert_path,
        client_key_path=client_key_path,
        client_key_password=client_key_password,
        client_p12_path=client_p12_path,
        client_p12_password=client_p12_password,
    )
    envelope = _signed_request_envelope(
        trade_day=trade_day,
        message_code=message_code,
        market_area=market_area,
        period_from=period_from,
        period_to=period_to,
        request_datetime=observed_at,
        version=version,
        username=username,
        password=password,
        username_token_created=token_created,
        username_token_nonce=nonce_bytes,
        timestamp_id=timestamp_id,
        username_token_id=username_token_id,
        body_id=body_id,
        binary_security_token_id=binary_security_token_id,
        wsa_action_id=wsa_action_id,
        wsa_to_id=wsa_to_id,
        wsa_message_id=wsa_message_id,
        wsa_reply_to_id=wsa_reply_to_id,
        service_url=service_url,
        soap_action=soap_action,
        message_id=message_id or f"urn:uuid:{uuid4()}",
        cert_bytes=cert_bytes,
    )
    security = envelope.find(f".//{{{SCMO_WSSE_NAMESPACE}}}Security")
    if security is None:
        raise ValueError("Signed SCMO request builder failed to create Security header.")

    key_info = _security_token_reference_key_info(binary_security_token_id)
    references = [
        SignatureReference(
            URI=f"#{body_id}",
            c14n_method=CanonicalizationMethod.EXCLUSIVE_XML_CANONICALIZATION_1_0,
        ),
        SignatureReference(
            URI=f"#{wsa_action_id}",
            c14n_method=CanonicalizationMethod.EXCLUSIVE_XML_CANONICALIZATION_1_0,
        ),
        SignatureReference(
            URI=f"#{wsa_to_id}",
            c14n_method=CanonicalizationMethod.EXCLUSIVE_XML_CANONICALIZATION_1_0,
        ),
        SignatureReference(
            URI=f"#{wsa_message_id}",
            c14n_method=CanonicalizationMethod.EXCLUSIVE_XML_CANONICALIZATION_1_0,
        ),
        SignatureReference(
            URI=f"#{wsa_reply_to_id}",
            c14n_method=CanonicalizationMethod.EXCLUSIVE_XML_CANONICALIZATION_1_0,
        ),
        SignatureReference(
            URI=f"#{username_token_id}",
            c14n_method=CanonicalizationMethod.EXCLUSIVE_XML_CANONICALIZATION_1_0,
        ),
        SignatureReference(
            URI=f"#{timestamp_id}",
            c14n_method=CanonicalizationMethod.EXCLUSIVE_XML_CANONICALIZATION_1_0,
        ),
    ]
    signature = XMLSigner(
        method=methods.detached,
        signature_algorithm="rsa-sha256",
        digest_algorithm="sha256",
        c14n_algorithm=CanonicalizationMethod.EXCLUSIVE_XML_CANONICALIZATION_1_0,
    ).sign(
        envelope,
        key=key_bytes,
        passphrase=key_passphrase,
        reference_uri=references,
        key_info=key_info,
        id_attribute="Id",
    )
    security.append(signature)
    return etree.tostring(
        envelope,
        encoding="utf-8",
        xml_declaration=True,
    ).decode("utf-8")


def sanitize_scmo_soap_request_for_artifact(request_xml: str) -> str:
    """Redact credential-bearing WS-Security fields before writing artifacts."""

    redacted = request_xml
    redactions = {
        r"(<wsse:Username>)(.*?)(</wsse:Username>)": "redacted-username",
        r"(<wsse:Password\b[^>]*>)(.*?)(</wsse:Password>)": "redacted-password-digest",
        r"(<wsse:Nonce\b[^>]*>)(.*?)(</wsse:Nonce>)": "redacted-nonce",
        (
            r"(<wsse:BinarySecurityToken\b[^>]*>)(.*?)"
            r"(</wsse:BinarySecurityToken>)"
        ): "redacted-binary-security-token",
        r"(<ds:SignatureValue\b[^>]*>)(.*?)(</ds:SignatureValue>)": (
            "redacted-signature-value"
        ),
        r"(<ds:DigestValue\b[^>]*>)(.*?)(</ds:DigestValue>)": (
            "redacted-digest-value"
        ),
    }
    for pattern, replacement in redactions.items():
        redacted = re.sub(pattern, rf"\1{replacement}\3", redacted, flags=re.DOTALL)
    return redacted


def build_scmo_dam_soap_download_probe(
    *,
    source_url: str,
    request_xml: str,
    status_code: int,
    content_type: str,
    response_text: str,
    final_url: str | None = None,
    retrieved_at: datetime | None = None,
) -> dict[str, Any]:
    """Classify an SCMO SOAP Download attempt as receipt source evidence."""

    observed_at = retrieved_at if retrieved_at is not None else datetime.now(UTC)
    normalized_response = response_text.casefold()
    wsdl_response_returned = _contains_any(
        normalized_response,
        ["wsdl:definitions", "<definitions"],
    )
    soap_fault_returned = _contains_any(normalized_response, ["<fault", ":fault"])
    auth_or_signature_required = _auth_or_signature_required(
        status_code=status_code,
        response_text=normalized_response,
    )
    download_response_found = not wsdl_response_returned and _contains_any(
        normalized_response,
        ["downloadresponse", "isotedata"],
    )
    candidate_receipt_source_found = (
        download_response_found and not auth_or_signature_required
    )
    source_probe_status = _source_probe_status(
        auth_or_signature_required=auth_or_signature_required,
        candidate_receipt_source_found=candidate_receipt_source_found,
        soap_fault_returned=soap_fault_returned,
        wsdl_response_returned=wsdl_response_returned,
        status_code=status_code,
    )
    service_kind = _service_kind(source_url)
    message_code = _request_attribute(request_xml, "message-code")
    trade_day = _request_attribute(request_xml, "trade-day")
    market_area = _request_attribute(request_xml, "market-area")
    lead_row = {
        "lead_id": (
            f"scmo_{service_kind.casefold()}_soap_download_attempt"
            f"{'_' + message_code if message_code else ''}"
        ),
        "source_url": source_url,
        "source_title": (
            f"SCMO {service_kind} SOAP Download attempt"
            f"{' ' + message_code if message_code else ''}"
        ),
        "lead_kind": "official_soap_download_probe",
        "metadata_scope": "row_level",
        "message_code": message_code,
        "trade_day": trade_day,
        "market_area": market_area,
        "has_timestamp_column": True,
        "has_source_publication_timestamp_column": False,
        "download_auth_required": auth_or_signature_required,
        "source_probe_status": source_probe_status,
        "market_execution_enabled": False,
    }
    return {
        "claim_scope": SCMO_DAM_SOAP_DOWNLOAD_PROBE_CLAIM_SCOPE,
        "source_url": source_url,
        "final_url": final_url or source_url,
        "status_code": int(status_code),
        "content_type": content_type,
        "retrieved_at": observed_at.isoformat(),
        "message_code": message_code,
        "trade_day": trade_day,
        "market_area": market_area,
        "request_sha256": _sha256_text(request_xml),
        "response_sha256": _sha256_text(response_text),
        "response_excerpt": response_text[:500],
        "wsdl_response_returned": wsdl_response_returned,
        "soap_fault_returned": soap_fault_returned,
        "auth_or_signature_required": auth_or_signature_required,
        "download_response_found": download_response_found,
        "source_probe_status": source_probe_status,
        "candidate_receipt_source_found": candidate_receipt_source_found,
        "lead_row": lead_row,
        "receipt_csv_generated": False,
        "validated_receipt_csv_ready": False,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
        "not_market_execution": True,
    }


def build_scmo_ws_security_credential_preflight(
    *,
    env: Mapping[str, str],
    required_env_vars: Mapping[str, str],
) -> dict[str, Any]:
    """Check signed-SCMO credential material without exposing secret values."""

    username_env = _required_env_name(required_env_vars, "username")
    password_env = _required_env_name(required_env_vars, "password")
    cert_env = _required_env_name(required_env_vars, "client_cert_path")
    key_env = _required_env_name(required_env_vars, "client_key_path")
    key_password_env = required_env_vars.get(
        "client_key_password", "SCMO_CLIENT_KEY_PASSWORD"
    ).strip()
    p12_env = required_env_vars.get("client_p12_path", "").strip()
    p12_password_env = required_env_vars.get(
        "client_p12_password", "SCMO_CLIENT_P12_PASSWORD"
    ).strip()
    username = env.get(username_env, "").strip()
    password = env.get(password_env, "")
    cert_path_raw = env.get(cert_env, "").strip()
    key_path_raw = env.get(key_env, "").strip()
    key_password = env.get(key_password_env, "") if key_password_env else ""
    p12_path_raw = env.get(p12_env, "").strip() if p12_env else ""
    p12_password = env.get(p12_password_env, "") if p12_password_env else ""
    missing_env_vars = [
        env_name
        for env_name, value in (
            (username_env, username),
            (password_env, password),
        )
        if not value
    ]
    p12_configured = bool(p12_env)
    pem_pair_declared = bool(cert_path_raw and key_path_raw)
    p12_declared = bool(p12_path_raw)
    if not pem_pair_declared and not p12_declared:
        missing_env_vars.extend(
            env_name
            for env_name, value in (
                (cert_env, cert_path_raw),
                (key_env, key_path_raw),
                (p12_env, p12_path_raw) if p12_configured else ("", ""),
            )
            if env_name and not value
        )
    missing_files: list[str] = []
    cert_sha256 = ""
    key_sha256 = ""
    p12_sha256 = ""
    if cert_path_raw:
        cert_path = Path(cert_path_raw)
        if cert_path.exists():
            cert_sha256 = _sha256_bytes(cert_path.read_bytes())
        else:
            missing_files.append(cert_env)
    if key_path_raw:
        key_path = Path(key_path_raw)
        if key_path.exists():
            key_sha256 = _sha256_bytes(key_path.read_bytes())
        else:
            missing_files.append(key_env)
    if p12_path_raw:
        p12_path = Path(p12_path_raw)
        if p12_path.exists():
            p12_sha256 = _sha256_bytes(p12_path.read_bytes())
        else:
            missing_files.append(p12_env)
    pem_pair_present = bool(cert_path_raw and key_path_raw) and not any(
        env_name in missing_files for env_name in (cert_env, key_env)
    )
    p12_present = bool(p12_path_raw) and p12_env not in missing_files
    if pem_pair_present:
        credential_material_format = "pem_cert_key_pair"
    elif p12_present:
        credential_material_format = "pkcs12"
    else:
        credential_material_format = "missing"
    credential_material_present = (
        bool(username)
        and bool(password)
        and credential_material_format != "missing"
        and not missing_files
    )
    credential_file_pair_valid = False
    credential_material_validation_status = "missing_env_or_file"
    credential_material_validation_error = ""
    if credential_material_present:
        if credential_material_format == "pem_cert_key_pair":
            cert_path = Path(cert_path_raw)
            key_path = Path(key_path_raw)
            (
                credential_file_pair_valid,
                credential_material_validation_status,
                credential_material_validation_error,
            ) = _validate_tls_cert_key_pair(
                cert_path=cert_path,
                key_path=key_path,
                key_password=key_password or None,
            )
        elif credential_material_format == "pkcs12":
            (
                credential_file_pair_valid,
                credential_material_validation_status,
                credential_material_validation_error,
            ) = _validate_pkcs12_cert_key_pair(
                p12_path=Path(p12_path_raw),
                p12_password=p12_password or None,
            )
    ready = credential_material_present and credential_file_pair_valid
    mtls_client_cert_ready = ready and credential_material_format == "pem_cert_key_pair"
    ws_security_signature_supported = True
    signed_download_request_ready = ready and ws_security_signature_supported
    ws_security_signature_blockers: list[str] = []
    if not ws_security_signature_supported:
        ws_security_signature_blockers.append(
            "signedparts_xml_signature_builder_not_available"
        )
    return {
        "claim_scope": SCMO_WS_SECURITY_CREDENTIAL_PREFLIGHT_CLAIM_SCOPE,
        "required_env_vars": {
            "username": username_env,
            "password": password_env,
            "client_cert_path": cert_env,
            "client_key_path": key_env,
            "client_p12_path": p12_env,
        },
        "optional_env_vars": {
            "client_key_password": key_password_env,
            "client_p12_password": p12_password_env,
        },
        "username_present": bool(username),
        "password_present": bool(password),
        "client_cert_path_present": bool(cert_path_raw),
        "client_key_path_present": bool(key_path_raw),
        "client_key_password_present": bool(key_password),
        "client_p12_path_present": bool(p12_path_raw),
        "client_p12_password_present": bool(p12_password),
        "client_cert_sha256": cert_sha256,
        "client_key_sha256": key_sha256,
        "client_p12_sha256": p12_sha256,
        "missing_env_vars": missing_env_vars,
        "missing_files": missing_files,
        "credential_material_format": credential_material_format,
        "pem_cert_key_pair_present": pem_pair_present,
        "pkcs12_bundle_present": p12_present,
        "credential_material_present": credential_material_present,
        "credential_file_pair_valid": credential_file_pair_valid,
        "credential_material_validation_status": (
            credential_material_validation_status
        ),
        "credential_material_validation_error": credential_material_validation_error,
        "credential_material_ready": ready,
        "mtls_client_cert_ready": mtls_client_cert_ready,
        "signed_download_request_ready": signed_download_request_ready,
        "ws_security_signature_supported": ws_security_signature_supported,
        "ws_security_signature_status": (
            SCMO_WS_SECURITY_SIGNATURE_AVAILABLE_STATUS
            if ws_security_signature_supported
            else SCMO_WS_SECURITY_SIGNATURE_NOT_IMPLEMENTED_STATUS
        ),
        "ws_security_signature_blockers": ws_security_signature_blockers,
        "ws_security_requirements": [
            "UsernameToken",
            "X509Token",
            "SignedParts",
            "AsymmetricBinding",
        ],
        "secret_values_written": False,
        "receipt_csv_generated": False,
        "validated_receipt_csv_ready": False,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
        "not_market_execution": True,
    }


def _request_datetime_value(value: str | datetime | None) -> str:
    if value is None:
        return datetime.now(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    return value


def _signed_request_envelope(
    *,
    trade_day: str,
    message_code: str,
    market_area: str,
    period_from: int,
    period_to: int,
    request_datetime: str,
    version: str,
    username: str,
    password: str,
    username_token_created: str,
    username_token_nonce: bytes,
    timestamp_id: str,
    username_token_id: str,
    body_id: str,
    binary_security_token_id: str,
    wsa_action_id: str,
    wsa_to_id: str,
    wsa_message_id: str,
    wsa_reply_to_id: str,
    service_url: str,
    soap_action: str,
    message_id: str,
    cert_bytes: bytes,
) -> etree._Element:
    envelope = etree.Element(
        _qname(SCMO_SOAP12_NAMESPACE, "Envelope"),
        nsmap={
            "s": SCMO_SOAP12_NAMESPACE,
            "wsse": SCMO_WSSE_NAMESPACE,
            "wsu": SCMO_WSU_NAMESPACE,
            "ds": SCMO_XMLDSIG_NAMESPACE,
            "wsa": SCMO_WSA_NAMESPACE,
        },
    )
    header = etree.SubElement(envelope, _qname(SCMO_SOAP12_NAMESPACE, "Header"))
    action = _append_text(header, _qname(SCMO_WSA_NAMESPACE, "Action"), soap_action)
    action.set(_qname(SCMO_WSU_NAMESPACE, "Id"), wsa_action_id)
    to = _append_text(header, _qname(SCMO_WSA_NAMESPACE, "To"), service_url)
    to.set(_qname(SCMO_WSU_NAMESPACE, "Id"), wsa_to_id)
    message = _append_text(
        header,
        _qname(SCMO_WSA_NAMESPACE, "MessageID"),
        message_id,
    )
    message.set(_qname(SCMO_WSU_NAMESPACE, "Id"), wsa_message_id)
    reply_to = etree.SubElement(header, _qname(SCMO_WSA_NAMESPACE, "ReplyTo"))
    reply_to.set(_qname(SCMO_WSU_NAMESPACE, "Id"), wsa_reply_to_id)
    _append_text(
        reply_to,
        _qname(SCMO_WSA_NAMESPACE, "Address"),
        SCMO_WSA_ANONYMOUS_ADDRESS,
    )
    security = etree.SubElement(header, _qname(SCMO_WSSE_NAMESPACE, "Security"))
    security.set(_qname(SCMO_SOAP12_NAMESPACE, "mustUnderstand"), "true")

    timestamp = etree.SubElement(security, _qname(SCMO_WSU_NAMESPACE, "Timestamp"))
    timestamp.set(_qname(SCMO_WSU_NAMESPACE, "Id"), timestamp_id)
    _append_text(timestamp, _qname(SCMO_WSU_NAMESPACE, "Created"), username_token_created)
    _append_text(
        timestamp,
        _qname(SCMO_WSU_NAMESPACE, "Expires"),
        _expires_timestamp_value(username_token_created),
    )

    username_token = etree.SubElement(
        security,
        _qname(SCMO_WSSE_NAMESPACE, "UsernameToken"),
    )
    username_token.set(_qname(SCMO_WSU_NAMESPACE, "Id"), username_token_id)
    _append_text(username_token, _qname(SCMO_WSSE_NAMESPACE, "Username"), username)
    password_element = _append_text(
        username_token,
        _qname(SCMO_WSSE_NAMESPACE, "Password"),
        _username_token_password_digest(
            nonce=username_token_nonce,
            created=username_token_created,
            password=password,
        ),
    )
    password_element.set("Type", SCMO_WSSE_PASSWORD_DIGEST_TYPE)
    nonce_element = _append_text(
        username_token,
        _qname(SCMO_WSSE_NAMESPACE, "Nonce"),
        base64.b64encode(username_token_nonce).decode("ascii"),
    )
    nonce_element.set("EncodingType", SCMO_WSSE_BASE64_ENCODING_TYPE)
    _append_text(
        username_token,
        _qname(SCMO_WSU_NAMESPACE, "Created"),
        username_token_created,
    )

    binary_token = _append_text(
        security,
        _qname(SCMO_WSSE_NAMESPACE, "BinarySecurityToken"),
        _pem_certificate_base64_body(cert_bytes),
    )
    binary_token.set(_qname(SCMO_WSU_NAMESPACE, "Id"), binary_security_token_id)
    binary_token.set("EncodingType", SCMO_WSSE_BASE64_ENCODING_TYPE)
    binary_token.set("ValueType", SCMO_WSSE_X509_V3_VALUE_TYPE)

    body = etree.SubElement(envelope, _qname(SCMO_SOAP12_NAMESPACE, "Body"))
    body.set(_qname(SCMO_WSU_NAMESPACE, "Id"), body_id)
    download_request = etree.SubElement(
        body,
        _qname(SCMO_EVALUATIONS_SERVICE_NAMESPACE, "DownloadRequest"),
        nsmap={None: SCMO_EVALUATIONS_SERVICE_NAMESPACE},
    )
    cdsreq = etree.SubElement(
        download_request,
        _qname(SCMO_UT_TYPES_NAMESPACE, "CDSREQ"),
        nsmap={None: SCMO_UT_TYPES_NAMESPACE},
    )
    cdsreq.set("message-code", message_code)
    cdsreq.set("date-time", request_datetime)
    trade = etree.SubElement(cdsreq, _qname(SCMO_UT_TYPES_NAMESPACE, "Trade"))
    trade.set("trade-day", trade_day)
    trade.set("period-from", str(int(period_from)))
    trade.set("period-to", str(int(period_to)))
    trade.set("version", version)
    trade.set("market-area", market_area)
    return envelope


def _security_token_reference_key_info(
    binary_security_token_id: str,
) -> etree._Element:
    key_info = etree.Element(
        _qname(SCMO_XMLDSIG_NAMESPACE, "KeyInfo"),
        nsmap={"ds": SCMO_XMLDSIG_NAMESPACE, "wsse": SCMO_WSSE_NAMESPACE},
    )
    security_token_reference = etree.SubElement(
        key_info,
        _qname(SCMO_WSSE_NAMESPACE, "SecurityTokenReference"),
    )
    reference = etree.SubElement(
        security_token_reference,
        _qname(SCMO_WSSE_NAMESPACE, "Reference"),
    )
    reference.set("URI", f"#{binary_security_token_id}")
    reference.set("ValueType", SCMO_WSSE_X509_V3_VALUE_TYPE)
    return key_info


def _append_text(parent: etree._Element, tag: str, text: str) -> etree._Element:
    child = etree.SubElement(parent, tag)
    child.text = text
    return child


def _qname(namespace: str, name: str) -> str:
    return f"{{{namespace}}}{name}"


def _expires_timestamp_value(created: str) -> str:
    normalized = created.replace("Z", "+00:00")
    try:
        created_at = datetime.fromisoformat(normalized)
    except ValueError:
        return created
    expires_at = created_at + timedelta(minutes=5)
    return expires_at.isoformat().replace("+00:00", "Z")


def _username_token_password_digest(
    *,
    nonce: bytes,
    created: str,
    password: str,
) -> str:
    return base64.b64encode(
        hashlib.sha1(
            nonce
            + created.encode("utf-8")
            + password.encode("utf-8")
        ).digest()
    ).decode("ascii")


def _pem_certificate_base64_body(cert_bytes: bytes) -> str:
    try:
        cert_text = cert_bytes.decode("ascii")
    except UnicodeDecodeError:
        return base64.b64encode(cert_bytes).decode("ascii")
    body_lines = [
        line.strip()
        for line in cert_text.splitlines()
        if line.strip() and "CERTIFICATE" not in line
    ]
    if body_lines:
        return "".join(body_lines)
    return base64.b64encode(cert_bytes).decode("ascii")


def _load_ws_security_signing_material(
    *,
    client_cert_path: str | Path | None,
    client_key_path: str | Path | None,
    client_key_password: str | None,
    client_p12_path: str | Path | None,
    client_p12_password: str | None,
) -> tuple[bytes, bytes, bytes | None]:
    if client_p12_path is not None:
        cert_bytes, key_bytes = _load_pkcs12_signing_material(
            p12_path=Path(client_p12_path),
            p12_password=client_p12_password,
        )
        return cert_bytes, key_bytes, None
    if client_cert_path is None or client_key_path is None:
        raise ValueError(
            "Signed SCMO request requires client_cert_path/client_key_path "
            "or client_p12_path."
        )
    key_passphrase = (
        client_key_password.encode("utf-8") if client_key_password else None
    )
    return Path(client_cert_path).read_bytes(), Path(client_key_path).read_bytes(), key_passphrase


def _load_pkcs12_signing_material(
    *,
    p12_path: Path,
    p12_password: str | None,
) -> tuple[bytes, bytes]:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.serialization import pkcs12

    try:
        private_key, certificate, _additional_certificates = (
            pkcs12.load_key_and_certificates(
                p12_path.read_bytes(),
                p12_password.encode("utf-8") if p12_password else None,
            )
        )
    except (OSError, ValueError) as exc:
        raise ValueError(
            f"Invalid SCMO PKCS#12 signing bundle: {_safe_exception_summary(exc)}"
        ) from exc
    if private_key is None or certificate is None:
        raise ValueError("SCMO PKCS#12 signing bundle must contain cert and key.")
    cert_bytes = certificate.public_bytes(serialization.Encoding.PEM)
    key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return cert_bytes, key_bytes


def _username_token_security_header(
    *,
    username: str | None,
    password: str | None,
    created: str,
    nonce: bytes | None,
) -> str:
    if username is None and password is None:
        return ""
    if not username or password is None:
        raise ValueError("Both username and password are required for UsernameToken.")
    nonce_bytes = nonce if nonce is not None else os.urandom(16)
    nonce_base64 = base64.b64encode(nonce_bytes).decode("ascii")
    digest = base64.b64encode(
        hashlib.sha1(
            nonce_bytes
            + created.encode("utf-8")
            + password.encode("utf-8")
        ).digest()
    ).decode("ascii")
    return (
        "  <s:Header>\n"
        f"    <wsse:Security xmlns:wsse={quoteattr(SCMO_WSSE_NAMESPACE)} "
        f"xmlns:wsu={quoteattr(SCMO_WSU_NAMESPACE)} "
        's:mustUnderstand="true">\n'
        "      <wsse:UsernameToken>\n"
        f"        <wsse:Username>{escape(username)}</wsse:Username>\n"
        "        <wsse:Password "
        'Type="http://docs.oasis-open.org/wss/2004/01/'
        'oasis-200401-wss-username-token-profile-1.0#PasswordDigest">'
        f"{digest}</wsse:Password>\n"
        "        <wsse:Nonce "
        'EncodingType="http://docs.oasis-open.org/wss/2004/01/'
        'oasis-200401-wss-soap-message-security-1.0#Base64Binary">'
        f"{nonce_base64}</wsse:Nonce>\n"
        f"        <wsu:Created>{escape(created)}</wsu:Created>\n"
        "      </wsse:UsernameToken>\n"
        "    </wsse:Security>\n"
        "  </s:Header>\n"
    )


def _auth_or_signature_required(*, status_code: int, response_text: str) -> bool:
    return status_code in {401, 403} or _contains_any(
        response_text,
        [
            "authentication",
            "authorization",
            "forbidden",
            "security",
            "signature",
            "signed",
            "token",
            "usernametoken",
            "x509",
        ],
    )


def _source_probe_status(
    *,
    auth_or_signature_required: bool,
    candidate_receipt_source_found: bool,
    soap_fault_returned: bool,
    wsdl_response_returned: bool,
    status_code: int,
) -> str:
    if status_code == 0:
        return "network_error_without_receipt_export"
    if candidate_receipt_source_found:
        return "candidate_soap_download_response_present"
    if wsdl_response_returned and auth_or_signature_required:
        return "wsdl_response_returned_signed_download_required"
    if wsdl_response_returned:
        return "wsdl_response_returned_without_receipt_export"
    if auth_or_signature_required:
        return "ws_security_required"
    if soap_fault_returned:
        return "soap_fault_without_receipt_export"
    if status_code >= 400:
        return "download_failed"
    return "not_sufficient_for_v13_receipts"


def _service_kind(source_url: str) -> str:
    if re.search("idmevaluations", source_url, flags=re.IGNORECASE):
        return "IdmEvaluations"
    return "Evaluations"


def _contains_any(value: str, needles: list[str]) -> bool:
    return any(needle in value for needle in needles)


def _request_attribute(request_xml: str, attribute_name: str) -> str:
    match = re.search(rf'\b{re.escape(attribute_name)}="([^"]+)"', request_xml)
    return match.group(1) if match else ""


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _validate_tls_cert_key_pair(
    *,
    cert_path: Path,
    key_path: Path,
    key_password: str | None,
) -> tuple[bool, str, str]:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    try:
        context.load_cert_chain(
            certfile=str(cert_path),
            keyfile=str(key_path),
            password=key_password,
        )
    except (OSError, ValueError, ssl.SSLError) as exc:
        return False, "invalid_cert_key_pair", _safe_exception_summary(exc)
    return True, "valid_cert_key_pair", ""


def _validate_pkcs12_cert_key_pair(
    *,
    p12_path: Path,
    p12_password: str | None,
) -> tuple[bool, str, str]:
    try:
        _load_pkcs12_signing_material(
            p12_path=p12_path,
            p12_password=p12_password,
        )
    except ValueError as exc:
        return False, "invalid_pkcs12_bundle", _safe_exception_summary(exc)
    return True, "valid_pkcs12_bundle", ""


def _safe_exception_summary(exc: BaseException) -> str:
    message = str(exc).splitlines()[0] if str(exc) else ""
    return f"{exc.__class__.__name__}: {message}"


def _required_env_name(required_env_vars: Mapping[str, str], key: str) -> str:
    value = required_env_vars.get(key, "").strip()
    if not value:
        raise ValueError(f"Missing required env var mapping for {key!r}.")
    return value


__all__ = [
    "SCMO_DAM_SOAP_DOWNLOAD_PROBE_CLAIM_SCOPE",
    "SCMO_WS_SECURITY_CREDENTIAL_PREFLIGHT_CLAIM_SCOPE",
    "build_scmo_dam_download_soap_request",
    "build_scmo_dam_soap_download_probe",
    "build_scmo_signed_ws_security_dam_download_soap_request",
    "build_scmo_ws_security_credential_preflight",
    "sanitize_scmo_soap_request_for_artifact",
]
