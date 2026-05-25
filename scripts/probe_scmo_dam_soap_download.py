"""Probe SCMO SOAP Download access and optionally normalize real receipt rows."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
import json
import os
from pathlib import Path
from typing import Any, Literal, Sequence, cast

import httpx
import polars as pl

from smart_arbitrage.dfl.scmo_dam_receipt_export import (
    ScmoReceiptExportFormat,
    normalize_scmo_dam_publication_receipt_export_frame,
    read_scmo_dam_receipt_export_bytes,
)
from smart_arbitrage.dfl.scmo_dam_soap_download_probe import (
    build_scmo_dam_download_soap_request,
    build_scmo_dam_soap_download_probe,
    build_scmo_signed_ws_security_dam_download_soap_request,
    build_scmo_ws_security_credential_preflight,
    sanitize_scmo_soap_request_for_artifact,
)

ScmoCredentialMode = Literal[
    "unsigned",
    "preflight-gated-mtls-username-token",
    "preflight-gated-signed-ws-security",
]
ScmoClientCert = str | tuple[str, str] | tuple[str, str, str]
SCMO_SOAP_DOWNLOAD_NORMALIZATION_CLAIM_SCOPE = (
    "scmo_dam_soap_download_normalization_not_market_execution"
)
SCMO_MTLS_USERNAME_TOKEN_MODE = "preflight-gated-mtls-username-token"
SCMO_SIGNED_WS_SECURITY_MODE = "preflight-gated-signed-ws-security"


@dataclass(frozen=True)
class ScmoSoapProbeResponse:
    source_url: str
    final_url: str
    status_code: int
    content_type: str
    body: bytes


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Post an unsigned SCMO SOAP Download request to classify access. "
            "This writes source-lead evidence by default. With "
            "--normalized-output, it writes V13 receipt rows only from a real "
            "candidate DownloadResponse/ISOTEDATA and does not permit DT/LAVA "
            "training or market execution."
        )
    )
    parser.add_argument(
        "--url",
        default="http://scmo.oree.com.ua/Interfaces/Evaluations/Service.svc",
    )
    parser.add_argument(
        "--soap-action",
        default=(
            "http://sfera.sk/ws/xmtrade/isot/interfaces/evaluations/services/"
            "2009/04/01/EvaluationsContract/Download"
        ),
    )
    parser.add_argument("--trade-day", required=True)
    parser.add_argument("--message-code", default="807")
    parser.add_argument("--market-area", default="UA_IPS")
    parser.add_argument("--period-from", type=int, default=1)
    parser.add_argument("--period-to", type=int, default=24)
    parser.add_argument(
        "--credential-mode",
        choices=[
            "unsigned",
            SCMO_MTLS_USERNAME_TOKEN_MODE,
            SCMO_SIGNED_WS_SECURITY_MODE,
        ],
        default="unsigned",
        help=(
            "Use unsigned probing or require a passing local SCMO credential "
            "preflight before sending a UsernameToken/client-cert request. "
            "The signed WS-Security mode also requires explicit SignedParts "
            "request readiness before any post is attempted."
        ),
    )
    parser.add_argument("--username-env-var", default="SCMO_USERNAME")
    parser.add_argument("--password-env-var", default="SCMO_PASSWORD")
    parser.add_argument("--client-cert-env-var", default="SCMO_CLIENT_CERT_PEM")
    parser.add_argument("--client-key-env-var", default="SCMO_CLIENT_KEY_PEM")
    parser.add_argument(
        "--client-key-password-env-var",
        default="SCMO_CLIENT_KEY_PASSWORD",
    )
    parser.add_argument("--client-p12-env-var", default="SCMO_CLIENT_P12")
    parser.add_argument(
        "--client-p12-password-env-var",
        default="SCMO_CLIENT_P12_PASSWORD",
    )
    parser.add_argument("--probe-output-json", type=Path, required=True)
    parser.add_argument("--lead-output-csv", type=Path, required=True)
    parser.add_argument(
        "--request-output-xml",
        type=Path,
        default=None,
        help="Optional path for the unsigned request envelope used in the probe.",
    )
    parser.add_argument(
        "--raw-response-output",
        type=Path,
        default=None,
        help="Optional path for the raw SOAP response body.",
    )
    parser.add_argument(
        "--normalized-output",
        type=Path,
        default=None,
        help=(
            "Optional canonical V13 receipt CSV output. Written only when the "
            "SOAP response contains a real candidate DownloadResponse/ISOTEDATA."
        ),
    )
    parser.add_argument(
        "--normalized-input-format",
        choices=["auto", "csv", "xml", "xlsx", "zip", "html"],
        default="auto",
        help="Input format for normalizing a real SOAP response. Defaults to auto.",
    )
    parser.add_argument(
        "--header",
        action="append",
        default=[],
        help="Additional HTTP header as 'Name: value'. May be repeated.",
    )
    args = parser.parse_args(argv)

    retrieved_at = datetime.now(UTC)
    credential_env_vars = {
        "username": args.username_env_var,
        "password": args.password_env_var,
        "client_cert_path": args.client_cert_env_var,
        "client_key_path": args.client_key_env_var,
        "client_key_password": args.client_key_password_env_var,
        "client_p12_path": args.client_p12_env_var,
        "client_p12_password": args.client_p12_password_env_var,
    }
    credential_preflight: dict[str, Any] | None = None
    credentialed_request_attempted = False
    client_cert: ScmoClientCert | None = None
    credential_mode = str(args.credential_mode)
    username: str | None = None
    password: str | None = None
    if credential_mode != "unsigned":
        credential_preflight = _credential_preflight(
            env=os.environ,
            required_env_vars=credential_env_vars,
        )
        signed_request_ready = bool(
            credential_preflight.get("signed_download_request_ready", False)
        )
        credential_material_ready = bool(
            credential_preflight.get("credential_material_ready", False)
        )
        mtls_client_cert_ready = bool(
            credential_preflight.get(
                "mtls_client_cert_ready",
                credential_material_ready,
            )
        )
        if (
            credential_mode == SCMO_MTLS_USERNAME_TOKEN_MODE
            and mtls_client_cert_ready
        ) or (
            credential_mode == SCMO_SIGNED_WS_SECURITY_MODE
            and signed_request_ready
        ):
            username = os.environ[args.username_env_var]
            password = os.environ[args.password_env_var]
            if mtls_client_cert_ready:
                client_cert = _client_cert_from_env(
                    env=os.environ,
                    cert_env_var=args.client_cert_env_var,
                    key_env_var=args.client_key_env_var,
                    key_password_env_var=args.client_key_password_env_var,
                )
    signed_request_ready = (
        credential_preflight is not None
        and credential_mode == SCMO_SIGNED_WS_SECURITY_MODE
        and bool(credential_preflight.get("signed_download_request_ready", False))
    )
    if signed_request_ready:
        request_xml = build_scmo_signed_ws_security_dam_download_soap_request(
            trade_day=args.trade_day,
            message_code=args.message_code,
            market_area=args.market_area,
            period_from=args.period_from,
            period_to=args.period_to,
            request_datetime=retrieved_at,
            username=username or "",
            password=password or "",
            username_token_created=retrieved_at,
            client_cert_path=os.environ.get(args.client_cert_env_var, "") or None,
            client_key_path=os.environ.get(args.client_key_env_var, "") or None,
            client_key_password=os.environ.get(args.client_key_password_env_var, "")
            or None,
            client_p12_path=os.environ.get(args.client_p12_env_var, "") or None,
            client_p12_password=os.environ.get(args.client_p12_password_env_var, "")
            or None,
            service_url=args.url,
            soap_action=args.soap_action,
        )
    else:
        request_xml = build_scmo_dam_download_soap_request(
            trade_day=args.trade_day,
            message_code=args.message_code,
            market_area=args.market_area,
            period_from=args.period_from,
            period_to=args.period_to,
            request_datetime=retrieved_at,
            username=username,
            password=password,
            username_token_created=retrieved_at,
        )
    artifact_request_xml = sanitize_scmo_soap_request_for_artifact(request_xml)
    if args.request_output_xml is not None:
        args.request_output_xml.parent.mkdir(parents=True, exist_ok=True)
        args.request_output_xml.write_text(artifact_request_xml, encoding="utf-8")

    skip_reason = _credentialed_post_skip_reason(
        credential_mode=credential_mode,
        credential_preflight=credential_preflight,
    )
    if skip_reason:
        response = ScmoSoapProbeResponse(
            source_url=args.url,
            final_url=args.url,
            status_code=0,
            content_type="text/plain; charset=utf-8",
            body=skip_reason.encode("utf-8"),
        )
    else:
        credentialed_request_attempted = credential_mode != "unsigned"
        try:
            response = _post(
                args.url,
                request_xml=request_xml,
                soap_action=args.soap_action,
                extra_headers=_headers(args.header),
                client_cert=client_cert,
            )
        except httpx.HTTPError as error:
            response = ScmoSoapProbeResponse(
                source_url=args.url,
                final_url=args.url,
                status_code=0,
                content_type="text/plain; charset=utf-8",
                body=f"{type(error).__name__}: {error}".encode("utf-8"),
            )
    response_text = _decode_body(response.body, response.content_type)
    if args.raw_response_output is not None:
        args.raw_response_output.parent.mkdir(parents=True, exist_ok=True)
        args.raw_response_output.write_bytes(response.body)

    probe = build_scmo_dam_soap_download_probe(
        source_url=response.source_url,
        final_url=response.final_url,
        request_xml=artifact_request_xml,
        status_code=response.status_code,
        content_type=response.content_type,
        response_text=response_text,
        retrieved_at=retrieved_at,
    )
    _attach_credential_metadata(
        probe,
        credential_mode=credential_mode,
        credential_preflight=credential_preflight,
        credentialed_request_attempted=credentialed_request_attempted,
        ws_security_signature_applied=(
            credentialed_request_attempted and signed_request_ready
        ),
    )
    if args.normalized_output is not None:
        normalization_summary = _maybe_normalize_response(
            response=response,
            probe=probe,
            normalized_output_path=args.normalized_output,
            input_format=cast(
                ScmoReceiptExportFormat,
                str(args.normalized_input_format),
            ),
        )
        probe["normalization_summary"] = normalization_summary

    args.probe_output_json.parent.mkdir(parents=True, exist_ok=True)
    args.probe_output_json.write_text(
        json.dumps(_json_ready(probe), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    args.lead_output_csv.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([probe["lead_row"]]).write_csv(args.lead_output_csv)
    print(f"Wrote SCMO SOAP Download probe: {args.probe_output_json}")
    print(f"Wrote SCMO SOAP Download source lead: {args.lead_output_csv}")
    return 0


def _maybe_normalize_response(
    *,
    response: ScmoSoapProbeResponse,
    probe: dict[str, Any],
    normalized_output_path: Path,
    input_format: ScmoReceiptExportFormat,
) -> dict[str, Any]:
    if not bool(probe.get("candidate_receipt_source_found", False)):
        return {
            "claim_scope": SCMO_SOAP_DOWNLOAD_NORMALIZATION_CLAIM_SCOPE,
            "claim_boundary": "v13_source_readiness_only_not_market_execution",
            "normalization_requested": True,
            "normalization_attempted": False,
            "normalization_skipped_reason": str(
                probe.get("source_probe_status", "not_candidate_receipt_source")
            ),
            "normalized_receipts_written": False,
            "normalized_receipts_csv": str(normalized_output_path),
            "receipt_rows": 0,
            "receipt_csv_generated": False,
            "validated_receipt_csv_ready": False,
            "dt_lava_ready": False,
            "permits_model_training": False,
            "market_execution_enabled": False,
        }

    raw_frame = read_scmo_dam_receipt_export_bytes(
        response.body,
        input_format=input_format,
        content_type=response.content_type,
        source_name=response.final_url,
    )
    normalized = normalize_scmo_dam_publication_receipt_export_frame(raw_frame)
    normalized_output_path.parent.mkdir(parents=True, exist_ok=True)
    normalized.write_csv(normalized_output_path)
    validated_ready = normalized.height > 0
    probe["receipt_csv_generated"] = True
    probe["validated_receipt_csv_ready"] = validated_ready
    lead_row = probe.get("lead_row")
    if isinstance(lead_row, dict):
        lead_row["has_source_publication_timestamp_column"] = True
        lead_row["receipt_csv_generated"] = True
        lead_row["validated_receipt_csv_ready"] = validated_ready

    return {
        "claim_scope": SCMO_SOAP_DOWNLOAD_NORMALIZATION_CLAIM_SCOPE,
        "claim_boundary": "v13_source_readiness_only_not_market_execution",
        "normalization_requested": True,
        "normalization_attempted": True,
        "normalization_skipped_reason": "",
        "normalized_receipts_written": True,
        "normalized_receipts_csv": str(normalized_output_path),
        "receipt_rows": normalized.height,
        "receipt_csv_generated": True,
        "validated_receipt_csv_ready": validated_ready,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
    }


def _post(
    url: str,
    *,
    request_xml: str,
    soap_action: str,
    extra_headers: dict[str, str],
    client_cert: ScmoClientCert | None,
) -> ScmoSoapProbeResponse:
    headers = {
        "Content-Type": f'application/soap+xml; charset=utf-8; action="{soap_action}"',
        "SOAPAction": soap_action,
        "Accept": "application/soap+xml,text/xml,application/xml,*/*",
        "User-Agent": "Mozilla/5.0",
        **extra_headers,
    }
    with httpx.Client(
        timeout=60.0,
        follow_redirects=True,
        headers=headers,
        cert=client_cert,
    ) as client:
        response = client.post(url, content=request_xml.encode("utf-8"))
    return ScmoSoapProbeResponse(
        source_url=url,
        final_url=str(response.url),
        status_code=response.status_code,
        content_type=response.headers.get("content-type", ""),
        body=response.content,
    )


def _credential_preflight(
    *,
    env: object,
    required_env_vars: dict[str, str],
) -> dict[str, Any]:
    if not isinstance(env, dict):
        env = os.environ
    return build_scmo_ws_security_credential_preflight(
        env=env,
        required_env_vars=required_env_vars,
    )


def _client_cert_from_env(
    *,
    env: os._Environ[str],
    cert_env_var: str,
    key_env_var: str,
    key_password_env_var: str,
) -> ScmoClientCert:
    cert_path = env[cert_env_var]
    key_path = env[key_env_var]
    key_password = env.get(key_password_env_var, "")
    if key_password:
        return (cert_path, key_path, key_password)
    return (cert_path, key_path)


def _attach_credential_metadata(
    probe: dict[str, Any],
    *,
    credential_mode: str,
    credential_preflight: dict[str, Any] | None,
    credentialed_request_attempted: bool,
    ws_security_signature_applied: bool,
) -> None:
    credential_preflight_ready = (
        bool(credential_preflight.get("credential_material_ready", False))
        if credential_preflight is not None
        else False
    )
    signed_download_request_ready = (
        bool(credential_preflight.get("signed_download_request_ready", False))
        if credential_preflight is not None
        else False
    )
    ws_security_signature_supported = (
        bool(credential_preflight.get("ws_security_signature_supported", False))
        if credential_preflight is not None
        else False
    )
    ws_security_signature_status = (
        str(credential_preflight.get("ws_security_signature_status", ""))
        if credential_preflight is not None
        else ""
    )
    mtls_client_cert_ready = (
        bool(
            credential_preflight.get(
                "mtls_client_cert_ready",
                credential_preflight_ready,
            )
        )
        if credential_preflight is not None
        else False
    )
    probe["credential_mode"] = credential_mode
    probe["credential_preflight_ready"] = credential_preflight_ready
    probe["mtls_client_cert_ready"] = mtls_client_cert_ready
    probe["credential_material_format"] = (
        str(credential_preflight.get("credential_material_format", ""))
        if credential_preflight is not None
        else ""
    )
    probe["signed_download_request_ready"] = signed_download_request_ready
    probe["credentialed_request_attempted"] = credentialed_request_attempted
    probe["ws_security_username_token_applied"] = credentialed_request_attempted
    probe["ws_security_signature_supported"] = ws_security_signature_supported
    probe["ws_security_signature_status"] = ws_security_signature_status
    probe["ws_security_signature_applied"] = ws_security_signature_applied
    probe["credential_material_validation_status"] = (
        str(credential_preflight.get("credential_material_validation_status", ""))
        if credential_preflight is not None
        else ""
    )
    blocker_status = _credentialed_probe_blocker_status(
        credential_mode=credential_mode,
        credential_preflight_ready=credential_preflight_ready,
        mtls_client_cert_ready=mtls_client_cert_ready,
        signed_download_request_ready=signed_download_request_ready,
    )
    if blocker_status:
        probe["source_probe_status"] = blocker_status
        probe["auth_or_signature_required"] = True
    lead_row = probe.get("lead_row")
    if isinstance(lead_row, dict):
        lead_row["credential_mode"] = credential_mode
        lead_row["credential_material_format"] = probe["credential_material_format"]
        lead_row["signed_download_request_ready"] = signed_download_request_ready
        lead_row["mtls_client_cert_ready"] = mtls_client_cert_ready
        lead_row["ws_security_signature_supported"] = ws_security_signature_supported
        lead_row["ws_security_signature_status"] = ws_security_signature_status
        lead_row["ws_security_signature_applied"] = ws_security_signature_applied
        if credential_mode != "unsigned":
            lead_id = str(lead_row.get("lead_id", "scmo_soap_download_attempt"))
            lead_row["lead_id"] = (
                f"{lead_id}_{credential_mode.replace('-', '_')}"
            )
            lead_row["lead_kind"] = "official_soap_download_credentialed_probe"
        if blocker_status:
            lead_row["source_probe_status"] = blocker_status
            lead_row["download_auth_required"] = True


def _credentialed_post_skip_reason(
    *,
    credential_mode: str,
    credential_preflight: dict[str, Any] | None,
) -> str:
    if credential_mode == "unsigned" or credential_preflight is None:
        return ""
    credential_preflight_ready = bool(
        credential_preflight.get("credential_material_ready", False)
    )
    mtls_client_cert_ready = bool(
        credential_preflight.get(
            "mtls_client_cert_ready",
            credential_preflight_ready,
        )
    )
    signed_download_request_ready = bool(
        credential_preflight.get("signed_download_request_ready", False)
    )
    return _credentialed_probe_blocker_status(
        credential_mode=credential_mode,
        credential_preflight_ready=credential_preflight_ready,
        mtls_client_cert_ready=mtls_client_cert_ready,
        signed_download_request_ready=signed_download_request_ready,
    )


def _credentialed_probe_blocker_status(
    *,
    credential_mode: str,
    credential_preflight_ready: bool,
    mtls_client_cert_ready: bool,
    signed_download_request_ready: bool,
) -> str:
    if credential_mode == "unsigned":
        return ""
    if not credential_preflight_ready:
        return "credential_material_not_ready"
    if (
        credential_mode == SCMO_MTLS_USERNAME_TOKEN_MODE
        and not mtls_client_cert_ready
    ):
        return "mtls_client_cert_not_ready"
    if (
        credential_mode == SCMO_SIGNED_WS_SECURITY_MODE
        and not signed_download_request_ready
    ):
        return "ws_security_signature_not_ready"
    return ""


def _headers(values: Sequence[str]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for value in values:
        if ":" not in value:
            raise ValueError(f"Header must use 'Name: value' format: {value}")
        name, raw_value = value.split(":", 1)
        if not name.strip():
            raise ValueError("Header name must not be empty.")
        headers[name.strip()] = raw_value.strip()
    return headers


def _decode_body(body: bytes, content_type: str) -> str:
    encoding = "utf-8"
    marker = "charset="
    if marker in content_type.casefold():
        encoding = content_type.casefold().split(marker, 1)[1].split(";", 1)[0].strip()
    try:
        return body.decode(encoding)
    except (LookupError, UnicodeDecodeError):
        return body.decode("utf-8", errors="replace")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


if __name__ == "__main__":
    raise SystemExit(main())
