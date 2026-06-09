from __future__ import annotations

import json
from pathlib import Path

import httpx
import polars as pl

from smart_arbitrage.dfl.scmo_dam_soap_download_probe import (
    build_scmo_dam_download_soap_request,
    build_scmo_dam_soap_download_probe,
    build_scmo_signed_ws_security_dam_download_soap_request,
    build_scmo_ws_security_credential_preflight,
    sanitize_scmo_soap_request_for_artifact,
)
from smart_arbitrage.dfl.ua_context_v13_receipt_lead_audit import (
    audit_dfl_ua_context_dam_receipt_source_leads_v13_frame,
)


def _write_self_signed_test_cert_key(tmp_path: Path) -> tuple[Path, Path]:
    from datetime import UTC, datetime, timedelta

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, "scmo-test-client")]
    )
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(UTC) - timedelta(days=1))
        .not_valid_after(datetime.now(UTC) + timedelta(days=1))
        .sign(private_key, hashes.SHA256())
    )
    cert_path = tmp_path / "client_cert.pem"
    key_path = tmp_path / "client_key.pem"
    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    return cert_path, key_path


def _write_self_signed_test_p12(
    tmp_path: Path,
    *,
    password: str,
) -> tuple[Path, bytes]:
    from datetime import UTC, datetime, timedelta

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives.serialization import pkcs12
    from cryptography.x509.oid import NameOID

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, "scmo-test-client-p12")]
    )
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(UTC) - timedelta(days=1))
        .not_valid_after(datetime.now(UTC) + timedelta(days=1))
        .sign(private_key, hashes.SHA256())
    )
    p12_path = tmp_path / "client_bundle.p12"
    p12_path.write_bytes(
        pkcs12.serialize_key_and_certificates(
            name=b"scmo-test-client-p12",
            key=private_key,
            cert=certificate,
            cas=None,
            encryption_algorithm=serialization.BestAvailableEncryption(
                password.encode("utf-8")
            ),
        )
    )
    return p12_path, certificate.public_bytes(serialization.Encoding.PEM)


def test_scmo_soap_probe_builds_download_request_without_execution_claim() -> None:
    request_xml = build_scmo_dam_download_soap_request(
        trade_day="2026-05-25",
        message_code="807",
        market_area="UA_IPS",
        period_from=1,
        period_to=24,
        request_datetime="2026-05-24T09:00:00Z",
    )

    assert "DownloadRequest" in request_xml
    assert 'message-code="807"' in request_xml
    assert 'trade-day="2026-05-25"' in request_xml
    assert 'market-area="UA_IPS"' in request_xml
    assert "ProposedBid" not in request_xml


def test_scmo_soap_probe_can_build_sanitizable_username_token_request() -> None:
    request_xml = build_scmo_dam_download_soap_request(
        trade_day="2026-05-25",
        message_code="807",
        market_area="UA_IPS",
        request_datetime="2026-05-24T09:00:00Z",
        username="operator@example.test",
        password="do-not-write",
        username_token_created="2026-05-24T09:00:00Z",
        username_token_nonce=b"fixed-test-nonce",
    )
    sanitized = sanitize_scmo_soap_request_for_artifact(request_xml)

    assert "UsernameToken" in request_xml
    assert "PasswordDigest" in request_xml
    assert "do-not-write" not in request_xml
    assert "operator@example.test" in request_xml
    assert "operator@example.test" not in sanitized
    assert "PasswordDigest" in sanitized
    assert "fixed-test-nonce" not in sanitized


def test_scmo_signed_ws_security_builder_adds_x509_signature_without_execution_claim(
    tmp_path: Path,
) -> None:
    cert_path, key_path = _write_self_signed_test_cert_key(tmp_path)

    request_xml = build_scmo_signed_ws_security_dam_download_soap_request(
        trade_day="2026-05-25",
        message_code="807",
        market_area="UA_IPS",
        request_datetime="2026-05-24T09:00:00Z",
        username="operator@example.test",
        password="do-not-write",
        client_cert_path=cert_path,
        client_key_path=key_path,
        username_token_created="2026-05-24T09:00:00Z",
        username_token_nonce=b"fixed-test-nonce",
        id_prefix="test",
    )
    sanitized = sanitize_scmo_soap_request_for_artifact(request_xml)

    assert "DownloadRequest" in request_xml
    assert "UsernameToken" in request_xml
    assert "BinarySecurityToken" in request_xml
    assert "<ds:Signature" in request_xml
    assert "<ds:SignatureValue>" in request_xml
    assert "<ds:DigestValue>" in request_xml
    assert "<wsa:Action" in request_xml
    assert "<wsa:To" in request_xml
    assert "<wsa:MessageID" in request_xml
    assert "<wsa:ReplyTo" in request_xml
    assert 'URI="#test-body"' in request_xml
    assert 'URI="#test-wsa-action"' in request_xml
    assert 'URI="#test-wsa-to"' in request_xml
    assert 'URI="#test-wsa-message-id"' in request_xml
    assert 'URI="#test-wsa-reply-to"' in request_xml
    assert 'URI="#test-username-token"' in request_xml
    assert 'URI="#test-timestamp"' in request_xml
    assert "operator@example.test" in request_xml
    assert "do-not-write" not in request_xml
    assert "ProposedBid" not in request_xml
    assert "operator@example.test" not in sanitized
    assert "redacted-binary-security-token" in sanitized
    assert "redacted-signature-value" in sanitized
    assert "redacted-digest-value" in sanitized

    from signxml import SignatureConfiguration, XMLVerifier

    verification = XMLVerifier().verify(
        request_xml.encode("utf-8"),
        x509_cert=cert_path.read_bytes(),
        id_attribute="Id",
        expect_config=SignatureConfiguration(
            require_x509=False,
            expect_references=7,
        ),
    )
    assert len(verification) == 7


def test_scmo_signed_ws_security_builder_accepts_pkcs12_bundle_without_execution_claim(
    tmp_path: Path,
) -> None:
    p12_path, cert_pem = _write_self_signed_test_p12(
        tmp_path,
        password="bundle-secret",
    )

    request_xml = build_scmo_signed_ws_security_dam_download_soap_request(
        trade_day="2026-05-25",
        message_code="807",
        market_area="UA_IPS",
        request_datetime="2026-05-24T09:00:00Z",
        username="operator@example.test",
        password="do-not-write",
        client_p12_path=p12_path,
        client_p12_password="bundle-secret",
        username_token_created="2026-05-24T09:00:00Z",
        username_token_nonce=b"fixed-test-nonce",
        id_prefix="test-p12",
    )
    sanitized = sanitize_scmo_soap_request_for_artifact(request_xml)

    assert "DownloadRequest" in request_xml
    assert "BinarySecurityToken" in request_xml
    assert "<ds:Signature" in request_xml
    assert 'URI="#test-p12-body"' in request_xml
    assert 'URI="#test-p12-wsa-action"' in request_xml
    assert "operator@example.test" not in sanitized
    assert "bundle-secret" not in sanitized
    assert "ProposedBid" not in request_xml

    from signxml import SignatureConfiguration, XMLVerifier

    verification = XMLVerifier().verify(
        request_xml.encode("utf-8"),
        x509_cert=cert_pem,
        id_attribute="Id",
        expect_config=SignatureConfiguration(
            require_x509=False,
            expect_references=7,
        ),
    )
    assert len(verification) == 7


def test_scmo_soap_probe_classifies_ws_security_fault_as_blocker() -> None:
    request_xml = build_scmo_dam_download_soap_request(
        trade_day="2026-05-25",
        message_code="807",
        market_area="UA_IPS",
        request_datetime="2026-05-24T09:00:00Z",
    )
    response_text = """
    <s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope">
      <s:Body>
        <s:Fault>
          <s:Reason>
            <s:Text>UsernameToken and X509 signature are required.</s:Text>
          </s:Reason>
        </s:Fault>
      </s:Body>
    </s:Envelope>
    """

    probe = build_scmo_dam_soap_download_probe(
        source_url="http://scmo.oree.com.ua/Interfaces/Evaluations/Service.svc",
        request_xml=request_xml,
        status_code=500,
        content_type="application/soap+xml",
        response_text=response_text,
    )

    assert probe["claim_scope"] == "scmo_dam_soap_download_probe_not_receipt"
    assert probe["soap_fault_returned"] is True
    assert probe["auth_or_signature_required"] is True
    assert probe["source_probe_status"] == "ws_security_required"
    assert probe["candidate_receipt_source_found"] is False
    assert probe["receipt_csv_generated"] is False
    assert probe["validated_receipt_csv_ready"] is False
    assert probe["permits_model_training"] is False
    assert probe["market_execution_enabled"] is False

    audit = audit_dfl_ua_context_dam_receipt_source_leads_v13_frame(
        pl.DataFrame([probe["lead_row"]])
    )
    assert audit["auth_blocked_count"] == 1
    assert audit["candidate_receipt_source_found"] is False
    assert audit["market_execution_enabled"] is False


def test_scmo_soap_probe_does_not_treat_wsdl_schema_as_download_response() -> None:
    request_xml = build_scmo_dam_download_soap_request(
        trade_day="2026-05-25",
        message_code="807",
        market_area="UA_IPS",
        request_datetime="2026-05-24T09:00:00Z",
    )
    response_text = """
    <wsdl:definitions xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/">
      <sp:X509Token xmlns:sp="http://schemas.xmlsoap.org/ws/2005/07/securitypolicy" />
      <wsdl:message name="DownloadResponse" />
    </wsdl:definitions>
    """

    probe = build_scmo_dam_soap_download_probe(
        source_url="http://scmo.oree.com.ua/Interfaces/Evaluations/Service.svc",
        request_xml=request_xml,
        status_code=200,
        content_type="text/xml; charset=UTF-8",
        response_text=response_text,
    )

    assert probe["wsdl_response_returned"] is True
    assert probe["download_response_found"] is False
    assert probe["message_code"] == "807"
    assert probe["lead_row"]["message_code"] == "807"
    assert probe["lead_row"]["lead_id"] == "scmo_evaluations_soap_download_attempt_807"
    assert probe["source_probe_status"] == (
        "wsdl_response_returned_signed_download_required"
    )
    assert probe["candidate_receipt_source_found"] is False
    assert probe["validated_receipt_csv_ready"] is False
    assert probe["market_execution_enabled"] is False


def test_scmo_soap_probe_cli_writes_probe_and_lead(tmp_path) -> None:
    from scripts.probe_scmo_dam_soap_download import main
    import scripts.probe_scmo_dam_soap_download as cli

    probe_path = tmp_path / "probe.json"
    lead_path = tmp_path / "lead.csv"

    def fake_post(
        url: str,
        *,
        request_xml: str,
        soap_action: str,
        extra_headers: dict[str, str],
        client_cert: cli.ScmoClientCert | None,
    ) -> cli.ScmoSoapProbeResponse:
        assert url == "http://scmo.oree.com.ua/Interfaces/Evaluations/Service.svc"
        assert "DownloadRequest" in request_xml
        assert soap_action.endswith("/Download")
        assert extra_headers == {"X-Test": "1"}
        assert client_cert is None
        return cli.ScmoSoapProbeResponse(
            source_url=url,
            final_url=url,
            status_code=500,
            content_type="application/soap+xml",
            body=b"<Fault>UsernameToken and X509 signature are required.</Fault>",
        )

    cli._post = fake_post
    exit_code = main(
        [
            "--url",
            "http://scmo.oree.com.ua/Interfaces/Evaluations/Service.svc",
            "--soap-action",
            "http://sfera.sk/ws/xmtrade/isot/interfaces/evaluations/services/2009/04/01/EvaluationsContract/Download",
            "--trade-day",
            "2026-05-25",
            "--message-code",
            "807",
            "--market-area",
            "UA_IPS",
            "--header",
            "X-Test: 1",
            "--probe-output-json",
            str(probe_path),
            "--lead-output-csv",
            str(lead_path),
        ]
    )

    assert exit_code == 0
    assert json.loads(probe_path.read_text(encoding="utf-8"))[
        "market_execution_enabled"
    ] is False
    assert "scmo_evaluations_soap_download_attempt" in lead_path.read_text(
        encoding="utf-8"
    )


def test_scmo_soap_probe_cli_writes_network_error_probe(tmp_path) -> None:
    from scripts.probe_scmo_dam_soap_download import main
    import scripts.probe_scmo_dam_soap_download as cli

    probe_path = tmp_path / "probe.json"
    lead_path = tmp_path / "lead.csv"

    def fake_post(
        url: str,
        *,
        request_xml: str,
        soap_action: str,
        extra_headers: dict[str, str],
        client_cert: cli.ScmoClientCert | None,
    ) -> cli.ScmoSoapProbeResponse:
        assert client_cert is None
        raise httpx.ReadError("connection aborted")

    cli._post = fake_post
    exit_code = main(
        [
            "--trade-day",
            "2026-05-25",
            "--message-code",
            "951",
            "--market-area",
            "UA_IPS",
            "--probe-output-json",
            str(probe_path),
            "--lead-output-csv",
            str(lead_path),
        ]
    )

    assert exit_code == 0
    probe = json.loads(probe_path.read_text(encoding="utf-8"))
    assert probe["status_code"] == 0
    assert probe["source_probe_status"] == "network_error_without_receipt_export"
    assert probe["candidate_receipt_source_found"] is False
    assert probe["validated_receipt_csv_ready"] is False
    assert probe["market_execution_enabled"] is False


def test_scmo_soap_probe_cli_skips_normalization_for_blocker_response(
    tmp_path: Path,
) -> None:
    from scripts.probe_scmo_dam_soap_download import main
    import scripts.probe_scmo_dam_soap_download as cli

    probe_path = tmp_path / "probe.json"
    lead_path = tmp_path / "lead.csv"
    normalized_path = tmp_path / "dam_receipts_v13.csv"

    def fake_post(
        url: str,
        *,
        request_xml: str,
        soap_action: str,
        extra_headers: dict[str, str],
        client_cert: cli.ScmoClientCert | None,
    ) -> cli.ScmoSoapProbeResponse:
        return cli.ScmoSoapProbeResponse(
            source_url=url,
            final_url=url,
            status_code=500,
            content_type="application/soap+xml",
            body=b"<Fault>UsernameToken and X509 signature are required.</Fault>",
        )

    cli._post = fake_post
    exit_code = main(
        [
            "--trade-day",
            "2026-05-25",
            "--message-code",
            "807",
            "--market-area",
            "UA_IPS",
            "--probe-output-json",
            str(probe_path),
            "--lead-output-csv",
            str(lead_path),
            "--normalized-output",
            str(normalized_path),
        ]
    )

    assert exit_code == 0
    probe = json.loads(probe_path.read_text(encoding="utf-8"))
    assert normalized_path.exists() is False
    assert probe["normalization_summary"]["normalization_requested"] is True
    assert probe["normalization_summary"]["normalization_attempted"] is False
    assert probe["normalization_summary"]["normalized_receipts_written"] is False
    assert probe["normalization_summary"]["receipt_csv_generated"] is False
    assert probe["normalization_summary"]["validated_receipt_csv_ready"] is False
    assert probe["receipt_csv_generated"] is False
    assert probe["validated_receipt_csv_ready"] is False
    assert probe["market_execution_enabled"] is False


def test_scmo_soap_probe_cli_normalizes_isotedata_download_response(
    tmp_path: Path,
) -> None:
    from scripts.probe_scmo_dam_soap_download import main
    import scripts.probe_scmo_dam_soap_download as cli

    probe_path = tmp_path / "probe.json"
    lead_path = tmp_path / "lead.csv"
    normalized_path = tmp_path / "dam_receipts_v13.csv"

    def fake_post(
        url: str,
        *,
        request_xml: str,
        soap_action: str,
        extra_headers: dict[str, str],
        client_cert: cli.ScmoClientCert | None,
    ) -> cli.ScmoSoapProbeResponse:
        return cli.ScmoSoapProbeResponse(
            source_url=url,
            final_url=url,
            status_code=200,
            content_type="application/soap+xml",
            body=(
                b'<?xml version="1.0" encoding="UTF-8"?>'
                b'<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope">'
                b"<s:Body>"
                b'<DownloadResponse xmlns="http://sfera.sk/ws/xmtrade/isot/interfaces/evaluations/services/2009/04/01">'
                b'<ISOTEDATA xmlns="http://sfera.sk/ws/xmtrade/isot/interfaces/evaluations/types/2009/04/01" '
                b'message-code="943" date-time="2025-12-31T13:27:00">'
                b'<Trade trade-day="2026-01-01" market-area="UA_IPS">'
                b'<ProfileData profile-role="SP02">'
                b'<Data period="1" value="1000.00" unit="UAH" />'
                b'<Data period="2" value="1100.00" unit="UAH" />'
                b"</ProfileData>"
                b"</Trade>"
                b"</ISOTEDATA>"
                b"</DownloadResponse>"
                b"</s:Body>"
                b"</s:Envelope>"
            ),
        )

    cli._post = fake_post
    exit_code = main(
        [
            "--trade-day",
            "2026-01-01",
            "--message-code",
            "943",
            "--market-area",
            "UA_IPS",
            "--probe-output-json",
            str(probe_path),
            "--lead-output-csv",
            str(lead_path),
            "--normalized-output",
            str(normalized_path),
        ]
    )

    assert exit_code == 0
    normalized = pl.read_csv(normalized_path, try_parse_dates=True)
    assert normalized.height == 2
    assert normalized["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").to_list() == [
        "2026-01-01T00:00:00",
        "2026-01-01T01:00:00",
    ]
    assert normalized["source_publication_timestamp"].dt.strftime(
        "%Y-%m-%dT%H:%M:%S"
    ).to_list() == ["2025-12-31T13:27:00", "2025-12-31T13:27:00"]
    assert normalized["market_execution_enabled"].to_list() == [False, False]
    probe = json.loads(probe_path.read_text(encoding="utf-8"))
    assert probe["download_response_found"] is True
    assert probe["candidate_receipt_source_found"] is True
    assert probe["normalization_summary"]["normalization_attempted"] is True
    assert probe["normalization_summary"]["receipt_rows"] == 2
    assert probe["receipt_csv_generated"] is True
    assert probe["validated_receipt_csv_ready"] is True
    assert probe["permits_model_training"] is False
    assert probe["market_execution_enabled"] is False
    lead = pl.read_csv(lead_path)
    assert lead["has_source_publication_timestamp_column"].to_list() == [True]
    assert lead["receipt_csv_generated"].to_list() == [True]


def test_scmo_soap_probe_cli_blocks_credentialed_attempt_when_preflight_missing(
    tmp_path: Path,
) -> None:
    from scripts.probe_scmo_dam_soap_download import main
    import scripts.probe_scmo_dam_soap_download as cli

    probe_path = tmp_path / "probe.json"
    lead_path = tmp_path / "lead.csv"

    def fake_post(
        url: str,
        *,
        request_xml: str,
        soap_action: str,
        extra_headers: dict[str, str],
        client_cert: cli.ScmoClientCert | None,
    ) -> cli.ScmoSoapProbeResponse:
        raise AssertionError("credentialed request must not be attempted")

    cli._post = fake_post
    exit_code = main(
        [
            "--credential-mode",
            "preflight-gated-mtls-username-token",
            "--trade-day",
            "2026-05-25",
            "--message-code",
            "807",
            "--market-area",
            "UA_IPS",
            "--probe-output-json",
            str(probe_path),
            "--lead-output-csv",
            str(lead_path),
        ]
    )

    assert exit_code == 0
    probe = json.loads(probe_path.read_text(encoding="utf-8"))
    assert probe["credential_mode"] == "preflight-gated-mtls-username-token"
    assert probe["credential_preflight_ready"] is False
    assert probe["credentialed_request_attempted"] is False
    assert probe["source_probe_status"] == "credential_material_not_ready"
    assert probe["candidate_receipt_source_found"] is False
    assert probe["validated_receipt_csv_ready"] is False
    assert probe["market_execution_enabled"] is False
    assert "credential_material_not_ready" in lead_path.read_text(encoding="utf-8")
    assert "preflight_gated_mtls_username_token" in lead_path.read_text(
        encoding="utf-8"
    )


def test_scmo_soap_probe_cli_blocks_signed_attempt_without_signature_builder(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from scripts.probe_scmo_dam_soap_download import main
    import scripts.probe_scmo_dam_soap_download as cli

    probe_path = tmp_path / "probe.json"
    lead_path = tmp_path / "lead.csv"
    request_path = tmp_path / "request.xml"

    def fake_preflight(
        *,
        env: object,
        required_env_vars: dict[str, str],
    ) -> dict[str, object]:
        return {
            "credential_material_ready": True,
            "credential_material_present": True,
            "credential_file_pair_valid": True,
            "credential_material_validation_status": "valid_cert_key_pair",
            "signed_download_request_ready": False,
            "ws_security_signature_supported": False,
            "ws_security_signature_status": "xml_signature_not_implemented",
            "missing_env_vars": [],
            "missing_files": [],
            "market_execution_enabled": False,
            "validated_receipt_csv_ready": False,
            "permits_model_training": False,
        }

    def fake_post(
        url: str,
        *,
        request_xml: str,
        soap_action: str,
        extra_headers: dict[str, str],
        client_cert: cli.ScmoClientCert | None,
    ) -> cli.ScmoSoapProbeResponse:
        raise AssertionError("signed request must not be attempted")

    monkeypatch.setattr(cli, "_credential_preflight", fake_preflight)
    monkeypatch.setattr(cli, "_post", fake_post)
    exit_code = main(
        [
            "--credential-mode",
            "preflight-gated-signed-ws-security",
            "--trade-day",
            "2026-05-25",
            "--message-code",
            "807",
            "--market-area",
            "UA_IPS",
            "--probe-output-json",
            str(probe_path),
            "--lead-output-csv",
            str(lead_path),
            "--request-output-xml",
            str(request_path),
        ]
    )

    assert exit_code == 0
    probe = json.loads(probe_path.read_text(encoding="utf-8"))
    request_artifact = request_path.read_text(encoding="utf-8")
    assert probe["credential_mode"] == "preflight-gated-signed-ws-security"
    assert probe["credential_preflight_ready"] is True
    assert probe["signed_download_request_ready"] is False
    assert probe["credentialed_request_attempted"] is False
    assert probe["ws_security_username_token_applied"] is False
    assert probe["ws_security_signature_applied"] is False
    assert probe["ws_security_signature_status"] == "xml_signature_not_implemented"
    assert probe["source_probe_status"] == "ws_security_signature_not_ready"
    assert probe["candidate_receipt_source_found"] is False
    assert probe["validated_receipt_csv_ready"] is False
    assert probe["market_execution_enabled"] is False
    assert "UsernameToken" not in request_artifact
    assert "ws_security_signature_not_ready" in lead_path.read_text(encoding="utf-8")


def test_scmo_soap_probe_cli_posts_signed_ws_security_request_without_secret_artifacts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from scripts.probe_scmo_dam_soap_download import main
    import scripts.probe_scmo_dam_soap_download as cli

    cert_path, key_path = _write_self_signed_test_cert_key(tmp_path)
    probe_path = tmp_path / "probe.json"
    lead_path = tmp_path / "lead.csv"
    request_path = tmp_path / "request.xml"
    monkeypatch.setenv("SCMO_USERNAME", "operator@example.test")
    monkeypatch.setenv("SCMO_PASSWORD", "do-not-write")
    monkeypatch.setenv("SCMO_CLIENT_CERT_PEM", str(cert_path))
    monkeypatch.setenv("SCMO_CLIENT_KEY_PEM", str(key_path))

    def fake_post(
        url: str,
        *,
        request_xml: str,
        soap_action: str,
        extra_headers: dict[str, str],
        client_cert: cli.ScmoClientCert | None,
    ) -> cli.ScmoSoapProbeResponse:
        assert "UsernameToken" in request_xml
        assert "BinarySecurityToken" in request_xml
        assert "<ds:Signature" in request_xml
        assert "PasswordDigest" in request_xml
        assert "operator@example.test" in request_xml
        assert "do-not-write" not in request_xml
        assert client_cert == (str(cert_path), str(key_path))
        return cli.ScmoSoapProbeResponse(
            source_url=url,
            final_url=url,
            status_code=500,
            content_type="application/soap+xml",
            body=b"<Fault>Signed request reached SCMO test boundary.</Fault>",
        )

    monkeypatch.setattr(cli, "_post", fake_post)
    exit_code = main(
        [
            "--credential-mode",
            "preflight-gated-signed-ws-security",
            "--trade-day",
            "2026-05-25",
            "--message-code",
            "807",
            "--market-area",
            "UA_IPS",
            "--probe-output-json",
            str(probe_path),
            "--lead-output-csv",
            str(lead_path),
            "--request-output-xml",
            str(request_path),
        ]
    )

    assert exit_code == 0
    probe = json.loads(probe_path.read_text(encoding="utf-8"))
    request_artifact = request_path.read_text(encoding="utf-8")
    assert probe["credential_mode"] == "preflight-gated-signed-ws-security"
    assert probe["credential_preflight_ready"] is True
    assert probe["signed_download_request_ready"] is True
    assert probe["credentialed_request_attempted"] is True
    assert probe["ws_security_username_token_applied"] is True
    assert probe["ws_security_signature_applied"] is True
    assert probe["ws_security_signature_status"] == "xml_signature_builder_available"
    assert probe["candidate_receipt_source_found"] is False
    assert probe["validated_receipt_csv_ready"] is False
    assert probe["market_execution_enabled"] is False
    assert "operator@example.test" not in request_artifact
    assert "do-not-write" not in request_artifact
    assert "redacted-binary-security-token" in request_artifact
    assert "redacted-signature-value" in request_artifact
    assert "do-not-write" not in json.dumps(probe)
    lead = pl.read_csv(lead_path)
    assert lead["ws_security_signature_applied"].to_list() == [True]


def test_scmo_soap_probe_cli_posts_credentialed_request_without_secret_artifacts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from scripts.probe_scmo_dam_soap_download import main
    import scripts.probe_scmo_dam_soap_download as cli

    probe_path = tmp_path / "probe.json"
    lead_path = tmp_path / "lead.csv"
    request_path = tmp_path / "request.xml"
    monkeypatch.setenv("SCMO_USERNAME", "operator@example.test")
    monkeypatch.setenv("SCMO_PASSWORD", "do-not-write")
    monkeypatch.setenv("SCMO_CLIENT_CERT_PEM", str(tmp_path / "client_cert.pem"))
    monkeypatch.setenv("SCMO_CLIENT_KEY_PEM", str(tmp_path / "client_key.pem"))
    monkeypatch.setenv("SCMO_CLIENT_KEY_PASSWORD", "key-secret")

    def fake_preflight(
        *,
        env: object,
        required_env_vars: dict[str, str],
    ) -> dict[str, object]:
        return {
            "credential_material_ready": True,
            "credential_material_present": True,
            "credential_file_pair_valid": True,
            "credential_material_validation_status": "valid_cert_key_pair",
            "missing_env_vars": [],
            "missing_files": [],
            "market_execution_enabled": False,
            "validated_receipt_csv_ready": False,
            "permits_model_training": False,
        }

    def fake_post(
        url: str,
        *,
        request_xml: str,
        soap_action: str,
        extra_headers: dict[str, str],
        client_cert: cli.ScmoClientCert | None,
    ) -> cli.ScmoSoapProbeResponse:
        assert "UsernameToken" in request_xml
        assert "operator@example.test" in request_xml
        assert "do-not-write" not in request_xml
        assert "PasswordDigest" in request_xml
        assert client_cert == (
            str(tmp_path / "client_cert.pem"),
            str(tmp_path / "client_key.pem"),
            "key-secret",
        )
        return cli.ScmoSoapProbeResponse(
            source_url=url,
            final_url=url,
            status_code=500,
            content_type="application/soap+xml",
            body=b"<Fault>SignedParts signature are required.</Fault>",
        )

    cli._credential_preflight = fake_preflight
    cli._post = fake_post
    exit_code = main(
        [
            "--credential-mode",
            "preflight-gated-mtls-username-token",
            "--trade-day",
            "2026-05-25",
            "--message-code",
            "807",
            "--market-area",
            "UA_IPS",
            "--probe-output-json",
            str(probe_path),
            "--lead-output-csv",
            str(lead_path),
            "--request-output-xml",
            str(request_path),
        ]
    )

    assert exit_code == 0
    probe = json.loads(probe_path.read_text(encoding="utf-8"))
    request_artifact = request_path.read_text(encoding="utf-8")
    assert probe["credential_preflight_ready"] is True
    assert probe["credentialed_request_attempted"] is True
    assert probe["ws_security_signature_applied"] is False
    assert probe["market_execution_enabled"] is False
    assert "operator@example.test" not in request_artifact
    assert "do-not-write" not in request_artifact
    assert "key-secret" not in json.dumps(probe)
    assert "key-secret" not in request_artifact


def test_scmo_ws_security_preflight_reports_missing_credentials() -> None:
    preflight = build_scmo_ws_security_credential_preflight(
        env={},
        required_env_vars={
            "username": "SCMO_USERNAME",
            "password": "SCMO_PASSWORD",
            "client_cert_path": "SCMO_CLIENT_CERT_PEM",
            "client_key_path": "SCMO_CLIENT_KEY_PEM",
        },
    )

    assert preflight["claim_scope"] == "scmo_ws_security_credential_preflight"
    assert preflight["credential_material_ready"] is False
    assert preflight["missing_env_vars"] == [
        "SCMO_USERNAME",
        "SCMO_PASSWORD",
        "SCMO_CLIENT_CERT_PEM",
        "SCMO_CLIENT_KEY_PEM",
    ]
    assert preflight["secret_values_written"] is False
    assert preflight["validated_receipt_csv_ready"] is False
    assert preflight["market_execution_enabled"] is False


def test_scmo_ws_security_preflight_hashes_present_material_without_secrets(
    tmp_path: Path,
) -> None:
    cert_path = tmp_path / "client_cert.pem"
    key_path = tmp_path / "client_key.pem"
    cert_path.write_text("-----BEGIN CERTIFICATE-----\nTEST\n-----END CERTIFICATE-----\n")
    key_path.write_text("-----BEGIN PRIVATE KEY-----\nTEST\n-----END PRIVATE KEY-----\n")

    preflight = build_scmo_ws_security_credential_preflight(
        env={
            "SCMO_USERNAME": "operator@example.test",
            "SCMO_PASSWORD": "do-not-write",
            "SCMO_CLIENT_CERT_PEM": str(cert_path),
            "SCMO_CLIENT_KEY_PEM": str(key_path),
        },
        required_env_vars={
            "username": "SCMO_USERNAME",
            "password": "SCMO_PASSWORD",
            "client_cert_path": "SCMO_CLIENT_CERT_PEM",
            "client_key_path": "SCMO_CLIENT_KEY_PEM",
        },
    )

    assert preflight["credential_material_present"] is True
    assert preflight["credential_file_pair_valid"] is False
    assert preflight["credential_material_ready"] is False
    assert preflight["credential_material_validation_status"] == "invalid_cert_key_pair"
    assert preflight["credential_material_validation_error"]
    assert preflight["missing_env_vars"] == []
    assert preflight["missing_files"] == []
    assert preflight["username_present"] is True
    assert preflight["password_present"] is True
    assert preflight["client_key_password_present"] is False
    assert "do-not-write" not in json.dumps(preflight)
    assert preflight["client_cert_sha256"]
    assert preflight["client_key_sha256"]
    assert preflight["secret_values_written"] is False
    assert preflight["market_execution_enabled"] is False


def test_scmo_ws_security_preflight_separates_credentials_from_signed_request(
    tmp_path: Path,
) -> None:
    cert_path, key_path = _write_self_signed_test_cert_key(tmp_path)
    preflight = build_scmo_ws_security_credential_preflight(
        env={
            "SCMO_USERNAME": "operator@example.test",
            "SCMO_PASSWORD": "do-not-write",
            "SCMO_CLIENT_CERT_PEM": str(cert_path),
            "SCMO_CLIENT_KEY_PEM": str(key_path),
        },
        required_env_vars={
            "username": "SCMO_USERNAME",
            "password": "SCMO_PASSWORD",
            "client_cert_path": "SCMO_CLIENT_CERT_PEM",
            "client_key_path": "SCMO_CLIENT_KEY_PEM",
        },
    )

    assert preflight["credential_material_ready"] is True
    assert preflight["credential_file_pair_valid"] is True
    assert preflight["signed_download_request_ready"] is True
    assert preflight["ws_security_signature_supported"] is True
    assert preflight["ws_security_signature_status"] == "xml_signature_builder_available"
    assert preflight["ws_security_signature_blockers"] == []
    assert "SignedParts" in preflight["ws_security_requirements"]
    assert preflight["validated_receipt_csv_ready"] is False
    assert preflight["permits_model_training"] is False
    assert preflight["market_execution_enabled"] is False


def test_scmo_ws_security_preflight_accepts_pkcs12_for_signed_request(
    tmp_path: Path,
) -> None:
    p12_path, _cert_pem = _write_self_signed_test_p12(
        tmp_path,
        password="bundle-secret",
    )

    preflight = build_scmo_ws_security_credential_preflight(
        env={
            "SCMO_USERNAME": "operator@example.test",
            "SCMO_PASSWORD": "do-not-write",
            "SCMO_CLIENT_P12": str(p12_path),
            "SCMO_CLIENT_P12_PASSWORD": "bundle-secret",
        },
        required_env_vars={
            "username": "SCMO_USERNAME",
            "password": "SCMO_PASSWORD",
            "client_cert_path": "SCMO_CLIENT_CERT_PEM",
            "client_key_path": "SCMO_CLIENT_KEY_PEM",
            "client_p12_path": "SCMO_CLIENT_P12",
            "client_p12_password": "SCMO_CLIENT_P12_PASSWORD",
        },
    )

    assert preflight["credential_material_format"] == "pkcs12"
    assert preflight["credential_material_ready"] is True
    assert preflight["credential_file_pair_valid"] is True
    assert preflight["mtls_client_cert_ready"] is False
    assert preflight["signed_download_request_ready"] is True
    assert preflight["client_p12_path_present"] is True
    assert preflight["client_p12_password_present"] is True
    assert preflight["client_p12_sha256"]
    assert preflight["missing_env_vars"] == []
    assert "bundle-secret" not in json.dumps(preflight)
    assert preflight["secret_values_written"] is False
    assert preflight["market_execution_enabled"] is False


def test_scmo_ws_security_preflight_cli_writes_sanitized_json(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from scripts.preflight_scmo_dam_ws_security_credentials import main

    output_path = tmp_path / "ws_security_preflight.json"
    monkeypatch.setenv("SCMO_USERNAME", "operator@example.test")
    monkeypatch.setenv("SCMO_PASSWORD", "do-not-write")

    exit_code = main(["--output", str(output_path)])

    assert exit_code == 0
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["credential_material_ready"] is False
    assert "SCMO_CLIENT_CERT_PEM" in payload["missing_env_vars"]
    assert "SCMO_CLIENT_KEY_PEM" in payload["missing_env_vars"]
    assert "do-not-write" not in output_path.read_text(encoding="utf-8")
    assert payload["market_execution_enabled"] is False
