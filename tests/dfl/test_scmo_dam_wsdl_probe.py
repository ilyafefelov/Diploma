from __future__ import annotations

import json

from smart_arbitrage.dfl.scmo_dam_wsdl_probe import (
    build_scmo_dam_wsdl_receipt_source_probe,
)
from smart_arbitrage.dfl.ua_context_v13_receipt_lead_audit import (
    audit_dfl_ua_context_dam_receipt_source_leads_v13_frame,
)

import polars as pl


MINIMAL_SCMO_WSDL = """<?xml version="1.0" encoding="utf-8"?>
<wsdl:definitions
  xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
  xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
  xmlns:sp="http://schemas.xmlsoap.org/ws/2005/07/securitypolicy"
  xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <wsdl:documentation>
    <sp:AsymmetricBinding>
      <sp:X509Token />
      <sp:UsernameToken />
      <sp:SignedParts />
    </sp:AsymmetricBinding>
  </wsdl:documentation>
  <wsdl:types>
    <xs:schema>
      <xs:complexType name="CDSREQType">
        <xs:sequence>
          <xs:element name="Trade">
            <xs:complexType>
              <xs:attribute name="trade-day" type="xs:date" use="required" />
              <xs:attribute name="period-from" type="xs:int" use="required" />
              <xs:attribute name="period-to" type="xs:int" use="required" />
              <xs:attribute name="version" type="xs:int" use="optional" />
              <xs:attribute name="market-area" use="required">
                <xs:simpleType>
                  <xs:restriction base="xs:string">
                    <xs:enumeration value="UA_IPS" />
                    <xs:enumeration value="UA_BEI" />
                  </xs:restriction>
                </xs:simpleType>
              </xs:attribute>
            </xs:complexType>
          </xs:element>
        </xs:sequence>
        <xs:attribute name="message-code" use="required">
          <xs:simpleType>
            <xs:restriction base="xs:string">
              <xs:enumeration value="807" />
              <xs:enumeration value="810" />
              <xs:enumeration value="831" />
            </xs:restriction>
          </xs:simpleType>
        </xs:attribute>
        <xs:attribute name="date-time" type="xs:dateTime" use="required" />
      </xs:complexType>
    </xs:schema>
  </wsdl:types>
  <wsdl:portType name="EvaluationService">
    <wsdl:operation name="Download" />
  </wsdl:portType>
  <wsdl:service name="EvaluationService">
    <wsdl:port name="EvaluationServicePort" binding="tns:EvaluationServiceBinding">
      <soap:address location="http://scmo.oree.com.ua/Interfaces/Evaluations/Service.svc" />
    </wsdl:port>
  </wsdl:service>
</wsdl:definitions>
"""


def test_scmo_wsdl_probe_extracts_download_contract_without_receipt_claim() -> None:
    probe = build_scmo_dam_wsdl_receipt_source_probe(
        source_url="https://scmo.oree.com.ua/interfaces/Evaluations/Service.svc?wsdl",
        wsdl_text=MINIMAL_SCMO_WSDL,
        service_kind="Evaluations",
    )

    assert probe["claim_scope"] == "scmo_dam_wsdl_probe_not_receipt"
    assert probe["wsdl_available"] is True
    assert probe["operations"] == ["Download"]
    assert probe["message_codes"] == ["807", "810", "831"]
    assert probe["market_areas"] == ["UA_BEI", "UA_IPS"]
    assert probe["security_policy_requirements"] == [
        "AsymmetricBinding",
        "SignedParts",
        "UsernameToken",
        "X509Token",
    ]
    assert probe["trade_request_attributes"] == [
        "trade-day",
        "period-from",
        "period-to",
        "version",
        "market-area",
    ]
    assert probe["candidate_receipt_source_found"] is False
    assert probe["receipt_csv_generated"] is False
    assert probe["validated_receipt_csv_ready"] is False
    assert probe["permits_model_training"] is False
    assert probe["market_execution_enabled"] is False

    lead_row = probe["lead_row"]
    assert lead_row["lead_id"] == "scmo_evaluations_wsdl_download_contract"
    assert lead_row["lead_kind"] == "official_wsdl_contract"
    assert lead_row["metadata_scope"] == "service_contract"
    assert lead_row["download_auth_required"] is True
    assert lead_row["security_policy_requirements"] == (
        "AsymmetricBinding;SignedParts;UsernameToken;X509Token"
    )

    audit = audit_dfl_ua_context_dam_receipt_source_leads_v13_frame(
        pl.DataFrame([lead_row])
    )
    assert audit["candidate_receipt_source_found"] is False
    assert audit["auth_blocked_count"] == 1
    assert audit["lead_rows"][0]["security_policy_requirements"] == (
        "AsymmetricBinding;SignedParts;UsernameToken;X509Token"
    )
    assert audit["market_execution_enabled"] is False


def test_scmo_wsdl_probe_cli_writes_probe_and_lead(tmp_path) -> None:
    from scripts.probe_scmo_dam_wsdl import main

    wsdl_path = tmp_path / "scmo.wsdl"
    probe_path = tmp_path / "probe.json"
    lead_path = tmp_path / "lead.csv"
    wsdl_path.write_text(MINIMAL_SCMO_WSDL, encoding="utf-8")

    exit_code = main(
        [
            "--input-wsdl",
            str(wsdl_path),
            "--url",
            "https://scmo.oree.com.ua/interfaces/Evaluations/Service.svc?wsdl",
            "--service-kind",
            "Evaluations",
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
    assert "scmo_evaluations_wsdl_download_contract" in lead_path.read_text(
        encoding="utf-8"
    )
