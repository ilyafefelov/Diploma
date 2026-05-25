"""Preflight SCMO WS-Security credential material without writing secrets."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Sequence

from smart_arbitrage.dfl.scmo_dam_soap_download_probe import (
    build_scmo_ws_security_credential_preflight,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Check local SCMO UsernameToken/X509 credential material before a "
            "signed SOAP Download attempt. This writes hashes and blocker "
            "status only, including credential_file_pair_valid and "
            "signed_download_request_ready; "
            "never writes secret values or receipt rows."
        )
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--username-env-var", default="SCMO_USERNAME")
    parser.add_argument("--password-env-var", default="SCMO_PASSWORD")
    parser.add_argument("--client-cert-env-var", default="SCMO_CLIENT_CERT_PEM")
    parser.add_argument("--client-key-env-var", default="SCMO_CLIENT_KEY_PEM")
    parser.add_argument(
        "--client-key-password-env-var",
        default="SCMO_CLIENT_KEY_PASSWORD",
        help=(
            "Optional env var for an encrypted client key password. Only "
            "presence is reported; the value is never written."
        ),
    )
    parser.add_argument(
        "--client-p12-env-var",
        default="SCMO_CLIENT_P12",
        help=(
            "Optional env var for a PKCS#12/PFX client certificate bundle. "
            "Only presence, file hash, and loadability are reported."
        ),
    )
    parser.add_argument(
        "--client-p12-password-env-var",
        default="SCMO_CLIENT_P12_PASSWORD",
        help=(
            "Optional env var for the PKCS#12/PFX bundle password. Only "
            "presence is reported; the value is never written."
        ),
    )
    args = parser.parse_args(argv)

    preflight = build_scmo_ws_security_credential_preflight(
        env=os.environ,
        required_env_vars={
            "username": args.username_env_var,
            "password": args.password_env_var,
            "client_cert_path": args.client_cert_env_var,
            "client_key_path": args.client_key_env_var,
            "client_key_password": args.client_key_password_env_var,
            "client_p12_path": args.client_p12_env_var,
            "client_p12_password": args.client_p12_password_env_var,
        },
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(preflight, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(preflight, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
