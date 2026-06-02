import base64
import json
import urllib.parse
from typing import Any, Dict, Optional


def parse_vmess(uri: str) -> Optional[Dict[str, Any]]:
    """
    Parses a vmess:// URI and returns an Xray outbound structure.
    vmess URIs have base64-encoded JSON metadata.
    """
    try:
        raw_b64 = uri[8:]
        missing_padding = len(raw_b64) % 4
        if missing_padding:
            raw_b64 += "=" * (4 - missing_padding)

        decoded_bytes = base64.b64decode(raw_b64)
        data = json.loads(decoded_bytes.decode("utf-8"))

        outbound: Dict[str, Any] = {
            "protocol": "vmess",
            "settings": {
                "vnext": [
                    {
                        "address": data.get("add"),
                        "port": int(data.get("port", 443)),
                        "users": [
                            {
                                "id": data.get("id"),
                                "alterId": int(data.get("aid", 0)),
                                "security": "auto",
                            }
                        ],
                    }
                ]
            },
            "streamSettings": {
                "network": data.get("net", "tcp"),
                "security": "tls" if data.get("tls") == "tls" else "none",
            },
        }

        network_type = data.get("net", "tcp")
        if network_type == "ws":
            ws_settings: Dict[str, Any] = {}
            if data.get("path"):
                ws_settings["path"] = urllib.parse.unquote(data["path"])
            if data.get("host"):
                ws_settings["headers"] = {"Host": data["host"]}
            outbound["streamSettings"]["wsSettings"] = ws_settings
        elif network_type == "grpc":
            grpc_settings: Dict[str, Any] = {}
            if data.get("path"):
                grpc_settings["serviceName"] = data["path"]
            outbound["streamSettings"]["grpcSettings"] = grpc_settings

        if data.get("tls") == "tls":
            tls_settings: Dict[str, Any] = {}
            if data.get("sni") or data.get("host"):
                tls_settings["serverName"] = data.get("sni") or data.get("host")
            if data.get("fp"):
                tls_settings["fingerprint"] = data["fp"]
            if data.get("alpn"):
                tls_settings["alpn"] = data["alpn"]
            outbound["streamSettings"]["tlsSettings"] = tls_settings

        return outbound
    except Exception as e:
        print(f"  Error parsing VMess config: {e}")
        return None


def parse_vless_trojan_ss(uri: str) -> Optional[Dict[str, Any]]:
    """
    Parses vless://, trojan://, and ss:// URIs.
    URL-decodes user info to handle special characters properly.
    """
    try:
        parsed = urllib.parse.urlparse(uri)
        protocol = parsed.scheme

        # Extract user info and URL-decode it
        user_info = parsed.username
        if user_info:
            user_info = urllib.parse.unquote(user_info)
        elif parsed.netloc and "@" in parsed.netloc:
            raw_user = parsed.netloc.split("@")[0]
            user_info = urllib.parse.unquote(raw_user)

        # SS: check if user_info is base64-encoded method:password
        if protocol == "ss" and user_info:
            try:
                missing_padding = len(user_info) % 4
                padded = user_info + ("=" * (4 - missing_padding)) if missing_padding else user_info
                decoded_user_info = base64.b64decode(padded).decode("utf-8")
                if ":" in decoded_user_info:
                    user_info = decoded_user_info
            except Exception:
                pass

        host = parsed.hostname
        port = parsed.port or 443

        params = urllib.parse.parse_qs(parsed.query)

        outbound: Dict[str, Any] = {
            "protocol": protocol,
            "settings": {},
            "streamSettings": {
                "network": params.get("type", ["tcp"])[0],
                "security": params.get("security", ["none"])[0],
            },
        }

        if protocol == "vless":
            outbound["settings"] = {
                "vnext": [
                    {
                        "address": host,
                        "port": int(port),
                        "users": [
                            {
                                "id": user_info,
                                "encryption": params.get("encryption", ["none"])[0],
                            }
                        ],
                    }
                ]
            }
        elif protocol == "trojan":
            outbound["settings"] = {
                "servers": [
                    {
                        "address": host,
                        "port": int(port),
                        "password": user_info,
                    }
                ]
            }
        elif protocol == "ss":
            method, password = "aes-256-gcm", user_info
            if user_info and ":" in user_info:
                method, password = user_info.split(":", 1)
            outbound["settings"] = {
                "servers": [
                    {
                        "address": host,
                        "port": int(port),
                        "method": method,
                        "password": password,
                    }
                ]
            }
        else:
            return None

        # Transport settings
        net_type = outbound["streamSettings"]["network"]
        if net_type == "ws":
            ws_settings: Dict[str, Any] = {}
            if params.get("path"):
                ws_settings["path"] = urllib.parse.unquote(params["path"][0])
            if params.get("host"):
                ws_settings["headers"] = {"Host": params["host"][0]}
            outbound["streamSettings"]["wsSettings"] = ws_settings
        elif net_type == "grpc":
            grpc_settings: Dict[str, Any] = {}
            if params.get("serviceName"):
                grpc_settings["serviceName"] = params["serviceName"][0]
            outbound["streamSettings"]["grpcSettings"] = grpc_settings

        # TLS / XTLS settings
        security = outbound["streamSettings"]["security"]
        if security in ("tls", "xtls"):
            tls_settings: Dict[str, Any] = {}
            sni = params.get("sni") or params.get("host")
            if sni:
                tls_settings["serverName"] = sni[0]
            if params.get("fp"):
                tls_settings["fingerprint"] = params["fp"][0]
            if params.get("alpn"):
                tls_settings["alpn"] = [urllib.parse.unquote(params["alpn"][0])]

            if security == "xtls":
                outbound["streamSettings"]["xtlsSettings"] = tls_settings
            else:
                outbound["streamSettings"]["tlsSettings"] = tls_settings

        return outbound
    except Exception as e:
        proto = uri.split("://")[0] if "://" in uri else "unknown"
        print(f"  Error parsing {proto} config: {e}")
        return None


def parse_uri_to_outbound(uri: str) -> Optional[Dict[str, Any]]:
    """
    Detects the protocol and converts the raw config URI into an Xray outbound structure.
    """
    uri = uri.strip()
    if not uri:
        return None

    if uri.startswith("vmess://"):
        return parse_vmess(uri)
    elif uri.startswith(("vless://", "trojan://", "ss://")):
        return parse_vless_trojan_ss(uri)

    return None
