"""Fixture launcher; deliberately not part of normal RDS provisioning."""
import os
import ssl

from proof_authorization import bootstrap

from ministack.core.mysqlproxy import Handler, Server

if __name__ == "__main__":
    with Server(("0.0.0.0", int(os.environ["SPIKE_PORT"])), Handler) as server:
        server.authorize = bootstrap()
        server.strict = os.environ.get("AUTH", "").lower() == "true"
        server.tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server.tls.minimum_version = ssl.TLSVersion.TLSv1_2
        server.tls.load_cert_chain("/tls/cert.pem", "/tls/key.pem")
        server.serve_forever()
