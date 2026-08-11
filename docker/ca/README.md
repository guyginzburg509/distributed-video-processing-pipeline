# Extra CA certificates (optional)

Only needed if `docker compose build` fails with:

```
SSLCertVerificationError: unable to get local issuer certificate
```

That means something on the network is intercepting HTTPS and re-signing it
with a private root CA — a corporate proxy, or an antivirus with HTTPS
scanning (AVG, Avast, ESET, Kaspersky all do this). The host trusts that CA;
a fresh container does not, so `pip install` inside the build cannot verify
PyPI.

**Fix:** drop the interceptor's root certificate here as a PEM file and build
with its filename:

```bash
EXTRA_CA_BUNDLE=my-root.pem docker compose build
```

On Windows, export the host's trusted roots with:

```powershell
$certs = Get-ChildItem Cert:\LocalMachine\Root
$lines = foreach ($c in $certs) {
  "-----BEGIN CERTIFICATE-----"
  [Convert]::ToBase64String($c.RawData, 'InsertLineBreaks')
  "-----END CERTIFICATE-----"
}
Set-Content docker\ca\my-root.pem $lines -Encoding ascii
```

Leave this directory empty otherwise — `EXTRA_CA_BUNDLE` defaults to empty and
the build step becomes a no-op. Nothing here is committed except this README.
