# Kaipanla public trust certificate

`kpl-quote.pem` is the public X.509 certificate shipped as
`marketdesk/assets/kpl-quote.pem` in the user-supplied 观潮行情 0.19.2 executable.
It contains no private key, account credential, or session data.

- Sample SHA-256: `8c94629f1b01f411f336fc9f570c1885af5bd32a6b107f1aa9ed6feb8d233442`.
- PEM SHA-256: `e5f5ebfacb8dc85b15786fa0c78b953a3e5a3f0fa2fb64e7a2b66695faae16ce`.
- Subject CN: `*.kaipanla.com`; valid until 2033-04-07 05:27:26 UTC.
- Verified on 2026-10-26 against the fixed command-2501 endpoint with
  `server_hostname=apphwshhq.kaipanla.com`, certificate and hostname checks enabled.

This is the adapter's sole trust certificate; it does not disable TLS validation
or use the operating system's general CA store. Expiry or certificate changes
must fail closed. A replacement needs separately verified provenance, review,
tests, and a release; never automatically trust a certificate fetched on failure.
The package resource must be included in both wheel and source releases.
