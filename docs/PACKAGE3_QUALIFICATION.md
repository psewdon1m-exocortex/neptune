# Package 3 candidate qualification

The separate `package3-qualification.yml` workflow runs only on `release-qualification/package3-*` branches. It calls ordinary CI with an optional frozen source manifest, then uses the existing protected `RELEASE_SIGNING_KEY` to sign the exact prepared Neptune 0.1.19 Linux and Windows release manifests. It verifies the expected published public-key identity before signing and checks each signature independently with OpenSSL. The workflow has `contents: read`; it creates no Git tags, releases, deployment, or public assets.

The source verifier checks the complete frozen checkout file set and restores the original qualified worktree bytes, including recorded CRLF/LF differences, before compiler/tests run. Empty optional inputs preserve ordinary CI behavior. Passing source CI does not claim a different compiled binary is the locally qualified artifact. The signing request binds the exact prebuilt artifact SHA values; Windows Authenticode changed only the signature envelope around the qualified PE image.

The Windows certificate is owner-selected and self-signed. It is separate from the RSA-PSS manifest key. Default public Windows trust and SmartScreen reputation are not claimed; the certificate must be explicitly trusted in the intended managed environment, and native trust/install/update qualification remains mandatory. No private certificate key is committed.

Native ARM64 is excluded from current package 3 acceptance by explicit owner decision because an appropriate environment is unavailable. That disposition is N/A, not a test PASS or a native ARM64 support claim. The unavailable historical Pluto reader remains a separate owner archival waiver, with old-reader/new-v8 and downgrade compatibility unconfirmed.
