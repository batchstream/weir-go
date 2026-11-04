# Security policy

## Report a vulnerability

Use GitHub's private
[Report a vulnerability](https://github.com/batchstream/weir-go/security/advisories/new)
form. Include the affected SDK version or commit, reproduction steps, impact,
and relevant deployment settings with credentials and private data removed.
Please keep exploit details out of public issues while a report is being
investigated.

## Versions

Check whether an issue is still present on the
[latest release](https://github.com/batchstream/weir-go/releases/latest). Older releases do not have a
guaranteed security backport schedule. Report suspected vulnerabilities even if
you cannot reproduce them on the latest release.

## Deployment guidance

The Weir application listener uses plaintext gRPC; restrict its network access.
