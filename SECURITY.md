# Security policy

RelayNode is a network-facing service. Keep public listeners behind TLS, use
an explicit destination policy, and keep registry credentials outside the
repository and outside browser-visible configuration.

Report a vulnerability through a private GitHub Security Advisory for this
repository. Include the affected commit, a minimal local reproduction, and
the endpoint or configuration involved. Do not include live server addresses,
credentials, or user data in a public issue.

Supported security fixes are maintained on the latest release line. Operators
should pin a release or container digest and review deployment configuration
before exposing a node to the Internet.
