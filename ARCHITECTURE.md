# Architecture

## Responsibility

Public donor, donation-intent, and payment-webhook HTTP API.

This repository is independently versioned and deployed. It may import `github.com/frederickmarvel/inflora-shared` after the shared library is released, but it must never import another service repository. Ports and connections are fixed by the almanac wire guide.

Phase 0 intentionally contains no handlers, persistence, messaging, or business logic.
